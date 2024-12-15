from fastapi import FastAPI, HTTPException, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse
import httpx
import uuid
import time
import logging
import asyncio

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Middleware to log request and response
class OrgEventMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request.state.start_time = time.time()
        request_id = uuid.uuid4()
        request_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        logger.info(f"[{request_id}] Request to {request.url.path} received at {request_time}")
        response = await call_next(request)
        response_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        process_time = time.time() - request.state.start_time
        logger.info(f"[{request_id}] Response: Status {response.status_code}, Process time: {process_time:.2f}s")
        return response

app = FastAPI()
app.add_middleware(OrgEventMiddleware)

# External Service URLs
EVENT_SERVICE_URL = "http://127.0.0.1:8002" 
RSVP_MANAGEMENT_URL = "http://127.0.0.1:8000"  
ORGANIZATIONS_URL = "http://127.0.0.1:8001" 

tasks = {}  # To track asynchronous task statuses

# HATEOAS Helper Function
def add_hateoas_to_event(event: dict):
    return {
        **event,
        "_links": {
            "self": f"/event/{event['id']}/rsvps",
            "organization": f"/organizations/{event['organizationId']}",
            "rsvps": f"/event/{event['id']}/rsvps"
        }
    }

def add_hateoas_to_rsvp(rsvp: dict):
    return {
        **rsvp,
        "_links": {
            "self": f"/rsvp/{rsvp['id']}",
            "event": f"/event/{rsvp['event_id']}",
        }
    }

def add_hateoas_to_organization(org: dict):
    return {
        **org,
        "_links": {
            "self": f"/organizations/{org['id']}",
            "events": f"/events?organizationId={org['id']}"
        }
    }

@app.get("/")
async def root():
    return {"message": "Welcome to the Composite Service"}

# Fetch all events with HATEOAS
@app.get("/event", tags=["Events"])
async def get_event_details():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{EVENT_SERVICE_URL}/events")
        response.raise_for_status()
        events = response.json()
        return [add_hateoas_to_event(event) for event in events]

# Fetch RSVP details with HATEOAS
@app.get("/rsvp", tags=["RSVPs"])
async def get_rsvp_details():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{RSVP_MANAGEMENT_URL}/rsvps")
        response.raise_for_status()
        rsvps = response.json()
        return [add_hateoas_to_rsvp(rsvp) for rsvp in rsvps]

# Get all organizations with HATEOAS
@app.get("/organizations", tags=["Organizations"])
async def get_organization(skip: int = 0, limit: int = 100):
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{ORGANIZATIONS_URL}/organizations/", params={"skip": skip, "limit": limit})
        response.raise_for_status()
        organizations = response.json()
        return [add_hateoas_to_organization(org) for org in organizations]

# Composite Endpoint: Event + RSVPs with HATEOAS
@app.get("/event/{event_id}/rsvps", tags=["Composite"])
async def get_event_rsvp_details(event_id: int):
    async with httpx.AsyncClient() as client:
        try:
            event_response = await client.get(f"{EVENT_SERVICE_URL}/events/{event_id}")
            event_response.raise_for_status()
            event_details = event_response.json()

            rsvp_response = await client.get(f"{RSVP_MANAGEMENT_URL}/events/{event_id}/rsvps/")
            rsvp_response.raise_for_status()
            rsvp_list = rsvp_response.json()

            filtered_rsvp_list = [
                add_hateoas_to_rsvp({"name": rsvp.get("name"), "email": rsvp.get("email"), "status": rsvp.get("status"), "id": rsvp.get("id"), "event_id": event_id})
                for rsvp in rsvp_list
            ]

            event_details["rsvp_count"] = len(filtered_rsvp_list)

            combined_data = {
                "Event_INFO": add_hateoas_to_event(event_details),
                "RSVP_LIST": filtered_rsvp_list
            }

            return combined_data
        except httpx.HTTPStatusError:
            raise HTTPException(status_code=404, detail="Event or RSVPs not found")

# Asynchronous Task for Aggregation with HATEOAS
@app.post("/event/{event_id}/aggregate", status_code=202, tags=["Async Tasks"])
async def start_aggregation_task(event_id: int):
    task_id = str(uuid.uuid4())
    tasks[task_id] = {"status": "IN_PROGRESS"}
    asyncio.create_task(run_aggregation_task(event_id, task_id))
    return JSONResponse(content={"task_id": task_id, "status_url": f"/tasks/{task_id}"}, status_code=202)

async def run_aggregation_task(event_id: int, task_id: str):
    try:
        async with httpx.AsyncClient() as client:
            event_response = await client.get(f"{EVENT_SERVICE_URL}/events/{event_id}")
            event_response.raise_for_status()
            event_details = event_response.json()

            rsvp_response = await client.get(f"{RSVP_MANAGEMENT_URL}/events/{event_id}/rsvps/")
            rsvp_response.raise_for_status()
            rsvp_list = rsvp_response.json()

            tasks[task_id] = {"status": "COMPLETED", "result": {"event": event_details, "rsvps": rsvp_list}}
    except Exception as e:
        tasks[task_id] = {"status": "FAILED", "error": str(e)}

@app.get("/tasks/{task_id}", tags=["Async Tasks"])
async def get_task_status(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    return tasks[task_id]

# GraphQL Endpoint for Events with HATEOAS
from graphql import graphql_sync, build_schema

schema = """
    type Event {
        id: Int
        organizationId: Int
        name: String
        description: String
        date: String
        time: String
        location: String
        category: String
        rsvpCount: Int
    }

    type Query {
        events(limit: Int): [Event]
    }
"""

graphql_schema = build_schema(schema)

def resolve_events(_, info, limit=10):
    response = httpx.get(f"{EVENT_SERVICE_URL}/events?limit={limit}")
    response.raise_for_status()
    events = response.json()
    return [add_hateoas_to_event(event) for event in events]

graphql_schema.query_type.fields["events"].resolve = resolve_events

@app.post("/graphql", tags=["GraphQL"])
async def graphql_endpoint(request: Request):
    body = await request.json()
    result = graphql_sync(graphql_schema, body["query"])
    return JSONResponse(content=result.to_dict())
