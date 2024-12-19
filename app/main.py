from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
import httpx
import asyncio
import json
from kafka import KafkaConsumer
import threading
import boto3
import logging

# Initialize FastAPI app
app = FastAPI(title="COMPOSITE", version="1.0.0")

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Welcome!"}

# Microservice URLs
EVENT_SERVICE_URL = "http://localhost:8001"
RSVP_MANAGEMENT_URL = "http://localhost:8003"
ORGANIZATIONS_URL = "http://localhost:8000"

# Step Functions client (AWS Orchestration)
stepfunctions = boto3.client("stepfunctions", region_name="us-east-1")

# Kafka Consumer Initialization
consumer = KafkaConsumer(
    'events_topic',
    'rsvps_topic',
    'organizations_topic',
    bootstrap_servers='localhost:9092',
    group_id='composite_service',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# In-memory caches for Kafka data
event_cache = {}
rsvp_cache = {}
organization_cache = {}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Consume Kafka messages
def consume_kafka_messages():
    for message in consumer:
        topic = message.topic
        data = message.value

        if topic == "events_topic":
            action = data.get("action")
            if action == "create":
                event_cache[data["event"]["id"]] = data["event"]
            elif action == "delete":
                event_cache.pop(data["event_id"], None)
        elif topic == "rsvps_topic":
            action = data.get("action")
            if action == "create":
                handle_new_rsvp(data)
            elif action == "delete":
                rsvp_cache.pop(data["rsvp_id"], None)
        elif topic == "organizations_topic":
            action = data.get("action")
            if action == "create":
                organization_cache[data["organization"]["id"]] = data["organization"]
            elif action == "delete":
                organization_cache.pop(data["organization_id"], None)

def handle_new_rsvp(data):
    rsvp = data.get("rsvp")
    event_id = data.get("event_id")

    if not event_id or not rsvp:
        logger.error("Invalid RSVP message format")
        return

    try:
        # Fetch the current event details from the events microservice
        with httpx.Client() as client:
            event_response = client.get(f"{EVENT_SERVICE_URL}/events/{event_id}")
            event_response.raise_for_status()
            event_details = event_response.json()

        # Increment the RSVP count
        current_rsvp_count = event_details.get("rsvpCount", 0)
        event_details["rsvpCount"] = current_rsvp_count + 1

        # Send updated event details back to the events microservice
        with httpx.Client() as client:
            update_response = client.put(f"{EVENT_SERVICE_URL}/events/{event_id}", json=event_details)
            update_response.raise_for_status()

        logger.info(f"Updated RSVP count for event {event_id} to {event_details['rsvpCount']}")
    except Exception as e:
        logger.error(f"Failed to update RSVP count for event {event_id}: {str(e)}")


# Start Kafka consumer thread
@app.on_event("startup")
def start_kafka_consumer():
    threading.Thread(target=consume_kafka_messages, daemon=True).start()

# ========== 1. Synchronous API Calls ==========
@app.get("/sync/events")
def get_events_sync():
    response = httpx.get(f"{EVENT_SERVICE_URL}/events")
    response.raise_for_status()
    return response.json()

@app.get("/sync/organizations")
def get_organizations_sync(skip: int = 0, limit: int = 100):
    response = httpx.get(f"{ORGANIZATIONS_URL}/organizations", params={"skip": skip, "limit": limit})
    response.raise_for_status()
    return response.json()

@app.get("/sync/rsvps/{event_id}")
def get_rsvps_sync(event_id: int):
    response = httpx.get(f"{RSVP_MANAGEMENT_URL}/events/{event_id}/rsvps/")
    response.raise_for_status()
    return response.json()

@app.get("/sync/{event_id}", tags=["Composite"])
async def get_event_rsvp_details(event_id: int):
    async with httpx.AsyncClient() as client:
        try:
            event_response = await client.get(f"{EVENT_SERVICE_URL}/events/{event_id}")
            event_response.raise_for_status()
            event_details = event_response.json()

            rsvp_response = await client.get(f"{RSVP_MANAGEMENT_URL}/events/{event_id}/rsvps/")
            rsvp_response.raise_for_status()
            rsvp_list = rsvp_response.json()

            combined_data = {
                "Event_INFO": event_details,
                "RSVP_LIST": rsvp_list
            }

            return combined_data
        except httpx.HTTPStatusError:
            raise HTTPException(status_code=404, detail="Event or RSVPs not found")

# ========== 2. Asynchronous API Calls ==========
@app.get("/async/composite-details/{event_id}")
async def get_composite_details(event_id: int):
    async with httpx.AsyncClient() as client:
        try:
            event_task = client.get(f"{EVENT_SERVICE_URL}/events/{event_id}")
            rsvp_task = client.get(f"{RSVP_MANAGEMENT_URL}/events/{event_id}/rsvps")
            org_task = client.get(f"{ORGANIZATIONS_URL}/organizations")
            responses = await asyncio.gather(event_task, rsvp_task, org_task)

            return {
                "event": responses[0].json(),
                "rsvps": responses[1].json(),
                "organizations": responses[2].json()
            }
        except httpx.RequestError as e:
            raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

# ========== 3. Service Choreography (Pub/Sub) ==========
@app.get("/choreography/events")
def get_events_from_cache():
    return {"events": list(event_cache.values())}

@app.get("/choreography/rsvps")
def get_rsvps_from_cache():
    return {"rsvps": list(rsvp_cache.values())}

@app.get("/choreography/organizations")
def get_organizations_from_cache():
    return {"organizations": list(organization_cache.values())}

# ========== 4. Service Orchestration (Step Functions) ==========
@app.post("/orchestrate/event/{event_id}")
def orchestrate_event_workflow(event_id: int):
    """
    Start a Step Functions execution for orchestrating workflows related to an event.
    """
    input_payload = {
        "event_id": event_id,
        "event_service_url": EVENT_SERVICE_URL,
        "rsvp_service_url": RSVP_MANAGEMENT_URL,
        "organization_service_url": ORGANIZATIONS_URL
    }

    try:
        response = stepfunctions.start_execution(
            stateMachineArn="arn:aws:states:us-east-1:account-id:stateMachine:OrchestrateEventWorkflow",
            input=json.dumps(input_payload),  # Pass input data for the workflow
        )
        logger.info(f"Started Step Function execution: {response['executionArn']}")
        return {
            "message": "Step Function execution started",
            "executionArn": response["executionArn"]
        }
    except Exception as e:
        logger.error(f"Step Functions error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Step Functions error: {str(e)}")

