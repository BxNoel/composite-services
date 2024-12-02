from fastapi import FastAPI, HTTPException
import httpx

app = FastAPI()

# URLs for the Event Service and RSVP Service
EVENT_SERVICE_URL = "http://54.164.234.52:8001"  # Replace with actual URL

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/event")
async def get_event_details(event_id: str):
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{EVENT_SERVICE_URL}/events/{event_id}")
        response.raise_for_status()  # This will raise an error if the request fails
        return response.json()