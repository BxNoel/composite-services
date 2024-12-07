from fastapi import FastAPI, HTTPException
import httpx

app = FastAPI()


# URLs for the Event Service and RSVP Service
EVENT_SERVICE_URL = "http://44.204.67.53:8001" 
RSVP_MANAGEMENT_URL = "http://3.82.202.221:8000" 
ORGANIZATIONS_URL = "http://18.234.239.121:8000"  

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/event")
async def get_event_details():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{EVENT_SERVICE_URL}/events")
        response.raise_for_status()  # This will raise an error if the request fails
        return response.json()
    
@app.get("/rsvp")
async def get_rsvp_details():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{RSVP_MANAGEMENT_URL}/rsvps/21")
        response.raise_for_status()  # This will raise an error if the request fails
        return response.json()

@app.get("/organizations")
async def get_organization(skip: int = 0, limit: int = 100):
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{ORGANIZATIONS_URL}/organizations/",
            params={"skip": skip, "limit": limit}
        )
        response.raise_for_status()
        return response.json()

"""
    Composite Service to retrieve event details and RSVPs for a specific event.

    Steps:
    1. Fetch event details from the EVENT_SERVICE based on the event ID.
    2. Fetch RSVP details for the same event from the RSVP_MANAGEMENT service.
    3. Filter the RSVP data to include only name, email, and status.
    4. Combine the event details and filtered RSVP list into a single response.
    5. Return the combined data as a JSON response.
    
"""
@app.get("/event/{event_id}/rsvps")
async def get_event_rsvp_details(event_id: int):
    async with httpx.AsyncClient() as client:
        
        # Fetch information from EVENT_SERVICE
        try:
            event_response = await client.get(f"{EVENT_SERVICE_URL}/events/{event_id}")
            event_response.raise_for_status()  # Raise an error for non-2xx responses
            event_details = event_response.json()
        except httpx.HTTPStatusError:
            raise HTTPException(status_code=404, detail=f"Event with ID {event_id} not found.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching event details: {str(e)}")
        
        # Fetch information from RSVP_MANAGEMENT
        try:
            rsvp_response = await client.get(f"{RSVP_MANAGEMENT_URL}/events/{event_id}/rsvps/")
            rsvp_response.raise_for_status()  # Raise an error for non-2xx responses
            rsvp_list = rsvp_response.json()
        except httpx.HTTPStatusError:
            raise HTTPException(status_code=404, detail=f"RSVP list for Event ID {event_id} not found.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching RSVP details: {str(e)}")
        
        # Filter RSVP data to include only name, email, and status
        filtered_rsvp_list = [
            {
                "name": rsvp.get("name"),
                "email": rsvp.get("email"),
                "status": rsvp.get("status")
            }
            for rsvp in rsvp_list
        ]
        
        # Combine Event and RSVP data
        combined_data = {
            "Event_INFO": event_details,
            "RSVP_LIST": filtered_rsvp_list
        }

        return combined_data


@app.get('/organization/event/{organization_id}')
async def get_event_rsvp_details(organization_id: int):
    async with httpx.AsyncClient() as client:
        
        # Fetch all events
        try:
            event_response = await client.get(f"{EVENT_SERVICE_URL}/events")
            event_response.raise_for_status()
            all_events = event_response.json()
            # Filter events to only include those that match the organization_id
            event_list = [event for event in all_events if event['organizationId'] == organization_id]
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=str(e))
        
        
        # Get organization details
        try:
            organization_details = await client.get(f"{ORGANIZATIONS_URL}/organizations/{organization_id}")
            organization_details.raise_for_status()
            organization_information = organization_details.json()
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=str(e))
        
        # Combine information
        organization_and_events = {
            "organization_information": organization_information,
            "organization_events": event_list
        }
        
        return organization_and_events