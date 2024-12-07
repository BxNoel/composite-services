# API Documentation

## Overview

This document outlines the functionalities provided by our API endpoints. The API is designed to fetch event and RSVP details, as well as event information specific to an organization.

## Endpoints

### 1. Fetch Event RSVP Details

- **Endpoint**: `/event/{event_id}/rsvps`
- **Method**: GET
- **Description**: Retrieves detailed information about a specific event along with a list of RSVPs. It filters RSVP data to include only the name, email, and RSVP status of the attendees.
- **Path Parameters**:
  - `event_id` (integer): The unique identifier of the event.
- **Responses**:
  - **200 OK**: Successfully retrieves event and RSVP details.
    - **Content**:
      ```json
      {
        "Event_INFO": {
          "id": integer,
          "name": string,
          "description": string,
          "date": string,
          "location": string,
          "category": string,
          "rsvpCount": integer
        },
        "RSVP_LIST": [
          {
            "name": string,
            "email": string,
            "status": string
          }
        ]
      }
      ```
  - **404 Not Found**: No event or RSVPs found with the provided `event_id`.
  - **500 Internal Server Error**: Error occurred while fetching data from external services.

### 2. Fetch Events by Organization

- **Endpoint**: `/organization/event/{organization_id}`
- **Method**: GET
- **Description**: Retrieves a list of all events associated with a specified organization along with organization details.
- **Path Parameters**:
  - `organization_id` (integer): The unique identifier of the organization.
- **Responses**:
  - **200 OK**: Successfully retrieves the list of events and organization details.
    - **Content**:
      ```json
      {
        "organization_information": {
          "id": integer,
          "name": string,
          "description": string
        },
        "organization_events": [
          {
            "id": integer,
            "organizationId": integer,
            "name": string,
            "description": string,
            "date": string,
            "location": string,
            "category": string,
            "rsvpCount": integer
          }
        ]
      }
      ```
  - **404 Not Found**: No organization or events found with the provided `organization_id`.
  - **500 Internal Server Error**: Error occurred while fetching data from external services.

## Error Codes

- **404 Not Found**: The specified resource could not be found. This can happen in cases where the event or organization ID does not exist.
- **500 Internal Server Error**: A server error occurred while processing the request. This could be due to issues with external service dependencies.

## Notes

- Ensure that the base URLs for `EVENT_SERVICE_URL` and `ORGANIZATIONS_URL` are correctly configured to point to the respective services handling event and organization data.
- The API is designed to be robust, handling errors gracefully and providing meaningful error messages to assist in troubleshooting.
