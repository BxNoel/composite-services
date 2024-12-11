schema = """
    type Event {
        id: Int
        organizationId: Int
        name: String
        description: String
        date: String
        time: Float
        location: String
        category: String
        rsvpCount: Int
    }

    type RSVP {
        id: Int            # Matches the "id" field from the API
        event_id: Int      # Matches the "event_id" field
        event_name: String # Matches the "event_name" field
        name: String       # Matches the "name" field
        email: String      # Matches the "email" field
        status: String     # Matches the "status" field
    }

    type Query {
        events(limit: Int): [Event]
        rsvps(limit: Int): [RSVP]
    }

"""