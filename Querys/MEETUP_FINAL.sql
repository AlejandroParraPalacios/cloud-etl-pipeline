CREATE OR REPLACE TABLE MEETUP_FINAL AS
    SELECT
        e.event_id,
        e.event_name,
        e.created_at,
        e.event_time,
        e.city,
        e.country,
        g.group_name,
        v.venue_name,
        c.state AS city_state,
        e.latitude,
        e.longitude
    FROM STG_EVENTS e
    LEFT JOIN STG_GROUPS g ON e.group_id = g.group_id
    LEFT JOIN STG_VENUES v ON e.venue_id = v.venue_id
    LEFT JOIN STG_CITIES c ON e.city = c.city AND e.country = c.country;