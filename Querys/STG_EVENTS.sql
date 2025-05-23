CREATE OR REPLACE TABLE STG_EVENTS AS
    SELECT
        event_id,
        INITCAP(event_name) AS event_name,
        TO_TIMESTAMP(created) AS created_at,
        TO_TIMESTAMP(event_time) AS event_time,
        group_id,
        venue_id,
        venue_city AS city,
        venue_country AS country,
        venue_lat AS latitude,
        venue_lon AS longitude
    FROM RAW_EVENTS;