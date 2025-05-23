CREATE OR REPLACE TABLE STG_VENUES AS
    SELECT
        venue_id,
        INITCAP(venue_name) AS venue_name,
        address_1,
        city,
        country,
        lat,
        lon
    FROM RAW_VENUES;