CREATE OR REPLACE TABLE STG_CITIES AS
    SELECT
        city,
        country,
        state,
        latitude,
        longitude
    FROM RAW_CITIES;