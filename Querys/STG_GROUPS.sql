CREATE OR REPLACE TABLE STG_GROUPS AS
    SELECT
        group_id,
        INITCAP(group_name) AS group_name,
        category_id,
        city,
        country
    FROM RAW_GROUPS;