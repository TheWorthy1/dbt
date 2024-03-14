{{
  config(
    post_hook=[
      "ALTER TABLE aircraft_dim ADD CONSTRAINT unique_constraint UNIQUE (AIRCRAFT_MODEL)",
      "ALTER TABLE aircraft_dim ADD CONSTRAINT primary_key_constraint PRIMARY KEY (aircraft_id)"
    ]
  )
}}


WITH aircraft_dim_data AS (
    SELECT
        AIRCRAFT_MANUFACTURER,
        AIRCRAFT_MODEL,
        AIRCRAFT_BODY_TYPE,
        COUNT(*) AS count,
        RANK() OVER (PARTITION BY AIRCRAFT_MODEL ORDER BY COUNT(*) DESC) AS rank
    FROM
        DBT_DB.STAGING_SCHEMA.UPDATED_LANDING
    GROUP BY
        AIRCRAFT_MANUFACTURER,
        AIRCRAFT_MODEL,
        AIRCRAFT_BODY_TYPE
    QUALIFY
        rank = 1
)

SELECT
    ROW_NUMBER() OVER (ORDER BY AIRCRAFT_MODEL) AS aircraft_id,
    AIRCRAFT_MANUFACTURER,
    AIRCRAFT_MODEL,
    AIRCRAFT_BODY_TYPE
FROM aircraft_dim_data



