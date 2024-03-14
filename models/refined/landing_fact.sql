{{
  config(
    post_hook=["ALTER TABLE landing_fact ADD CONSTRAINT unique_constraint UNIQUE (ACTIVITY_PERIOD_ID,OPERATING_AIRLINE_IATA_CODE_ID,PUBLISHED_AIRLINE_IATA_CODE_ID,GEO_REGION_ID,AIRCRAFT_MODEL_ID)",
      "ALTER TABLE landing_fact ADD CONSTRAINT fk_landing1 FOREIGN KEY (ACTIVITY_PERIOD_ID) REFERENCES date_dim (time_id)",
      "ALTER TABLE landing_fact ADD CONSTRAINT fk_landing2 FOREIGN KEY (OPERATING_AIRLINE_IATA_CODE_ID) REFERENCES airline_dim(airline_id)",
      "ALTER TABLE landing_fact ADD CONSTRAINT fk_landing3 FOREIGN KEY (PUBLISHED_AIRLINE_IATA_CODE_ID) REFERENCES airline_dim(airline_id)",
      "ALTER TABLE landing_fact ADD CONSTRAINT fk_landing4 FOREIGN KEY (GEO_REGION_ID) REFERENCES geography_dim (geo_id)",
      "ALTER TABLE landing_fact ADD CONSTRAINT fk_landing5 FOREIGN KEY (AIRCRAFT_MODEL_ID) REFERENCES aircraft_dim (aircraft_id)"
    ]
  )
}}

WITH landing_fact_data AS (
    SELECT
        d.time_id AS ACTIVITY_PERIOD_ID,
        ad.airline_id AS OPERATING_AIRLINE_IATA_CODE_ID,
        ad1.airline_id AS PUBLISHED_AIRLINE_IATA_CODE_ID,
        gd.geo_id AS GEO_REGION_ID,
        ac.aircraft_id AS AIRCRAFT_MODEL_ID,
        l.LANDING_COUNT,
        l.TOTAL_LANDED_WEIGHT
    FROM
        DBT_DB.STAGING_SCHEMA.UPDATED_LANDING l
    JOIN
        {{ ref('date_dim') }} d ON TO_CHAR(l.ACTIVITY_PERIOD_START_DATE, 'YYYYMMDD') = d.time_id
    JOIN
        {{ ref('airline_dim') }} ad ON l.OPERATING_AIRLINE_IATA_CODE = ad.AIRLINE_IATA_CODE AND l.operating_airline = ad.airline
    JOIN
        {{ ref('airline_dim') }} ad1 ON l.PUBLISHED_AIRLINE_IATA_CODE = ad1.AIRLINE_IATA_CODE AND l.PUBLISHED_AIRLINE = ad1.airline
    JOIN
        {{ ref('geography_dim') }} gd ON l.GEO_REGION = gd.GEO_REGION
    JOIN
        {{ ref('aircraft_dim') }} ac ON l.AIRCRAFT_MODEL = ac.AIRCRAFT_MODEL
)

SELECT * FROM landing_fact_data
