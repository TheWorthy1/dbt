{{
  config(
    post_hook=["ALTER TABLE passenger_fact ADD CONSTRAINT unique_constraint2 UNIQUE (ACTIVITY_PERIOD_ID,OPERATING_AIRLINE_IATA_CODE_ID,PUBLISHED_AIRLINE_IATA_CODE_ID,GEO_REGION_ID)",
      "ALTER TABLE passenger_fact ADD CONSTRAINT fk_passenger1 FOREIGN KEY (ACTIVITY_PERIOD_ID) REFERENCES date_dim (time_id)",
      "ALTER TABLE passenger_fact ADD CONSTRAINT fk_passenger2 FOREIGN KEY (OPERATING_AIRLINE_IATA_CODE_ID) REFERENCES airline_dim(airline_id)",
      "ALTER TABLE passenger_fact ADD CONSTRAINT fk_passenger3 FOREIGN KEY (PUBLISHED_AIRLINE_IATA_CODE_ID) REFERENCES airline_dim(airline_id)",
      "ALTER TABLE passenger_fact ADD CONSTRAINT fk_passenger4 FOREIGN KEY (GEO_REGION_ID) REFERENCES geography_dim (geo_id)"
    ]
  )
}}

WITH passenger_fact_data AS (
    SELECT
        d.time_id AS ACTIVITY_PERIOD_ID,
        ad.airline_id AS OPERATING_AIRLINE_IATA_CODE_ID,
        ad1.airline_id AS PUBLISHED_AIRLINE_IATA_CODE_ID,
        gd.geo_id AS GEO_REGION_ID,
        P.PASSENGER_COUNT
    FROM
        DBT_DB.STAGING_SCHEMA.UPDATED_MONTHLY_PASSENGERS P
    JOIN
        {{ ref('date_dim') }} d ON TO_CHAR(P.ACTIVITY_PERIOD_START_DATE, 'YYYYMMDD') = d.time_id
    JOIN
        {{ ref('airline_dim') }} ad ON P.OPERATING_AIRLINE_IATA_CODE = ad.AIRLINE_IATA_CODE AND P.operating_airline = ad.airline
    JOIN
        {{ ref('airline_dim') }} ad1 ON P.PUBLISHED_AIRLINE_IATA_CODE = ad1.AIRLINE_IATA_CODE AND P.PUBLISHED_AIRLINE = ad1.airline
    JOIN
        {{ ref('geography_dim') }} gd ON P.GEO_REGION = gd.GEO_REGION
)

SELECT * FROM passenger_fact_data
