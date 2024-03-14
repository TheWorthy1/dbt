{{
  config(
    post_hook=["ALTER TABLE cargo_fact ADD CONSTRAINT unique_constraint1 UNIQUE (ACTIVITY_PERIOD_ID,OPERATING_AIRLINE_IATA_CODE_ID,PUBLISHED_AIRLINE_IATA_CODE_ID,GEO_REGION_ID)",
      "ALTER TABLE cargo_fact ADD CONSTRAINT fk_cargo6 FOREIGN KEY (ACTIVITY_PERIOD_ID) REFERENCES date_dim (time_id)",
      "ALTER TABLE cargo_fact ADD CONSTRAINT fk_cargo2 FOREIGN KEY (OPERATING_AIRLINE_IATA_CODE_ID) REFERENCES airline_dim(airline_id)",
      "ALTER TABLE cargo_fact ADD CONSTRAINT fk_cargo3 FOREIGN KEY (PUBLISHED_AIRLINE_IATA_CODE_ID) REFERENCES airline_dim(airline_id)",
      "ALTER TABLE cargo_fact ADD CONSTRAINT fk_cargo4 FOREIGN KEY (GEO_REGION_ID) REFERENCES geography_dim (geo_id)"
    ]
  )
}}

WITH cargo_fact_data AS (
    SELECT
        d.time_id AS ACTIVITY_PERIOD_ID,
        ad.airline_id AS OPERATING_AIRLINE_IATA_CODE_ID,
        ad1.airline_id AS PUBLISHED_AIRLINE_IATA_CODE_ID,
        gd.geo_id AS GEO_REGION_ID,
        c.CARGO_METRIC_TONS,
        c.CARGO_WEIGHT_LBS
    FROM
        DBT_DB.STAGING_SCHEMA.UPDATED_CARGO c
    JOIN
        {{ ref('date_dim') }} d ON TO_CHAR(c.ACTIVITY_PERIOD_START_DATE, 'YYYYMMDD') = d.time_id
    JOIN
        {{ ref('airline_dim') }} ad ON c.OPERATING_AIRLINE_IATA_CODE = ad.AIRLINE_IATA_CODE AND c.operating_airline = ad.airline
    JOIN
        {{ ref('airline_dim') }} ad1 ON c.PUBLISHED_AIRLINE_IATA_CODE = ad1.AIRLINE_IATA_CODE AND c.PUBLISHED_AIRLINE = ad1.airline
    JOIN
        {{ ref('geography_dim') }} gd ON c.GEO_REGION = gd.GEO_REGION
)

SELECT * FROM cargo_fact_data
