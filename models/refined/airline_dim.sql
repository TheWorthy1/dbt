{{
  config(
    post_hook=[
      "ALTER TABLE airline_dim ADD CONSTRAINT unique_constraint UNIQUE (AIRLINE,AIRLINE_IATA_CODE)",
      "ALTER TABLE airline_dim ADD CONSTRAINT primary_key_constraint PRIMARY KEY (airline_id)"
    ]
  )
}}

WITH airline_dim_data AS (
    SELECT
        AIRLINE,
        AIRLINE_IATA_CODE,
        TO_NUMBER(TO_CHAR(DATE_TRUNC('MONTH', MIN(START_DATE)), 'YYYYMMDD')) AS START_DATE,
        TO_NUMBER(TO_CHAR(DATE_TRUNC('MONTH', MAX(END_DATE)), 'YYYYMMDD')) AS END_DATE
    FROM
        (
            SELECT
                COALESCE(OPERATING_AIRLINE, PUBLISHED_AIRLINE) AS AIRLINE,
                COALESCE(OPERATING_AIRLINE_IATA_CODE, PUBLISHED_AIRLINE_IATA_CODE) AS AIRLINE_IATA_CODE,
                MIN(ACTIVITY_PERIOD_START_DATE) AS START_DATE,
                MAX(ACTIVITY_PERIOD_START_DATE) AS END_DATE
            FROM
                (
                    SELECT OPERATING_AIRLINE, PUBLISHED_AIRLINE,ACTIVITY_PERIOD_START_DATE,OPERATING_AIRLINE_IATA_CODE, PUBLISHED_AIRLINE_IATA_CODE FROM DBT_DB.STAGING_SCHEMA.UPDATED_CARGO
                    UNION ALL
                    SELECT OPERATING_AIRLINE, PUBLISHED_AIRLINE,ACTIVITY_PERIOD_START_DATE,OPERATING_AIRLINE_IATA_CODE, PUBLISHED_AIRLINE_IATA_CODE FROM DBT_DB.STAGING_SCHEMA.UPDATED_MONTHLY_PASSENGERS
                    UNION ALL
                    SELECT OPERATING_AIRLINE, PUBLISHED_AIRLINE,ACTIVITY_PERIOD_START_DATE,OPERATING_AIRLINE_IATA_CODE, PUBLISHED_AIRLINE_IATA_CODE FROM DBT_DB.STAGING_SCHEMA.UPDATED_LANDING
                ) combined_data
            GROUP BY
                AIRLINE,
                AIRLINE_IATA_CODE,
                ACTIVITY_PERIOD_START_DATE
        ) aggregated_data
    GROUP BY
        AIRLINE,
        AIRLINE_IATA_CODE
)

SELECT
    ROW_NUMBER() OVER (ORDER BY AIRLINE, AIRLINE_IATA_CODE) AS airline_id,
    AIRLINE,
    AIRLINE_IATA_CODE,
    START_DATE,
    END_DATE
FROM airline_dim_data
