{{
  config(
    post_hook=[
      "ALTER TABLE geography_dim ADD CONSTRAINT unique_constraint UNIQUE (GEO_REGION)",
      "ALTER TABLE geography_dim ADD CONSTRAINT primary_key_constraint PRIMARY KEY (geo_id)"
    ]
  )
}}

WITH geography_dim_data AS (
    SELECT
        GEO_REGION,
        GEO_SUMMARY,
        COUNT(*) AS count,
        RANK() OVER (PARTITION BY GEO_REGION ORDER BY COUNT(*) DESC) AS rank
    FROM
        (
            SELECT GEO_REGION, GEO_SUMMARY FROM DBT_DB.STAGING_SCHEMA.UPDATED_CARGO
            UNION ALL
            SELECT GEO_REGION, GEO_SUMMARY FROM DBT_DB.STAGING_SCHEMA.UPDATED_MONTHLY_PASSENGERS
            UNION ALL
            SELECT GEO_REGION, GEO_SUMMARY FROM DBT_DB.STAGING_SCHEMA.UPDATED_LANDING
        ) AS CombinedData
    GROUP BY
        GEO_REGION,
        GEO_SUMMARY
    QUALIFY
        rank = 1
)

SELECT
    ROW_NUMBER() OVER (ORDER BY GEO_REGION) AS geo_id,
    GEO_REGION,
    GEO_SUMMARY
FROM geography_dim_data
