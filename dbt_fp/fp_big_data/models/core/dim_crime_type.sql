{{
  config(
    materialized='table',
    tags=['dimension', 'gold']
  )
}}

WITH crime_types AS (
    SELECT
        iucr_code,
        primary_description,
        secondary_description,
        index_code
    FROM {{ ref('stg_ucr_codes') }}
)

SELECT
    iucr_code,
    primary_description,
    secondary_description,
    index_code
FROM crime_types
WHERE iucr_code IS NOT NULL
ORDER BY iucr_code