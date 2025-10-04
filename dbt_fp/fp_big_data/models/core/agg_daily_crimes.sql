{{
  config(
    materialized='incremental',
    unique_key=['crime_date', 'primary_type', 'district_id', 'community_area_id'],
    tags=['aggregate', 'gold']
  )
}}

WITH daily_crimes AS (
    SELECT
        crime_date,
        primary_description AS primary_type,
        district_id,
        community_area_id,

        COUNT(*) AS total_crimes,
        SUM(CASE WHEN is_arrest THEN 1 ELSE 0 END) AS crimes_with_arrest,
        SUM(CASE WHEN is_domestic THEN 1 ELSE 0 END) AS domestic_crimes

    FROM {{ ref('fact_crimes') }}

    {% if is_incremental() %}
    WHERE crime_date > (SELECT MAX(crime_date) FROM {{ this }})
    {% endif %}

    GROUP BY
        crime_date,
        primary_description,
        district_id,
        community_area_id
)

SELECT
    crime_date,
    primary_type,
    district_id,
    community_area_id,
    total_crimes,
    crimes_with_arrest,
    domestic_crimes
FROM daily_crimes
ORDER BY crime_date DESC, total_crimes DESC