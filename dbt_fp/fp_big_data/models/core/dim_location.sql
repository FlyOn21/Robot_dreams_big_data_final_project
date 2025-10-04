{{
  config(
    materialized='table',
    tags=['dimension', 'gold']
  )
}}

WITH unique_locations AS (
    SELECT DISTINCT
        block,
        location_description,
        latitude,
        longitude,
        x_coordinate,
        y_coordinate,
        beat_id,
        district_id,
        ward_id,
        community_area_id
    FROM {{ ref('stg_crimes') }}
    WHERE latitude IS NOT NULL
      AND longitude IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'block',
        'latitude',
        'longitude'
    ]) }} AS location_key,

    block,
    location_description,
    latitude,
    longitude,
    x_coordinate,
    y_coordinate,
    beat_id,
    district_id,
    ward_id,
    community_area_id,

    -- Additional enrichment
    CONCAT(
        COALESCE(block, ''),
        ' - ',
        COALESCE(location_description, '')
    ) AS location_full_text

FROM unique_locations