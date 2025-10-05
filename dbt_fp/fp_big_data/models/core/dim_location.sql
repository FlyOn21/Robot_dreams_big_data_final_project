
{{
  config(
    materialized='table',
    tags=['dimension', 'gold']
  )
}}

WITH unique_locations AS (
    SELECT DISTINCT
        COALESCE(block, 'Unknown') AS block,
        COALESCE(location_description, 'Unknown') AS location_description,
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
),

deduplicated_locations AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                block,
                location_description,
                latitude,
                longitude,
                COALESCE(beat_id, -1),
                COALESCE(district_id, -1),
                COALESCE(ward_id, -1),
                COALESCE(community_area_id, -1)
            ORDER BY block
        ) AS rn
    FROM unique_locations
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'block',
        'location_description',
        'latitude',
        'longitude',
        'COALESCE(beat_id, -1)',
        'COALESCE(district_id, -1)',
        'COALESCE(ward_id, -1)',
        'COALESCE(community_area_id, -1)'
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
        block,
        ' - ',
        location_description
    ) AS location_full_text

FROM deduplicated_locations
WHERE rn = 1