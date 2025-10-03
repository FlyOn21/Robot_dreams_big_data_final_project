{{
    config(
        materialized='incremental',
        unique_key='crime_id',
        on_schema_change='append_new_columns',
        schema='staging',
        post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_crimes_case_number ON {{ this }} (case_number)",
        "CREATE INDEX IF NOT EXISTS idx_crimes_iucr_code ON {{ this }} (iucr_code)"
    ]
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'silver_crimes') }}

    {% if is_incremental() %}
        WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
    {% endif %}
),

renamed AS (
    SELECT
        crime_id,

        case_number,
        TRIM(UPPER(iucr_code)) AS iucr_code,
        fbi_code,

        crime_datetime,
        crime_date,
        EXTRACT(HOUR FROM crime_datetime) AS crime_hour,
        EXTRACT(DOW FROM crime_datetime) AS crime_day_of_week_num,
        -- FIX: PostgreSQL DOW returns 0=Sunday, 1=Monday, etc.
        CASE EXTRACT(DOW FROM crime_datetime)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS crime_day_of_week,
        EXTRACT(MONTH FROM crime_datetime) AS crime_month,
        EXTRACT(YEAR FROM crime_datetime) AS crime_year,
        EXTRACT(QUARTER FROM crime_datetime) AS crime_quarter,

        TRIM(primary_type) AS primary_type,
        TRIM(description) AS description,

        TRIM(block) AS block,
        TRIM(location_description) AS location_description,
        location_full_text,

        beat_id,
        district_id,
        ward_id,
        community_area_id,

        -- FIX: Validate coordinates and set invalid ones to NULL
        CASE
            WHEN CAST(latitude AS NUMERIC) BETWEEN 41.6 AND 42.1
            THEN ROUND(CAST(latitude AS NUMERIC), 6)
            ELSE NULL
        END AS latitude,
        CASE
            WHEN CAST(longitude AS NUMERIC) BETWEEN -88.0 AND -87.5
            THEN ROUND(CAST(longitude AS NUMERIC), 6)
            ELSE NULL
        END AS longitude,
        x_coordinate,
        y_coordinate,
        location_geo_point,

        is_arrest,
        is_domestic,

        hash_row,
        load_timestamp,
        source_file

    FROM source
),

enriched AS (
    SELECT
        r.*,
        -- FIX: Use COALESCE to provide defaults when no UCR code match found
        COALESCE(u.primary_description, 'Unknown') AS ucr_primary_description,
        COALESCE(u.secondary_description, 'Unknown') AS ucr_secondary_description,
        COALESCE(u.index_code, 'Unknown') AS index_code,
        COALESCE(u.is_index_crime, FALSE) AS is_index_crime,
        COALESCE(u.crime_category, 'Other Crime') AS crime_category,

        CASE
            WHEN r.crime_hour BETWEEN 6 AND 11 THEN 'Morning'
            WHEN r.crime_hour BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN r.crime_hour BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,

        CASE
            WHEN r.crime_day_of_week_num IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        CASE
            WHEN LOWER(r.location_description) LIKE '%residence%' OR LOWER(r.location_description) LIKE '%apartment%'
            THEN 'Residential'
            WHEN LOWER(r.location_description) LIKE '%street%' OR LOWER(r.location_description) LIKE '%sidewalk%'
            THEN 'Street'
            WHEN LOWER(r.location_description) LIKE '%retail%' OR LOWER(r.location_description) LIKE '%store%'
            THEN 'Commercial'
            WHEN LOWER(r.location_description) LIKE '%school%' OR LOWER(r.location_description) LIKE '%college%'
            THEN 'Educational'
            WHEN LOWER(r.location_description) LIKE '%parking%' OR LOWER(r.location_description) LIKE '%garage%'
            THEN 'Parking'
            ELSE 'Other'
        END AS location_type

    FROM renamed r
    LEFT JOIN {{ ref('stg_ucr_codes') }} u
        ON r.iucr_code = u.iucr_code
)

SELECT * FROM enriched