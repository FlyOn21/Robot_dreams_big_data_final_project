{{
  config(
    materialized='incremental',
    unique_key='crime_id',
    tags=['fact', 'gold']
  )
}}

WITH crimes AS (
    SELECT
        crime_id,
        case_number,
        crime_datetime,
        crime_date,
        crime_hour,
        crime_day_of_week,
        crime_month,
        crime_year,
        is_arrest,
        is_domestic,
        fbi_code,
        iucr_code,
        primary_type,

        -- Location attributes for join
        COALESCE(block, 'Unknown') AS block,
        COALESCE(location_description, 'Unknown') AS location_description,
        latitude,
        longitude,
        beat_id,
        district_id,
        ward_id,
        community_area_id
    FROM {{ ref('stg_crimes') }}

    {% if is_incremental() %}
    WHERE crime_date > (SELECT MAX(crime_date) FROM {{ this }})
    {% endif %}
),

crimes_with_location_key AS (
    SELECT
        c.*,
        {{ dbt_utils.generate_surrogate_key([
            'c.block',
            'c.location_description',
            'c.latitude',
            'c.longitude',
            'COALESCE(c.beat_id, -1)',
            'COALESCE(c.district_id, -1)',
            'COALESCE(c.ward_id, -1)',
            'COALESCE(c.community_area_id, -1)'
        ]) }} AS location_key
    FROM crimes c
),

crime_types AS (
    SELECT
        iucr_code,
        primary_description,
        secondary_description,
        index_code
    FROM {{ ref('dim_crime_type') }}
)

SELECT
    c.crime_id,
    c.case_number,
    c.crime_datetime,
    c.crime_date,
    c.crime_hour,
    c.crime_day_of_week,
    c.crime_month,
    c.crime_year,
    c.is_arrest,
    c.is_domestic,
    c.fbi_code,

    -- Location and crime type references
    c.location_key,
    c.iucr_code,

    -- Location attributes for aggregations
    c.beat_id,
    c.district_id,
    c.ward_id,
    c.community_area_id,

    -- Crime type descriptions
    COALESCE(ct.primary_description, 'Unknown') AS primary_description,
    COALESCE(ct.secondary_description, 'Unknown') AS secondary_description,

    -- Additional flag for violent crimes
    CASE
        WHEN ct.index_code = 'I'
          OR ct.primary_description IN ('HOMICIDE', 'ASSAULT', 'BATTERY', 'ROBBERY', 'SEXUAL ASSAULT')
        THEN TRUE
        ELSE FALSE
    END AS is_violent_crime

FROM crimes_with_location_key c
LEFT JOIN crime_types ct
    ON c.iucr_code = ct.iucr_code