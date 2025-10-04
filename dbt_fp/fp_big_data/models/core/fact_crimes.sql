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

        -- Location attributes
        block,
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

locations AS (
    SELECT
        location_key,
        block,
        latitude,
        longitude
    FROM {{ ref('dim_location') }}
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

    l.location_key,
    c.iucr_code,

    ct.primary_description,
    ct.secondary_description,

    CASE
        WHEN ct.index_code = 'I'
          OR ct.primary_description IN ('HOMICIDE', 'ASSAULT', 'BATTERY', 'ROBBERY', 'SEXUAL ASSAULT')
        THEN TRUE
        ELSE FALSE
    END AS is_violent_crime

FROM crimes c
LEFT JOIN locations l
    ON c.block = l.block
    AND c.latitude = l.latitude
    AND c.longitude = l.longitude
LEFT JOIN crime_types ct
    ON c.iucr_code = ct.iucr_code