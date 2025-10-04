{{
  config(
    materialized='incremental',
    unique_key='arrest_id',
    tags=['fact', 'gold']
  )
}}

WITH arrests AS (
    SELECT
        arrest_id,
        case_number,
        arrest_date,
        arrestee_race,
        arrestee_gender,
        primary_charge_code,
        primary_charge_description,
        charge_type
    FROM {{ ref('stg_arrests') }}

    {% if is_incremental() %}
    WHERE arrest_date > (SELECT MAX(arrest_date) FROM {{ this }})
    {% endif %}
),

crime_types AS (
    SELECT
        iucr_code,
        primary_description,
        secondary_description
    FROM {{ ref('dim_crime_type') }}
),

crimes AS (
    SELECT
        crime_id,
        case_number,
        crime_date
    FROM {{ ref('fact_crimes') }}
)

SELECT
    a.arrest_id,
    a.case_number,
    a.arrest_date,
    a.arrestee_race,
    a.arrestee_gender,
    a.primary_charge_code,
    a.primary_charge_description,
    a.charge_type,

    ct.primary_description AS charge_primary_type,
    ct.secondary_description AS charge_secondary_type,

    c.crime_id

FROM arrests a
LEFT JOIN crime_types ct
    ON a.primary_charge_code = ct.iucr_code
LEFT JOIN crimes c
    ON a.case_number = c.case_number
    AND a.arrest_date >= c.crime_date
    AND a.arrest_date <= DATEADD(day, 30, c.crime_date) -- arrest within 30 days after crime
