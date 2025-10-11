{{
  config(
    materialized='incremental',
    unique_key='arrest_id',
    tags=['fact', 'gold']
  )
}}

WITH source_arrests AS (
    SELECT
        arrest_id,
        case_number,
        arrest_date,
        arrestee_race,
        charge_one_statute,
        charge_one_description,
        charge_one_type,
        charge_one_class,
        is_felony_charge,
        has_multiple_charges,
        charge_count
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
),

arrests_with_crimes AS (
    SELECT
        a.*,
        ct.primary_description AS charge_primary_type,
        ct.secondary_description AS charge_secondary_type,
        c.crime_id,
        ROW_NUMBER() OVER (
            PARTITION BY a.arrest_id
            ORDER BY c.crime_date DESC
        ) AS rn
    FROM source_arrests a
    LEFT JOIN crime_types ct
        ON a.charge_one_statute = ct.iucr_code
    LEFT JOIN crimes c
        ON a.case_number = c.case_number
        AND a.arrest_date >= c.crime_date
        AND a.arrest_date <= c.crime_date + INTERVAL '30 days'
)

SELECT
    arrest_id,
    case_number,
    arrest_date,
    arrestee_race,
    charge_one_statute AS primary_charge_code,
    charge_one_description AS primary_charge_description,
    charge_one_type AS charge_type,
    charge_one_class,
    is_felony_charge,
    has_multiple_charges,
    charge_count,
    charge_primary_type,
    charge_secondary_type,
    crime_id
FROM arrests_with_crimes
WHERE rn = 1