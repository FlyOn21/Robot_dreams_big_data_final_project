{{
    config(
        materialized='table',
        schema='staging'
    )
}}


WITH source_arrests AS (
    SELECT * FROM {{ source('silver', 'silver_arrests') }}

    UNION ALL

    SELECT * FROM {{ source('silver', 'silver_arrests_stream') }}
),

source_crimes AS (
    SELECT DISTINCT case_number
    FROM {{ source('silver', 'silver_crimes') }}
    WHERE case_number IS NOT NULL
),

orphan_arrests AS (
    SELECT a.*
    FROM source_arrests a
    LEFT JOIN source_crimes c
        ON a.case_number = c.case_number
    WHERE c.case_number IS NULL
),

renamed AS (
    SELECT
        arrest_id,
        case_number,

        arrest_date,
        EXTRACT(MONTH FROM arrest_date) AS arrest_month,
        EXTRACT(YEAR FROM arrest_date) AS arrest_year,
        EXTRACT(QUARTER FROM arrest_date) AS arrest_quarter,
        EXTRACT(DOW FROM arrest_date) AS arrest_day_of_week_num,

        CASE EXTRACT(DOW FROM arrest_date)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS arrest_day_of_week,

        TRIM(arrestee_race) AS arrestee_race,

        TRIM(charge_one_statute) AS charge_one_statute,
        TRIM(charge_one_description) AS charge_one_description,
        TRIM(charge_one_type) AS charge_one_type,
        TRIM(charge_one_class) AS charge_one_class,

        NULLIF(TRIM(charge_2_statute), '') AS charge_2_statute,
        NULLIF(TRIM(charge_2_description), '') AS charge_2_description,
        NULLIF(TRIM(charge_2_type), '') AS charge_2_type,
        NULLIF(TRIM(charge_2_class), '') AS charge_2_class,

        NULLIF(TRIM(charge_3_statute), '') AS charge_3_statute,
        NULLIF(TRIM(charge_3_description), '') AS charge_3_description,
        NULLIF(TRIM(charge_3_type), '') AS charge_3_type,
        NULLIF(TRIM(charge_3_class), '') AS charge_3_class,

        NULLIF(TRIM(charge_4_statute), '') AS charge_4_statute,
        NULLIF(TRIM(charge_4_description), '') AS charge_4_description,
        NULLIF(TRIM(charge_4_type), '') AS charge_4_type,
        NULLIF(TRIM(charge_4_class), '') AS charge_4_class,

        hash_row,
        load_timestamp,
        source_file

    FROM orphan_arrests
),

enriched AS (
    SELECT
        r.*,

        CASE WHEN r.charge_2_statute IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN r.charge_3_statute IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN r.charge_4_statute IS NOT NULL THEN 1 ELSE 0 END + 1 AS charge_count,

        CASE
            WHEN r.arrest_day_of_week_num IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend_arrest,

        CASE
            WHEN r.charge_one_type = 'Felony' THEN TRUE
            ELSE FALSE
        END AS is_felony_charge,

        CASE
            WHEN r.charge_2_statute IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_multiple_charges,

        CASE
            WHEN r.arrestee_race IN ('WHITE', 'BLACK', 'WHITE HISPANIC', 'BLACK HISPANIC')
            THEN r.arrestee_race
            WHEN r.arrestee_race = 'Unknown'
            THEN 'Unknown'
            ELSE 'Other'
        END AS arrestee_race_group,

        -- Quarantine metadata
        'Missing crime record' AS quarantine_reason,
        CURRENT_TIMESTAMP AS quarantined_at

    FROM renamed r
)

SELECT * FROM enriched