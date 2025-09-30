{{
    config(
        materialized='table',
        schema='staging'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'silver_ucr_codes') }}
),

renamed AS (
    SELECT
        TRIM(UPPER(iucr_code)) AS iucr_code,

        TRIM(primary_description) AS primary_description,
        TRIM(secondary_description) AS secondary_description,
        TRIM(index_code) AS index_code,

        is_active,

        load_timestamp

    FROM source
),

final AS (
    SELECT
        *,

        CASE
            WHEN index_code = 'I' THEN TRUE
            ELSE FALSE
        END AS is_index_crime,

        CASE
            WHEN primary_description IN ('HOMICIDE', 'CRIMINAL SEXUAL ASSAULT', 'ROBBERY', 'AGGRAVATED ASSAULT')
            THEN 'Violent Crime'
            WHEN primary_description IN ('BURGLARY', 'THEFT', 'MOTOR VEHICLE THEFT', 'ARSON')
            THEN 'Property Crime'
            ELSE 'Other Crime'
        END AS crime_category

    FROM renamed
)

SELECT * FROM final