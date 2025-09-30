{% macro test_charge_consistency(model) %}

-- Custom test to validate charge_count matches actual non-null charges

WITH validation AS (
    SELECT
        arrest_id,
        charge_count,
        CASE WHEN charge_one_statute IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN charge_2_statute IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN charge_3_statute IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN charge_4_statute IS NOT NULL THEN 1 ELSE 0 END AS actual_count
    FROM {{ model }}
),

inconsistent AS (
    SELECT *
    FROM validation
    WHERE charge_count != actual_count
)

SELECT * FROM inconsistent

{% endmacro %}