{% macro test_no_orphan_arrests(model) %}

WITH arrests AS (
    SELECT DISTINCT case_number
    FROM {{ model }}
    WHERE case_number IS NOT NULL
),

crimes AS (
    SELECT DISTINCT case_number
    FROM {{ ref('stg_crimes') }}
    WHERE case_number IS NOT NULL
)

SELECT a.case_number
FROM arrests a
LEFT JOIN crimes c
    ON a.case_number = c.case_number
WHERE c.case_number IS NULL

{% endmacro %}