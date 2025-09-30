{% macro test_no_orphan_arrests(model) %}

-- This checks if case_number in arrests exists in crimes

WITH arrests AS (
    SELECT DISTINCT case_number
    FROM {{ model }}
    WHERE case_number IS NOT NULL
),

crimes AS (
    SELECT DISTINCT case_number
    FROM {{ ref('stg_crimes') }}
),

orphan_arrests AS (
    SELECT case_number
    FROM arrests
    WHERE case_number NOT IN (SELECT case_number FROM crimes)
)

SELECT * FROM orphan_arrests

{% endmacro %}