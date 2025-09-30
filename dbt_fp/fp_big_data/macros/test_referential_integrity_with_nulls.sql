{% macro test_referential_integrity_with_nulls(model, column_name, to, field) %}

-- Custom test for referential integrity that allows NULLs

WITH child AS (
    SELECT {{ column_name }} AS fk
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
),

parent AS (
    SELECT {{ field }} AS pk
    FROM {{ to }}
),

fk_not_in_parent AS (
    SELECT DISTINCT fk
    FROM child
    WHERE fk NOT IN (SELECT pk FROM parent)
)

SELECT * FROM fk_not_in_parent

{% endmacro %}