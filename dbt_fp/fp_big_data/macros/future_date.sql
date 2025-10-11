{% macro test_future_date(model, column_name) %}

WITH validation AS (
    SELECT
        {{ column_name }} AS date_column
    FROM {{ model }}
    WHERE
        {{ column_name }} > CURRENT_DATE
)

SELECT * FROM validation

{% endmacro %}