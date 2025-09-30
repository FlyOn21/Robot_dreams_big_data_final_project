{% macro test_valid_timestamp_range(model, column_name, start_year=2001) %}

-- Custom test to validate timestamp is within reasonable range

WITH validation AS (
    SELECT
        {{ column_name }} AS timestamp_column
    FROM {{ model }}
    WHERE
        {{ column_name }} < '{{ start_year }}-01-01'::TIMESTAMP
        OR {{ column_name }} > CURRENT_TIMESTAMP
)

SELECT * FROM validation

{% endmacro %}