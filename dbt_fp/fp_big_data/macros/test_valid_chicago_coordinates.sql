{% macro test_valid_chicago_coordinates(model, column_lat, column_lon) %}

WITH validation AS (
    SELECT
        {{ column_lat }} AS latitude,
        {{ column_lon }} AS longitude
    FROM {{ model }}
    WHERE
        {{ column_lat }} IS NOT NULL
        AND {{ column_lon }} IS NOT NULL
        AND (
            {{ column_lat }} < 41.6
            OR {{ column_lat }} > 42.1
            OR {{ column_lon }} < -88.0
            OR {{ column_lon }} > -87.5
        )
)

SELECT * FROM validation

{% endmacro %}