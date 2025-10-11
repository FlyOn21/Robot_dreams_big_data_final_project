{% macro generate_staging_stats(model_name) %}


SELECT
    '{{ model_name }}' AS model_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT load_timestamp) AS distinct_load_timestamps,
    MIN(load_timestamp) AS earliest_load,
    MAX(load_timestamp) AS latest_load,
    CURRENT_TIMESTAMP AS stats_generated_at
FROM {{ ref(model_name) }}

{% endmacro %}