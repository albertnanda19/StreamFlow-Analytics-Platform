{% macro incremental_predicate(timestamp_column) %}
    {% if target.type == 'clickhouse' %}
        {{ timestamp_column }} > (
            select coalesce(
                max({{ timestamp_column }}),
                toDateTime('1970-01-01 00:00:00')
            )
            from {{ this }}
        ) - interval {{ var('incremental_lookback_hours', 3) }} hour
    {% elif target.type == 'duckdb' %}
        {{ timestamp_column }} > (
            select coalesce(
                max({{ timestamp_column }}),
                timestamp '1970-01-01 00:00:00'
            )
            from {{ this }}
        ) - interval '{{ var("incremental_lookback_hours", 3) }} hours'
    {% else %}
        {{ timestamp_column }} > (
            select coalesce(
                max({{ timestamp_column }}),
                '1970-01-01 00:00:00'::timestamp
            )
            from {{ this }}
        ) - interval '{{ var("incremental_lookback_hours", 3) }} hours'
    {% endif %}
{% endmacro %}
