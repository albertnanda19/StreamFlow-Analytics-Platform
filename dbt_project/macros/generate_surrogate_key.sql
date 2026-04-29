{% macro generate_surrogate_key(column_names) %}
    md5(
        concat_ws(
            '-',
            {% for col in column_names %}
                coalesce(cast({{ col }} as varchar), 'NULL')
                {%- if not loop.last %},{% endif %}
            {% endfor %}
        )
    )
{% endmacro %}
