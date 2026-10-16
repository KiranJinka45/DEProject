{% macro generate_surrogate_key(field_list) %}
    {#
        Generates a deterministic MD5 surrogate key from a list of column expressions.
        Coalesces nulls to a sentinel string to ensure identical hashing semantics.
    #}
    md5(
        concat_ws(
            '||',
            {% for field in field_list %}
                coalesce(cast({{ field }} as text), '_dbt_null_')
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        )
    )
{% endmacro %}
