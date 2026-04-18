{% macro drop_elementary_schema() %}
    {% call statement('drop_elementary') %}
        DROP DATABASE IF EXISTS {{ target.lakehouse }}.elementary CASCADE
    {% endcall %}
    {{ log("Dropped elementary schema", info=True) }}
{% endmacro %}
