{% macro drop_defer_tables() %}
    {% set tables = ['defer_base', 'defer_agg', 'defer_customers'] %}
    {% set views = ['defer_summary'] %}

    {% for tbl in tables %}
        {% call statement('drop_' ~ tbl) %}
            DROP TABLE IF EXISTS {{ target.lakehouse }}.dbo.{{ tbl }}
        {% endcall %}
    {% endfor %}

    {% for v in views %}
        {% call statement('drop_' ~ v) %}
            DROP VIEW IF EXISTS {{ target.lakehouse }}.dbo.{{ v }}
        {% endcall %}
    {% endfor %}

    {{ log("Dropped all defer test tables and views", info=True) }}
{% endmacro %}


{% macro drop_seeds() %}
    {% set seeds = ['seed_customers', 'seed_orders'] %}
    {% for s in seeds %}
        {% call statement('drop_seed_' ~ s) %}
            DROP TABLE IF EXISTS {{ target.lakehouse }}.dbo.{{ s }}
        {% endcall %}
    {% endfor %}
    {{ log("Dropped all seed tables", info=True) }}
{% endmacro %}
