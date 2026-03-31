{% macro fabricspark__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    {# create schema if not exists {{relation}} #}
    select 1
  {% endcall %}
{% endmacro %}

{% macro fabricspark__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    {# drop schema if exists {{ relation }} cascade #}
    select 1
  {%- endcall -%}
{% endmacro %}

{% macro fabricspark__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    show databases
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro fabricspark__generate_database_name(custom_database_name=none, node=none) -%}
  {%- if custom_database_name is none -%}
    {%- if node is not none and node|attr('database') -%}
      {{ return(node.database) }}
    {%- elif 'config' in target -%}
      {{ return(target.database) }}
    {%- else -%}
      {{ return(target.database) }}
    {%- endif -%}
  {%- else -%}
    {{ return(custom_database_name) }}
  {%- endif -%}
{%- endmacro %}
