{% macro fabricspark__create_view_as(relation, sql) -%}
  {#-- WARNING: OneLake's OnelakeExternalCatalog does NOT support standard Spark VIEWs.
       Only MANAGED, EXTERNAL, and MATERIALIZED_LAKE_VIEW table types are supported.
       This macro will fail when used with OneLake Lakehouse.
       Consider using materialized='table' instead. --#}
  {{ exceptions.warn("VIEW materialization is not supported in Fabric Lakehouse (OneLake). "
                     "Consider using materialized='table' with file_format='delta' instead. "
                     "This operation may fail with: 'Only the following table types are supported: "
                     "MANAGED, MATERIALIZED_LAKE_VIEW, EXTERNAL'") }}
  create or replace view {{ relation }}
  {% if config.persist_column_docs() -%}
    {% set model_columns = model.columns %}
    {% set query_columns = get_columns_in_query(sql) %}
    (
    {{ get_persist_docs_column_list(model_columns, query_columns) }}
    )
  {% endif %}
  {{ comment_clause() }}
  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif %}
  as
    {{ sql }}
{% endmacro %}