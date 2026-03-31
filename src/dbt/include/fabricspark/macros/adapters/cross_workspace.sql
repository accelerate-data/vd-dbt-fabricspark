{#--
    Macros for cross-workspace queries using Fabric Spark SQL four-part naming.

    Fabric Spark SQL natively supports four-part naming:
        workspace.lakehouse.schema.table

    This enables cross-workspace joins and queries without requiring shortcuts.
--#}


{#--
    Render a four-part table reference for cross-workspace queries.

    Usage:
        {{ fabric_ref('workspace_name', 'lakehouse_name', 'schema_name', 'table_name') }}
        {{ fabric_ref('analytics_ws', 'gold_lakehouse', 'dbo', 'dim_customers') }}

    For tables in the current workspace, you can omit the workspace:
        {{ fabric_ref(none, 'other_lakehouse', 'dbo', 'dim_customers') }}
--#}
{% macro fabric_ref(workspace, lakehouse, schema, table) %}
    {%- if workspace -%}
        {{ workspace }}.{{ lakehouse }}.{{ schema }}.{{ table }}
    {%- elif lakehouse and schema -%}
        {{ lakehouse }}.{{ schema }}.{{ table }}
    {%- else -%}
        {{ schema }}.{{ table }}
    {%- endif -%}
{% endmacro %}


{#--
    Render a cross-workspace table reference with automatic relation creation.

    Usage:
        {% set remote_table = fabric_relation(
            workspace='analytics_workspace',
            lakehouse='gold_lakehouse',
            schema='dbo',
            identifier='dim_customers'
        ) %}
        SELECT * FROM {{ remote_table }}
--#}
{% macro fabric_relation(workspace, lakehouse, schema, identifier) %}
    {%- set relation = api.Relation.create(
        database=lakehouse,
        schema=schema,
        identifier=identifier,
        workspace=workspace
    ) -%}
    {{ return(relation) }}
{% endmacro %}


{#--
    Create a cross-workspace source reference.

    This macro allows you to reference tables in other workspaces as sources.

    Usage in a model:
        SELECT * FROM {{ fabric_source('analytics_workspace', 'gold_lakehouse', 'dbo', 'raw_orders') }}

    For three-part naming (same workspace, different lakehouse):
        SELECT * FROM {{ fabric_source(none, 'silver_lakehouse', 'staging', 'stg_orders') }}
--#}
{% macro fabric_source(workspace, lakehouse, schema, table) %}
    {{ fabric_ref(workspace, lakehouse, schema, table) }}
{% endmacro %}


{#--
    Join tables across workspaces.

    Example:
        SELECT
            o.order_id,
            c.customer_name
        FROM {{ fabric_ref('sales_ws', 'transactions_lh', 'dbo', 'orders') }} o
        JOIN {{ fabric_ref('analytics_ws', 'gold_lh', 'dbo', 'dim_customers') }} c
            ON o.customer_id = c.customer_id
--#}


{#--
    Get the current workspace name from credentials.
    Returns the workspace_name if configured, otherwise returns workspaceid.
--#}
{% macro current_workspace() %}
    {%- if target.workspace_name -%}
        {{ return(target.workspace_name) }}
    {%- else -%}
        {{ return(target.workspaceid) }}
    {%- endif -%}
{% endmacro %}


{#--
    Get the current lakehouse name from credentials.
--#}
{% macro current_lakehouse() %}
    {{ return(target.lakehouse or target.schema) }}
{% endmacro %}


{#--
    Check if a workspace reference is the current workspace.
--#}
{% macro is_current_workspace(workspace) %}
    {%- set current_ws_name = target.workspace_name -%}
    {%- set current_ws_id = target.workspaceid -%}
    {%- if workspace == current_ws_name or workspace == current_ws_id -%}
        {{ return(true) }}
    {%- else -%}
        {{ return(false) }}
    {%- endif -%}
{% endmacro %}
