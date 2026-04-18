{#-- Verification macros for defer testing.
     These check table state to prove defer worked correctly.

     NOTE: compiled SQL verification is done via shell (grep) in the test
     script, not here — load_file_contents is not available in run-operation. --#}


{% macro verify_table_row_count(lakehouse, schema_name, table_name, min_rows) %}
    {#-- Verify a table exists and has at least min_rows.
         Usage:
           dbt run-operation verify_table_row_count \
             --args '{lakehouse: kuruma_dev_lake, schema_name: dbo, table_name: defer_agg, min_rows: 1}'
    --#}
    {% set min_rows_int = min_rows | int %}

    {% call statement('count_rows', fetch_result=True) %}
        SELECT count(*) as cnt FROM {{ lakehouse }}.{{ schema_name }}.{{ table_name }}
    {% endcall %}

    {% set row_count = load_result('count_rows')['data'][0][0] | int %}

    {% if row_count >= min_rows_int %}
        {{ log("VERIFY PASS: " ~ lakehouse ~ "." ~ schema_name ~ "." ~ table_name ~ " has " ~ row_count ~ " rows (min: " ~ min_rows_int ~ ")", info=True) }}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "VERIFY FAIL: " ~ lakehouse ~ "." ~ schema_name ~ "." ~ table_name ~
            " has " ~ row_count ~ " rows, expected at least " ~ min_rows_int
        ) %}
    {% endif %}
{% endmacro %}


{% macro verify_table_not_exists(lakehouse, schema_name, table_name) %}
    {#-- Verify a table/view does NOT exist.
         Usage:
           dbt run-operation verify_table_not_exists \
             --args '{lakehouse: kuruma_dev_lake, schema_name: dbo, table_name: defer_customers}'
    --#}
    {% set rel = adapter.get_relation(database=lakehouse, schema=schema_name, identifier=table_name) %}

    {% if rel is none %}
        {{ log("VERIFY PASS: " ~ lakehouse ~ "." ~ schema_name ~ "." ~ table_name ~ " does not exist (expected)", info=True) }}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "VERIFY FAIL: " ~ lakehouse ~ "." ~ schema_name ~ "." ~ table_name ~
            " exists but should NOT"
        ) %}
    {% endif %}
{% endmacro %}


{% macro verify_table_exists(lakehouse, schema_name, table_name) %}
    {#-- Verify a table/view EXISTS.
         Usage:
           dbt run-operation verify_table_exists \
             --args '{lakehouse: kuruma_dev_lake, schema_name: dbo, table_name: defer_agg}'
    --#}
    {% set rel = adapter.get_relation(database=lakehouse, schema=schema_name, identifier=table_name) %}

    {% if rel is not none %}
        {{ log("VERIFY PASS: " ~ lakehouse ~ "." ~ schema_name ~ "." ~ table_name ~ " exists", info=True) }}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "VERIFY FAIL: " ~ lakehouse ~ "." ~ schema_name ~ "." ~ table_name ~
            " does not exist but should"
        ) %}
    {% endif %}
{% endmacro %}
