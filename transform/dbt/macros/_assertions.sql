{% macro assert_env() %}
  {% if execute %}
    {% set p = env_var('DUCKDB_PATH') %}
    {% if not p %}
      {% do exceptions.raise_compiler_error("DUCKDB_PATH is empty") %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro assert_source(schema, name) %}
  {# Allows CI runs to opt-out via DBT_SKIP_ASSERT_SOURCE #}
  {% if execute and env_var('DBT_SKIP_ASSERT_SOURCE', 'false') | lower != 'true' %}
    {% set rel = adapter.get_relation(database=target.database, schema=schema, identifier=name) %}
    {% if rel is none %}
      {% do exceptions.raise_compiler_error("Missing relation " ~ schema ~ "." ~ name ~ " in " ~ env_var('DUCKDB_PATH')) %}
    {% endif %}
  {% endif %}
{% endmacro %}
