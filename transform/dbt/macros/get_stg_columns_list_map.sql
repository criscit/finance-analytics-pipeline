{% macro get_stg_columns_list_map(bank, require=true) %}

  {% set rows = [] %}
  {% if execute %}
    {% set q %}
      select target_col, expr
      from {{ ref('transactions_column_map') }}
      where bank = {{ "'" ~ bank ~ "'" }}
      order by 1
    {% endset %}
    {% set res = run_query(q) %}
    {% if res is not none %}{% set rows = res.rows %}{% endif %}

    {# общий заголовок лога #}
    {% do log("get_stg_columns_list_map: bank=" ~ bank ~ ", rows=" ~ (rows|length), true) %}

    {% if require and (rows|length == 0) %}
      {% do exceptions.raise_compiler_error("No column map rows for bank=" ~ bank) %}
    {% endif %}
  {% endif %}

  {% if execute and rows|length > 0 %}
    {%- set pieces = [] -%}
    {# __load_key #}
    {%- do pieces.append('__load_key') -%}
    {%- for r in rows -%}
      {%- set target   = (r[0] | string) -%}
      {%- set raw_expr = (r[1] | string) -%}
      {%- if ';' in raw_expr %}
        {% do log("WARN: semicolon found in expr for " ~ target ~ ": " ~ raw_expr, true) %}
      {%- endif %}

      {%- set expr = raw_expr | replace('\r','') | trim | replace(';','') -%}

      {# построчный лог итогового выражения #}
      {% do log("MAP: " ~ target ~ " := (" ~ expr ~ ")", true) %}

      {%- do pieces.append('(' ~ expr ~ ') as ' ~ adapter.quote(target)) -%}
    {%- endfor -%}

    {# __ingested_at #}
    {%- do pieces.append('current_timestamp at time zone \'UTC\' as __ingested_at') -%}

    {{ pieces | join(',\n  ') }}
  {% else %}
    1 as __macro_placeholder
  {% endif %}
{% endmacro %}
