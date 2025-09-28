{# macros/generate_alias_name.sql #}
{% macro generate_alias_name(custom_alias_name, node) -%}
  {%- set base = (custom_alias_name | trim) if custom_alias_name is not none else node.name -%}
  {%- set idx = base.find('_') -%}
  {%- if idx != -1 -%}
    {%- set result = base[idx + 1:] -%}
    {%- set result = result.replace('load_', '') -%}
    {%- set result = result.replace('bind_', 'view_') -%}
    {{ result }}
  {%- else -%}
    {{ base }}
  {%- endif -%}
{%- endmacro %}
