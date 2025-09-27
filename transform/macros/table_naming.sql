{# macros/generate_alias_name.sql #}
{% macro generate_alias_name(custom_alias_name, node) -%}
  {%- set base = (custom_alias_name | trim) if custom_alias_name is not none else node.name -%}
  {%- set idx = base.find('_') -%}
  {%- if idx != -1 -%}
    {{ base[idx + 1:] }}
  {%- else -%}
    {{ base }}
  {%- endif -%}
{%- endmacro %}
