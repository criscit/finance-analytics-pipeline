{# macros/generate_schema_name.sql #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Always prefix custom schema with target schema from profile.
        This ensures consistent naming: prod_stg, prod_core, prod_mart, etc.

        - If custom_schema_name is set: returns {target_schema}_{custom_schema_name}
        - If custom_schema_name is not set: returns {target_schema}
    #}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
