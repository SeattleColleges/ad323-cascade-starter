{#
  By default dbt glues schema names together ("main_gold"). This override
  uses the folder's schema name as-is, so tables land in `staging` and `gold`.
  Provided infrastructure: students don't need to change this.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
