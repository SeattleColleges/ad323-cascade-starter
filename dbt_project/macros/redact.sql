{#
  WORKED EXAMPLE: a masking macro.

  A macro is a reusable piece of SQL. Call it in a model like a function:
      select {{ redact('phone_number') }} as phone_number from ...
      select {{ redact('card_number', keep_last=4) }} as card_number from ...
  and dbt pastes the SQL below in its place. Writing masking rules as macros
  means every model masks the same way, and a rule changes in one place.
#}

{% macro redact(column, keep_last=0) -%}
    {%- if keep_last > 0 -%}
        repeat('*', greatest(length({{ column }}) - {{ keep_last }}, 0)) || right({{ column }}, {{ keep_last }})
    {%- else -%}
        '[redacted]'
    {%- endif -%}
{%- endmacro %}
