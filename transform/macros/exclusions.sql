{#
  SPEC exclusions: Main_Page, the namespace prefixes, and the literal title "-".
  Ingestion already dropped these (ingest_hour.py); staging applies them again so
  the models never depend on that, and a test proves none survive.
#}
{% macro is_excluded(col) -%}
  ({{ col }} = 'Main_Page'
   OR {{ col }} = '-'
   OR regexp_like({{ col }}, '^(Special|Talk|File|Category|Template|Help|Portal|Wikipedia|User):'))
{%- endmacro %}

{% macro window_filter(col='dt') -%}
  {{ col }} BETWEEN DATE '{{ var("window_start") }}' AND DATE '{{ var("window_end") }}'
{%- endmacro %}
