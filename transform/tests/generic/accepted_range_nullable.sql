{# Accepted range, NULLs allowed (a censored spike has no half-life). Written
   here rather than taken from dbt_utils: one tiny macro is cheaper than a
   package dependency. #}
{% test accepted_range_nullable(model, column_name, min_value, max_value) %}
select * from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})
{% endtest %}
