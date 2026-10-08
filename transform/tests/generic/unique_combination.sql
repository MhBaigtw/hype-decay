{% test unique_combination(model, combination) %}
select {{ combination | join(', ') }}, count(*) as n
from {{ model }}
group by {{ combination | join(', ') }}
having count(*) > 1
{% endtest %}
