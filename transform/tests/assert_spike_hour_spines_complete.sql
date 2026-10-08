-- Every spike's hour spine is gap-free: one row per hour from 48h before the
-- spike start to 770h after, or to the last hour of data. A gap would make the
-- 3-hour debounce look across a hole. Any row returned is a failure.
with expected as (
    select
        spike_id,
        date_diff('hour',
                  cast(spike_start as timestamp) - interval '48' hour,
                  least(cast(spike_start as timestamp) + interval '770' hour,
                        timestamp '{{ var("data_end_hour") }}')) + 1  as hours
    from {{ ref('int_spikes') }}
),

actual as (
    select spike_id, count(*) as hours
    from {{ ref('int_spike_hours') }}
    group by spike_id
)

select e.spike_id, e.hours as expected_hours, a.hours as actual_hours
from expected e
left join actual a on a.spike_id = e.spike_id
where a.hours is null or a.hours != e.hours
