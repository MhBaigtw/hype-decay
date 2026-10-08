-- Spike detection (SPEC). A day qualifies when, against its own baseline:
--   daily_views >= 5 * baseline, >= baseline + 500, and >= 1000.
-- 30-day rule: a page opens at most one spike per 30-day window. The window is
-- anchored at the spike START: a page that qualifies again fewer than 30 days
-- after the start extends that spike; 30 or more days after, it opens a new
-- one. Computed exactly with reduce() over each page's sorted qualifying days,
-- because anchoring at the start is not expressible as a fixed gap between rows.
with qualifying as (
    select page_title, dt, daily_views, baseline
    from {{ ref('int_baselines') }}
    where baseline is not null
      and daily_views >= 5 * baseline
      and daily_views >= baseline + 500
      and daily_views >= 1000
),

starts as (
    select
        page_title,
        reduce(
            array_sort(array_agg(dt)),
            cast(array[] as array(date)),
            (s, d) -> if(cardinality(s) = 0 or date_diff('day', element_at(s, -1), d) >= 30,
                         s || d, s),
            s -> s)                                              as spike_starts
    from qualifying
    group by page_title
),

spikes as (
    select page_title, spike_start
    from starts
    cross join unnest(spike_starts) as t(spike_start)
)

select
    s.page_title || '|' || cast(s.spike_start as varchar)        as spike_id,
    s.page_title,
    s.spike_start,
    q0.daily_views                                               as start_daily_views,
    q0.baseline,
    q0.baseline / 24.0                                           as baseline_hourly,
    count(q.dt)                                                  as qualifying_days,
    max(q.dt)                                                    as last_qualifying_day
from spikes s
join qualifying q0
    on q0.page_title = s.page_title and q0.dt = s.spike_start
join qualifying q
    on q.page_title = s.page_title
   and q.dt between s.spike_start and date_add('day', 29, s.spike_start)
group by 1, 2, 3, 4, 5, 6
