-- Baseline per candidate page-day: the EXACT median of daily_views over the 28
-- days ending 2 days before (rows D-29 .. D-2 of the zero-filled spine; one row
-- per day, so rows are days). NULL days (incomplete source) are left out of the
-- median. Only days that could possibly qualify are kept -- daily_views >= 1000,
-- and on or after window_start + evaluation_offset_days, since the first 30 days
-- have no complete window.
with windowed as (
    select
        page_title,
        dt,
        daily_views,
        filter(
            array_agg(daily_views) over (
                partition by page_title order by dt
                rows between 29 preceding and 2 preceding),
            x -> x is not null)                                   as window_views
    from {{ ref('int_page_daily') }}
),

sorted as (
    select page_title, dt, daily_views,
           array_sort(window_views) as w,
           cardinality(window_views) as n
    from windowed
    where daily_views >= 1000
      and dt >= date_add('day', {{ var("evaluation_offset_days") }}, date '{{ var("window_start") }}')
)

select
    page_title,
    dt,
    daily_views,
    n                                                             as baseline_days,
    case
        when n = 0 then null
        when n % 2 = 1 then cast(element_at(w, (n + 1) / 2) as double)
        else (element_at(w, n / 2) + element_at(w, n / 2 + 1)) / 2.0
    end                                                           as baseline
from sorted
