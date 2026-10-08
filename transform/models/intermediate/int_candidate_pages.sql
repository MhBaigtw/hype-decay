-- Materialized ONCE: every page with at least one day of 1,000+ views. A spike
-- day must clear 1,000 by definition (SPEC), so no other page can ever spike,
-- and every later model joins against this small table instead of the full
-- history -- the only affordable way to read page_daily (SPEC, Task 4 finding).
select
    page_title,
    max(daily_views)                     as max_daily_views,
    count(*)                             as days_over_1000,
    min(dt)                              as first_day_over_1000
from {{ ref('stg_page_daily') }}
where daily_views >= 1000
group by page_title
