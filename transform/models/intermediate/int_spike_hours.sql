-- Hourly views around each spike over an hour spine, zero-filled.
--
-- page_hour stores only hours with 10+ views, and all 17,520 source hours are
-- present (Task 3), so a missing (page, hour) means FEWER THAN 10 VIEWS, not
-- missing data: it is filled with 0 (the true value is 0 to 9). The spine runs
-- from 48h before the spike start to 770h after: the peak lies within 48h of
-- the start, so this covers 48h before the peak to 30 days (720h) after it,
-- plus the 2 hours the 3-hour debounce needs. Capped at the last hour of data.
with windows as (
    select
        spike_id,
        page_title,
        cast(spike_start as timestamp) - interval '48' hour       as w_start,
        least(cast(spike_start as timestamp) + interval '770' hour,
              timestamp '{{ var("data_end_hour") }}')              as w_end
    from {{ ref('int_spikes') }}
),

spine as (
    select w.spike_id, w.page_title, cast(h as timestamp(3)) as hour_start
    from windows w
    cross join unnest(sequence(w.w_start, w.w_end, interval '1' hour)) as t(h)
),

observed as (
    -- Range join against the small windows table, so only in-window rows of
    -- page_hour survive the scan.
    select w.spike_id, p.hour_start, p.views
    from {{ ref('stg_page_hour') }} p
    join windows w
      on w.page_title = p.page_title
     and p.hour_start between w.w_start and w.w_end
)

select
    spine.spike_id,
    spine.page_title,
    spine.hour_start,
    coalesce(o.views, 0)                     as views,
    o.views is null                          as zero_filled
from spine
left join observed o
    on o.spike_id = spine.spike_id and o.hour_start = spine.hour_start
