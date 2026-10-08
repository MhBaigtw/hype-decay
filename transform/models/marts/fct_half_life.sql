-- One row per spike. Primary metric: the ATTENTION half-life, measured on
-- cumulative excess from the onset of the spike (SPEC, Task 5 revision).
--
-- Why not the old peak-hour half-life: measured against the single busiest hour,
-- a breaking-news spike "halves" within an hour even while attention stays high
-- for days. Pope_Leo_XIV (2025-05-08) had 3.86M views in the announcement hour
-- and 1.25M the next, so its peak-hour half-life was 1 hour, while its days ran
-- 7.54M, 2.97M, 933k. Liam_Payne (2024-10-16) also scored 1 hour, yet his
-- second day (3.70M) was bigger than the first (2.44M). The old figure is kept
-- as peak_hour_half_life_hours, a diagnostic only.
with spikes as (
    select * from {{ ref('int_spikes') }}
),

hours as (
    select * from {{ ref('int_spike_hours') }}
),

ranked_peak as (
    -- peak_hour: the hour with the most views within 48h of spike start
    -- (00:00 UTC of the first qualifying day); the earliest wins a tie.
    select
        h.spike_id, h.hour_start, h.views,
        row_number() over (partition by h.spike_id
                           order by h.views desc, h.hour_start asc) as rn
    from hours h
    join spikes s on s.spike_id = h.spike_id
    where h.hour_start >= cast(s.spike_start as timestamp)
      and h.hour_start <  cast(s.spike_start as timestamp) + interval '48' hour
),

peaks as (
    select
        p.spike_id,
        s.page_title,
        p.hour_start                              as peak_hour,
        p.views                                   as peak_views,
        p.views - s.baseline_hourly               as peak_excess,
        s.baseline_hourly,
        s.baseline
    from ranked_peak p
    join spikes s on s.spike_id = p.spike_id
    where p.rn = 1
),

onsets as (
    -- onset: the earliest hour within the 48 hours before the peak (the peak
    -- included) whose excess reaches 10% of the peak hour's excess.
    select p.spike_id, min(h.hour_start) as onset_hour
    from peaks p
    join hours h on h.spike_id = p.spike_id
    where h.hour_start between p.peak_hour - interval '48' hour and p.peak_hour
      and greatest(h.views - p.baseline_hourly, 0) >= 0.1 * p.peak_excess
    group by p.spike_id
),

curve as (
    -- excess(h) = views(h) - baseline_hourly, floored at 0, over the 720 hours
    -- from onset (index 0 = the onset hour). Zero-filled hours are already 0
    -- views in int_spike_hours, so they contribute nothing.
    select
        o.spike_id,
        date_diff('hour', o.onset_hour, h.hour_start)             as idx,
        greatest(h.views - p.baseline_hourly, 0)                  as excess
    from onsets o
    join peaks p on p.spike_id = o.spike_id
    join hours h on h.spike_id = o.spike_id
    where h.hour_start >= o.onset_hour
      and h.hour_start <  o.onset_hour + interval '720' hour
),

running as (
    select
        spike_id, idx, excess,
        sum(excess) over (partition by spike_id order by idx
                          rows between unbounded preceding and current row) as cum,
        sum(excess) over (partition by spike_id)                           as total
    from curve
),

attention as (
    -- attention_half_life_hours: hours from onset until the running total of
    -- excess reaches 50% of the 720-hour total, counted to the END of the hour
    -- in which it does (so all-in-the-first-hour is 1, not 0).
    -- long_tail_share: the share of that total arriving after hour 168 (from
    -- index 168 on: after the first 7 days).
    select
        spike_id,
        max(total)                                                as total_excess_720h,
        min(case when cum >= 0.5 * total then idx + 1 end)        as half_life_raw,
        sum(case when idx >= 168 then excess else 0 end)
          / nullif(max(total), 0)                                 as long_tail_raw
    from running
    group by spike_id
),

after_peak as (
    -- DIAGNOSTIC ONLY: the original peak-hour half-life (first hour after the
    -- peak at or under half the peak excess, and the next two too).
    select
        h.spike_id,
        h.hour_start,
        date_diff('hour', p.peak_hour, h.hour_start)                      as hours_after_peak,
        greatest(h.views - p.baseline_hourly, 0) <= 0.5 * p.peak_excess   as below
    from hours h
    join peaks p on p.spike_id = h.spike_id
    where h.hour_start > p.peak_hour
),

peak_hour_half_life as (
    select spike_id, min(hours_after_peak) as peak_hour_half_life_hours
    from (
        select spike_id, hours_after_peak,
               below and lead(below, 1) over w and lead(below, 2) over w as settled
        from after_peak
        window w as (partition by spike_id order by hour_start)
    ) x
    where settled and hours_after_peak <= 720
    group by spike_id
),

neighbours as (
    -- Burst test: the largest hour within 2 hours either side of the peak.
    select p.spike_id, max(h.views) as max_neighbour_views
    from peaks p
    join hours h on h.spike_id = p.spike_id
    where h.hour_start between p.peak_hour - interval '2' hour and p.peak_hour + interval '2' hour
      and h.hour_start != p.peak_hour
    group by p.spike_id
),

peak_day as (
    -- The peak's UTC day, from the daily grain (exact, not summed from hours,
    -- which miss sub-10-view hours).
    select p.spike_id, d.daily_views as peak_day_views
    from peaks p
    join {{ ref('int_page_daily') }} d
      on d.page_title = p.page_title and d.dt = cast(p.peak_hour as date)
)

select
    s.spike_id,
    s.page_title,
    s.spike_start,
    s.start_daily_views,
    s.baseline,
    s.baseline_hourly,
    s.qualifying_days,
    p.peak_hour,
    p.peak_views,
    p.peak_excess,
    o.onset_hour,
    -- window_end censoring: the 720 hours from onset run past the data, so the
    -- metric cannot be computed honestly. never_halved is retired: cumulative
    -- excess always reaches 50% of its own total.
    o.onset_hour + interval '719' hour > timestamp '{{ var("data_end_hour") }}'  as window_end,
    case when o.onset_hour + interval '719' hour <= timestamp '{{ var("data_end_hour") }}'
         then a.half_life_raw end                                 as attention_half_life_hours,
    case when o.onset_hour + interval '719' hour <= timestamp '{{ var("data_end_hour") }}'
         then a.long_tail_raw end                                 as long_tail_share,
    a.total_excess_720h,
    ph.peak_hour_half_life_hours,
    pd.peak_day_views,
    pd.peak_day_views - s.baseline                                as peak_day_excess,
    coalesce(n.max_neighbour_views, 0)                            as max_neighbour_views,
    -- burst: the peak hour is at least 10x the largest hour within 2 hours
    -- either side. Advisory, never a filter (CLAUDE.md); it replaces the
    -- flat-profile flag, which flagged the 2024 election and missed bursts.
    p.peak_views >= 10 * coalesce(n.max_neighbour_views, 0)       as burst,
    -- calendar list pages (Deaths_in_<Month>_<Year> and other month-and-year
    -- lists) fill up over their month instead of decaying.
    regexp_like(s.page_title, '^Deaths_in_')
      or regexp_like(s.page_title,
           '(January|February|March|April|May|June|July|August|September|October|November|December)_[0-9]{4}')
                                                                  as calendar_page,
    -- Leaderboard eligibility: a peak day with 20,000+ views of excess, not a
    -- burst, not a calendar page.
    (pd.peak_day_views - s.baseline) >= {{ var("leaderboard_min_peak_day_excess") }}
      and not (p.peak_views >= 10 * coalesce(n.max_neighbour_views, 0))
      and not (regexp_like(s.page_title, '^Deaths_in_')
               or regexp_like(s.page_title,
                    '(January|February|March|April|May|June|July|August|September|October|November|December)_[0-9]{4}'))
                                                                  as leaderboard_eligible
from spikes s
join peaks p on p.spike_id = s.spike_id
join onsets o on o.spike_id = s.spike_id
left join attention a on a.spike_id = s.spike_id
left join peak_hour_half_life ph on ph.spike_id = s.spike_id
left join neighbours n on n.spike_id = s.spike_id
left join peak_day pd on pd.spike_id = s.spike_id
