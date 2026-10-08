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
--
-- ONE SCAN of int_spike_hours (547M rows). Athena does not reuse a CTE that is
-- referenced twice -- each reference is a fresh scan -- and with one CTE per
-- measure this model read the table six times and tripped the dbt workgroup's
-- 25 GiB per-query cap. So every per-spike measure is a window function over
-- the single scan below, and the spike-level answer is one aggregation.

with base as (
    -- Every spine hour of every spike, with the spike's start and baseline.
    select
        h.spike_id,
        h.hour_start,
        h.views,
        s.spike_start,
        s.baseline_hourly,
        greatest(h.views - s.baseline_hourly, 0)                          as excess,
        h.hour_start >= cast(s.spike_start as timestamp)
          and h.hour_start < cast(s.spike_start as timestamp) + interval '48' hour
                                                                          as in_peak_window
    from {{ ref('int_spike_hours') }} h
    join {{ ref('int_spikes') }} s on s.spike_id = h.spike_id
),

with_peak_views as (
    -- peak_hour: the most views within 48h of spike start (00:00 UTC of the
    -- first qualifying day); the earliest such hour wins a tie.
    select *,
        max(case when in_peak_window then views end) over (partition by spike_id) as peak_views
    from base
),

with_peak as (
    select *,
        min(case when in_peak_window and views = peak_views then hour_start end)
            over (partition by spike_id)                                  as peak_hour
    from with_peak_views
),

with_onset as (
    -- onset: the earliest hour within the 48 hours before the peak (the peak
    -- included) whose excess reaches 10% of the peak hour's excess.
    select *,
        peak_views - baseline_hourly                                      as peak_excess,
        min(case when hour_start between peak_hour - interval '48' hour and peak_hour
                  and excess >= 0.1 * (peak_views - baseline_hourly)
                 then hour_start end) over (partition by spike_id)        as onset_hour,
        date_diff('hour', peak_hour, hour_start)                          as rp
    from with_peak
),

measured as (
    select *,
        date_diff('hour', onset_hour, hour_start)                         as idx,
        -- Running and total excess over the 720 hours from onset (idx 0-719).
        sum(case when hour_start >= onset_hour and hour_start < onset_hour + interval '720' hour
                 then excess else 0 end)
            over (partition by spike_id order by hour_start
                  rows between unbounded preceding and current row)       as cum,
        sum(case when hour_start >= onset_hour and hour_start < onset_hour + interval '720' hour
                 then excess else 0 end)
            over (partition by spike_id)                                  as total,
        -- Rekindled: excess per 24-hour block from onset (day 1 = idx 0-23).
        sum(case when hour_start >= onset_hour and hour_start < onset_hour + interval '720' hour
                 then excess else 0 end)
            over (partition by spike_id,
                  floor(date_diff('hour', onset_hour, hour_start) / 24.0))  as block_excess,
        -- Diagnostic: the old peak-hour half-life's 3-hour debounce.
        excess <= 0.5 * (peak_views - baseline_hourly)                    as below,
        lead(excess <= 0.5 * (peak_views - baseline_hourly), 1)
            over (partition by spike_id order by hour_start)              as below_1,
        lead(excess <= 0.5 * (peak_views - baseline_hourly), 2)
            over (partition by spike_id order by hour_start)              as below_2
    from with_onset
),

per_spike as (
    select
        spike_id,
        max(peak_hour)                                                    as peak_hour,
        max(peak_views)                                                   as peak_views,
        max(peak_excess)                                                  as peak_excess,
        max(onset_hour)                                                   as onset_hour,
        max(total)                                                        as total_excess_720h,
        -- attention_half_life_hours: to the END of the hour in which the
        -- running total first reaches half the 720-hour total.
        min(case when idx between 0 and 719 and cum >= 0.5 * total then idx + 1 end)
                                                                          as half_life_raw,
        -- long_tail_share: excess after hour 168 (from idx 168 on).
        sum(case when idx between 168 and 719 then excess else 0 end)
          / nullif(max(total), 0)                                         as long_tail_raw,
        min(case when rp between 1 and 720 and below and below_1 and below_2 then rp end)
                                                                          as peak_hour_half_life_hours,
        -- Burst, 10x rule: the largest hour within 2 hours either side.
        coalesce(max(case when rp between -2 and 2 and rp != 0 then views end), 0)
                                                                          as max_neighbour_views,
        -- Short-burst inputs (missing rows are 0 after zero-fill).
        coalesce(max(case when rp between -3 and -1 then views end), 0)   as max_views_3h_before,
        coalesce(max(case when rp = 2 then views end), 0)                 as views_2h_after,
        sum(case when rp in (0, 1) then excess else 0 end)                as excess_peak_2h,
        sum(case when idx between 0 and 23 then excess else 0 end)        as excess_24h_from_onset,
        -- Rekindled inputs: the best day among days 1-3, and among days 4-30.
        max(case when idx between 0 and 71 then block_excess end)         as peak_block_excess,
        max(case when idx between 72 and 719 then block_excess end)       as max_later_block_excess
    from measured
    group by spike_id
),

flagged as (
    select
        p.*,
        p.peak_views >= 10 * p.max_neighbour_views                        as burst_10x,
        -- Short burst (SPEC): out of silence -- the 3 hours before the peak
        -- each under 10 views -- with the peak and the next hour holding half
        -- the first day's excess, and the hour 2 after down to a tenth.
        p.max_views_3h_before < 10
          and p.excess_peak_2h >= 0.5 * p.excess_24h_from_onset
          and p.views_2h_after <= 0.1 * p.peak_views                      as short_burst,
        coalesce(p.max_later_block_excess > p.peak_block_excess, false)   as rekindled
    from per_spike p
),

peak_day as (
    -- The peak's UTC day, from the daily grain (exact: hours miss sub-10s).
    select f.spike_id, d.daily_views as peak_day_views
    from flagged f
    join {{ ref('int_spikes') }} s on s.spike_id = f.spike_id
    join {{ ref('int_page_daily') }} d
      on d.page_title = s.page_title and d.dt = cast(f.peak_hour as date)
)

select
    s.spike_id,
    s.page_title,
    s.spike_start,
    s.start_daily_views,
    s.baseline,
    s.baseline_hourly,
    s.qualifying_days,
    f.peak_hour,
    f.peak_views,
    f.peak_excess,
    f.onset_hour,
    -- window_end censoring: the 720 hours from onset run past the data.
    -- never_halved is retired: a running total always reaches half its total.
    f.onset_hour + interval '719' hour > timestamp '{{ var("data_end_hour") }}'  as window_end,
    case when f.onset_hour + interval '719' hour <= timestamp '{{ var("data_end_hour") }}'
         then f.half_life_raw end                                         as attention_half_life_hours,
    case when f.onset_hour + interval '719' hour <= timestamp '{{ var("data_end_hour") }}'
         then f.long_tail_raw end                                         as long_tail_share,
    f.total_excess_720h,
    f.peak_hour_half_life_hours,
    pd.peak_day_views,
    pd.peak_day_views - s.baseline                                        as peak_day_excess,
    f.max_neighbour_views,
    -- burst: advisory, never a filter (CLAUDE.md). Replaces flat_profile.
    f.burst_10x or f.short_burst                                          as burst,
    f.short_burst,
    -- rekindled: a later day in the window out-drew the peak day (SPEC).
    f.rekindled,
    f.peak_block_excess,
    f.max_later_block_excess,
    -- calendar list pages (Deaths_in_<Month>_<Year>, month-and-year lists)
    -- fill up over their month instead of decaying.
    regexp_like(s.page_title, '^Deaths_in_')
      or regexp_like(s.page_title,
           '(January|February|March|April|May|June|July|August|September|October|November|December)_[0-9]{4}')
                                                                          as calendar_page,
    -- Leaderboard eligibility: 20,000+ views of excess on the peak day, and
    -- not a burst, not rekindled, not a calendar page.
    coalesce(
      (pd.peak_day_views - s.baseline) >= {{ var("leaderboard_min_peak_day_excess") }}
        and not (f.burst_10x or f.short_burst)
        and not f.rekindled
        and not (regexp_like(s.page_title, '^Deaths_in_')
                 or regexp_like(s.page_title,
                      '(January|February|March|April|May|June|July|August|September|October|November|December)_[0-9]{4}')),
      false)                                                              as leaderboard_eligible
from {{ ref('int_spikes') }} s
join flagged f on f.spike_id = s.spike_id
left join peak_day pd on pd.spike_id = s.spike_id
