-- One row per spike: peak, half-life, censoring, and the flat-profile flag.
-- Definitions per SPEC.
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
        p.hour_start                              as peak_hour,
        p.views                                   as peak_views,
        p.views - s.baseline_hourly               as peak_excess,
        s.baseline_hourly
    from ranked_peak p
    join spikes s on s.spike_id = p.spike_id
    where p.rn = 1
),

after_peak as (
    -- excess(h) = views(h) - baseline_hourly, floored at 0; below = at or under
    -- half the peak excess.
    select
        h.spike_id,
        h.hour_start,
        date_diff('hour', p.peak_hour, h.hour_start)                      as hours_after_peak,
        greatest(h.views - p.baseline_hourly, 0) <= 0.5 * p.peak_excess   as below
    from hours h
    join peaks p on p.spike_id = h.spike_id
    where h.hour_start > p.peak_hour
),

settled as (
    -- The 3-hour debounce: this hour and the next two all below. lead() past the
    -- end of the spine is NULL, so a run cut off by the data end does not count.
    select
        spike_id,
        hours_after_peak,
        below
          and lead(below, 1) over w
          and lead(below, 2) over w                                       as settled
    from after_peak
    window w as (partition by spike_id order by hour_start)
),

half_life as (
    select spike_id, min(hours_after_peak) as half_life_hours
    from settled
    where settled and hours_after_peak <= 720
    group by spike_id
),

shares as (
    -- Flat-profile marker (CLAUDE.md): human attention has a daily rhythm,
    -- crawlers do not. For each UTC day from the peak day through 6 days after,
    -- each hour's share of that day's views.
    select
        h.spike_id,
        hour(h.hour_start)                                                as hod,
        date(h.hour_start)                                                as d,
        h.views,
        sum(h.views) over (partition by h.spike_id, date(h.hour_start))   as day_total
    from hours h
    join peaks p on p.spike_id = h.spike_id
    where date(h.hour_start) between date(p.peak_hour) and date_add('day', 6, date(p.peak_hour))
),

diurnal as (
    -- Mean share per hour of day over the days with 240+ views (10 an hour on
    -- average), so a quiet day of zero-filled hours reads as neither rhythm nor
    -- flatness. Amplitude = (max - min) of the 24 mean shares, relative to a
    -- flat 1/24: about 0 for a crawler, well above 0.5 for a human audience.
    select spike_id,
           (max(mean_share) - min(mean_share)) * 24                       as diurnal_amplitude,
           max(days_used)                                                 as diurnal_days
    from (
        select spike_id, hod,
               avg(views / cast(day_total as double))                     as mean_share,
               count(*)                                                   as days_used
        from shares
        where day_total >= 240
        group by spike_id, hod
    ) per_hour
    group by spike_id
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
    hl.half_life_hours,
    hl.half_life_hours is null                                            as is_censored,
    -- Two censoring flags. never_halved: 30 full days after the peak were
    -- observed and it never decayed to half. window_end: the data ends before
    -- 30 days had passed, so it may yet have.
    hl.half_life_hours is null
      and p.peak_hour + interval '720' hour <= timestamp '{{ var("data_end_hour") }}' as never_halved,
    hl.half_life_hours is null
      and p.peak_hour + interval '720' hour >  timestamp '{{ var("data_end_hour") }}' as window_end,
    d.diurnal_amplitude,
    d.diurnal_days,
    -- Advisory only: annotates a spike, never removes one.
    case when d.diurnal_days >= {{ var("flat_profile_min_days") }}
         then d.diurnal_amplitude < {{ var("flat_profile_max_amplitude") }}
    end                                                                   as flat_profile
from spikes s
join peaks p on p.spike_id = s.spike_id
left join half_life hl on hl.spike_id = s.spike_id
left join diurnal d on d.spike_id = s.spike_id
