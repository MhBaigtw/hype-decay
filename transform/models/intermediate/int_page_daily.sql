-- Daily views for candidate pages over a date spine, zero-filled (SPEC baseline
-- requirements). A spine day with no page_daily row is 0: the page had fewer
-- than 10 views (the floor) or none. A spine day whose SOURCE is incomplete --
-- not in complete_days, built from the manifest -- is NULL, never 0, so an
-- outage is never read as a quiet day.
with days as (
    select cast(d as date) as dt   -- sequence() over dates yields timestamp(0)
    from unnest(sequence(date '{{ var("window_start") }}',
                         date '{{ var("window_end") }}',
                         interval '1' day)) as t(d)
),

spine as (
    select c.page_title, days.dt
    from {{ ref('int_candidate_pages') }} c
    cross join days
),

observed as (
    select s.page_title, s.dt, s.daily_views
    from {{ ref('stg_page_daily') }} s
    join {{ ref('int_candidate_pages') }} c on c.page_title = s.page_title
)

select
    spine.page_title,
    spine.dt,
    case when cd.dt is null then null
         else coalesce(observed.daily_views, 0)
    end                                  as daily_views,
    observed.daily_views is null         as zero_filled,
    cd.dt is not null                    as source_complete
from spine
left join observed
    on observed.page_title = spine.page_title and observed.dt = spine.dt
left join {{ ref('complete_days') }} cd
    on cd.dt = spine.dt
