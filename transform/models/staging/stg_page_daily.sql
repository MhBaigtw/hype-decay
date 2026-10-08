-- Cleaned daily grain. One row per (page, day) with daily_views >= 10 (the
-- compaction floor). SPEC exclusions re-applied; the window filter is a real
-- partition filter on dt, so even a whole-history read satisfies CLAUDE.md.
select
    page_title,
    dt,
    views as daily_views
from {{ source('curated', 'page_daily') }}
where {{ window_filter('dt') }}
  and not {{ is_excluded('page_title') }}
