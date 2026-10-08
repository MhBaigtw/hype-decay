-- Cleaned hourly grain. One row per (page, hour) with views >= 10. hour_start is
-- the START of the capture window (filename hour minus one, applied at ingest).
select
    page_title,
    hour_start,
    dt,
    views
from {{ source('curated', 'page_hour') }}
where {{ window_filter('dt') }}
  and not {{ is_excluded('page_title') }}
