-- Proves the SPEC exclusions hold in staging: no Main_Page, no "-", and no
-- title with a namespace prefix survives, in either grain. Any row is a failure.
-- Written out literally rather than through is_excluded(), so a bug in the
-- macro cannot hide itself.
select 'stg_page_daily' as model, page_title
from {{ ref('stg_page_daily') }}
where page_title = 'Main_Page'
   or page_title = '-'
   or page_title like 'Special:%' or page_title like 'Talk:%'
   or page_title like 'File:%' or page_title like 'Category:%'
   or page_title like 'Template:%' or page_title like 'Help:%'
   or page_title like 'Portal:%' or page_title like 'Wikipedia:%'
   or page_title like 'User:%'

union all

select 'stg_page_hour' as model, page_title
from {{ ref('stg_page_hour') }}
where page_title = 'Main_Page'
   or page_title = '-'
   or page_title like 'Special:%' or page_title like 'Talk:%'
   or page_title like 'File:%' or page_title like 'Category:%'
   or page_title like 'Template:%' or page_title like 'Help:%'
   or page_title like 'Portal:%' or page_title like 'Wikipedia:%'
   or page_title like 'User:%'
