

select utm_campaign, banner, landing_page

from
(select 

utm_campaign, 
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as banner,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page,
sum(not isnull(drupal.contribution_tracking.contribution_id)) as donations

from drupal.contribution_tracking

where ts >= '%s' and ts < '%s'  and utm_campaign REGEXP '%s'

group by 1,2,3) as tmp

where donations >= 10;

