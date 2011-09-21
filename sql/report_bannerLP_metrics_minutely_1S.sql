

select

if(imp.dt_min < 10, concat(imp.dt_hr, '0', imp.dt_min,'00'), concat(imp.dt_hr, imp.dt_min,'00')) as day_hr,
concat(imp.utm_source,'-', lp.landing_page) as utm_source,
floor(impressions * (views / total_views)) as impressions, 
views,
donations,
amount,
amount50,
(views / impressions) * (total_views / views) as click_rate,
round((donations / impressions) * (total_views / views), 6) as don_per_imp,
(amount / impressions) * (total_views / views) as amt_per_imp,
(amount50 / impressions) * (total_views / views) as amt50_per_imp,
donations / views as don_per_view,
amount / views as amt_per_view,
amount50 / views as amt50_per_view,
amount / donations as avg_donation,
amount50 / donations as avg_donation50
	
from

(select 
DATE_FORMAT(on_minute,'%sY%sm%sd%sH') as dt_hr,
FLOOR(MINUTE(on_minute) / %s) * %s as dt_min,
utm_source, 
sum(counts) as impressions
from banner_impressions 
where on_minute > '%s' and on_minute < '%s' 
group by 1,2,3) as imp

join

(select 
DATE_FORMAT(ts,'%sY%sm%sd%sH') as dt_hr,
FLOOR(MINUTE(ts) / %s) * %s as dt_min,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as utm_source, 
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page, 
count(*) as views,
utm_campaign

from drupal.contribution_tracking  
where ts >= '%s' and ts < '%s' and utm_campaign = '%s'

group by 1,2,3,4) as lp

on imp.utm_source =  lp.utm_source and imp.dt_hr =  lp.dt_hr and imp.dt_min =  lp.dt_min

join 

(select 

DATE_FORMAT(ts,'%sY%sm%sd%sH') as dt_hr,
FLOOR(MINUTE(ts) / %s) * %s as dt_min,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as utm_source,
count(*) as total_views

from drupal.contribution_tracking

where ts >= '%s' and ts < '%s'
group by 1,2,3) as lp_tot

on imp.utm_source =  lp_tot.utm_source and imp.dt_hr =  lp_tot.dt_hr and imp.dt_min =  lp_tot.dt_min

left join

(select 
DATE_FORMAT(receive_date,'%sY%sm%sd%sH') as hr,
FLOOR(MINUTE(receive_date) / %s) * %s as dt_min,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as banner,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page,
count(*) as total_clicks,
sum(not isnull(drupal.contribution_tracking.contribution_id)) as donations,
sum(total_amount) as amount,
sum(if(total_amount > 50, 50, total_amount)) as amount50
from
drupal.contribution_tracking LEFT JOIN civicrm.civicrm_contribution
ON (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
where receive_date >=  '%s' and receive_date < '%s'
and utm_campaign REGEXP '%s'
group by 1,2,3,4) as ecomm

on ecomm.banner = lp.utm_source and ecomm.landing_page = lp.landing_page and ecomm.hr = lp.dt_hr and ecomm.dt_min = lp.dt_min

where lp.utm_campaign REGEXP '%s'
group by 1,2
order by 1 asc;
