
select

lp.landing_page,
floor(impressions * (views / total_views)) as impressions,
views,
donations,
amount,
amount50,
donations / views as don_per_view,
amount / views as amt_per_view,
amount50 / views as amt50_per_view,
amount / donations as avg_donation,
amount50 / donations as avg_donation50,
views / floor(impressions * (views / total_views)) as click_rate 

from

(select 
utm_source, 
sum(counts) as impressions
from banner_impressions 
where on_minute > '%s' and on_minute < '%s' 
group by 1) as imp

join

(select 
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as utm_source, 
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page, 
count(*) as views,
utm_campaign
from drupal.contribution_tracking  
where ts >= '%s' and ts < '%s' and utm_campaign = '%s'
group by 1,2) as lp

on imp.utm_source =  lp.utm_source

join 

(select 
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as utm_source, 
count(*) as total_views

from drupal.contribution_tracking

where ts >= '%s' and ts < '%s'
group by 1) as lp_tot

on imp.utm_source = lp_tot.utm_source 

left join

(select 
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page,
sum(not isnull(drupal.contribution_tracking.contribution_id)) as donations,
sum(total_amount) as amount,
sum(if(total_amount > 50, 50, total_amount)) as amount50
from
drupal.contribution_tracking LEFT JOIN civicrm.civicrm_contribution
ON (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
where receive_date >=  '%s' and receive_date < '%s'
and utm_campaign REGEXP '%s'
group by 1) as ecomm

on ecomm.landing_page  = lp.landing_page

where lp.utm_campaign REGEXP '%s'
group by 1
order by 1 desc;
