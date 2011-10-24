
--
-- Ryan Faulkner - October 24th, 2011
-- report_banner_metrics.sql
--
-- This query returns aggregate banner results 
-- This is consumed by the test view /Fundraising_Tools/web_reporting/tests
--


select

imp.utm_source,
floor(impressions * (views / total_views)) as impressions, 
views,
donations,
amount,
amount_normal,
(views / impressions) * (total_views / views) as click_rate,
round((donations / impressions) * (total_views / views), 6) as don_per_imp,
(amount / impressions) * (total_views / views) as amt_per_imp,
(amount_normal / impressions) * (total_views / views) as amt_norm_per_imp,
amount / donations as avg_donation,
amount_normal / donations as avg_donation_norm
	
from

(select 
utm_source, 
sum(counts) as impressions
from banner_impressions 
where on_minute > '%s' and on_minute < '%s' 
group by 1) as imp

join

(select 
utm_source, 
count(*) as views,
utm_campaign
from landing_page_requests
where request_time >=  '%s' and request_time < '%s'
and utm_campaign = '%s'
group by 1) as lp

on imp.utm_source =  lp.utm_source

join 

(select 
utm_source, 
count(*) as total_views
from landing_page_requests
where request_time >= '%s' and request_time < '%s'
group by 1) as lp_tot

on imp.utm_source =  lp_tot.utm_source 

left join

-- Temporary table that stores rows of donation data from civicrm and drupal tables
-- 

(
select 

all_contributions.banner,
count(*) as donations,
sum(amount) as amount,
round(sum(if(amount > avg_amount, avg_amount, amount)),2) as amount_normal

from 

(
select
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as banner,
total_amount as amount

from
drupal.contribution_tracking join civicrm.civicrm_contribution
ON (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)

where receive_date >= '%s' and receive_date <'%s' and utm_campaign = '%s'
) as all_contributions

join 

(select
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as banner,
avg(total_amount) as avg_amount

from
drupal.contribution_tracking left join civicrm.civicrm_contribution
ON (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)

where receive_date >= '%s' and receive_date <'%s' and utm_campaign = '%s'
group by 1) as avg_contributions

on all_contributions.banner = avg_contributions.banner

group by 1
) as ecomm

on ecomm.banner = lp.utm_source

where lp.utm_campaign = '%s' and lp.views > %s
group by 1
order by 1 desc;
