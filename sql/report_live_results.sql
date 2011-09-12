--
-- Ryan Faulkner - September 12th, 2011
-- report_live_results.sql
--
-- This query returns the results over campaigns, banners, and lps
-- This is consumed by the live results view /Fundraising_Tools/web_reporting/live_results
--

select

ecomm.utm_campaign,
ecomm.banner,
ecomm.landing_page,
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
utm_source, 
sum(counts) as impressions
from banner_impressions 
where on_minute > '%s' and on_minute < '%s' 
group by 1) as imp

right outer join

(select 
utm_campaign,
utm_source, 
landing_page,
count(*) as views

from landing_page_requests

where request_time >=  '%s' and request_time < '%s'
and (utm_campaign REGEXP '^C_' or utm_campaign REGEXP '^C11_')
group by 1,2,3) as lp

on imp.utm_source =  lp.utm_source 

join

-- This temporary table is used to compute the total views for a banner
-- from this the impressions can be normalized over several landing pages

(select 
utm_campaign,
utm_source, 
count(*) as total_views

from landing_page_requests
where request_time >=  '%s' and request_time < '%s'
and (utm_campaign REGEXP '^C_' or utm_campaign REGEXP '^C11_')
group by 1,2) as lp_tot

on lp_tot.utm_campaign = lp.utm_campaign and lp_tot.utm_source = lp.utm_source

right outer join

(select 
utm_campaign,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as banner,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page,
sum(not isnull(drupal.contribution_tracking.contribution_id)) as donations,
sum(total_amount) as amount,
sum(if(total_amount > 50, 50, total_amount)) as amount50

from
drupal.contribution_tracking LEFT JOIN civicrm.civicrm_contribution
ON (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)

where receive_date >=  '%s' and receive_date < '%s'
and (utm_campaign REGEXP '^C_' or utm_campaign REGEXP '^C11_')
group by 1,2,3) as ecomm

on ecomm.utm_campaign = lp.utm_campaign and ecomm.banner = lp.utm_source and ecomm.landing_page = lp.landing_page

group by 1,2,3
order by 1 asc;