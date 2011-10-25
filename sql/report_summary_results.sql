--
-- Ryan Faulkner - September 12th, 2011
-- report_live_results.sql
--
-- This query returns the results over campaigns, banners, and lps
-- This is consumed by the live results view /Fundraising_Tools/web_reporting/live_results
--

select

ecomm_full.utm_campaign,
ecomm_full.banner,
ecomm_full.landing_page,
floor(impressions * (views / total_views)) as impressions, 
views,
ecomm_full.donations,
ecomm_full.amount,
ecomm_full.amount_normal,
(views / impressions) * (total_views / views) as click_rate,
round((ecomm_truncated.donations / impressions) * (total_views / views), 6) as don_per_imp,
(ecomm_truncated.amount / impressions) * (total_views / views) as amt_per_imp,
(ecomm_truncated.amount_normal / impressions) * (total_views / views) as amt_norm_per_imp,
ecomm_truncated.donations / views as don_per_view,
ecomm_truncated.amount / views as amt_per_view,
ecomm_truncated.amount_normal / views as amt_norm_per_view,
ecomm_full.amount / ecomm_full.donations as avg_donation,
ecomm_full.amount_normal / ecomm_full.donations as avg_donation_norm
	
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

-- use a right join here since we are interested in campaigns that are tallying donations
-- 

right outer join

(
select 

all_contributions.utm_campaign,
all_contributions.banner,
all_contributions.landing_page,
count(*) as donations,
sum(amount) as amount,
round(sum(if(amount > avg_amount, avg_amount, amount)),2) as amount_normal

from 

(
select
utm_campaign,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as banner,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page,
total_amount as amount

from
drupal.contribution_tracking join civicrm.civicrm_contribution
ON (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)

where receive_date >= '%s' and receive_date <'%s'
and (utm_campaign REGEXP '^C_' or utm_campaign REGEXP '^C11_')) as all_contributions

join 

(select
utm_campaign,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as banner,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page,
avg(total_amount) as avg_amount

from
drupal.contribution_tracking LEFT JOIN civicrm.civicrm_contribution
ON (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)

where receive_date >= '%s' and receive_date <'%s'
and (utm_campaign REGEXP '^C_' or utm_campaign REGEXP '^C11_')
group by 1,2,3) as avg_contributions

on all_contributions.banner = avg_contributions.banner 
and all_contributions.landing_page = avg_contributions.landing_page 
and all_contributions.utm_campaign = avg_contributions.utm_campaign

group by 1,2,3
) as ecomm_full

on ecomm_full.utm_campaign = lp.utm_campaign and ecomm_full.banner = lp.utm_source and ecomm_full.landing_page = lp.landing_page

left join

(
select 

all_contributions.utm_campaign,
all_contributions.banner,
all_contributions.landing_page,
count(*) as donations,
sum(amount) as amount,
round(sum(if(amount > avg_amount, avg_amount, amount)),2) as amount_normal

from 

(
select
utm_campaign,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as banner,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page,
total_amount as amount

from
drupal.contribution_tracking join civicrm.civicrm_contribution
ON (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)

where receive_date >= '%s' and receive_date <'%s'
and (utm_campaign REGEXP '^C_' or utm_campaign REGEXP '^C11_')) as all_contributions

join 

(select
utm_campaign,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as banner,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page,
avg(total_amount) as avg_amount

from
drupal.contribution_tracking LEFT JOIN civicrm.civicrm_contribution
ON (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)

where receive_date >= '%s' and receive_date < '%s'
and (utm_campaign REGEXP '^C_' or utm_campaign REGEXP '^C11_')
group by 1,2,3) as avg_contributions

on all_contributions.banner = avg_contributions.banner 
and all_contributions.landing_page = avg_contributions.landing_page 
and all_contributions.utm_campaign = avg_contributions.utm_campaign

group by 1,2,3
) as ecomm_truncated

on ecomm_full.utm_campaign = ecomm_truncated.utm_campaign 
and ecomm_full.banner = ecomm_truncated.banner 
and ecomm_full.landing_page = ecomm_truncated.landing_page

group by 1,2,3
order by 1 asc;