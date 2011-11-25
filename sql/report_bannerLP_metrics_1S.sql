
--
-- Ryan Faulkner - October 24th, 2011
-- report_bannerLP_metrics_1S.sql
--
-- This query returns aggregate banner/landing page combo results for one step tests 
-- This is consumed by the test view /Fundraising_Tools/web_reporting/tests
--

select

lp.utm_campaign,
lp.utm_source,
lp.landing_page,
floor(impressions * (views / total_views)) as impressions, 
views,
donations,
amount,
amount_normal,
(views / impressions) * (total_views / views) as click_rate,
round((donations / impressions) * (total_views / views), 6) as don_per_imp,
(amount / impressions) * (total_views / views) as amt_per_imp,
(amount_normal / impressions) * (total_views / views) as amt_norm_per_imp,
donations / views as don_per_view,
amount / views as amt_per_view,
amount_normal / views as amt_norm_per_view,
amount / donations as avg_donation,
amount_normal / donations as avg_donation_norm
	
from

(select 
utm_source, 
sum(counts) as impressions
from banner_impressions 
where on_minute > '%s' and on_minute < '%s' 
and country regexp '%s' 
group by 1) as imp

join

(select
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as utm_source, 
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page, 
count(*) as views,
utm_campaign

from drupal.contribution_tracking left join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id
join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id

where ts >= '%s' and ts < '%s' 
and utm_campaign REGEXP '%s'
and iso_code regexp '%s' 
group by 1,2) as lp

on imp.utm_source =  lp.utm_source

join 

(select 
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as utm_source, 
count(*) as total_views

from drupal.contribution_tracking left join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id
join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id

where ts >= '%s' and ts < '%s'
and iso_code regexp '%s' 
group by 1) as lp_tot

on imp.utm_source =  lp_tot.utm_source

right join

-- Temporary table that stores rows of donation data from civicrm and drupal tables
-- 

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
drupal.contribution_tracking join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id
join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id

where receive_date >= '%s' and receive_date <'%s' 
and utm_campaign REGEXP '%s'
and iso_code regexp '%s' 
) as all_contributions

join 

(select
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as banner,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page,
avg(total_amount) as avg_amount

from
drupal.contribution_tracking join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id
join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id

where receive_date >= '%s' and receive_date <'%s' 
and utm_campaign REGEXP '%s'
and iso_code regexp '%s' 

group by 1,2) as avg_contributions

on all_contributions.banner = avg_contributions.banner
and all_contributions.landing_page = avg_contributions.landing_page

group by 1,2,3
) as ecomm

on ecomm.banner = lp.utm_source and ecomm.landing_page = lp.landing_page

%s
group by 1,2,3 
order by 1 desc;
