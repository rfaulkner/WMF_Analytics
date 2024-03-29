
--
-- Ryan Faulkner - October 24th, 2011
-- report_LP_metrics_minutely_1S.sql
--
-- This query returns minute by minute landing page results for one step landing pages
-- This is consumed by the test view /Fundraising_Tools/web_reporting/tests
--

select

if(lp.dt_min < 10, concat(lp.dt_hr, '0', lp.dt_min,'00'), concat(lp.dt_hr, lp.dt_min,'00')) as day_hr,
lp.landing_page,
views,
donations,
amount,
amount_normal,
donations / views as don_per_view,
amount / views as amt_per_view,
amount_normal / views as amt_norm_per_view,
amount / donations as avg_donation,
amount_normal / donations as avg_donation_norm

from

(select 
DATE_FORMAT(ts,'%sY%sm%sd%sH') as dt_hr,
FLOOR(MINUTE(ts) / %s) * %s as dt_min,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page, 
count(*) as views,
utm_campaign

from drupal.contribution_tracking on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id
join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id

where ts >= '%s' and ts < '%s' 
and utm_campaign = '%s'
and iso_code regexp '%s'

group by 1,2,3) as lp

left join

-- Temporary table that stores rows of donation data from civicrm and drupal tables
-- 

(
select 

DATE_FORMAT(receive_date,'%sY%sm%sd%sH') as hr,
FLOOR(MINUTE(receive_date) / %s) * %s as dt_min,
all_contributions.landing_page,
count(*) as donations,
sum(amount) as amount,
round(sum(if(amount > avg_amount, avg_amount, amount)),2) as amount_normal

from 

(
select
receive_date,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page,
total_amount as amount

from
drupal.contribution_tracking join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id
join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id

where receive_date >= '%s' and receive_date <'%s' 
and utm_campaign = '%s'
and iso_code regexp '%s'
) as all_contributions

join 

(select
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page,
avg(total_amount) as avg_amount

from
drupal.contribution_tracking left join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id
join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id

where receive_date >= '%s' and receive_date <'%s' 
and utm_campaign = '%s'
and iso_code regexp '%s'
group by 1) as avg_contributions

on all_contributions.landing_page = avg_contributions.landing_page 

group by 1,2,3
) as ecomm

on ecomm.landing_page = lp.landing_page and ecomm.hr = lp.dt_hr and ecomm.dt_min = lp.dt_min

where lp.utm_campaign = '%s' and views > 10
group by 1,2
order by 1 asc;
