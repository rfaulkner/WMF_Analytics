
--
-- Ryan Faulkner - October 24th, 2011
-- report_LP_metrics.sql
--
-- This query returns aggregate landing page results
-- This is consumed by the test view /Fundraising_Tools/web_reporting/tests
--

select

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
landing_page,
count(*) as views,
utm_campaign

from landing_page_requests

where request_time >=  '%s' and request_time < '%s'
and utm_campaign REGEXP '%s'
and country regexp '%s' 

group by 1) as lp

right join

-- Temporary table that stores rows of donation data from civicrm and drupal tables
-- 

(
select 

all_contributions.landing_page,
count(*) as donations,
sum(amount) as amount,
round(sum(if(amount > avg_amount, avg_amount, amount)),2) as amount_normal

from 

(
select
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
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page,
avg(total_amount) as avg_amount

from
drupal.contribution_tracking left join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id
join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id

where receive_date >= '%s' and receive_date <'%s' 
and utm_campaign REGEXP '%s'
and iso_code regexp '%s' 

group by 1) as avg_contributions

on all_contributions.landing_page = avg_contributions.landing_page 

group by 1
) as ecomm

on ecomm.landing_page = lp.landing_page

%s 
group by 1 order by 1 desc;
