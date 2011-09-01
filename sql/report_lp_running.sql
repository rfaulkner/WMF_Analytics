--
-- Ryan Faulkner - August 31, 2011
-- report_lp_running.sql
--
-- This query returns the landing page, language, and country running within the time frame provided 
--

select

lp.country, lp.language, 
concat(civi_data.language,'.wikipedia.org/wiki/Main_Page?Country=',civi_data.country) as live_banners,
lp.landing_page as landing_page,
views, donations, amount
	
from

(select 
civicrm.civicrm_country.iso_code as country,	
drupal.contribution_tracking.language,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page, 
sum(not isnull(drupal.contribution_tracking.contribution_id)) as donations, 
sum(total_amount) as amount


from 
drupal.contribution_tracking 
join civicrm.civicrm_contribution on drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id
join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id
join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id

where receive_date >=  '%s' and receive_date < '%s' group by 1,2,3) as civi_data

left join

(select landing_page, country, lang as language, count(*) as views  from faulkner.landing_page_requests where request_time >= '%s' and request_time < '%s' group by 1,2,3) as lp

on civi_data.landing_page = lp.landing_page and civi_data.country = lp.country and civi_data.language = lp.language

where views > 10
order by 5 desc;

