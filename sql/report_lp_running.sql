--
-- Ryan Faulkner - August 31, 2011
-- report_lp_running.sql
--
-- This query returns the landing page, language, and country running within the time frame provided 
-- This is consumed by live landing page view /Fundraising_Tools/web_reporting/live_lps
--

select

civi_data.utm_campaign,
civi_data.country, 
civi_data.language, 
concat('http://',civi_data.language,'.wikipedia.org/wiki/Main_Page?country=',civi_data.country) as live_banners,
-- concat('http://wikimediafoundation.org/wiki/',civi_data.landing_page,'/',civi_data.language,'/',civi_data.country) as lp_link,
civi_data.landing_page as landing_page,
views, donations, amount



from

(select 
utm_campaign,
civicrm.civicrm_country.iso_code as country,	
drupal.contribution_tracking.language,
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page, 
sum(not isnull(drupal.contribution_tracking.contribution_id)) as donations, 
sum(total_amount) as amount


from 
drupal.contribution_tracking 
left join civicrm.civicrm_contribution on drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id
left join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id
left join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id

where receive_date >=  '%s' and receive_date < '%s' group by 1,2,3,4) as civi_data

left join

(select utm_campaign, landing_page, country, lang as language, count(*) as views  from faulkner.landing_page_requests where request_time >= '%s' and request_time < '%s' group by 1,2,3,4) as lp

on civi_data.landing_page = lp.landing_page and civi_data.country = lp.country and civi_data.language = lp.language and civi_data.utm_campaign = lp.utm_campaign 

where donations > %s and civi_data.utm_campaign regexp 'C_|C11_' and civi_data.landing_page != ''

order by 1,2,3,5 desc;

