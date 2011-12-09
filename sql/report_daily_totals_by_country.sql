
--
-- Ryan Faulkner - December 8th, 2011
-- report_daily_totals_by_country.sql
--
-- This query returns aggregate donation results for countries by day
--


select 

DATE_FORMAT(receive_date,'%sY-%sm-%sd') as dt_day,
iso_code as country,
count(*) as donations,
sum(total_amount) as amount

from

civicrm.civicrm_contribution 
left join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id
left join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id

where receive_date >= '%s' and receive_date < '%s' and iso_code regexp '%s' 

group by 1,2
having count(*) >= %s
%s
 