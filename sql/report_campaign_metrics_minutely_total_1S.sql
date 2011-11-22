

select

if(lp_tot.dt_min < 10, concat(lp_tot.dt_hr, '0', lp_tot.dt_min,'00'), concat(lp_tot.dt_hr, lp_tot.dt_min,'00')) as ts,
'%s' as pipeline_name,
views,
donations
	
from

(select 
DATE_FORMAT(ts,'%sY%sm%sd%sH') as dt_hr,
FLOOR(MINUTE(ts) / %s) * %s as dt_min,
count(*) as views

from drupal.contribution_tracking left join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id
join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id

where ts >= '%s' and ts < '%s' 
and utm_campaign regexp '%s'
and iso_code regexp '%s'

group by 1,2) as lp_tot

left join

(select 

DATE_FORMAT(receive_date,'%sY%sm%sd%sH') as hr,
FLOOR(MINUTE(receive_date) / %s) * %s as dt_min,
sum(not isnull(drupal.contribution_tracking.contribution_id)) as donations

from

drupal.contribution_tracking join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id
join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id

where receive_date >=  '%s' and receive_date < '%s' 
and utm_campaign regexp '%s'
and iso_code regexp '%s'

group by 1,2) as ecomm

on ecomm.hr = lp_tot.dt_hr and ecomm.dt_min = lp_tot.dt_min

group by 1,2

order by 1 asc;
