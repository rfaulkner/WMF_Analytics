

select

civi.artifact,
bracket_name,
count(*),
sum(total_amount) as amount,
min_val,
max_val

from 

donor_brackets

join 

(
select
-- %s
SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as artifact,
total_amount

from drupal.contribution_tracking left join civicrm.civicrm_contribution on (contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
-- where receive_date > '%s' and receive_date <= '%s' and utm_campaign = '%s'
where receive_date > '20110722181600' and receive_date <= '20110722192000' and utm_campaign = 'C_KA2LP_Julytest_US'
) as civi



 on total_amount >= donor_brackets.min_val and total_amount < donor_brackets.max_val

group by 1,2;



