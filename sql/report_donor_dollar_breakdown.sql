-- formatted SQL query for breaking donation counts into categories based on amount donated 
-- used by DataLoader.DonorBracketsReportingLoader

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
%s
total_amount

from drupal.contribution_tracking left join civicrm.civicrm_contribution on (contribution_tracking.contribution_id = civicrm.civicrm_contribution.id)
where receive_date > '%s' and receive_date <= '%s' and utm_campaign = '%s'
) as civi



 on total_amount >= donor_brackets.min_val and total_amount < donor_brackets.max_val

group by 1,2;



