
--
-- Ryan Faulkner - December 5th, 2011
-- report_total_donations.sql
--
-- This query returns total campaign results from civi
-- This is consumed by the test view /Fundraising_Tools/web_reporting/tests
--

select

'Totals' as campaign,
sum(donations) as donations,
sum(amount) as amount,
sum(amount_normal) as amount_normal
	
from

(

%s

) as civi_data;
