
--
-- Ryan Faulkner - November 17th, 2011
-- report_country_impressions.sql
--
-- This query returns aggregate banner/landing page combo results 
-- This is consumed by the test view /Fundraising_Tools/web_reporting/tests
--

select

lp.utm_campaign,
bi.utm_source,
lp.landing_page,
bi.country as Country,
if( views > 0, floor(impressions * (views / total_views)), impressions) as impressions, 
views,
(views / impressions) * (total_views / views) as click_rate
	
from

(select 
utm_source, 
country,
sum(counts) as impressions
from banner_impressions 
where on_minute > '%s' and on_minute < '%s' 
group by 1,2) as bi

left join

(select 
utm_campaign,
utm_source, 
landing_page,
country,
count(*) as views

from landing_page_requests
where request_time >=  '%s' and request_time < '%s'
group by 1,2,3,4) as lp

on bi.utm_source =  lp.utm_source and bi.country =  lp.country

left join 

(select 
utm_source, 
country,
count(*) as total_views

from landing_page_requests

where request_time >= '%s' and request_time < '%s'
group by 1,2) as lp_tot

on bi.utm_source = lp_tot.utm_source and bi.country =  lp_tot.country

%s
order by 4,5 desc
