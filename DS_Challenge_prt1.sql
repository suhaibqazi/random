-- SQL DS Challenge
--Part 1
-----------
a)
/*
Moving average will be calculated using windows function as documented here:
https://www.compose.com/articles/metrics-maven-calculating-a-moving-average-in-postgresql/

I believe this is only available on later versions of postgresql

will use MA code to work out (by date) total number of logged in visits and total number of visits for the last week

Will then divide one by the other to work out, by device type - the % of logged in visits
*/

select 
	  a.device_devicecategory
	, a.date
	, cast(logged_in_visit_counts) as decimal)/ visit_counts as perc
from
(
	SELECT  
		  a.device_devicecategory
		, a.date
		, count(distinct case when member_id is not null then visitid end)
			OVER(ORDER BY a.device_devicecategory, a.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS logged_in_visit_counts
		, count(distinct visitid)
			OVER(ORDER BY a.device_devicecategory, a.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS visit_counts				
	FROM wa_rollup.wa_sessions_device a  
	wa_rollup.wa_sessions_memberid b  
	on a.visitorid = b.visitorid and a.visitid = b.visitid and a.date = b.date
	group by
		  a.device_devicecategory
		, a.date	
)
group by 
	  a.device_devicecategory
	, a.date




b)
/*
First step will be to work across the two schemas to work out which sessions are app visits

We want to know % of users who uploaded on app and then purchased on website. Timeframe was not mentioned (e.g. next session or within a week etc),
so over their lifetime, if they have purchased after an upload, we mark them as our target visitor and divide by all visitors
*/


create temp table session_type_tbl
as
SELECT a.visitid
	, case when b.visitid is null then 'website' else 'app' end as session_type
from (select visitid from wa_rollup.wa_sessions_memberid group by 1) a
left join (select visitid from wa_app_rollup.wa_sessions_memberid group by 1) b
on a.visitid = b.visitid


select cast(sum(case when min_upload_date < max_purchase_date then 1 else 0 end) as decimal)/ count(distinct visitorid) as perc
from
(
	select
		  visitorid
		, min(case when session_type = 'app' and events = 'upload' then cast(date as date) else null end) as min_upload_date
		, max(case when session_type = 'website' and events = 'purchase' then cast(date as date) else null end) as max_purchase_date
	from wa_rollup.wa_sessions_memberid a
	left join session_type_tbl b
	on a.visitid = b.visitid
	group by 
		  visitorid
) tbl1