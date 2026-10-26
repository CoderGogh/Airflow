-- Airflow 3 메타DB 기준 (is_active -> is_stale, schedule_interval -> timetable_type)
-- 날짜는 세션 타임존과 무관하게 KST 기준으로 계산
with base_date as (
	select (now() at time zone 'Asia/Seoul')::date as today
)
, today_dagrun as (
	select 
		 dag_id
		,count(1) as run_cnt
		,count(case when state = 'success' then 'success' end) as success_cnt
		,count(case when state = 'failed' then 'failed' end) as failed_cnt
		,count(case when state = 'running' then 'running' end) as running_cnt
		,max(case when state = 'failed' then run_after end) as last_failed_date
		,max(case when state = 'success' then run_after end) as last_success_date
		,max(case when state = 'running' then run_after end) as running_date
	from dag_run, base_date b
	where (run_after at time zone 'Asia/Seoul')::date between b.today - 1 and b.today
	group by dag_id
)
, today_dag as (
	SELECT 
		 d.dag_id
		,d.next_dagrun
	FROM dag d, base_date b
	where d.is_paused = false
	and d.is_stale = false
	and d.timetable_type not in ('NullTimetable', 'AssetTriggeredTimetable', 'PartitionedAssetTimetable')
	and ((d.next_dagrun at time zone 'Asia/Seoul')::date between b.today - 1 and b.today		-- 어제나 오늘이 배치일인 대상
	or d.dag_id in (select dag_id from today_dagrun))										-- 어제나 오늘 수행된 대상
)
select 
	 d.dag_id
	,coalesce(r.run_cnt, 0) as run_cnt
	,coalesce(r.success_cnt, 0) as success_cnt
	,coalesce(r.failed_cnt, 0) as failed_cnt
	,coalesce(r.running_cnt, 0) as running_cnt
	,r.last_failed_date
	,r.last_success_date
	,r.running_date
	,d.next_dagrun
from today_dag d
left join today_dagrun  r
	on d.dag_id = r.dag_id
order by d.dag_id
