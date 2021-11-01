---jobname:scct-tms-hive-stg1-scct_db.tms_dock_hours 
--------------------------------------------------------------------------
---target: stage1 table: scct_db.tms_dock_hours 
---source: tms_stage0 table : ca2015_dock_hours
---load type: full
---back posting: no
--
-- History Information
--
-- Revision    Date     	JIRA       Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 31/07/2019	SCCTP4-2253	pnaayan			Initial Version
--------------------------------------------------------------------------

-------------------------
---job related hive settings to read data from hbase
-------------------------

set hive.auto.convert.join=false;

-------------------------
---loading stage0 data into hbase staging table
-------------------------

set tez.job.name=staging:maprdb_dock_hours_raw :load target from ca2015_dock_hours ;

! echo started loading into scct_raw_db.maprdb_dock_hours_raw ;

insert into table scct_raw_db.maprdb_dock_hours_raw
select
concat(facility_id,"|",dock_id,"|",dock_hours_id,"|",to_utc_timestamp(last_updated_dttm, "EST5EDT")) as key
,facility_id
,dock_id
,dock_hours_id
,start_dayofweek
,start_time
,end_dayofweek
,end_time
,dock_action_type
,allowed_num_appts
,enforce_end_time
,use_for_tf
,use_for_ds
,tc_company_id
,priority
,wave_id
,is_next_day_wave
,mark_for_deletion
,protection_level_id
,size_uom_id
,minimum_capacity
,created_dttm
,last_updated_dttm
from tms_stage0.ca2015_dock_hours;

-------------------------
---loading staging data into stage1 tms_dock_hours table
-------------------------

set tez.job.name=stage1:tms_dock_hours :load target table from maprdb_dock_hours_raw ;

! echo started loading into scct_db.tms_dock_hours ;

insert overwrite table scct_db.tms_dock_hours
select
coalesce((case when trim(facility_id)='' then NULL else trim(facility_id) END),0) as facility_id
,coalesce((case when trim(dock_id)='' then NULL else trim(dock_id) END),'N/A')   as dock_id
,coalesce((case when trim(dock_hours_id)='' then NULL else trim(dock_hours_id) END),0) as dock_hours_id
,coalesce((case when trim(start_dayofweek)='' then NULL else trim(start_dayofweek) END),0) as start_dayofweek
,substr(coalesce(to_utc_timestamp(start_time, "EST5EDT"), '00:00:00'),12) as start_time
,coalesce((case when trim(end_dayofweek)='' then NULL else trim(end_dayofweek) END),0) as end_dayofweek
,substr(coalesce(to_utc_timestamp(end_time, "EST5EDT"), '00:00:00'),12) as end_time
,coalesce((case when trim(dock_action_type)='' then NULL else trim(dock_action_type) END),'N/A')   as dock_action_type
,coalesce((case when trim(allowed_num_appts)='' then NULL else trim(allowed_num_appts) END),0.0000) as allowed_num_appts
,coalesce((case when trim(enforce_end_time)='' then NULL else trim(enforce_end_time) END),'N/A')   as enforce_end_time
,coalesce((case when trim(use_for_tf)='' then NULL else trim(use_for_tf ) END),0) as use_for_tf
,coalesce((case when trim(use_for_ds)='' then NULL else trim(use_for_ds ) END),0) as use_for_ds
,coalesce((case when trim(tc_company_id)='' then NULL else trim(tc_company_id) END),0) as tc_company_id
,coalesce((case when trim(priority)='' then NULL else trim(priority) END),0) as priority
,coalesce((case when trim(wave_id)='' then NULL else trim(wave_id) END),0) as wave_id
,coalesce((case when trim(is_next_day_wave)='' then NULL else trim(is_next_day_wave) END),0) as is_next_day_wave
,coalesce((case when trim(mark_for_deletion)='' then NULL else trim(mark_for_deletion) END),0) as mark_for_deletion
,coalesce((case when trim(protection_level_id)='' then NULL else trim(protection_level_id) END),0) as protection_level_id
,coalesce((case when trim(size_uom_id)='' then NULL else trim(size_uom_id) END),0) as size_uom_id
,coalesce((case when trim(minimum_capacity)='' then NULL else trim(minimum_capacity) END),0.0000) as  minimum_capacity
,coalesce(to_utc_timestamp(created_dttm, "EST5EDT"), '9999-12-31 00:00:00') as created_dttm
,coalesce(to_utc_timestamp(last_updated_dttm, "EST5EDT"), '9999-12-31 00:00:00') as last_updated_dttm
,coalesce(to_date(to_utc_timestamp(last_updated_dttm, "EST5EDT")), '9999-12-31') as strt_dt
,lag(date_sub(to_utc_timestamp(last_updated_dttm, "EST5EDT"),1),1,'9999-12-31') over(partition by facility_id, dock_id, dock_hours_id order by key desc) as end_dt
from scct_raw_db.maprdb_dock_hours_raw;

! echo completed loading into scct_db.tms_dock_hours ;

--------------------------
---cocatenate files for target table tms_dock_hours
-------------------------

SET tez.job.name=STAGE1:tms_dock_hours:cocatenate files on tms_dock_hours;

! echo Started cocatenate files;

alter table scct_db.tms_dock_hours concatenate;

! echo Completed cocatenate files;

-------------------------
---end script
-------------------------
