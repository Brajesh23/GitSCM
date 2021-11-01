---jobname:scct-tms-hive-stg1-scct_db.tms_location
--------------------------------------------------------------------------
---target: stage1 table: scct_db.tms_location
---source: tms_stage0 table : ca2015_location
---load type: full
---back posting: no
--
-- History Information
--
-- Revision    Date     	JIRA       Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 06/09/2019	SCCTP4-2253	mgohain			Initial Version
--------------------------------------------------------------------------

-------------------------
---job related hive settings to read data from hbase
-------------------------

set hive.auto.convert.join=false;

-------------------------
---loading stage0 data into hbase staging table
-------------------------

set tez.job.name=staging:maprdb_location_raw :load target from ca2015_location;

! echo started loading into scct_raw_db.maprdb_location_raw;

insert into table scct_raw_db.maprdb_location_raw
select
concat(location_id,"|",to_utc_timestamp(last_updated_dttm, "EST5EDT")) as key
,location_id
,ilm_object_type
,tc_company_id
,location_objid_pk1
,location_objid_pk2
,location_objid_pk3
,location_objid_pk4
,current_quantity
,sequence_no
,is_location_defined_in_svg
,mark_for_deletion
,created_dttm
,last_updated_dttm
from tms_stage0.ca2015_location;

-------------------------
---loading staging data into stage1 tms_location table
-------------------------

set tez.job.name=stage1:tms_location :load target table from maprdb_location_raw ;

! echo started loading into scct_db.tms_location;

insert overwrite table scct_db.tms_location
select
coalesce((case when trim(location_id)='' then NULL else trim(location_id) END),0) as location_id
,coalesce((case when trim(ilm_object_type)='' then NULL else trim(ilm_object_type) END),0) as ilm_object_type
,coalesce((case when trim(tc_company_id)='' then NULL else trim(tc_company_id) END),0) as tc_company_id
,coalesce((case when trim(location_objid_pk1)='' then NULL else trim(location_objid_pk1) END),'N/A') as location_objid_pk1
,coalesce((case when trim(location_objid_pk2)='' then NULL else trim(location_objid_pk2) END),'N/A') as location_objid_pk2
,coalesce((case when trim(location_objid_pk3)='' then NULL else trim(location_objid_pk3) END),'N/A') as location_objid_pk3
,coalesce((case when trim(location_objid_pk4)='' then NULL else trim(location_objid_pk4) END),'N/A') as location_objid_pk4
,coalesce((case when trim(current_quantity)='' then NULL else trim(current_quantity) END),0) as current_quantity
,coalesce((case when trim(sequence_no)='' then NULL else trim(sequence_no) END),0) as sequence_no
,coalesce((case when trim(is_location_defined_in_svg)='' then NULL else trim(is_location_defined_in_svg) END),0) as is_location_defined_in_svg
,coalesce((case when trim(mark_for_deletion)='' then NULL else trim(mark_for_deletion) END),0) as mark_for_deletion
,coalesce(to_utc_timestamp(created_dttm, "EST5EDT"), '9999-12-31 00:00:00') as created_dttm
,coalesce(to_utc_timestamp(last_updated_dttm, "EST5EDT"), '9999-12-31 00:00:00') as last_updated_dttm
,coalesce(to_date(to_utc_timestamp(last_updated_dttm, "EST5EDT")), '9999-12-31') as strt_dt
,lag(date_sub(to_utc_timestamp(last_updated_dttm, "EST5EDT"),1),1,'9999-12-31') over(partition by location_id order by key desc) as end_dt
from scct_raw_db.maprdb_location_raw;

! echo completed loading into scct_db.tms_location ;

--------------------------
---cocatenate files for target table tms_location
-------------------------

SET tez.job.name=STAGE1:tms_location:cocatenate files on tms_location;

! echo Started cocatenate files;

alter table scct_db.tms_location concatenate;

! echo Completed cocatenate files;

-------------------------
---end script
-------------------------