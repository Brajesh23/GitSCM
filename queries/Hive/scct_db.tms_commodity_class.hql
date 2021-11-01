---jobname:scct-tms-hive-stg1-scct_db.tms_commodity_class
--------------------------------------------------------------------------
---target: stage1 table: scct_db.tms_commodity_class 
---source: tms_stage0 table : ca2015_commodity_class
---load type: full
---back posting: no
--
-- History Information
--
-- Revision    Date     	JIRA       	Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 31JULY19		SCCTP4-2253	ssoma			Initial Version
--------------------------------------------------------------------------

-------------------------
---job related hive settings to read data from hbase
-------------------------

set hive.auto.convert.join=false;

-------------------------
---loading stage0 data into hbase staging table
-------------------------

set tez.job.name=staging:maprdb_commodity_class_raw :load target from ca2015_commodity_class ;

! echo started loading into scct_raw_db.maprdb_commodity_class_raw ;

insert into table scct_raw_db.maprdb_commodity_class_raw
select
concat(commodity_class,"|",to_utc_timestamp(last_updated_dttm, "EST5EDT")) as key
,commodity_class
,description
,created_dttm
,last_updated_dttm
from tms_stage0.ca2015_commodity_class;

-------------------------
---loading staging data into stage1 tms_commodity_class table
-------------------------

set tez.job.name=stage1:tms_commodity_class :load target table from maprdb_commodity_class_raw ;

! echo started loading into scct_db.tms_commodity_class ;

insert overwrite table scct_db.tms_commodity_class
select
commodity_class
,description
,to_utc_timestamp(created_dttm, "EST5EDT") created_dttm
,to_utc_timestamp(last_updated_dttm, "EST5EDT") last_updated_dttm
,to_date(to_utc_timestamp(last_updated_dttm, "EST5EDT")) as strt_dt
,lag(date_sub(to_utc_timestamp(last_updated_dttm, "EST5EDT"),1),1,'9999-12-31') over(partition by commodity_class order by key desc) as end_dt
from scct_raw_db.maprdb_commodity_class_raw;

! echo completed loading into scct_db.tms_commodity_class ;

--------------------------
---cocatenate files for target table tms_commodity_class
-------------------------

SET tez.job.name=STAGE1:tms_commodity_class:cocatenate files on tms_commodity_class;

! echo Started cocatenate files;

alter table scct_db.tms_commodity_class concatenate;

! echo Completed cocatenate files;

-------------------------
---end script
-------------------------