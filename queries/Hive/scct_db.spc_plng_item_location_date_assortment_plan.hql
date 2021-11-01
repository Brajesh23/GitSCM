---jobname:scct-ikb-hive-stg1-scct_db.spc_plng_item_location_date_assortment_plan
---------------------------------------------------------------------------
---Target: STAGE1 table: scct_db.spc_plng_item_location_date_assortment_plan
---Source: pkms_stage0 table : ltsmstr_lbi_reporting
---Load Type: Full
---Back posting: No
--------------------------------------------------------------------------

-- History Information
--
-- Revision    Date     JIRA       Developer-ID Comments
-- ----------- ------- ----------  ------------ ----------------------------
-- 01.00.00.01 20NOV18             SSOMA        Initial Version
-- 01.00.00.02 23SEP19 SCCTP4-2471 RYADAV       Mapr Remediation changes for column names, item and location length


-------------------------
---Job related Hive settings to merge files for Target table
-------------------------

set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

-------------------------
---loading stage1 table spc_plng_item_location_date_assortment_plan from source table ltsmstr_lbi_reporting
-------------------------

insert overwrite table  scct_db.spc_plng_item_location_date_assortment_plan partition(snapshot_dt)
select distinct
coalesce((case when trim(productid)='' then NULL else LPAD(trim(productid),18,0) END),'N/A')  as itm_nbr
,coalesce((case when trim(storenumber)='' then NULL else COALESCE(l.location_number,storenumber) END),'N/A')  as loc_nbr
,coalesce((case when trim(mpq)='' then NULL else trim(mpq ) END),0.00) as  minimum_presentation_qty
,coalesce((case when trim(startdate)='' then NULL else from_unixtime(unix_timestamp(startdate,'yyyyMMdd'),'yyyy-MM-dd') end), '9999-12-31') as effective_dt
,coalesce((case when trim(enddate)='' then NULL else from_unixtime(unix_timestamp(enddate,'yyyyMMdd'),'yyyy-MM-dd') end), '9999-12-31') as discontinue_dt
,coalesce((case when trim(poglocation)='' then NULL else trim(poglocation) END),'N/A')  as pog_location
,coalesce((case when trim(pogkey)='' then NULL else trim(pogkey) END),'N/A')  as pog_key
,coalesce((case when trim(pogname)='' then NULL else trim(pogname) END),'N/A')  as pog_name
,date_sub(current_date,1) as  snapshot_dt
from ikb_stage0.ltsmstr_lbi_reporting llr
left outer join (select location_number from  analytics_db.location where location_type_code = '04')l 
on lpad(llr.storenumber,10,0) = l.location_number
DISTRIBUTE BY snapshot_dt;




-------------------------
---end script
-------------------------