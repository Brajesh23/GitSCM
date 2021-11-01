---jobname:scct-sap-hive-stg1-scct_db.str_shp_vis_rcvd_crtn_proc_tm
--------------------------------------------------------------------------
---Target: STAGE1 table: scct_db.str_shp_vis_rcvd_crtn_proc_tm
---Source: scct_work_db.str_shp_vis_rcvd_crtn_proc_tmp
---Load Type: Upsert
---Back posting: No
--------------------------------------------------------------------------

-- History Information
--
-- Revision    Date     JIRA        Developer-ID  Comments
-- ----------- -------  ----------  ------------ ----------------------------
-- 01.00.00.01 16DEC19  SCCTP4-2817 RYADAV        Initial Version
-- 01.00.00.02 20JAN20  SCCTP4-2971 RYADAV        Updated delta process logic

-------------------------
---Job related Hive settings to merge files for Target table
-------------------------

set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

-------------------------
---Loading scct_db.str_shp_vis_rcvd_crtn_proc_tm table data
-------------------------

SET tez.job.name=STAGE1_SCCT_DB:str_shp_vis_rcvd_crtn_proc_tm :load target table str_shp_vis_rcvd_crtn_proc_tm ;
! echo Started Loading into scct_db.str_shp_vis_rcvd_crtn_proc_tm ;


insert overwrite table scct_db.str_shp_vis_rcvd_crtn_proc_tm
select bol_nbr,
ext_hndlg_unt_nbr_2,
crtn_nbr,
load_nbr,
item_nbr,
proc_tm,
proc_tm_sec,
ship_unt,
crtn_cont_desc,
delv_doc_nbr,
recv_loc_nbr,
trip_desc,
ship_dt,
exptd_recpt_dt,
ack_dt
from scct_work_db.str_shp_vis_rcvd_crtn_proc_tmp;



! echo Completed Loading into scct_db.str_shp_vis_rcvd_crtn_proc_tm ;

 -------------------------
 ---end script
 -------------------------

