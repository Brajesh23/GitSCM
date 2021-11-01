--------------------------------------------------------------------------
---Target: STAGE1 table: scct_db.supply_chain_summary_node_wk_loc_cat 
---Source: scct_work_db table : supply_chain_summary_node_wk_loc_prod_ty, supply_chain_summary_node_wk_loc_prod_ly
---Load Type: Full
---Back posting: No
--------------------------------------------------------------------------

-- History Information
--
-- Revision    Date     JIRA       Developer-ID Comments
-- ----------- ------- ----------  ------------ ----------------------------
-- 01.00.00.01 15JUN19             RYADAV       Initial Version
-- 01.00.00.02 23SEP19 SCCTP4-2471 RYADAV       Mapr Remediation changes for column names, item and location length

-------------------------
---Job related Hive settings to merge files for Target table
-------------------------

set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.dynamic.partitions.pernode=20000;


-------------------------
---Loading supply_chain_summary_node_wk_loc_cat target table data
-------------------------

SET tez.job.name=scct-E2E_Dashboard-hive-stg1-scct_db.supply_chain_summary_node_wk_loc_cat ;
! echo Started Loading into SCCT_DB.supply_chain_summary_node_wk_loc_cat ;


insert overwrite table scct_db.supply_chain_summary_node_wk_loc_cat partition(fiscal_wk_end_dt,supply_chain_node_type_id)
select coalesce(ty.location_nbr,ly.location_nbr,'N/A') as loc_nbr
,coalesce(ty.category_cd,ly.category_cd,'N/A') as category_cd
,coalesce(ty.time_period_id,ly.time_period_id, 0) as time_period_id
,coalesce(ty.company_cd,ly.company_cd,'N/A') as company_cd
,coalesce(ty.quantity, 0.00) as quantity
,coalesce(ty.cost, 0.00) as cost
,coalesce(ty.speed_days, 0.00) as speed_days
,coalesce(ty.next_node_quantity, 0.00) as next_node_quantity
,coalesce(ty.days_count, 0) as days_count
,coalesce(ly.quantity, 0.00) as qty_ly
,coalesce(ly.cost, 0.00) as cost_ly
,coalesce(ly.speed_days, 0.00) as speed_days_ly
,coalesce(ly.next_node_quantity, 0.00) as next_node_qty_ly
,coalesce(ly.days_count, 0) as days_count_ly
,coalesce(ty.fiscal_wk_end_dt, ly.fiscal_wk_end_dt,'9999-12-31') as fiscal_wk_end_dt
,coalesce(ty.supply_chain_node_type_id,ly.supply_chain_node_type_id) as supply_chain_node_type_id
from
scct_work_db.supply_chain_summary_node_wk_loc_prod_ty ty 
full outer join
scct_work_db.supply_chain_summary_node_wk_loc_prod_ly ly
on ty.fiscal_wk_end_dt=ly.fiscal_wk_end_dt
and ty.location_nbr=ly.location_nbr
and ty.category_cd=ly.category_cd
and ty.supply_chain_node_type_id=ly.supply_chain_node_type_id
and ty.company_cd=ly.company_cd
and ty.time_period_id =ly.time_period_id;



! echo Completed Loading into scct_db.supply_chain_summary_node_wk_loc_cat ;

 -------------------------
 ---end script
 -------------------------
