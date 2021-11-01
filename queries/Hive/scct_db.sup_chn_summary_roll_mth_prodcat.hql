---jobname:scct-E2E_Dashboard-hive-stg1-scct_db.sup_chn_summary_roll_mth_prodcat
--------------------------------------------------------------------------
---Target: STAGE1 table: SCCT_DB.SUPPLY_CHAIN_SUMMARY_ROLL_MTH_PRODCAT 
---Source: SCCT_WORK_DB table : STG_SUPCHN_ROLL_MTH_PRODCAT,OPERATIONS_DB table : ENTERPRISE_CALENDAR
---Load Type: Full
---Back posting: No
--------------------------------------------------------------------------

-- History Information
--
-- Revision    Date     JIRA       Developer-ID Comments
-- ----------- ------- ----------  ------------ ----------------------------
-- 01.00.00.01 09JAN19             RYADAV       Initial Version
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
---Loading SUPPLY_CHAIN_SUMMARY_ROLL_MTH_PRODCAT target table data
-------------------------

SET tez.job.name=STAGE1:SUPPLY_CHAIN_SUMMARY_ROLL_MTH_PRODCAT :load target table SUPPLY_CHAIN_SUMMARY_ROLL_MTH_PRODCAT ;
! echo Started Loading into SCCT_DB.SUPPLY_CHAIN_SUMMARY_ROLL_MTH_PRODCAT ;


insert overwrite table scct_db.supply_chain_summary_roll_mth_prodcat partition(fiscal_wk_end_dt, supply_chain_node_type_id)
 select
 f.fiscal_month_end_date fiscal_month_end_date ,
 f.prod_cat_cd prod_cat_cd,
 f.company_cd,
 max(coalesce( f.qty,0)) qty ,
 max( coalesce(f.cost,0)) cost ,
 max( coalesce(f.speed_days,0) ) speed_days ,
 max(coalesce( ly.qty ,0)) quantity_ly ,
 max(coalesce( ly.cost,0)) cost_ly ,
 max( coalesce(ly.speed_days ,0) ) speed_days_ly,
 f.fiscal_week_end_date fiscal_week_end_date ,
 f.supply_chain_node_type_id supply_chain_node_type_id 
 from
 (select fiscal_month_end_date,prod_cat_cd,company_cd,qty,cost,speed_days,fiscal_week_end_date,supply_chain_node_type_id from SCCT_WORK_DB.stg_supchn_roll_mth_prodcat
 where  fiscal_week_end_date between '2017-01-29' and current_date) f
 full outer join
 ( select
 l.fiscal_month_end_date,
 t.prod_cat_cd,
 t.supply_chain_node_type_id,
 t.company_cd,
 t.qty,
 t.cost,
 t.speed_days
 from SCCT_WORK_DB.stg_supchn_roll_mth_prodcat t
 full outer join
 (select distinct a.fiscal_month_end_date period_end_date_key_ly,
 b.fiscal_month_end_date
 from analytics_view.enterprise_calendar a 
 join analytics_view.enterprise_calendar b
 on a.business_date = b.fiscal_ly_business_date
 where b.fiscal_month_end_date between '2017-01-29' and current_date) l
 on t.fiscal_month_end_date = l.period_end_date_key_ly )ly
 on ( f.fiscal_month_end_date = ly.fiscal_month_end_date
 and f.prod_cat_cd = ly.prod_cat_cd
 and f.supply_chain_node_type_id = ly.supply_chain_node_type_id
 and f.company_cd = ly.company_cd)
 group by f.fiscal_week_end_date ,
 f.fiscal_month_end_date ,
 f.prod_cat_cd ,
 f.supply_chain_node_type_id,
 f.company_cd;


! echo Completed Loading into SCCT_DB.SUPPLY_CHAIN_SUMMARY_ROLL_MTH_PRODCAT ;


 -------------------------
 ---end script
 -------------------------
