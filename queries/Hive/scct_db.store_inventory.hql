---jobname:scct-E2E_Dashboard-hive-stg2-scct_db.store_inventory
--------------------------------------------------------------------------
---Target: STAGE2 table: scct_db.supply_chain_node_day_loc_item
---Source: manu_stage1 table : scct_db.inventory_history, and prepack_component, SCCT_JOB_CONTROL, analytics_view table : item, location
---Load Type: Full
---Back posting: No
--------------------------------------------------------------------------

-- History Information
--
-- Revision    Date     JIRA       Developer-ID Comments
-- ----------- ------- ----------  ------------ ----------------------------
-- 01.00.00.01 19DEC18             SSOMA        Initial Version
-- 01.00.00.02 23SEP19 SCCTP4-2471 RYADAV       Mapr Remediation changes for column names, item and location length

-------------------------
---Job related Hive settings to merge files for Target table
-------------------------

-------------------------
---loading store_inventory work table
-------------------------

! echo Started Loading into scct_db.supply_chain_node_day_loc_item;

insert overwrite table scct_work_db.prepack_tmp
select p.ppck_cpnt_item_nbr,p.ppck_item_nbr ppck_item_nbr,p.ppck_cpnt_item_qty,(p.ppck_cpnt_item_qty/ccnt.ppck_cpnt_item_tot_qty)  as mltp
from scct_db.prepack_component p
left outer join (
    select ppck_item_nbr
    ,sum(ppck_cpnt_item_qty) as ppck_cpnt_item_tot_qty
    from  scct_db.prepack_component
    group by ppck_item_nbr
) ccnt
on (p.ppck_item_nbr = ccnt.ppck_item_nbr);


insert overwrite table scct_db.supply_chain_node_day_loc_item  partition(posting_dt, supply_chain_node_type_id)
select
ih.loc_nbr as loc_nbr
,coalesce(p.ppck_cpnt_item_nbr,ih.itm_nbr,'0') as itm_nbr
,l.organization_number as organization_nbr
,((ih.avail_stk - ih.intr_stk) * coalesce(p.ppck_cpnt_item_qty,1)) as qty
,((ih.avail_val - ih.intr_val)* coalesce(p.mltp,1)) as cost
,0 as speed_days
,ih.inv_bal_dt posting_dt
,9 as supply_chain_node_type_id
from scct_db.inventory_history ih
join  analytics_view.location l
on ih.loc_nbr = l.location_number
left outer join scct_work_db.prepack_tmp p
on ih.itm_nbr = p.ppck_item_nbr
join analytics_view.item im
on ih.itm_nbr = im.item_number
where ih.inv_bal_dt in (select job_control_date from scct_db.SCCT_JOB_CONTROL where run_date = current_date and driver_table ='ZMM_INV_BAL') 
and l.location_type_code in ('04')
and  (l.location_number between '0000000001'and '0000002899'
or l.location_number between '0000003000'and '0000007699'
or l.location_number between '0000007900'and '0000009999')
and im.item_type_code ='FERT';

! echo Completed Loading into scct_db.supply_chain_node_day_loc_item;

-------------------------
---end script
-------------------------
