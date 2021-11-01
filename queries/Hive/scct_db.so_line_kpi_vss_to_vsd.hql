---jobname:scct-ufposo-hive-stg1-so_line_kpi_vss_to_vsd
--------------------------------------------------------------------------
---target: stage1 table: scct_db.so_line_kpi 
---source: tms_stage0 table : delivery_master, handling_master and item_unit_measure_master
---load type: full
---back posting: no
--
-- History Information
--
-- Revision    Date     	JIRA       	Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 07JAN20		SCCTP4-2924	ssoma			Initial Version
-- 01.00.00.02 06MAR20		SCCTP4-3316	ssoma			remove reducer parameter
-- 01.00.00.03 04MAY20		SCCTP4-3331	ssoma			HQL Optimization 
--------------------------------------------------------------------------

-------------------------
---job related hive settings 
-------------------------

  set hive.auto.convert.join.noconditionaltask.size=200000000;
  set mapreduce.job.reduces=200;

-------------------------
---loading dd line kpi's data into so_line_kpi table
-------------------------

set tez.job.name=load dd line KPIs:so_line_kpi :load target from delivery_master ;

! echo started loading into scct_db.so_line_kpi;

insert overwrite table scct_work_db.so_line_kpi_work partition(business_process,to_node,po_doc_typ)
select
'DC' as spply_chn_nd
,'DC_VSS' as from_node
,so.sls_ord_nbr
,so.sls_ord_itm_nbr
,so.itm_nbr
,coalesce(min(case when im.movmt_type in(601,901) then im.posting_dt end),'9999-12-31') as so_frst_shp_dt
,coalesce(max(case when im.movmt_type in(601,901) then im.posting_dt end),'9999-12-31') as so_lst_shp_dt
,coalesce(sum(case when im.movmt_type in(601,901) then im.trx_qty when im.movmt_type in(602,902) then im.trx_qty*-1 else 0 end),0) as so_tot_shp_units
,'Stock_Transfer_Intra_Company' as business_process
,'DC_VSD' as to_node
,po.purch_doc_type_cd as po_doc_typ
from (select * from scct_work_db.so_master_work where cust_prch_nbr<>'N/A' and recv_loc_nbr<>'0' and ship_to_loc_nbr<>'0') so 
inner join (select * from scct_db.location_master where org_num='1108') a on so.recv_loc_nbr=a.loc_nbr  
inner join (select * from scct_work_db.po_master_work WHERE  purch_doc_type_cd='ZBPO' AND (del_ind<>'L' OR (del_ind='L' AND gr_qty>0))) po 
on so.cust_prch_nbr=po.purch_ord_nbr and so.cust_prch_itm_nbr=po.purch_ord_itm_nbr and so.itm_nbr=po.itm_nbr
inner join (select * from scct_work_db.delivery_master_work where del_ind<>'D' and del_type not in('EL','ELST') and itm_nbr<>'0') dm 
on so.sls_ord_nbr=dm.ref_doc_nbr and so.sls_ord_itm_nbr= dm.ref_doc_itm_nbr and po.itm_nbr=dm.itm_nbr 
inner join (select * from scct_db.location_master where loc_typ_cd='01') b on so.ship_to_loc_nbr=b.loc_nbr 
inner join (select * from scct_db.inventory_movement where posting_dt>date_sub(current_date,276) and itm_nbr<>'N/A' and movmt_type in(601,602,901,902) ) im 
on dm.delv_doc_nbr=im.delv_doc_nbr and dm.delv_doc_itm_nbr= im.delv_doc_itm_nbr and dm.itm_nbr=im.itm_nbr 
group by so.sls_ord_nbr,so.sls_ord_itm_nbr,so.itm_nbr,po.purch_doc_type_cd;

! echo Completed so_line_kpi data load;

-------------------------
---end script
-------------------------
