---jobname:scct-ufposo-hive-stg1-so_line_kpi_final
--------------------------------------------------------------------------
---target: stage1 table: scct_db.po_line_kpi 
---source: tms_stage0 table : so_line_kpi_work and so_line_kpi_work1
---load type: full
---back posting: no
--
-- History Information
--
-- Revision    Date     	JIRA       	Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 04MAY20		SCCTP4-2922	ssoma			Initial Version
---------------------------------------------------------------------------

-------------------------
---job related hive settings 
-------------------------

set mapreduce.job.reduces=200;
set hive.auto.convert.join.noconditionaltask.size=200000000;

-------------------------
---loading po line kpi's data into dd_line_kpi table
-------------------------

set tez.job.name=load po line KPIs:so_line_kpi:load target from po_master ;

! echo started loading scct_work_db.so_line_kpi_work1 for all the modules;

insert overwrite table scct_work_db.so_line_kpi_work1 partition(business_process,to_node,po_doc_typ)
select
 coalesce(b.spply_chn_nd, a.spply_chn_nd)
,coalesce(b.from_node, a.from_node)
,coalesce(b.sls_ord_nbr, a.sls_ord_nbr)
,coalesce(b.sls_ord_itm_nbr, a.sls_ord_itm_nbr)
,coalesce(b.itm_nbr, a.itm_nbr)
,coalesce(b.so_frst_shp_dt, a.so_frst_shp_dt)
,coalesce(b.so_lst_shp_dt, a.so_lst_shp_dt)
,coalesce(b.so_tot_shp_units, a.so_tot_shp_units)
,coalesce(b.business_process, a.business_process)
,coalesce(b.to_node, a.to_node)
,coalesce(b.po_doc_typ, a.po_doc_typ)
from scct_db.so_line_kpi a full outer join scct_work_db.so_line_kpi_work b on
a.business_process=b.business_process and
a.to_node=b.to_node and
a.po_doc_typ=b.po_doc_typ and
a.sls_ord_nbr=b.sls_ord_nbr and
a.sls_ord_itm_nbr=b.sls_ord_itm_nbr and
a.itm_nbr=b.itm_nbr;

! echo started loading scct_work_db.so_line_kpi_work for all the modules;

insert overwrite table scct_db.so_line_kpi partition(business_process,to_node,po_doc_typ)
select
spply_chn_nd
,from_node
,sls_ord_nbr
,sls_ord_itm_nbr
,itm_nbr
,so_frst_shp_dt
,so_lst_shp_dt
,so_tot_shp_units
,business_process
,to_node
,po_doc_typ
from scct_work_db.so_line_kpi_work1;

! echo Completed so_line_kpi data load;
-------------------------
---end script
-------------------------