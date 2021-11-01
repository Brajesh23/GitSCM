! echo completed so_line_kpi data load;
---jobname:scct-ufposo-hive-stg1-so_line_kpi_final -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_line_kpi  ---source: tms_stage0 table : so_line_kpi_work and so_line_kpi_work1 ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 04may20 scctp4-2922 ssoma initial version ---------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------  set mapreduce.job.reduces=200;
------------------------- ---loading po line kpi's data into dd_line_kpi table -------------------------  set tez.job.name=load po line kpis:so_line_kpi:load target from po_master ;
! echo started loading scct_work_db.so_line_kpi_work1 for all the modules;
insert overwrite table scct_work_db.so_line_kpi_work1 partition(business_process,to_node,po_doc_typ) select  coalesce(b.spply_chn_nd, a.spply_chn_nd) ,coalesce(b.from_node, a.from_node) ,coalesce(b.sls_ord_nbr, a.sls_ord_nbr) ,coalesce(b.sls_ord_itm_nbr, a.sls_ord_itm_nbr) ,coalesce(b.itm_nbr, a.itm_nbr) ,coalesce(b.so_frst_shp_dt, a.so_frst_shp_dt) ,coalesce(b.so_lst_shp_dt, a.so_lst_shp_dt) ,coalesce(b.so_tot_shp_units, a.so_tot_shp_units) ,coalesce(b.business_process, a.business_process) ,coalesce(b.to_node, a.to_node) ,coalesce(b.po_doc_typ, a.po_doc_typ) from scct_db.so_line_kpi a full outer join scct_work_db.so_line_kpi_work b on a.business_process=b.business_process and a.to_node=b.to_node and a.po_doc_typ=b.po_doc_typ and a.sls_ord_nbr=b.sls_ord_nbr and a.sls_ord_itm_nbr=b.sls_ord_itm_nbr and a.itm_nbr=b.itm_nbr
! echo started loading scct_work_db.so_line_kpi_work for all the modules;
insert overwrite table scct_db.so_line_kpi partition(business_process,to_node,po_doc_typ) select spply_chn_nd ,from_node ,sls_ord_nbr ,sls_ord_itm_nbr ,itm_nbr ,so_frst_shp_dt ,so_lst_shp_dt ,so_tot_shp_units ,business_process ,to_node ,po_doc_typ from scct_work_db.so_line_kpi_work1
! echo completed so_line_kpi data load;
! echo completed so_line_kpi data load;
---jobname:scct-ufposo-hive-stg1-so_line_kpi_final -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_line_kpi  ---source: tms_stage0 table : so_line_kpi_work and so_line_kpi_work1 ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 04may20 scctp4-2922 ssoma initial version ---------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------  set mapreduce.job.reduces=200;
------------------------- ---loading po line kpi's data into dd_line_kpi table -------------------------  set tez.job.name=load po line kpis:so_line_kpi:load target from po_master ;
! echo started loading scct_work_db.so_line_kpi_work1 for all the modules;
insert overwrite table scct_work_db.so_line_kpi_work1 partition(business_process,to_node,po_doc_typ) select  coalesce(b.spply_chn_nd, a.spply_chn_nd) ,coalesce(b.from_node, a.from_node) ,coalesce(b.sls_ord_nbr, a.sls_ord_nbr) ,coalesce(b.sls_ord_itm_nbr, a.sls_ord_itm_nbr) ,coalesce(b.itm_nbr, a.itm_nbr) ,coalesce(b.so_frst_shp_dt, a.so_frst_shp_dt) ,coalesce(b.so_lst_shp_dt, a.so_lst_shp_dt) ,coalesce(b.so_tot_shp_units, a.so_tot_shp_units) ,coalesce(b.business_process, a.business_process) ,coalesce(b.to_node, a.to_node) ,coalesce(b.po_doc_typ, a.po_doc_typ) from scct_db.so_line_kpi a full outer join scct_work_db.so_line_kpi_work b on a.business_process=b.business_process and a.to_node=b.to_node and a.po_doc_typ=b.po_doc_typ and a.sls_ord_nbr=b.sls_ord_nbr and a.sls_ord_itm_nbr=b.sls_ord_itm_nbr and a.itm_nbr=b.itm_nbr
! echo started loading scct_work_db.so_line_kpi_work for all the modules;
insert overwrite table scct_db.so_line_kpi partition(business_process,to_node,po_doc_typ) select spply_chn_nd ,from_node ,sls_ord_nbr ,sls_ord_itm_nbr ,itm_nbr ,so_frst_shp_dt ,so_lst_shp_dt ,so_tot_shp_units ,business_process ,to_node ,po_doc_typ from scct_work_db.so_line_kpi_work1
! echo completed so_line_kpi data load;
! echo completed so_line_kpi data load;
---jobname:scct-ufposo-hive-stg1-so_line_kpi_final -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_line_kpi  ---source: tms_stage0 table : so_line_kpi_work and so_line_kpi_work1 ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 04may20 scctp4-2922 ssoma initial version ---------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------  set mapreduce.job.reduces=200;
------------------------- ---loading po line kpi's data into dd_line_kpi table -------------------------  set tez.job.name=load po line kpis:so_line_kpi:load target from po_master ;
! echo started loading scct_work_db.so_line_kpi_work1 for all the modules;
insert overwrite table scct_work_db.so_line_kpi_work1 partition(business_process,to_node,po_doc_typ) select  coalesce(b.spply_chn_nd, a.spply_chn_nd) ,coalesce(b.from_node, a.from_node) ,coalesce(b.sls_ord_nbr, a.sls_ord_nbr) ,coalesce(b.sls_ord_itm_nbr, a.sls_ord_itm_nbr) ,coalesce(b.itm_nbr, a.itm_nbr) ,coalesce(b.so_frst_shp_dt, a.so_frst_shp_dt) ,coalesce(b.so_lst_shp_dt, a.so_lst_shp_dt) ,coalesce(b.so_tot_shp_units, a.so_tot_shp_units) ,coalesce(b.business_process, a.business_process) ,coalesce(b.to_node, a.to_node) ,coalesce(b.po_doc_typ, a.po_doc_typ) from scct_db.so_line_kpi a full outer join scct_work_db.so_line_kpi_work b on a.business_process=b.business_process and a.to_node=b.to_node and a.po_doc_typ=b.po_doc_typ and a.sls_ord_nbr=b.sls_ord_nbr and a.sls_ord_itm_nbr=b.sls_ord_itm_nbr and a.itm_nbr=b.itm_nbr
! echo started loading scct_work_db.so_line_kpi_work for all the modules;
insert overwrite table scct_db.so_line_kpi partition(business_process,to_node,po_doc_typ) select spply_chn_nd ,from_node ,sls_ord_nbr ,sls_ord_itm_nbr ,itm_nbr ,so_frst_shp_dt ,so_lst_shp_dt ,so_tot_shp_units ,business_process ,to_node ,po_doc_typ from scct_work_db.so_line_kpi_work1
! echo completed so_line_kpi data load;
! echo completed so_line_kpi data load;
---jobname:scct-ufposo-hive-stg1-so_line_kpi_final -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_line_kpi  ---source: tms_stage0 table : so_line_kpi_work and so_line_kpi_work1 ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 04may20 scctp4-2922 ssoma initial version ---------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------  set mapreduce.job.reduces=200;
------------------------- ---loading po line kpi's data into dd_line_kpi table -------------------------  set tez.job.name=load po line kpis:so_line_kpi:load target from po_master ;
! echo started loading scct_work_db.so_line_kpi_work1 for all the modules;
insert overwrite table scct_work_db.so_line_kpi_work1 partition(business_process,to_node,po_doc_typ) select  coalesce(b.spply_chn_nd, a.spply_chn_nd) ,coalesce(b.from_node, a.from_node) ,coalesce(b.sls_ord_nbr, a.sls_ord_nbr) ,coalesce(b.sls_ord_itm_nbr, a.sls_ord_itm_nbr) ,coalesce(b.itm_nbr, a.itm_nbr) ,coalesce(b.so_frst_shp_dt, a.so_frst_shp_dt) ,coalesce(b.so_lst_shp_dt, a.so_lst_shp_dt) ,coalesce(b.so_tot_shp_units, a.so_tot_shp_units) ,coalesce(b.business_process, a.business_process) ,coalesce(b.to_node, a.to_node) ,coalesce(b.po_doc_typ, a.po_doc_typ) from scct_db.so_line_kpi a full outer join scct_work_db.so_line_kpi_work b on a.business_process=b.business_process and a.to_node=b.to_node and a.po_doc_typ=b.po_doc_typ and a.sls_ord_nbr=b.sls_ord_nbr and a.sls_ord_itm_nbr=b.sls_ord_itm_nbr and a.itm_nbr=b.itm_nbr
! echo started loading scct_work_db.so_line_kpi_work for all the modules;
insert overwrite table scct_db.so_line_kpi partition(business_process,to_node,po_doc_typ) select spply_chn_nd ,from_node ,sls_ord_nbr ,sls_ord_itm_nbr ,itm_nbr ,so_frst_shp_dt ,so_lst_shp_dt ,so_tot_shp_units ,business_process ,to_node ,po_doc_typ from scct_work_db.so_line_kpi_work1
! echo completed so_line_kpi data load;
! echo completed loading into scct_db.scem_expected_events;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_events -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_events  ---source: sap_stage0 table : scem_tracking_events_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_events ---load type: full ---back posting: no --------------------------------------------------------------------------  -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 23jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti changed patition key to eh_created_date and added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic --  ------------------------- ---job related hive settings to merge files for target table ------------------------- set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_events target table data -------------------------  set tez.job.name=stage2:scem_tracking_events :load target table scem_tracking_events ;
! echo started loading into scct_db.scem_tracking_events ;
! echo completed loading into scct_db.scem_tracking_events ;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_objects -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_objects  ---source: scct_work_db table : scem_tracking_objects_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_objects ---load type: full ---back posting: no ---author: sdhal ---created date: 29/07/2019 -------------------------------------------------------------------------- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 29jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 19sep19 scctp4-2460 ryadav increased tez container size -- 01.00.00.04 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic -- 01.00.00.05 21jan20 scctp4-2988 akoti changed column name from act_po_gr_date to act_gac_date --  ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_objects target table data -------------------------  set tez.job.name=stage2:scem_tracking_objects :load target table scem_tracking_objects ;
! echo started loading into scct_db.scem_tracking_objects ;
! echo completed loading into scct_db.scem_tracking_objects ;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_types -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_types  ---source: scct_work_db table : scem_tracking_types_temp, scct_db table : scem_tracking_types ---load type: full ---back posting: no ---author: ryadav ---created date: 03/14/2019 --------------------------------------------------------------------------   ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_types target table data -------------------------  set tez.job.name=stage1:scem_tracking_types :load target table scem_tracking_types ;
! echo started loading into scct_db.scem_tracking_types ;
! echo completed loading into scct_db.scem_tracking_types ;
---jobname:scct-ufposo-hive-stg1-so_line_kpi_dcb_to_dcm_aprl -------------------------------------------------------------------------- ---target: stage1 table: scct_db.so_line_kpi  ---source: tms_stage0 table : delivery_master, handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 07jan20 scctp4-2924 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter --------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------   set hive.auto.convert.join.noconditionaltask.size=200000000;
------------------------- ---loading dd line kpi's data into so_line_kpi table -------------------------  set tez.job.name=load dd line kpis:so_line_kpi :load target from delivery_master ;
! echo started loading into scct_db.so_line_kpi;
! echo completed so_line_kpi data load;
---jobname:scct-usposo-hive-stg1-so_line_kpi_dcoutboundso -------------------------------------------------------------------------- ---target: stage1 table: scct_db.so_line_kpi  ---source: tms_stage0 table : delivery_master, handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 07jan20 scctp4-2924 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter --------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------   set hive.auto.convert.join.noconditionaltask.size=200000000;
------------------------- ---loading dd line kpi's data into so_line_kpi table -------------------------  set tez.job.name=load dd line kpis:so_line_kpi :load target from delivery_master ;
! echo started loading into scct_db.so_line_kpi;
! echo completed so_line_kpi data load;
---jobname:scct-ufposo-hive-stg1-so_line_kpi_final -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_line_kpi  ---source: tms_stage0 table : so_line_kpi_work and so_line_kpi_work1 ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 04may20 scctp4-2922 ssoma initial version ---------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------  set mapreduce.job.reduces=200;
------------------------- ---loading po line kpi's data into dd_line_kpi table -------------------------  set tez.job.name=load po line kpis:so_line_kpi:load target from po_master ;
! echo started loading scct_work_db.so_line_kpi_work1 for all the modules;
! echo started loading scct_work_db.so_line_kpi_work for all the modules;
! echo completed so_line_kpi data load;
! echo completed loading into scct_db.scem_expected_events;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_events -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_events  ---source: sap_stage0 table : scem_tracking_events_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_events ---load type: full ---back posting: no --------------------------------------------------------------------------  -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 23jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti changed patition key to eh_created_date and added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic --  ------------------------- ---job related hive settings to merge files for target table ------------------------- set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_events target table data -------------------------  set tez.job.name=stage2:scem_tracking_events :load target table scem_tracking_events ;
! echo started loading into scct_db.scem_tracking_events ;
! echo completed loading into scct_db.scem_tracking_events ;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_objects -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_objects  ---source: scct_work_db table : scem_tracking_objects_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_objects ---load type: full ---back posting: no ---author: sdhal ---created date: 29/07/2019 -------------------------------------------------------------------------- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 29jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 19sep19 scctp4-2460 ryadav increased tez container size -- 01.00.00.04 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic -- 01.00.00.05 21jan20 scctp4-2988 akoti changed column name from act_po_gr_date to act_gac_date --  ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_objects target table data -------------------------  set tez.job.name=stage2:scem_tracking_objects :load target table scem_tracking_objects ;
! echo started loading into scct_db.scem_tracking_objects ;
! echo completed loading into scct_db.scem_tracking_objects ;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_types -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_types  ---source: scct_work_db table : scem_tracking_types_temp, scct_db table : scem_tracking_types ---load type: full ---back posting: no ---author: ryadav ---created date: 03/14/2019 --------------------------------------------------------------------------   ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_types target table data -------------------------  set tez.job.name=stage1:scem_tracking_types :load target table scem_tracking_types ;
! echo started loading into scct_db.scem_tracking_types ;
! echo completed loading into scct_db.scem_tracking_types ;
---jobname:scct-ufposo-hive-stg1-so_line_kpi_dcb_to_dcm_aprl -------------------------------------------------------------------------- ---target: stage1 table: scct_db.so_line_kpi  ---source: tms_stage0 table : delivery_master, handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 07jan20 scctp4-2924 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter --------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------   set hive.auto.convert.join.noconditionaltask.size=200000000;
------------------------- ---loading dd line kpi's data into so_line_kpi table -------------------------  set tez.job.name=load dd line kpis:so_line_kpi :load target from delivery_master ;
! echo started loading into scct_db.so_line_kpi;
! echo completed so_line_kpi data load;
---jobname:scct-usposo-hive-stg1-so_line_kpi_dcoutboundso -------------------------------------------------------------------------- ---target: stage1 table: scct_db.so_line_kpi  ---source: tms_stage0 table : delivery_master, handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 07jan20 scctp4-2924 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter --------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------   set hive.auto.convert.join.noconditionaltask.size=200000000;
------------------------- ---loading dd line kpi's data into so_line_kpi table -------------------------  set tez.job.name=load dd line kpis:so_line_kpi :load target from delivery_master ;
! echo started loading into scct_db.so_line_kpi;
! echo completed so_line_kpi data load;
---jobname:scct-ufposo-hive-stg1-so_line_kpi_final -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_line_kpi  ---source: tms_stage0 table : so_line_kpi_work and so_line_kpi_work1 ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 04may20 scctp4-2922 ssoma initial version ---------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------  set mapreduce.job.reduces=200;
------------------------- ---loading po line kpi's data into dd_line_kpi table -------------------------  set tez.job.name=load po line kpis:so_line_kpi:load target from po_master ;
! echo started loading scct_work_db.so_line_kpi_work1 for all the modules;
! echo started loading scct_work_db.so_line_kpi_work for all the modules;
! echo completed so_line_kpi data load;
! echo completed loading into scct_db.scem_expected_events;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_events -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_events  ---source: sap_stage0 table : scem_tracking_events_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_events ---load type: full ---back posting: no --------------------------------------------------------------------------  -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 23jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti changed patition key to eh_created_date and added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic --  ------------------------- ---job related hive settings to merge files for target table ------------------------- set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_events target table data -------------------------  set tez.job.name=stage2:scem_tracking_events :load target table scem_tracking_events ;
! echo started loading into scct_db.scem_tracking_events ;
! echo completed loading into scct_db.scem_tracking_events ;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_objects -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_objects  ---source: scct_work_db table : scem_tracking_objects_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_objects ---load type: full ---back posting: no ---author: sdhal ---created date: 29/07/2019 -------------------------------------------------------------------------- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 29jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 19sep19 scctp4-2460 ryadav increased tez container size -- 01.00.00.04 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic -- 01.00.00.05 21jan20 scctp4-2988 akoti changed column name from act_po_gr_date to act_gac_date --  ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_objects target table data -------------------------  set tez.job.name=stage2:scem_tracking_objects :load target table scem_tracking_objects ;
! echo started loading into scct_db.scem_tracking_objects ;
! echo completed loading into scct_db.scem_tracking_objects ;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_types -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_types  ---source: scct_work_db table : scem_tracking_types_temp, scct_db table : scem_tracking_types ---load type: full ---back posting: no ---author: ryadav ---created date: 03/14/2019 --------------------------------------------------------------------------   ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_types target table data -------------------------  set tez.job.name=stage1:scem_tracking_types :load target table scem_tracking_types ;
! echo started loading into scct_db.scem_tracking_types ;
! echo completed loading into scct_db.scem_tracking_types ;
---jobname:scct-ufposo-hive-stg1-so_line_kpi_dcb_to_dcm_aprl -------------------------------------------------------------------------- ---target: stage1 table: scct_db.so_line_kpi  ---source: tms_stage0 table : delivery_master, handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 07jan20 scctp4-2924 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter --------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------   set hive.auto.convert.join.noconditionaltask.size=200000000;
------------------------- ---loading dd line kpi's data into so_line_kpi table -------------------------  set tez.job.name=load dd line kpis:so_line_kpi :load target from delivery_master ;
! echo started loading into scct_db.so_line_kpi;
! echo completed so_line_kpi data load;
---jobname:scct-usposo-hive-stg1-so_line_kpi_dcoutboundso -------------------------------------------------------------------------- ---target: stage1 table: scct_db.so_line_kpi  ---source: tms_stage0 table : delivery_master, handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 07jan20 scctp4-2924 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter --------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------   set hive.auto.convert.join.noconditionaltask.size=200000000;
------------------------- ---loading dd line kpi's data into so_line_kpi table -------------------------  set tez.job.name=load dd line kpis:so_line_kpi :load target from delivery_master ;
! echo started loading into scct_db.so_line_kpi;
! echo completed so_line_kpi data load;
---jobname:scct-ufposo-hive-stg1-so_line_kpi_final -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_line_kpi  ---source: tms_stage0 table : so_line_kpi_work and so_line_kpi_work1 ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 04may20 scctp4-2922 ssoma initial version ---------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------  set mapreduce.job.reduces=200;
------------------------- ---loading po line kpi's data into dd_line_kpi table -------------------------  set tez.job.name=load po line kpis:so_line_kpi:load target from po_master ;
! echo started loading scct_work_db.so_line_kpi_work1 for all the modules;
! echo started loading scct_work_db.so_line_kpi_work for all the modules;
! echo completed so_line_kpi data load;
! echo completed loading into scct_db.scem_expected_events;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_events -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_events  ---source: sap_stage0 table : scem_tracking_events_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_events ---load type: full ---back posting: no --------------------------------------------------------------------------  -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 23jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti changed patition key to eh_created_date and added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic --  ------------------------- ---job related hive settings to merge files for target table ------------------------- set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_events target table data -------------------------  set tez.job.name=stage2:scem_tracking_events :load target table scem_tracking_events ;
! echo started loading into scct_db.scem_tracking_events ;
! echo completed loading into scct_db.scem_tracking_events ;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_objects -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_objects  ---source: scct_work_db table : scem_tracking_objects_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_objects ---load type: full ---back posting: no ---author: sdhal ---created date: 29/07/2019 -------------------------------------------------------------------------- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 29jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 19sep19 scctp4-2460 ryadav increased tez container size -- 01.00.00.04 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic -- 01.00.00.05 21jan20 scctp4-2988 akoti changed column name from act_po_gr_date to act_gac_date --  ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_objects target table data -------------------------  set tez.job.name=stage2:scem_tracking_objects :load target table scem_tracking_objects ;
! echo started loading into scct_db.scem_tracking_objects ;
! echo completed loading into scct_db.scem_tracking_objects ;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_types -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_types  ---source: scct_work_db table : scem_tracking_types_temp, scct_db table : scem_tracking_types ---load type: full ---back posting: no ---author: ryadav ---created date: 03/14/2019 --------------------------------------------------------------------------   ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_types target table data -------------------------  set tez.job.name=stage1:scem_tracking_types :load target table scem_tracking_types ;
! echo started loading into scct_db.scem_tracking_types ;
! echo completed loading into scct_db.scem_tracking_types ;
---jobname:scct-ufposo-hive-stg1-so_line_kpi_dcb_to_dcm_aprl -------------------------------------------------------------------------- ---target: stage1 table: scct_db.so_line_kpi  ---source: tms_stage0 table : delivery_master, handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 07jan20 scctp4-2924 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter --------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------   set hive.auto.convert.join.noconditionaltask.size=200000000;
------------------------- ---loading dd line kpi's data into so_line_kpi table -------------------------  set tez.job.name=load dd line kpis:so_line_kpi :load target from delivery_master ;
! echo started loading into scct_db.so_line_kpi;
! echo completed so_line_kpi data load;
---jobname:scct-usposo-hive-stg1-so_line_kpi_dcoutboundso -------------------------------------------------------------------------- ---target: stage1 table: scct_db.so_line_kpi  ---source: tms_stage0 table : delivery_master, handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 07jan20 scctp4-2924 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter --------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------   set hive.auto.convert.join.noconditionaltask.size=200000000;
------------------------- ---loading dd line kpi's data into so_line_kpi table -------------------------  set tez.job.name=load dd line kpis:so_line_kpi :load target from delivery_master ;
! echo started loading into scct_db.so_line_kpi;
! echo completed so_line_kpi data load;
---jobname:scct-ufposo-hive-stg1-so_line_kpi_final -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_line_kpi  ---source: tms_stage0 table : so_line_kpi_work and so_line_kpi_work1 ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 04may20 scctp4-2922 ssoma initial version ---------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------  set mapreduce.job.reduces=200;
------------------------- ---loading po line kpi's data into dd_line_kpi table -------------------------  set tez.job.name=load po line kpis:so_line_kpi:load target from po_master ;
! echo started loading scct_work_db.so_line_kpi_work1 for all the modules;
! echo started loading scct_work_db.so_line_kpi_work for all the modules;
! echo completed so_line_kpi data load;