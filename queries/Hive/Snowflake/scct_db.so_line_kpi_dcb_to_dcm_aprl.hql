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
INSERT OVERWRITE INTO SCCT_WORK_DB.SO_LINE_KPI_WORK
(SELECT 'dc' AS SPPLY_CHN_ND, 'dc_brand' AS FROM_NODE, SO.SLS_ORD_NBR, SO.SLS_ORD_ITM_NBR, SO.ITM_NBR, COALESCE(MIN(CASE WHEN IM.MOVMT_TYPE IN (901) THEN IM.POSTING_DT ELSE NULL END), '9999-12-31') AS SO_FRST_SHP_DT, COALESCE(MAX(CASE WHEN IM.MOVMT_TYPE IN (901) THEN IM.POSTING_DT ELSE NULL END), '9999-12-31') AS SO_LST_SHP_DT, COALESCE(SUM(CASE WHEN IM.MOVMT_TYPE IN (901) THEN IM.TRX_QTY WHEN IM.MOVMT_TYPE IN (902) THEN IM.TRX_QTY * -1 ELSE 0 END), 0) AS SO_TOT_SHP_UNITS, 'stock_transfer_inter_company' AS BUSINESS_PROCESS, 'dc_mast' AS TO_NODE, PO.PURCH_DOC_TYPE_CD AS PO_DOC_TYP
FROM (SELECT *
FROM SCCT_WORK_DB.SO_MASTER_WORK
WHERE SLS_DOC_TYPE = 'zor7' AND SLS_ORD_NBR <> 'n/a' AND RECV_LOC_NBR <> '0' AND SHIP_TO_LOC_NBR <> '0') AS SO
INNER JOIN (SELECT *
FROM SCCT_WORK_DB.PO_MASTER_WORK
WHERE PURCH_DOC_TYPE_CD = 'zatp' AND (DEL_IND <> 'l' OR DEL_IND = 'l' AND GR_QTY > 0)) AS PO ON SO.SLS_ORD_NBR = PO.PURCH_ORD_NBR AND SO.SLS_ORD_ITM_NBR = PO.PURCH_ORD_ITM_NBR AND SO.ITM_NBR = PO.ITM_NBR
INNER JOIN (SELECT *
FROM SCCT_DB.LOCATION_MASTER
WHERE LOC_TYP_CD = '01') AS A ON SO.RECV_LOC_NBR = A.LOC_NBR
INNER JOIN (SELECT *
FROM SCCT_DB.LOCATION_MASTER
WHERE LOC_TYP_CD IN ('02', '13') AND ORG_NUM = '1283') AS B ON SO.SHIP_TO_LOC_NBR = B.LOC_NBR
INNER JOIN (SELECT *
FROM SCCT_WORK_DB.DELIVERY_MASTER_WORK
WHERE ITM_NBR <> '0' AND DEL_IND <> 'd' AND DEL_TYPE NOT IN ('el', 'elst')) AS DM ON PO.PURCH_ORD_NBR = DM.REF_DOC_NBR AND PO.PURCH_ORD_ITM_NBR = DM.REF_DOC_ITM_NBR AND PO.ITM_NBR = DM.ITM_NBR
INNER JOIN (SELECT *
FROM SCCT_DB.INVENTORY_MOVEMENT
WHERE POSTING_DT > DATE_SUB(CURRENT_DATE, 276) AND ITM_NBR <> 'n/a' AND MOVMT_TYPE IN (901, 902)) AS IM ON DM.DELV_DOC_NBR = IM.DELV_DOC_NBR AND DM.DELV_DOC_ITM_NBR = IM.DELV_DOC_ITM_NBR AND PO.ITM_NBR = IM.ITM_NBR
GROUP BY SO.SLS_ORD_NBR, SO.SLS_ORD_ITM_NBR, SO.ITM_NBR, PO.PURCH_DOC_TYPE_CD);
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
INSERT OVERWRITE INTO SCCT_WORK_DB.SO_LINE_KPI_WORK
(SELECT 'dc' AS SPPLY_CHN_ND, 'dc_brand' AS FROM_NODE, SO.SLS_ORD_NBR, SO.SLS_ORD_ITM_NBR, SO.ITM_NBR, COALESCE(MIN(CASE WHEN IM.MOVMT_TYPE IN (901) THEN IM.POSTING_DT ELSE NULL END), '9999-12-31') AS SO_FRST_SHP_DT, COALESCE(MAX(CASE WHEN IM.MOVMT_TYPE IN (901) THEN IM.POSTING_DT ELSE NULL END), '9999-12-31') AS SO_LST_SHP_DT, COALESCE(SUM(CASE WHEN IM.MOVMT_TYPE IN (901) THEN IM.TRX_QTY WHEN IM.MOVMT_TYPE IN (902) THEN IM.TRX_QTY * -1 ELSE 0 END), 0) AS SO_TOT_SHP_UNITS, 'stock_transfer_inter_company' AS BUSINESS_PROCESS, 'dc_mast' AS TO_NODE, PO.PURCH_DOC_TYPE_CD AS PO_DOC_TYP
FROM (SELECT *
FROM SCCT_WORK_DB.SO_MASTER_WORK
WHERE SLS_DOC_TYPE = 'zor7' AND SLS_ORD_NBR <> 'n/a' AND RECV_LOC_NBR <> '0' AND SHIP_TO_LOC_NBR <> '0') AS SO
INNER JOIN (SELECT *
FROM SCCT_WORK_DB.PO_MASTER_WORK
WHERE PURCH_DOC_TYPE_CD = 'zatp' AND (DEL_IND <> 'l' OR DEL_IND = 'l' AND GR_QTY > 0)) AS PO ON SO.SLS_ORD_NBR = PO.PURCH_ORD_NBR AND SO.SLS_ORD_ITM_NBR = PO.PURCH_ORD_ITM_NBR AND SO.ITM_NBR = PO.ITM_NBR
INNER JOIN (SELECT *
FROM SCCT_DB.LOCATION_MASTER
WHERE LOC_TYP_CD = '01') AS A ON SO.RECV_LOC_NBR = A.LOC_NBR
INNER JOIN (SELECT *
FROM SCCT_DB.LOCATION_MASTER
WHERE LOC_TYP_CD IN ('02', '13') AND ORG_NUM = '1283') AS B ON SO.SHIP_TO_LOC_NBR = B.LOC_NBR
INNER JOIN (SELECT *
FROM SCCT_WORK_DB.DELIVERY_MASTER_WORK
WHERE ITM_NBR <> '0' AND DEL_IND <> 'd' AND DEL_TYPE NOT IN ('el', 'elst')) AS DM ON PO.PURCH_ORD_NBR = DM.REF_DOC_NBR AND PO.PURCH_ORD_ITM_NBR = DM.REF_DOC_ITM_NBR AND PO.ITM_NBR = DM.ITM_NBR
INNER JOIN (SELECT *
FROM SCCT_DB.INVENTORY_MOVEMENT
WHERE POSTING_DT > DATE_SUB(CURRENT_DATE, 276) AND ITM_NBR <> 'n/a' AND MOVMT_TYPE IN (901, 902)) AS IM ON DM.DELV_DOC_NBR = IM.DELV_DOC_NBR AND DM.DELV_DOC_ITM_NBR = IM.DELV_DOC_ITM_NBR AND PO.ITM_NBR = IM.ITM_NBR
GROUP BY SO.SLS_ORD_NBR, SO.SLS_ORD_ITM_NBR, SO.ITM_NBR, PO.PURCH_DOC_TYPE_CD);
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
INSERT OVERWRITE INTO SCCT_WORK_DB.SO_LINE_KPI_WORK
(SELECT 'dc' AS SPPLY_CHN_ND, 'dc_brand' AS FROM_NODE, SO.SLS_ORD_NBR, SO.SLS_ORD_ITM_NBR, SO.ITM_NBR, COALESCE(MIN(CASE WHEN IM.MOVMT_TYPE IN (901) THEN IM.POSTING_DT ELSE NULL END), '9999-12-31') AS SO_FRST_SHP_DT, COALESCE(MAX(CASE WHEN IM.MOVMT_TYPE IN (901) THEN IM.POSTING_DT ELSE NULL END), '9999-12-31') AS SO_LST_SHP_DT, COALESCE(SUM(CASE WHEN IM.MOVMT_TYPE IN (901) THEN IM.TRX_QTY WHEN IM.MOVMT_TYPE IN (902) THEN IM.TRX_QTY * -1 ELSE 0 END), 0) AS SO_TOT_SHP_UNITS, 'stock_transfer_inter_company' AS BUSINESS_PROCESS, 'dc_mast' AS TO_NODE, PO.PURCH_DOC_TYPE_CD AS PO_DOC_TYP
FROM (SELECT *
FROM SCCT_WORK_DB.SO_MASTER_WORK
WHERE SLS_DOC_TYPE = 'zor7' AND SLS_ORD_NBR <> 'n/a' AND RECV_LOC_NBR <> '0' AND SHIP_TO_LOC_NBR <> '0') AS SO
INNER JOIN (SELECT *
FROM SCCT_WORK_DB.PO_MASTER_WORK
WHERE PURCH_DOC_TYPE_CD = 'zatp' AND (DEL_IND <> 'l' OR DEL_IND = 'l' AND GR_QTY > 0)) AS PO ON SO.SLS_ORD_NBR = PO.PURCH_ORD_NBR AND SO.SLS_ORD_ITM_NBR = PO.PURCH_ORD_ITM_NBR AND SO.ITM_NBR = PO.ITM_NBR
INNER JOIN (SELECT *
FROM SCCT_DB.LOCATION_MASTER
WHERE LOC_TYP_CD = '01') AS A ON SO.RECV_LOC_NBR = A.LOC_NBR
INNER JOIN (SELECT *
FROM SCCT_DB.LOCATION_MASTER
WHERE LOC_TYP_CD IN ('02', '13') AND ORG_NUM = '1283') AS B ON SO.SHIP_TO_LOC_NBR = B.LOC_NBR
INNER JOIN (SELECT *
FROM SCCT_WORK_DB.DELIVERY_MASTER_WORK
WHERE ITM_NBR <> '0' AND DEL_IND <> 'd' AND DEL_TYPE NOT IN ('el', 'elst')) AS DM ON PO.PURCH_ORD_NBR = DM.REF_DOC_NBR AND PO.PURCH_ORD_ITM_NBR = DM.REF_DOC_ITM_NBR AND PO.ITM_NBR = DM.ITM_NBR
INNER JOIN (SELECT *
FROM SCCT_DB.INVENTORY_MOVEMENT
WHERE POSTING_DT > DATE_SUB(CURRENT_DATE, 276) AND ITM_NBR <> 'n/a' AND MOVMT_TYPE IN (901, 902)) AS IM ON DM.DELV_DOC_NBR = IM.DELV_DOC_NBR AND DM.DELV_DOC_ITM_NBR = IM.DELV_DOC_ITM_NBR AND PO.ITM_NBR = IM.ITM_NBR
GROUP BY SO.SLS_ORD_NBR, SO.SLS_ORD_ITM_NBR, SO.ITM_NBR, PO.PURCH_DOC_TYPE_CD);
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
INSERT OVERWRITE INTO SCCT_WORK_DB.SO_LINE_KPI_WORK
(SELECT 'dc' AS SPPLY_CHN_ND, 'dc_brand' AS FROM_NODE, SO.SLS_ORD_NBR, SO.SLS_ORD_ITM_NBR, SO.ITM_NBR, COALESCE(MIN(CASE WHEN IM.MOVMT_TYPE IN (901) THEN IM.POSTING_DT ELSE NULL END), '9999-12-31') AS SO_FRST_SHP_DT, COALESCE(MAX(CASE WHEN IM.MOVMT_TYPE IN (901) THEN IM.POSTING_DT ELSE NULL END), '9999-12-31') AS SO_LST_SHP_DT, COALESCE(SUM(CASE WHEN IM.MOVMT_TYPE IN (901) THEN IM.TRX_QTY WHEN IM.MOVMT_TYPE IN (902) THEN IM.TRX_QTY * -1 ELSE 0 END), 0) AS SO_TOT_SHP_UNITS, 'stock_transfer_inter_company' AS BUSINESS_PROCESS, 'dc_mast' AS TO_NODE, PO.PURCH_DOC_TYPE_CD AS PO_DOC_TYP
FROM (SELECT *
FROM SCCT_WORK_DB.SO_MASTER_WORK
WHERE SLS_DOC_TYPE = 'zor7' AND SLS_ORD_NBR <> 'n/a' AND RECV_LOC_NBR <> '0' AND SHIP_TO_LOC_NBR <> '0') AS SO
INNER JOIN (SELECT *
FROM SCCT_WORK_DB.PO_MASTER_WORK
WHERE PURCH_DOC_TYPE_CD = 'zatp' AND (DEL_IND <> 'l' OR DEL_IND = 'l' AND GR_QTY > 0)) AS PO ON SO.SLS_ORD_NBR = PO.PURCH_ORD_NBR AND SO.SLS_ORD_ITM_NBR = PO.PURCH_ORD_ITM_NBR AND SO.ITM_NBR = PO.ITM_NBR
INNER JOIN (SELECT *
FROM SCCT_DB.LOCATION_MASTER
WHERE LOC_TYP_CD = '01') AS A ON SO.RECV_LOC_NBR = A.LOC_NBR
INNER JOIN (SELECT *
FROM SCCT_DB.LOCATION_MASTER
WHERE LOC_TYP_CD IN ('02', '13') AND ORG_NUM = '1283') AS B ON SO.SHIP_TO_LOC_NBR = B.LOC_NBR
INNER JOIN (SELECT *
FROM SCCT_WORK_DB.DELIVERY_MASTER_WORK
WHERE ITM_NBR <> '0' AND DEL_IND <> 'd' AND DEL_TYPE NOT IN ('el', 'elst')) AS DM ON PO.PURCH_ORD_NBR = DM.REF_DOC_NBR AND PO.PURCH_ORD_ITM_NBR = DM.REF_DOC_ITM_NBR AND PO.ITM_NBR = DM.ITM_NBR
INNER JOIN (SELECT *
FROM SCCT_DB.INVENTORY_MOVEMENT
WHERE POSTING_DT > DATE_SUB(CURRENT_DATE, 276) AND ITM_NBR <> 'n/a' AND MOVMT_TYPE IN (901, 902)) AS IM ON DM.DELV_DOC_NBR = IM.DELV_DOC_NBR AND DM.DELV_DOC_ITM_NBR = IM.DELV_DOC_ITM_NBR AND PO.ITM_NBR = IM.ITM_NBR
GROUP BY SO.SLS_ORD_NBR, SO.SLS_ORD_ITM_NBR, SO.ITM_NBR, PO.PURCH_DOC_TYPE_CD);
