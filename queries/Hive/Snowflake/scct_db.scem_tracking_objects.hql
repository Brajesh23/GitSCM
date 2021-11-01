! echo completed loading into scct_db.scem_expected_events;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_events -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_events  ---source: sap_stage0 table : scem_tracking_events_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_events ---load type: full ---back posting: no --------------------------------------------------------------------------  -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 23jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti changed patition key to eh_created_date and added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic --  ------------------------- ---job related hive settings to merge files for target table ------------------------- set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_events target table data -------------------------  set tez.job.name=stage2:scem_tracking_events :load target table scem_tracking_events ;
! echo started loading into scct_db.scem_tracking_events ;
! echo completed loading into scct_db.scem_tracking_events ;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_objects -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_objects  ---source: scct_work_db table : scem_tracking_objects_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_objects ---load type: full ---back posting: no ---author: sdhal ---created date: 29/07/2019 -------------------------------------------------------------------------- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 29jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 19sep19 scctp4-2460 ryadav increased tez container size -- 01.00.00.04 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic -- 01.00.00.05 21jan20 scctp4-2988 akoti changed column name from act_po_gr_date to act_gac_date --  ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_objects target table data -------------------------  set tez.job.name=stage2:scem_tracking_objects :load target table scem_tracking_objects ;
! echo started loading into scct_db.scem_tracking_objects ;
INSERT OVERWRITE INTO SCCT_DB.SCEM_TRACKING_OBJECTS
(SELECT APO_INTEGR, ARRIV_HARB, ARRIV_LOC, ASN_DEL_DT, ASN_QUANT, BATCH, BOM, BOM_ALT, BOOKING_ID, BRAND, BW_PROFILE, CARRIER, CARRIER_P, CARRIER_S, CFS_BYPASS, CFS_HOLD, CHOICE, CLASS, COMPANY_CODE, CONTAINER_NUMBER, CARGO_RECEIPT_QTY_TOTAL_CTN, CURRENCY, CUSTOMS_DIST_ID, DAILY_CUT_DATE, DAILY_CUT_QTY, DAILY_FD_OFF_DT, DAILY_FD_OFF_QT, DEL_DATE, DEL_PRIORITY, DELET_IND, DELIV_LOC, DELV_NUMB, DEPT_LOC, DOX_RECV_DATE, DOX_RECV_TIME, DEPARTURE_QTY_TOTAL_CTN, DSR_COMMENTS, DSR_ETA_FINAL, DSR_ETA_CFS, DSR_REASON_CODE, DSR_REASON_DESC, DSR_STATUS, EH_ACTIVE_DATE, EH_ACTIVE_TIME, EH_APPOBJID, EH_APPOBJTYPE, EH_APPSYS, EH_CREATED_BY, EH_CREATED_DATE_EST, EH_CREATED_TIME_EST, EH_CREATED_DATE_TIME_EST, EH_DURATION_01, EH_DURATION_02, EH_DURATION_03, EH_DURATION_04, EH_DURATION_05, EH_GUID, EH_INACTIVE_DATE, EH_INACTIVE_TIME, EH_PROC_MODE, EH_START_DATE, EH_START_TIME, EH_TRKID_CD, EH_TRKID_CS, EH_TYPE, EH_TZONE, EH_UPDATED_BY, EH_UPDATED_DATE_EST, EH_UPDATED_TIME_EST, EH_UPDATED_DATE_TIME_EST, EM_CLIENT, ENTERPRISE, EQUIPMENT, EXPLOSION_DT, FCR_NUMBER, DEPARTURE_EDI_FINL_DEST_ETA_DT, FINL_DEST_LOC_ID, FUNCT_LOC, GR_QUANT, HAWBFCR, INCO1, INCO2, INFO_TYPE, INSP_LOT, INV_DOC_NO, ITM_CAT, LBI_OWNERSHIP_DT, MATERIAL, MAWB, MRP_AREA, MRP_CONTRL, NET_PRICE, NOT_CAT_TYPE, NOT_CODE_GRP, NOT_CODING, NOT_FINISH_DT, NOT_START_DT, NOT_TYPE, OBOL, OI_EBELN, ORD_CATEG, ORD_TYPE, ORDER_RS_CODE, PACK_DATE_DAILY, PACK_QTY_DAILY, PLANNED_MODE, PLANT, PLN_FINISH_DT, PLN_START_DT, PO_CREATOR, PO_DELI_DT, PO_DOC_DT, POI_ACK_QT, POI_QUANT, PORT_ENT_ETA_DAT, PORT_ENT_LOC_ID, PORT_LAD_ETD_DT, PORT_OF_DISCHARG, PORT_OF_ENTRY, PORT_OF_LADING, PRIORITY, PROD_SCHED, PROD_ST_DT_ACT, PROD_ST_DT_PLN, PROD_VERS, PRT_DISCH_ETA_DT, PRT_DISCH_LOC_ID, PURCH_GRP, PURCH_ORD_LN_NO, PURCH_ORG, QUANTITY, QUANTITY_UNIT, REPORT_BY, REQ_DEL_DATE, REQUISIT, RESP_PARTNER, ROCANCEL, ROUT_CNT, ROUT_GRP, ROUTE, ROUTE_P, ROUTE_S, SALESORDER_NO, SECTOR, SHIP_TO_PARTY, SHIPMT_TYPE, SHIPPING_COND, SHIPPING_POINT, SHIPPING_TYPE, SOLD_TO_PARTY, STYLE, TOTAL_VOL_UNIT, TOTAL_VOLUME, TOTAL_WEIGHT, TOTAL_WGHT_UNIT, TRAILER_NUMBER, TRANS_MODE, UNLOADING_POINT, UOM, VENDOR, VENDOR_COUNTRY, VENDOR_NAME, VENDOR_ORD_TYPE, VENDOR_SEASON, VENDOR_YEAR, VESSEL_NAME, VOYAGE, WORK_CENTER, Z_AMOUNT_CC, Z_DATE1, Z_DATE1_SUP, Z_DATE2, Z_DATE2_SUP, Z_DATE3, Z_DATE3_SUP, Z_DATE4, Z_DATE4_SUP, Z_DATE5, Z_DATE5_SUP, Z_INSUFFICIENT_GREIGE, Z_NOT_ASSIGNED, Z_QTY1, Z_QTY1_SUP, Z_QTY2, Z_QTY2_SUP, Z_QTY3, Z_QTY3_SUP, Z_QTY4, Z_QTY4_SUP, Z_QTY5, Z_QTY5_SUP, Z_RMPO_BRAND, Z_RMPO_BUY_STRATEGY, Z_RMPO_CATEGORY, Z_RMPO_DEPT_CODE, Z_RMPO_FABRIC_STATUS, Z_RMPO_FLOOR_SET, Z_RMPO_FLOORSET_NAME, Z_RMPO_MATERIAL_TYPE_COLOR, Z_RMPO_ORDER_DATE, Z_RMPO_ORDER_QTY, Z_RMPO_ORIG_COMMITT_TYPE, Z_RMPO_PLAN_GRP, Z_RMPO_QUALITY_NUM, Z_RMPO_STAT, Z_RMPO_SUPPLIER, Z_RMPO_VENDOR, Z_ROLLING, Z_VENDOR, Z_VENDOR_RMPO, Z_VPO, ZCOLLECTION, ZCOLOR_MRKT, CPO_NUMBER, ZDELIVERY_IND, ZFACTORY_COST, ZFIRST_COST, ZFLEX_OPTIVA, ZLANDED_COST, ZLBI_CNF_GAC, ZLBI_CNF_MODE, ZLBI_CNF_NDC, ZLBI_CNF_STAT, ZLBI_IN_MODE, ZLBI_NEW_AIR_NDC, ZLBI_NEW_OCN_NDC, ZLBI_PROD_DELAY, ZLBI_REMARKS_CNTRL, ZLBI_SO_NO, ZLBI_SO_PO_PREV, ZLBI_SOPO_DATE, ZLBI_TOT_CO_QTY, ZLBI_TOT_QTY, ZLBI_VQ_GAC, ZLBI_VQ_MODE, ZLBI_VQ_NDC, ZLBI_VQ_STAT, ZORIG_LBI_OWN_DT, ZORIGINAL_GAC, ZORIGINAL_NDC, ZREADY_DATE, ZREADY_QT, ZRETAIL_PRICE, ZRMPO_AGE, ZRMPO_BRAND, ZRMPO_BUY_STRATEGY, ZRMPO_C_LIABILITY_TYP, ZRMPO_CATEGORY, ZRMPO_CLOSE_DT, ZRMPO_COLLECTION, ZRMPO_COLOR, ZRMPO_COLOR_DESC, ZRMPO_COST, ZRMPO_CREATE_DT, ZRMPO_CUR_OWNERSHIP, ZRMPO_DEPT_CODE, ZRMPO_DISPUTE_CASE, ZRMPO_EMO_SPACE, ZRMPO_EST_YY, ZRMPO_EXPIRE_DT, ZRMPO_FABRIC_STATUS, ZRMPO_FLEX_RM_ID, ZRMPO_FLEX_STYLE_NUM, ZRMPO_FLOOR_SET, ZRMPO_FLOORSET_NAME, ZRMPO_MATERIAL_DESC, ZRMPO_MATERIAL_TYPE, ZRMPO_MATERIAL_TYPE_COLOR, ZRMPO_MATERIAL_WIDTH, ZRMPO_MONTH_TILL_EXPIRE, ZRMPO_ORIG_COMMITT_TYPE, ZRMPO_ORIG_OWNERSHIP, ZRMPO_ORIG_SEASON, ZRMPO_ORIG_VALUE, ZRMPO_PENDING_UPD, ZRMPO_PLAN_COLLECTION, ZRMPO_PLAN_GRP, ZRMPO_PLAN_ORDER_DT, ZRMPO_PO_COLOR_DESC, ZRMPO_PREV_OWNERSHIP, ZRMPO_QUALITY_NUM, ZRMPO_READY_DT, ZRMPO_RISK_AMOUNT, ZRMPO_RISK_PERCENT, ZRMPO_SHARED_RM, ZRMPO_STAT, ZRMPO_STYLE_DESC, ZRMPO_SUBBRAND, ZRMPO_SUP_COST, ZRMPO_SUP_EXPIRE_DT, ZRMPO_SUP_NOTES, ZRMPO_SUP_READY_DT, ZRMPO_SUPPLIER, ZRMPO_TO_PARTY, ZRMPO_TOTAL_VALUE, ZRMPO_UNITS_YY, ZRMPO_UOM, ZRMPO_UPDATE_DT, ZRMPO_UPDATE_DT_USR, ZRMPO_UPDATE_STAT, ZRPO_BRAND, ZRPO_BUY_STRATEGY, ZRPO_DEPT_CAT, ZRPO_DEPT_CODE, ZRPO_EST_COST, ZRPO_FISCAL_MONTH, ZRPO_FLEX_STYLE, ZRPO_GENERIC, ZRPO_HQ_VENDOR, ZRPO_ORDER_DATE, ZRPO_ORDER_QTY, ZRPO_ORDER_STATUS, ZRPO_RAPID_PO, ZRPO_REASON_DESC, ZSC_SEASON_CODE, ACT_GAC_DT, ZSCV_CATEGORY, ZSCV_CUT_QTY, ZSCV_DELETED, ZSCV_DEPARTMENT, ZSCV_DIVISION, ZSCV_FSET_DATE, ZSCV_LICS_PONUM, ZSCV_LZ_MCQ_AMT, ZSCV_LZ_MCQ_CONF, ZSCV_LZ_QT_PRICE, ZSCV_LZA_CALLOUT, ZSCV_MARKUP, ZSCV_MAST_FLAG, ZSCV_PACK_DATE, ZSCV_PACK_QTY, ZSCV_PO_ACK, ZSCV_PO_MULTIPLE, ZSCV_PO_PRICE, ZSCV_PO_STATUS, ZSCV_PRODOFFICE, ZSCV_REVISEDGAC, ZSCV_REVISEDNDC, ZSCV_REVISEMODE, ZSCV_RM_VENDNAME, ZSCV_RM_VENDNUM, ZSCV_SEASON, ZSCV_SEW_ADATE, ZSCV_SEW_PDATE, ZSCV_SEW_QTY, ZSCV_SO_CREATE, ZSCV_SO_MULTIPLE, ZSCV_TBC_STATUS, ZSCV_TOP_DATE, ZSCV_VEND_CLOSE, ZSCV_VEND_FLAG, ZSCV_VEND_REFNUN, ZSCV_XMILL_ADT, ZSCV_XMILL_PDT, ZSCV_YOUR_REF, ZSEASON_CAT, ZSEASON_YEAR, ZSHIP_TO_NAME, ZVENDOR_REF_NUM, ZVSDSKU, EH_CREATED_DATE
FROM SCCT_WORK_DB.SCEM_TRACKING_OBJECTS_TMP
WHERE DI_OPERATION_TYPE IN ('i', 'u', 'e'));
! echo completed loading into scct_db.scem_expected_events;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_events -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_events  ---source: sap_stage0 table : scem_tracking_events_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_events ---load type: full ---back posting: no --------------------------------------------------------------------------  -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 23jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti changed patition key to eh_created_date and added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic --  ------------------------- ---job related hive settings to merge files for target table ------------------------- set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_events target table data -------------------------  set tez.job.name=stage2:scem_tracking_events :load target table scem_tracking_events ;
! echo started loading into scct_db.scem_tracking_events ;
! echo completed loading into scct_db.scem_tracking_events ;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_objects -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_objects  ---source: scct_work_db table : scem_tracking_objects_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_objects ---load type: full ---back posting: no ---author: sdhal ---created date: 29/07/2019 -------------------------------------------------------------------------- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 29jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 19sep19 scctp4-2460 ryadav increased tez container size -- 01.00.00.04 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic -- 01.00.00.05 21jan20 scctp4-2988 akoti changed column name from act_po_gr_date to act_gac_date --  ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_objects target table data -------------------------  set tez.job.name=stage2:scem_tracking_objects :load target table scem_tracking_objects ;
! echo started loading into scct_db.scem_tracking_objects ;
INSERT OVERWRITE INTO SCCT_DB.SCEM_TRACKING_OBJECTS
(SELECT APO_INTEGR, ARRIV_HARB, ARRIV_LOC, ASN_DEL_DT, ASN_QUANT, BATCH, BOM, BOM_ALT, BOOKING_ID, BRAND, BW_PROFILE, CARRIER, CARRIER_P, CARRIER_S, CFS_BYPASS, CFS_HOLD, CHOICE, CLASS, COMPANY_CODE, CONTAINER_NUMBER, CARGO_RECEIPT_QTY_TOTAL_CTN, CURRENCY, CUSTOMS_DIST_ID, DAILY_CUT_DATE, DAILY_CUT_QTY, DAILY_FD_OFF_DT, DAILY_FD_OFF_QT, DEL_DATE, DEL_PRIORITY, DELET_IND, DELIV_LOC, DELV_NUMB, DEPT_LOC, DOX_RECV_DATE, DOX_RECV_TIME, DEPARTURE_QTY_TOTAL_CTN, DSR_COMMENTS, DSR_ETA_FINAL, DSR_ETA_CFS, DSR_REASON_CODE, DSR_REASON_DESC, DSR_STATUS, EH_ACTIVE_DATE, EH_ACTIVE_TIME, EH_APPOBJID, EH_APPOBJTYPE, EH_APPSYS, EH_CREATED_BY, EH_CREATED_DATE_EST, EH_CREATED_TIME_EST, EH_CREATED_DATE_TIME_EST, EH_DURATION_01, EH_DURATION_02, EH_DURATION_03, EH_DURATION_04, EH_DURATION_05, EH_GUID, EH_INACTIVE_DATE, EH_INACTIVE_TIME, EH_PROC_MODE, EH_START_DATE, EH_START_TIME, EH_TRKID_CD, EH_TRKID_CS, EH_TYPE, EH_TZONE, EH_UPDATED_BY, EH_UPDATED_DATE_EST, EH_UPDATED_TIME_EST, EH_UPDATED_DATE_TIME_EST, EM_CLIENT, ENTERPRISE, EQUIPMENT, EXPLOSION_DT, FCR_NUMBER, DEPARTURE_EDI_FINL_DEST_ETA_DT, FINL_DEST_LOC_ID, FUNCT_LOC, GR_QUANT, HAWBFCR, INCO1, INCO2, INFO_TYPE, INSP_LOT, INV_DOC_NO, ITM_CAT, LBI_OWNERSHIP_DT, MATERIAL, MAWB, MRP_AREA, MRP_CONTRL, NET_PRICE, NOT_CAT_TYPE, NOT_CODE_GRP, NOT_CODING, NOT_FINISH_DT, NOT_START_DT, NOT_TYPE, OBOL, OI_EBELN, ORD_CATEG, ORD_TYPE, ORDER_RS_CODE, PACK_DATE_DAILY, PACK_QTY_DAILY, PLANNED_MODE, PLANT, PLN_FINISH_DT, PLN_START_DT, PO_CREATOR, PO_DELI_DT, PO_DOC_DT, POI_ACK_QT, POI_QUANT, PORT_ENT_ETA_DAT, PORT_ENT_LOC_ID, PORT_LAD_ETD_DT, PORT_OF_DISCHARG, PORT_OF_ENTRY, PORT_OF_LADING, PRIORITY, PROD_SCHED, PROD_ST_DT_ACT, PROD_ST_DT_PLN, PROD_VERS, PRT_DISCH_ETA_DT, PRT_DISCH_LOC_ID, PURCH_GRP, PURCH_ORD_LN_NO, PURCH_ORG, QUANTITY, QUANTITY_UNIT, REPORT_BY, REQ_DEL_DATE, REQUISIT, RESP_PARTNER, ROCANCEL, ROUT_CNT, ROUT_GRP, ROUTE, ROUTE_P, ROUTE_S, SALESORDER_NO, SECTOR, SHIP_TO_PARTY, SHIPMT_TYPE, SHIPPING_COND, SHIPPING_POINT, SHIPPING_TYPE, SOLD_TO_PARTY, STYLE, TOTAL_VOL_UNIT, TOTAL_VOLUME, TOTAL_WEIGHT, TOTAL_WGHT_UNIT, TRAILER_NUMBER, TRANS_MODE, UNLOADING_POINT, UOM, VENDOR, VENDOR_COUNTRY, VENDOR_NAME, VENDOR_ORD_TYPE, VENDOR_SEASON, VENDOR_YEAR, VESSEL_NAME, VOYAGE, WORK_CENTER, Z_AMOUNT_CC, Z_DATE1, Z_DATE1_SUP, Z_DATE2, Z_DATE2_SUP, Z_DATE3, Z_DATE3_SUP, Z_DATE4, Z_DATE4_SUP, Z_DATE5, Z_DATE5_SUP, Z_INSUFFICIENT_GREIGE, Z_NOT_ASSIGNED, Z_QTY1, Z_QTY1_SUP, Z_QTY2, Z_QTY2_SUP, Z_QTY3, Z_QTY3_SUP, Z_QTY4, Z_QTY4_SUP, Z_QTY5, Z_QTY5_SUP, Z_RMPO_BRAND, Z_RMPO_BUY_STRATEGY, Z_RMPO_CATEGORY, Z_RMPO_DEPT_CODE, Z_RMPO_FABRIC_STATUS, Z_RMPO_FLOOR_SET, Z_RMPO_FLOORSET_NAME, Z_RMPO_MATERIAL_TYPE_COLOR, Z_RMPO_ORDER_DATE, Z_RMPO_ORDER_QTY, Z_RMPO_ORIG_COMMITT_TYPE, Z_RMPO_PLAN_GRP, Z_RMPO_QUALITY_NUM, Z_RMPO_STAT, Z_RMPO_SUPPLIER, Z_RMPO_VENDOR, Z_ROLLING, Z_VENDOR, Z_VENDOR_RMPO, Z_VPO, ZCOLLECTION, ZCOLOR_MRKT, CPO_NUMBER, ZDELIVERY_IND, ZFACTORY_COST, ZFIRST_COST, ZFLEX_OPTIVA, ZLANDED_COST, ZLBI_CNF_GAC, ZLBI_CNF_MODE, ZLBI_CNF_NDC, ZLBI_CNF_STAT, ZLBI_IN_MODE, ZLBI_NEW_AIR_NDC, ZLBI_NEW_OCN_NDC, ZLBI_PROD_DELAY, ZLBI_REMARKS_CNTRL, ZLBI_SO_NO, ZLBI_SO_PO_PREV, ZLBI_SOPO_DATE, ZLBI_TOT_CO_QTY, ZLBI_TOT_QTY, ZLBI_VQ_GAC, ZLBI_VQ_MODE, ZLBI_VQ_NDC, ZLBI_VQ_STAT, ZORIG_LBI_OWN_DT, ZORIGINAL_GAC, ZORIGINAL_NDC, ZREADY_DATE, ZREADY_QT, ZRETAIL_PRICE, ZRMPO_AGE, ZRMPO_BRAND, ZRMPO_BUY_STRATEGY, ZRMPO_C_LIABILITY_TYP, ZRMPO_CATEGORY, ZRMPO_CLOSE_DT, ZRMPO_COLLECTION, ZRMPO_COLOR, ZRMPO_COLOR_DESC, ZRMPO_COST, ZRMPO_CREATE_DT, ZRMPO_CUR_OWNERSHIP, ZRMPO_DEPT_CODE, ZRMPO_DISPUTE_CASE, ZRMPO_EMO_SPACE, ZRMPO_EST_YY, ZRMPO_EXPIRE_DT, ZRMPO_FABRIC_STATUS, ZRMPO_FLEX_RM_ID, ZRMPO_FLEX_STYLE_NUM, ZRMPO_FLOOR_SET, ZRMPO_FLOORSET_NAME, ZRMPO_MATERIAL_DESC, ZRMPO_MATERIAL_TYPE, ZRMPO_MATERIAL_TYPE_COLOR, ZRMPO_MATERIAL_WIDTH, ZRMPO_MONTH_TILL_EXPIRE, ZRMPO_ORIG_COMMITT_TYPE, ZRMPO_ORIG_OWNERSHIP, ZRMPO_ORIG_SEASON, ZRMPO_ORIG_VALUE, ZRMPO_PENDING_UPD, ZRMPO_PLAN_COLLECTION, ZRMPO_PLAN_GRP, ZRMPO_PLAN_ORDER_DT, ZRMPO_PO_COLOR_DESC, ZRMPO_PREV_OWNERSHIP, ZRMPO_QUALITY_NUM, ZRMPO_READY_DT, ZRMPO_RISK_AMOUNT, ZRMPO_RISK_PERCENT, ZRMPO_SHARED_RM, ZRMPO_STAT, ZRMPO_STYLE_DESC, ZRMPO_SUBBRAND, ZRMPO_SUP_COST, ZRMPO_SUP_EXPIRE_DT, ZRMPO_SUP_NOTES, ZRMPO_SUP_READY_DT, ZRMPO_SUPPLIER, ZRMPO_TO_PARTY, ZRMPO_TOTAL_VALUE, ZRMPO_UNITS_YY, ZRMPO_UOM, ZRMPO_UPDATE_DT, ZRMPO_UPDATE_DT_USR, ZRMPO_UPDATE_STAT, ZRPO_BRAND, ZRPO_BUY_STRATEGY, ZRPO_DEPT_CAT, ZRPO_DEPT_CODE, ZRPO_EST_COST, ZRPO_FISCAL_MONTH, ZRPO_FLEX_STYLE, ZRPO_GENERIC, ZRPO_HQ_VENDOR, ZRPO_ORDER_DATE, ZRPO_ORDER_QTY, ZRPO_ORDER_STATUS, ZRPO_RAPID_PO, ZRPO_REASON_DESC, ZSC_SEASON_CODE, ACT_GAC_DT, ZSCV_CATEGORY, ZSCV_CUT_QTY, ZSCV_DELETED, ZSCV_DEPARTMENT, ZSCV_DIVISION, ZSCV_FSET_DATE, ZSCV_LICS_PONUM, ZSCV_LZ_MCQ_AMT, ZSCV_LZ_MCQ_CONF, ZSCV_LZ_QT_PRICE, ZSCV_LZA_CALLOUT, ZSCV_MARKUP, ZSCV_MAST_FLAG, ZSCV_PACK_DATE, ZSCV_PACK_QTY, ZSCV_PO_ACK, ZSCV_PO_MULTIPLE, ZSCV_PO_PRICE, ZSCV_PO_STATUS, ZSCV_PRODOFFICE, ZSCV_REVISEDGAC, ZSCV_REVISEDNDC, ZSCV_REVISEMODE, ZSCV_RM_VENDNAME, ZSCV_RM_VENDNUM, ZSCV_SEASON, ZSCV_SEW_ADATE, ZSCV_SEW_PDATE, ZSCV_SEW_QTY, ZSCV_SO_CREATE, ZSCV_SO_MULTIPLE, ZSCV_TBC_STATUS, ZSCV_TOP_DATE, ZSCV_VEND_CLOSE, ZSCV_VEND_FLAG, ZSCV_VEND_REFNUN, ZSCV_XMILL_ADT, ZSCV_XMILL_PDT, ZSCV_YOUR_REF, ZSEASON_CAT, ZSEASON_YEAR, ZSHIP_TO_NAME, ZVENDOR_REF_NUM, ZVSDSKU, EH_CREATED_DATE
FROM SCCT_WORK_DB.SCEM_TRACKING_OBJECTS_TMP
WHERE DI_OPERATION_TYPE IN ('i', 'u', 'e'));
! echo completed loading into scct_db.scem_expected_events;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_events -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_events  ---source: sap_stage0 table : scem_tracking_events_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_events ---load type: full ---back posting: no --------------------------------------------------------------------------  -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 23jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti changed patition key to eh_created_date and added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic --  ------------------------- ---job related hive settings to merge files for target table ------------------------- set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_events target table data -------------------------  set tez.job.name=stage2:scem_tracking_events :load target table scem_tracking_events ;
! echo started loading into scct_db.scem_tracking_events ;
! echo completed loading into scct_db.scem_tracking_events ;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_objects -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_objects  ---source: scct_work_db table : scem_tracking_objects_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_objects ---load type: full ---back posting: no ---author: sdhal ---created date: 29/07/2019 -------------------------------------------------------------------------- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 29jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 19sep19 scctp4-2460 ryadav increased tez container size -- 01.00.00.04 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic -- 01.00.00.05 21jan20 scctp4-2988 akoti changed column name from act_po_gr_date to act_gac_date --  ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_objects target table data -------------------------  set tez.job.name=stage2:scem_tracking_objects :load target table scem_tracking_objects ;
! echo started loading into scct_db.scem_tracking_objects ;
INSERT OVERWRITE INTO SCCT_DB.SCEM_TRACKING_OBJECTS
(SELECT APO_INTEGR, ARRIV_HARB, ARRIV_LOC, ASN_DEL_DT, ASN_QUANT, BATCH, BOM, BOM_ALT, BOOKING_ID, BRAND, BW_PROFILE, CARRIER, CARRIER_P, CARRIER_S, CFS_BYPASS, CFS_HOLD, CHOICE, CLASS, COMPANY_CODE, CONTAINER_NUMBER, CARGO_RECEIPT_QTY_TOTAL_CTN, CURRENCY, CUSTOMS_DIST_ID, DAILY_CUT_DATE, DAILY_CUT_QTY, DAILY_FD_OFF_DT, DAILY_FD_OFF_QT, DEL_DATE, DEL_PRIORITY, DELET_IND, DELIV_LOC, DELV_NUMB, DEPT_LOC, DOX_RECV_DATE, DOX_RECV_TIME, DEPARTURE_QTY_TOTAL_CTN, DSR_COMMENTS, DSR_ETA_FINAL, DSR_ETA_CFS, DSR_REASON_CODE, DSR_REASON_DESC, DSR_STATUS, EH_ACTIVE_DATE, EH_ACTIVE_TIME, EH_APPOBJID, EH_APPOBJTYPE, EH_APPSYS, EH_CREATED_BY, EH_CREATED_DATE_EST, EH_CREATED_TIME_EST, EH_CREATED_DATE_TIME_EST, EH_DURATION_01, EH_DURATION_02, EH_DURATION_03, EH_DURATION_04, EH_DURATION_05, EH_GUID, EH_INACTIVE_DATE, EH_INACTIVE_TIME, EH_PROC_MODE, EH_START_DATE, EH_START_TIME, EH_TRKID_CD, EH_TRKID_CS, EH_TYPE, EH_TZONE, EH_UPDATED_BY, EH_UPDATED_DATE_EST, EH_UPDATED_TIME_EST, EH_UPDATED_DATE_TIME_EST, EM_CLIENT, ENTERPRISE, EQUIPMENT, EXPLOSION_DT, FCR_NUMBER, DEPARTURE_EDI_FINL_DEST_ETA_DT, FINL_DEST_LOC_ID, FUNCT_LOC, GR_QUANT, HAWBFCR, INCO1, INCO2, INFO_TYPE, INSP_LOT, INV_DOC_NO, ITM_CAT, LBI_OWNERSHIP_DT, MATERIAL, MAWB, MRP_AREA, MRP_CONTRL, NET_PRICE, NOT_CAT_TYPE, NOT_CODE_GRP, NOT_CODING, NOT_FINISH_DT, NOT_START_DT, NOT_TYPE, OBOL, OI_EBELN, ORD_CATEG, ORD_TYPE, ORDER_RS_CODE, PACK_DATE_DAILY, PACK_QTY_DAILY, PLANNED_MODE, PLANT, PLN_FINISH_DT, PLN_START_DT, PO_CREATOR, PO_DELI_DT, PO_DOC_DT, POI_ACK_QT, POI_QUANT, PORT_ENT_ETA_DAT, PORT_ENT_LOC_ID, PORT_LAD_ETD_DT, PORT_OF_DISCHARG, PORT_OF_ENTRY, PORT_OF_LADING, PRIORITY, PROD_SCHED, PROD_ST_DT_ACT, PROD_ST_DT_PLN, PROD_VERS, PRT_DISCH_ETA_DT, PRT_DISCH_LOC_ID, PURCH_GRP, PURCH_ORD_LN_NO, PURCH_ORG, QUANTITY, QUANTITY_UNIT, REPORT_BY, REQ_DEL_DATE, REQUISIT, RESP_PARTNER, ROCANCEL, ROUT_CNT, ROUT_GRP, ROUTE, ROUTE_P, ROUTE_S, SALESORDER_NO, SECTOR, SHIP_TO_PARTY, SHIPMT_TYPE, SHIPPING_COND, SHIPPING_POINT, SHIPPING_TYPE, SOLD_TO_PARTY, STYLE, TOTAL_VOL_UNIT, TOTAL_VOLUME, TOTAL_WEIGHT, TOTAL_WGHT_UNIT, TRAILER_NUMBER, TRANS_MODE, UNLOADING_POINT, UOM, VENDOR, VENDOR_COUNTRY, VENDOR_NAME, VENDOR_ORD_TYPE, VENDOR_SEASON, VENDOR_YEAR, VESSEL_NAME, VOYAGE, WORK_CENTER, Z_AMOUNT_CC, Z_DATE1, Z_DATE1_SUP, Z_DATE2, Z_DATE2_SUP, Z_DATE3, Z_DATE3_SUP, Z_DATE4, Z_DATE4_SUP, Z_DATE5, Z_DATE5_SUP, Z_INSUFFICIENT_GREIGE, Z_NOT_ASSIGNED, Z_QTY1, Z_QTY1_SUP, Z_QTY2, Z_QTY2_SUP, Z_QTY3, Z_QTY3_SUP, Z_QTY4, Z_QTY4_SUP, Z_QTY5, Z_QTY5_SUP, Z_RMPO_BRAND, Z_RMPO_BUY_STRATEGY, Z_RMPO_CATEGORY, Z_RMPO_DEPT_CODE, Z_RMPO_FABRIC_STATUS, Z_RMPO_FLOOR_SET, Z_RMPO_FLOORSET_NAME, Z_RMPO_MATERIAL_TYPE_COLOR, Z_RMPO_ORDER_DATE, Z_RMPO_ORDER_QTY, Z_RMPO_ORIG_COMMITT_TYPE, Z_RMPO_PLAN_GRP, Z_RMPO_QUALITY_NUM, Z_RMPO_STAT, Z_RMPO_SUPPLIER, Z_RMPO_VENDOR, Z_ROLLING, Z_VENDOR, Z_VENDOR_RMPO, Z_VPO, ZCOLLECTION, ZCOLOR_MRKT, CPO_NUMBER, ZDELIVERY_IND, ZFACTORY_COST, ZFIRST_COST, ZFLEX_OPTIVA, ZLANDED_COST, ZLBI_CNF_GAC, ZLBI_CNF_MODE, ZLBI_CNF_NDC, ZLBI_CNF_STAT, ZLBI_IN_MODE, ZLBI_NEW_AIR_NDC, ZLBI_NEW_OCN_NDC, ZLBI_PROD_DELAY, ZLBI_REMARKS_CNTRL, ZLBI_SO_NO, ZLBI_SO_PO_PREV, ZLBI_SOPO_DATE, ZLBI_TOT_CO_QTY, ZLBI_TOT_QTY, ZLBI_VQ_GAC, ZLBI_VQ_MODE, ZLBI_VQ_NDC, ZLBI_VQ_STAT, ZORIG_LBI_OWN_DT, ZORIGINAL_GAC, ZORIGINAL_NDC, ZREADY_DATE, ZREADY_QT, ZRETAIL_PRICE, ZRMPO_AGE, ZRMPO_BRAND, ZRMPO_BUY_STRATEGY, ZRMPO_C_LIABILITY_TYP, ZRMPO_CATEGORY, ZRMPO_CLOSE_DT, ZRMPO_COLLECTION, ZRMPO_COLOR, ZRMPO_COLOR_DESC, ZRMPO_COST, ZRMPO_CREATE_DT, ZRMPO_CUR_OWNERSHIP, ZRMPO_DEPT_CODE, ZRMPO_DISPUTE_CASE, ZRMPO_EMO_SPACE, ZRMPO_EST_YY, ZRMPO_EXPIRE_DT, ZRMPO_FABRIC_STATUS, ZRMPO_FLEX_RM_ID, ZRMPO_FLEX_STYLE_NUM, ZRMPO_FLOOR_SET, ZRMPO_FLOORSET_NAME, ZRMPO_MATERIAL_DESC, ZRMPO_MATERIAL_TYPE, ZRMPO_MATERIAL_TYPE_COLOR, ZRMPO_MATERIAL_WIDTH, ZRMPO_MONTH_TILL_EXPIRE, ZRMPO_ORIG_COMMITT_TYPE, ZRMPO_ORIG_OWNERSHIP, ZRMPO_ORIG_SEASON, ZRMPO_ORIG_VALUE, ZRMPO_PENDING_UPD, ZRMPO_PLAN_COLLECTION, ZRMPO_PLAN_GRP, ZRMPO_PLAN_ORDER_DT, ZRMPO_PO_COLOR_DESC, ZRMPO_PREV_OWNERSHIP, ZRMPO_QUALITY_NUM, ZRMPO_READY_DT, ZRMPO_RISK_AMOUNT, ZRMPO_RISK_PERCENT, ZRMPO_SHARED_RM, ZRMPO_STAT, ZRMPO_STYLE_DESC, ZRMPO_SUBBRAND, ZRMPO_SUP_COST, ZRMPO_SUP_EXPIRE_DT, ZRMPO_SUP_NOTES, ZRMPO_SUP_READY_DT, ZRMPO_SUPPLIER, ZRMPO_TO_PARTY, ZRMPO_TOTAL_VALUE, ZRMPO_UNITS_YY, ZRMPO_UOM, ZRMPO_UPDATE_DT, ZRMPO_UPDATE_DT_USR, ZRMPO_UPDATE_STAT, ZRPO_BRAND, ZRPO_BUY_STRATEGY, ZRPO_DEPT_CAT, ZRPO_DEPT_CODE, ZRPO_EST_COST, ZRPO_FISCAL_MONTH, ZRPO_FLEX_STYLE, ZRPO_GENERIC, ZRPO_HQ_VENDOR, ZRPO_ORDER_DATE, ZRPO_ORDER_QTY, ZRPO_ORDER_STATUS, ZRPO_RAPID_PO, ZRPO_REASON_DESC, ZSC_SEASON_CODE, ACT_GAC_DT, ZSCV_CATEGORY, ZSCV_CUT_QTY, ZSCV_DELETED, ZSCV_DEPARTMENT, ZSCV_DIVISION, ZSCV_FSET_DATE, ZSCV_LICS_PONUM, ZSCV_LZ_MCQ_AMT, ZSCV_LZ_MCQ_CONF, ZSCV_LZ_QT_PRICE, ZSCV_LZA_CALLOUT, ZSCV_MARKUP, ZSCV_MAST_FLAG, ZSCV_PACK_DATE, ZSCV_PACK_QTY, ZSCV_PO_ACK, ZSCV_PO_MULTIPLE, ZSCV_PO_PRICE, ZSCV_PO_STATUS, ZSCV_PRODOFFICE, ZSCV_REVISEDGAC, ZSCV_REVISEDNDC, ZSCV_REVISEMODE, ZSCV_RM_VENDNAME, ZSCV_RM_VENDNUM, ZSCV_SEASON, ZSCV_SEW_ADATE, ZSCV_SEW_PDATE, ZSCV_SEW_QTY, ZSCV_SO_CREATE, ZSCV_SO_MULTIPLE, ZSCV_TBC_STATUS, ZSCV_TOP_DATE, ZSCV_VEND_CLOSE, ZSCV_VEND_FLAG, ZSCV_VEND_REFNUN, ZSCV_XMILL_ADT, ZSCV_XMILL_PDT, ZSCV_YOUR_REF, ZSEASON_CAT, ZSEASON_YEAR, ZSHIP_TO_NAME, ZVENDOR_REF_NUM, ZVSDSKU, EH_CREATED_DATE
FROM SCCT_WORK_DB.SCEM_TRACKING_OBJECTS_TMP
WHERE DI_OPERATION_TYPE IN ('i', 'u', 'e'));
! echo completed loading into scct_db.scem_expected_events;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_events -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_events  ---source: sap_stage0 table : scem_tracking_events_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_events ---load type: full ---back posting: no --------------------------------------------------------------------------  -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 23jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti changed patition key to eh_created_date and added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic --  ------------------------- ---job related hive settings to merge files for target table ------------------------- set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_events target table data -------------------------  set tez.job.name=stage2:scem_tracking_events :load target table scem_tracking_events ;
! echo started loading into scct_db.scem_tracking_events ;
! echo completed loading into scct_db.scem_tracking_events ;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_objects -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_objects  ---source: scct_work_db table : scem_tracking_objects_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_objects ---load type: full ---back posting: no ---author: sdhal ---created date: 29/07/2019 -------------------------------------------------------------------------- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 29jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 19sep19 scctp4-2460 ryadav increased tez container size -- 01.00.00.04 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic -- 01.00.00.05 21jan20 scctp4-2988 akoti changed column name from act_po_gr_date to act_gac_date --  ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_objects target table data -------------------------  set tez.job.name=stage2:scem_tracking_objects :load target table scem_tracking_objects ;
! echo started loading into scct_db.scem_tracking_objects ;
INSERT OVERWRITE INTO SCCT_DB.SCEM_TRACKING_OBJECTS
(SELECT APO_INTEGR, ARRIV_HARB, ARRIV_LOC, ASN_DEL_DT, ASN_QUANT, BATCH, BOM, BOM_ALT, BOOKING_ID, BRAND, BW_PROFILE, CARRIER, CARRIER_P, CARRIER_S, CFS_BYPASS, CFS_HOLD, CHOICE, CLASS, COMPANY_CODE, CONTAINER_NUMBER, CARGO_RECEIPT_QTY_TOTAL_CTN, CURRENCY, CUSTOMS_DIST_ID, DAILY_CUT_DATE, DAILY_CUT_QTY, DAILY_FD_OFF_DT, DAILY_FD_OFF_QT, DEL_DATE, DEL_PRIORITY, DELET_IND, DELIV_LOC, DELV_NUMB, DEPT_LOC, DOX_RECV_DATE, DOX_RECV_TIME, DEPARTURE_QTY_TOTAL_CTN, DSR_COMMENTS, DSR_ETA_FINAL, DSR_ETA_CFS, DSR_REASON_CODE, DSR_REASON_DESC, DSR_STATUS, EH_ACTIVE_DATE, EH_ACTIVE_TIME, EH_APPOBJID, EH_APPOBJTYPE, EH_APPSYS, EH_CREATED_BY, EH_CREATED_DATE_EST, EH_CREATED_TIME_EST, EH_CREATED_DATE_TIME_EST, EH_DURATION_01, EH_DURATION_02, EH_DURATION_03, EH_DURATION_04, EH_DURATION_05, EH_GUID, EH_INACTIVE_DATE, EH_INACTIVE_TIME, EH_PROC_MODE, EH_START_DATE, EH_START_TIME, EH_TRKID_CD, EH_TRKID_CS, EH_TYPE, EH_TZONE, EH_UPDATED_BY, EH_UPDATED_DATE_EST, EH_UPDATED_TIME_EST, EH_UPDATED_DATE_TIME_EST, EM_CLIENT, ENTERPRISE, EQUIPMENT, EXPLOSION_DT, FCR_NUMBER, DEPARTURE_EDI_FINL_DEST_ETA_DT, FINL_DEST_LOC_ID, FUNCT_LOC, GR_QUANT, HAWBFCR, INCO1, INCO2, INFO_TYPE, INSP_LOT, INV_DOC_NO, ITM_CAT, LBI_OWNERSHIP_DT, MATERIAL, MAWB, MRP_AREA, MRP_CONTRL, NET_PRICE, NOT_CAT_TYPE, NOT_CODE_GRP, NOT_CODING, NOT_FINISH_DT, NOT_START_DT, NOT_TYPE, OBOL, OI_EBELN, ORD_CATEG, ORD_TYPE, ORDER_RS_CODE, PACK_DATE_DAILY, PACK_QTY_DAILY, PLANNED_MODE, PLANT, PLN_FINISH_DT, PLN_START_DT, PO_CREATOR, PO_DELI_DT, PO_DOC_DT, POI_ACK_QT, POI_QUANT, PORT_ENT_ETA_DAT, PORT_ENT_LOC_ID, PORT_LAD_ETD_DT, PORT_OF_DISCHARG, PORT_OF_ENTRY, PORT_OF_LADING, PRIORITY, PROD_SCHED, PROD_ST_DT_ACT, PROD_ST_DT_PLN, PROD_VERS, PRT_DISCH_ETA_DT, PRT_DISCH_LOC_ID, PURCH_GRP, PURCH_ORD_LN_NO, PURCH_ORG, QUANTITY, QUANTITY_UNIT, REPORT_BY, REQ_DEL_DATE, REQUISIT, RESP_PARTNER, ROCANCEL, ROUT_CNT, ROUT_GRP, ROUTE, ROUTE_P, ROUTE_S, SALESORDER_NO, SECTOR, SHIP_TO_PARTY, SHIPMT_TYPE, SHIPPING_COND, SHIPPING_POINT, SHIPPING_TYPE, SOLD_TO_PARTY, STYLE, TOTAL_VOL_UNIT, TOTAL_VOLUME, TOTAL_WEIGHT, TOTAL_WGHT_UNIT, TRAILER_NUMBER, TRANS_MODE, UNLOADING_POINT, UOM, VENDOR, VENDOR_COUNTRY, VENDOR_NAME, VENDOR_ORD_TYPE, VENDOR_SEASON, VENDOR_YEAR, VESSEL_NAME, VOYAGE, WORK_CENTER, Z_AMOUNT_CC, Z_DATE1, Z_DATE1_SUP, Z_DATE2, Z_DATE2_SUP, Z_DATE3, Z_DATE3_SUP, Z_DATE4, Z_DATE4_SUP, Z_DATE5, Z_DATE5_SUP, Z_INSUFFICIENT_GREIGE, Z_NOT_ASSIGNED, Z_QTY1, Z_QTY1_SUP, Z_QTY2, Z_QTY2_SUP, Z_QTY3, Z_QTY3_SUP, Z_QTY4, Z_QTY4_SUP, Z_QTY5, Z_QTY5_SUP, Z_RMPO_BRAND, Z_RMPO_BUY_STRATEGY, Z_RMPO_CATEGORY, Z_RMPO_DEPT_CODE, Z_RMPO_FABRIC_STATUS, Z_RMPO_FLOOR_SET, Z_RMPO_FLOORSET_NAME, Z_RMPO_MATERIAL_TYPE_COLOR, Z_RMPO_ORDER_DATE, Z_RMPO_ORDER_QTY, Z_RMPO_ORIG_COMMITT_TYPE, Z_RMPO_PLAN_GRP, Z_RMPO_QUALITY_NUM, Z_RMPO_STAT, Z_RMPO_SUPPLIER, Z_RMPO_VENDOR, Z_ROLLING, Z_VENDOR, Z_VENDOR_RMPO, Z_VPO, ZCOLLECTION, ZCOLOR_MRKT, CPO_NUMBER, ZDELIVERY_IND, ZFACTORY_COST, ZFIRST_COST, ZFLEX_OPTIVA, ZLANDED_COST, ZLBI_CNF_GAC, ZLBI_CNF_MODE, ZLBI_CNF_NDC, ZLBI_CNF_STAT, ZLBI_IN_MODE, ZLBI_NEW_AIR_NDC, ZLBI_NEW_OCN_NDC, ZLBI_PROD_DELAY, ZLBI_REMARKS_CNTRL, ZLBI_SO_NO, ZLBI_SO_PO_PREV, ZLBI_SOPO_DATE, ZLBI_TOT_CO_QTY, ZLBI_TOT_QTY, ZLBI_VQ_GAC, ZLBI_VQ_MODE, ZLBI_VQ_NDC, ZLBI_VQ_STAT, ZORIG_LBI_OWN_DT, ZORIGINAL_GAC, ZORIGINAL_NDC, ZREADY_DATE, ZREADY_QT, ZRETAIL_PRICE, ZRMPO_AGE, ZRMPO_BRAND, ZRMPO_BUY_STRATEGY, ZRMPO_C_LIABILITY_TYP, ZRMPO_CATEGORY, ZRMPO_CLOSE_DT, ZRMPO_COLLECTION, ZRMPO_COLOR, ZRMPO_COLOR_DESC, ZRMPO_COST, ZRMPO_CREATE_DT, ZRMPO_CUR_OWNERSHIP, ZRMPO_DEPT_CODE, ZRMPO_DISPUTE_CASE, ZRMPO_EMO_SPACE, ZRMPO_EST_YY, ZRMPO_EXPIRE_DT, ZRMPO_FABRIC_STATUS, ZRMPO_FLEX_RM_ID, ZRMPO_FLEX_STYLE_NUM, ZRMPO_FLOOR_SET, ZRMPO_FLOORSET_NAME, ZRMPO_MATERIAL_DESC, ZRMPO_MATERIAL_TYPE, ZRMPO_MATERIAL_TYPE_COLOR, ZRMPO_MATERIAL_WIDTH, ZRMPO_MONTH_TILL_EXPIRE, ZRMPO_ORIG_COMMITT_TYPE, ZRMPO_ORIG_OWNERSHIP, ZRMPO_ORIG_SEASON, ZRMPO_ORIG_VALUE, ZRMPO_PENDING_UPD, ZRMPO_PLAN_COLLECTION, ZRMPO_PLAN_GRP, ZRMPO_PLAN_ORDER_DT, ZRMPO_PO_COLOR_DESC, ZRMPO_PREV_OWNERSHIP, ZRMPO_QUALITY_NUM, ZRMPO_READY_DT, ZRMPO_RISK_AMOUNT, ZRMPO_RISK_PERCENT, ZRMPO_SHARED_RM, ZRMPO_STAT, ZRMPO_STYLE_DESC, ZRMPO_SUBBRAND, ZRMPO_SUP_COST, ZRMPO_SUP_EXPIRE_DT, ZRMPO_SUP_NOTES, ZRMPO_SUP_READY_DT, ZRMPO_SUPPLIER, ZRMPO_TO_PARTY, ZRMPO_TOTAL_VALUE, ZRMPO_UNITS_YY, ZRMPO_UOM, ZRMPO_UPDATE_DT, ZRMPO_UPDATE_DT_USR, ZRMPO_UPDATE_STAT, ZRPO_BRAND, ZRPO_BUY_STRATEGY, ZRPO_DEPT_CAT, ZRPO_DEPT_CODE, ZRPO_EST_COST, ZRPO_FISCAL_MONTH, ZRPO_FLEX_STYLE, ZRPO_GENERIC, ZRPO_HQ_VENDOR, ZRPO_ORDER_DATE, ZRPO_ORDER_QTY, ZRPO_ORDER_STATUS, ZRPO_RAPID_PO, ZRPO_REASON_DESC, ZSC_SEASON_CODE, ACT_GAC_DT, ZSCV_CATEGORY, ZSCV_CUT_QTY, ZSCV_DELETED, ZSCV_DEPARTMENT, ZSCV_DIVISION, ZSCV_FSET_DATE, ZSCV_LICS_PONUM, ZSCV_LZ_MCQ_AMT, ZSCV_LZ_MCQ_CONF, ZSCV_LZ_QT_PRICE, ZSCV_LZA_CALLOUT, ZSCV_MARKUP, ZSCV_MAST_FLAG, ZSCV_PACK_DATE, ZSCV_PACK_QTY, ZSCV_PO_ACK, ZSCV_PO_MULTIPLE, ZSCV_PO_PRICE, ZSCV_PO_STATUS, ZSCV_PRODOFFICE, ZSCV_REVISEDGAC, ZSCV_REVISEDNDC, ZSCV_REVISEMODE, ZSCV_RM_VENDNAME, ZSCV_RM_VENDNUM, ZSCV_SEASON, ZSCV_SEW_ADATE, ZSCV_SEW_PDATE, ZSCV_SEW_QTY, ZSCV_SO_CREATE, ZSCV_SO_MULTIPLE, ZSCV_TBC_STATUS, ZSCV_TOP_DATE, ZSCV_VEND_CLOSE, ZSCV_VEND_FLAG, ZSCV_VEND_REFNUN, ZSCV_XMILL_ADT, ZSCV_XMILL_PDT, ZSCV_YOUR_REF, ZSEASON_CAT, ZSEASON_YEAR, ZSHIP_TO_NAME, ZVENDOR_REF_NUM, ZVSDSKU, EH_CREATED_DATE
FROM SCCT_WORK_DB.SCEM_TRACKING_OBJECTS_TMP
WHERE DI_OPERATION_TYPE IN ('i', 'u', 'e'));
