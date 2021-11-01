---jobname:scct-scem-hive-stg1-scct_db.scem_consolidated_view_intl_ib_tracking
--------------------------------------------------------------------------
---Target: Stage1 table: scct_db.scem_consolidated_view_intl_ib_tracking 
---Source: SAP_STAGE0 table : saptrx_eh_hdr_scem1,saptrx_eh_cntrl_scem2
---Description: This script will load the data to target table scct_db.scem_consolidated_view_intl_ib_tracking
---Load Type: Full
---Back posting: No
---Author: akoti
---Created date: 23/07/2019
--------------------------------------------------------------------------

-- History Information
--
-- Revision    Date     JIRA       Developer-ID Comments
-- ----------- ------- ----------  ------------ ----------------------------
-- 01.00.00.01 23JUL19             SDHAL        Initial Version
-- 01.00.00.02 26AUG19 SCCTP4-2295 RYADAV       Added semicolon at the end of HQL
-- 01.00.00.03 06SEP19 SCCTP4-2187 AKOTI        Changed patition key to EH_CREATED_DATE and added logic to ignore records where di_operation_type <> 'D'
-- 01.00.00.04 28NOV19 SCCTP4-2623 AKOTI        Redesigned the code to pick the data from SCEM1 & SCEM2 final table, converted to full refresh
-- 01.00.00.05 21JAN20 SCCTP4-2988 AKOTI        Changed column name from ACT_PO_GR_DATE to ACT_GAC_DATE
--

-------------------------
---Job related Hive settings to merge files for Target table
-------------------------

set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

-------------------------
---Loading scem_consolidated_view_intl_ib_tracking target table data
-------------------------

SET tez.job.name=STAGE2:scem_consolidated_view_intl_ib_tracking :load target table scem_consolidated_view_intl_ib_tracking ;
! echo Started Loading into scct_db.scem_consolidated_view_intl_ib_tracking ;


INSERT OVERWRITE TABLE SCCT_DB.SCEM_CONSOLIDATED_VIEW_INTL_IB_TRACKING
SELECT
  COALESCE(SCEM1.EH_GUID, SCEM2.EH_GUID)
, COALESCE(SCEM1.BOOKING_ID, 'N/A')
, COALESCE(SCEM1.BRAND, 'N/A')
, COALESCE(SCEM1.BW_PROFILE, 'N/A')
, COALESCE(SCEM1.CFS_BYPASS, 'N/A')
, COALESCE(SCEM1.CFS_HOLD, 'N/A')
, COALESCE(SCEM1.COMPANY_CODE, 'N/A')
, COALESCE(SCEM1.CONTAINER_NUMBER, 'N/A')
, COALESCE(SCEM1.CARGO_RECEIPT_QTY_TOTAL_CTN, 'N/A')
, COALESCE(SCEM1.CUSTOMS_DIST_ID, 'N/A')
, COALESCE(SCEM1.DELV_NUMB, 'N/A')
, COALESCE(SCEM1.DEL_DATE, '1900-01-01')
, COALESCE(SCEM1.DEL_PRIORITY, 0.00)
, COALESCE(SCEM1.DOX_RECV_DATE, '1900-01-01')
, COALESCE(SCEM1.DOX_RECV_TIME, '00:00:00')
, COALESCE(SCEM1.DEPARTURE_QTY_TOTAL_CTN, 'N/A')
, COALESCE(SCEM1.DSR_COMMENTS, 'N/A')
, COALESCE(SCEM1.DSR_ETA_FINAL, '1900-01-01 00:00:00')
, COALESCE(SCEM1.DSR_REASON_CODE, 'N/A')
, COALESCE(SCEM1.DSR_REASON_DESC, 'N/A')
, COALESCE(SCEM1.DSR_STATUS, 'N/A')
, COALESCE(SCEM1.EH_APPOBJID, 'N/A')
, COALESCE(SCEM1.EH_APPOBJTYPE, 'N/A')
, COALESCE(SCEM1.EH_APPSYS, 'N/A')
, COALESCE(SCEM1.EH_CREATED_BY, 'N/A')
, COALESCE(SCEM1.EH_CREATED_DATE_EST, '1900-01-01')
, COALESCE(SCEM1.EH_CREATED_TIME_EST, '00:00:00')
, COALESCE(SCEM1.EH_CREATED_DATE_TIME_EST, '1900-01-01 00:00:00')
, COALESCE(SCEM1.EH_UPDATED_DATE_EST, '1900-01-01')
, COALESCE(SCEM1.EH_UPDATED_TIME_EST, '00:00:00')
, COALESCE(SCEM1.EH_UPDATED_DATE_TIME_EST, '1900-01-01 00:00:00')
, COALESCE(SCEM1.EH_TRKID_CD, 'N/A')
, COALESCE(SCEM1.EH_TRKID_CS, 'N/A')
, COALESCE(SCEM1.FCR_NUMBER, 'N/A')
, COALESCE(SCEM1.DEPARTURE_EDI_FINL_DEST_ETA_DT, '1900-01-01')
, COALESCE(SCEM1.FINL_DEST_LOC_ID, 'N/A')
, COALESCE(SCEM1.HAWBFCR, 'N/A')
, COALESCE(SCEM1.INCO1, 'N/A')
, COALESCE(SCEM1.INCO2, 'N/A')
, COALESCE(SCEM1.LBI_OWNERSHIP_DT, '1900-01-01')
, COALESCE(SCEM1.OBOL, 'N/A')
, COALESCE(SCEM1.OI_EBELN, 'N/A')
, COALESCE(SCEM1.PLANT, 'N/A')
, COALESCE(SCEM1.PORT_ENT_ETA_DAT, '1900-01-01')
, COALESCE(SCEM1.PORT_LAD_ETD_DT, '1900-01-01')
, COALESCE(SCEM1.PORT_OF_DISCHARG, 'N/A')
, COALESCE(SCEM1.PORT_OF_ENTRY, 'N/A')
, COALESCE(SCEM1.PORT_OF_LADING, 'N/A')
, COALESCE(SCEM1.PO_DELI_DT, '1900-01-01')
, COALESCE(SCEM1.PRT_DISCH_ETA_DT, '1900-01-01')
, COALESCE(SCEM1.PRT_DISCH_LOC_ID, 'N/A')
, COALESCE(SCEM1.PURCH_GRP, 'N/A')
, COALESCE(SCEM1.PURCH_ORG, 'N/A')
, COALESCE(SCEM1.SHIPPING_COND, 'N/A')
, COALESCE(SCEM1.SHIPPING_POINT, 'N/A')
, COALESCE(SCEM1.SHIPPING_TYPE, 'N/A')
, COALESCE(SCEM1.SHIP_TO_PARTY, 'N/A')
, COALESCE(SCEM1.SOLD_TO_PARTY, 'N/A')
, COALESCE(SCEM1.TOTAL_VOLUME, 0.00)
, COALESCE(SCEM1.TOTAL_VOL_UNIT, 'N/A')
, COALESCE(SCEM1.TOTAL_WEIGHT, 0.00)
, COALESCE(SCEM1.TOTAL_WGHT_UNIT, 'N/A')
, COALESCE(SCEM1.TRAILER_NUMBER, 0.00)
, COALESCE(SCEM1.TRANS_MODE, 'N/A')
, COALESCE(SCEM1.UNLOADING_POINT, 'N/A')
, COALESCE(SCEM1.VENDOR, 'N/A')
, COALESCE(SCEM1.VENDOR_COUNTRY, 'N/A')
, COALESCE(SCEM1.VESSEL_NAME, 'N/A')
, COALESCE(SCEM1.VOYAGE, 'N/A')
, COALESCE(SCEM1.CPO_NUMBER, 'N/A')
, COALESCE(SCEM1.ACT_GAC_DT, '1900-01-01')
, COALESCE(SCEM1.Z_VPO, 'N/A') AS VPO_NUMBER
, COALESCE(SCEM2.DEP_IND, 'N/A')
, COALESCE(SCEM2.ACT_CARGO_RECEIVED_DT_TM, '1900-01-01 00:00:00')
, COALESCE(SCEM2.ACT_DEP_ORIGIN_DT_TM, '1900-01-01 00:00:00')
, COALESCE(SCEM2.ACT_CFS_ARRIVAL_DT_TM, '1900-01-01 00:00:00')
, COALESCE(SCEM2.ACT_CFS_SHIPMENT_DT_TM, '1900-01-01 00:00:00')
, COALESCE(SCEM2.ACT_IN_YARD_DT_TM, '1900-01-01 00:00:00')
, COALESCE(SCEM2.ACT_DC_RECEIPT_DT_TM, '1900-01-01 00:00:00')
FROM SCCT_WORK_DB.SCEM_TRACKING_OBJECTS_SHIP_BW1 SCEM1
FULL OUTER JOIN SCCT_WORK_DB.SCEM_CONSOLIDATED_VIEW_TRANSPOSED_TMP SCEM2
ON TRIM(SCEM1.EH_GUID) = TRIM(SCEM2.EH_GUID)
;

! echo Completed Loading into scct_db.scem_consolidated_view_intl_ib_tracking ;

 -------------------------
 ---end script
 -------------------------