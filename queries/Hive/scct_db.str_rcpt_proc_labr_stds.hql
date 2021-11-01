---jobname:scct-sap-hive-stg1-scct_db.str_rcpt_proc_labr_stds
--------------------------------------------------------------------------
---Target: STAGE1 table: scct_db.STR_RCPT_PROC_LABR_STDS
---Source: SAP_STAGE0.ZZLSMAINT
---Load Type: New Snapshot Insert
---Back posting: No
--------------------------------------------------------------------------

-- History Information
--
-- Revision    Date     JIRA        Developer-ID  Comments
-- ----------- -------  ----------  ------------ ----------------------------
-- 01.00.00.01 03Nov19  SCCTP4-2797 AKOTI        Initial Version
-- 01.00.00.02 05Dec19  SCCTP4-2797 AKOTI        Added new column PROC_TM_SEC as per user request

-------------------------
---Job related Hive settings to merge files for Target table
-------------------------

set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

-------------------------
---Loading SCCT_DB.STR_RCPT_PROC_LABR_STDS work table data
-------------------------

SET tez.job.name=STAGE1_SCCT_DB:STR_RCPT_PROC_LABR_STDS :load target table STR_RCPT_PROC_LABR_STDS ;
! echo Started Loading into SCCT_DB.STR_RCPT_PROC_LABR_STDS ;

INSERT OVERWRITE TABLE SCCT_DB.STR_RCPT_PROC_LABR_STDS PARTITION(SNAPSHOT_DT)
SELECT 
  COALESCE((CASE WHEN TRIM(ZBRAND) = '' THEN NULL ELSE TRIM(ZBRAND) END),'N/A') AS BRND_CD
, COALESCE((CASE WHEN TRIM(ZSCNUM) = '' THEN NULL ELSE TRIM(ZSCNUM) END),'N/A') AS SCLS_CD
, COALESCE((CASE WHEN TRIM(ZSGR) = '' THEN NULL ELSE TRIM(ZSGR) END),'N/A') AS STR_GRPNG_NBR
, COALESCE((CASE WHEN TRIM(ZMCNUM) = '' THEN NULL ELSE TRIM(ZMCNUM) END),'N/A') AS CTGY_CD
, COALESCE((CASE WHEN TRIM(ZMCDESC) = '' THEN NULL ELSE TRIM(ZMCDESC) END),'N/A') AS CTGY_DESC
, COALESCE((CASE WHEN TRIM(ZCLNUM) = '' THEN NULL ELSE TRIM(ZCLNUM) END),'N/A') AS CLS_CD
, COALESCE((CASE WHEN TRIM(ZCLDESC) = '' THEN NULL ELSE TRIM(ZCLDESC) END),'N/A') AS CLS_DESC
, COALESCE((CASE WHEN TRIM(UNITTIME) = '' THEN NULL ELSE TRIM(UNITTIME) END),'00:00:00') AS PROC_TM
, COALESCE((HOUR(CONCAT('1900-01-01 ',TRIM(UNITTIME)))*60*60)+(MINUTE(CONCAT('1900-01-01 ',TRIM(UNITTIME)))*60) + (SECOND(CONCAT('1900-01-01 ',TRIM(UNITTIME)))), 0) AS PROC_TM_SEC
, COALESCE((CASE WHEN TRIM(CRTUSER) = '' THEN NULL ELSE TRIM(CRTUSER) END),'N/A') AS REC_CRTD_BY
, COALESCE((CASE WHEN TRIM(CRTDATE) = '' THEN NULL ELSE TRIM(CRTDATE) END),'9999-12-31') AS REC_CRTD_DT
, COALESCE((CASE WHEN TRIM(CRTTIME) = '' THEN NULL ELSE TRIM(CRTTIME) END),'00:00:00') AS REC_CRTD_TM
, DATE_FORMAT(DATE_SUB(CURRENT_DATE,1),'yyyy-MM-dd') AS SNAPSHOT_DT
FROM SAP_STAGE0.ZZLSMAINT
;

! echo Completed Loading into SCCT_DB.STR_RCPT_PROC_LABR_STDS ;

 -------------------------
 ---end script
 -------------------------