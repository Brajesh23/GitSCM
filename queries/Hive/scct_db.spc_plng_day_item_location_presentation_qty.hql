---jobname:scct-ikb-hive-stg2-scct_db.spc_plng_day_item_location_presentation_qty
---------------------------------------------------------------------------
---Target: STAGE1 table: scct_db.SPC_PLNG_DAY_ITEM_LOCATION_PRESENTATION_QTY 
---Source: SCCT_WORK_DB table : SPC_PRESENTATION_TEMP2
---Load Type: Full
---Back posting: No
--------------------------------------------------------------------------

-- History Information
--
-- Revision    Date     JIRA       Developer-ID Comments
-- ----------- ------- ----------  ------------ ----------------------------
-- 01.00.00.01 20NOV18             RYADAV       Initial Version
-- 01.00.00.02 23SEP19 SCCTP4-2471 RYADAV       Mapr Remediation changes for column names, item and location length

-------------------------
---Loading SPC_PLNG_DAY_ITEM_LOCATION_PRESENTATION_QTY stage 1 base data
-------------------------

SET tez.job.name=STAGE1:SPC_PLNG_DAY_ITEM_LOCATION_PRESENTATION_QTY :load target table SPC_PLNG_DAY_ITEM_LOCATION_PRESENTATION_QTY ;
! echo Started Loading into scct_db.SPC_PLNG_DAY_ITEM_LOCATION_PRESENTATION_QTY ;

set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.dynamic.partitions.pernode=20000;

INSERT OVERWRITE TABLE SCCT_DB.SPC_PLNG_DAY_ITEM_LOCATION_PRESENTATION_QTY PARTITION (BUSINESS_DT)
  SELECT LPAD(ITEM_NBR,18,0)
        ,COALESCE(l.location_number,SPT2.LOCATION_NBR,'N/A')
        ,MINIMUM_PRESENTATION_QTY
        ,BUSINESS_DT
  FROM (SELECT LPAD(ITEM_NBR,18,0) ITEM_NBR
        ,LOCATION_NBR
        ,MINIMUM_PRESENTATION_QTY
        ,BUSINESS_DT
  FROM SCCT_WORK_DB.SPC_PRESENTATION_TEMP2 
  WHERE BUSINESS_DT BETWEEN DATE_SUB(CURRENT_DATE,800) AND DATE_SUB(CURRENT_DATE,500))SPT2
  left outer join (select location_number from  analytics_db.location where location_type_code = '04')l 
  on lpad(SPT2.LOCATION_NBR,10,0) = l.location_number;

  INSERT OVERWRITE TABLE SCCT_DB.SPC_PLNG_DAY_ITEM_LOCATION_PRESENTATION_QTY PARTITION (BUSINESS_DT)
  SELECT LPAD(ITEM_NBR,18,0)
        ,COALESCE(l.location_number,SPT2.LOCATION_NBR,'N/A')
        ,MINIMUM_PRESENTATION_QTY
      ,BUSINESS_DT
  FROM (SELECT LPAD(ITEM_NBR,18,0) ITEM_NBR
        ,LOCATION_NBR
        ,MINIMUM_PRESENTATION_QTY
        ,BUSINESS_DT
  FROM SCCT_WORK_DB.SPC_PRESENTATION_TEMP2
  WHERE BUSINESS_DT BETWEEN DATE_SUB(CURRENT_DATE,499) AND DATE_SUB(CURRENT_DATE,300))SPT2
  left outer join (select location_number from  analytics_db.location where location_type_code = '04')l 
  on lpad(SPT2.LOCATION_NBR,10,0) = l.location_number;

  INSERT OVERWRITE TABLE SCCT_DB.SPC_PLNG_DAY_ITEM_LOCATION_PRESENTATION_QTY PARTITION (BUSINESS_DT)
  SELECT LPAD(ITEM_NBR,18,0) 
        ,COALESCE(l.location_number,SPT2.LOCATION_NBR,'N/A')
        ,MINIMUM_PRESENTATION_QTY
      ,BUSINESS_DT
  FROM (SELECT LPAD(ITEM_NBR,18,0) ITEM_NBR
        ,LOCATION_NBR
        ,MINIMUM_PRESENTATION_QTY
        ,BUSINESS_DT
  FROM SCCT_WORK_DB.SPC_PRESENTATION_TEMP2
  WHERE BUSINESS_DT BETWEEN DATE_SUB(CURRENT_DATE,299) AND DATE_SUB(CURRENT_DATE,100))SPT2
  left outer join (select location_number from  analytics_db.location where location_type_code = '04')l 
  on lpad(SPT2.LOCATION_NBR,10,0) = l.location_number;

  INSERT OVERWRITE TABLE SCCT_DB.SPC_PLNG_DAY_ITEM_LOCATION_PRESENTATION_QTY PARTITION (BUSINESS_DT)
  SELECT LPAD(ITEM_NBR,18,0)
        ,COALESCE(l.location_number,SPT2.LOCATION_NBR,'N/A')
        ,MINIMUM_PRESENTATION_QTY
      ,BUSINESS_DT
  FROM (SELECT LPAD(ITEM_NBR,18,0) ITEM_NBR
        ,LOCATION_NBR
        ,MINIMUM_PRESENTATION_QTY
        ,BUSINESS_DT
  FROM SCCT_WORK_DB.SPC_PRESENTATION_TEMP2
  WHERE BUSINESS_DT BETWEEN DATE_SUB(CURRENT_DATE,99) AND DATE_ADD(CURRENT_DATE,100))SPT2
  left outer join (select location_number from  analytics_db.location where location_type_code = '04')l 
  on lpad(SPT2.LOCATION_NBR,10,0) = l.location_number;





! echo Completed Loading into scct_db.SPC_PLNG_DAY_ITEM_LOCATION_PRESENTATION_QTY ;


 -------------------------
 ---end script
 -------------------------