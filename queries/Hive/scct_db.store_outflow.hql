---jobname:scct-E2E_Dashboard-hive-stg1-scct_db.store_outflow
--------------------------------------------------------------------------
---Target: STAGE1 table: SCCT_DB.SUPPLY_CHAIN_NODE_DAY_LOC_ITEM 
---Source: SCCT_DB table : INVENTORY_MOVEMENT, analytics_view table : item, location
---Load Type: Full
---Back posting: No
--------------------------------------------------------------------------

-- History Information
--
-- Revision    Date     JIRA       Developer-ID Comments
-- ----------- ------- ----------  ------------ ----------------------------
-- 01.00.00.01 04JAN19             RYADAV       Initial Version
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
SET hive.compute.splits.in.am=false;

-------------------------
---Loading SUPPLY_CHAIN_NODE_DAY_LOC_ITEM work table data
-------------------------

SET tez.job.name=STAGE1:SUPPLY_CHAIN_NODE_DAY_LOC_ITEM :load target table for STORE_OUTFLOW ;
! echo Started Loading into SCCT_DB.SUPPLY_CHAIN_NODE_DAY_LOC_ITEM ;

INSERT OVERWRITE TABLE SCCT_DB.SUPPLY_CHAIN_NODE_DAY_LOC_ITEM PARTITION (POSTING_DT, SUPPLY_CHAIN_NODE_TYPE_ID)
SELECT LOCATION_NBR as LOC_NBR, 
item_nbr as ITM_NBR, 
ORGANIZATION_NBR, 
SUM(QTY), 
SUM(COST), 
SPEED_DAYS, 
POSTING_DT, 
SUPPLY_CHAIN_NODE_TYPE_ID 
FROM
(SELECT 
TRIM(M.LOC_NBR ) AS LOCATION_NBR,
COALESCE(M.ITM_NBR,'0') AS ITEM_NBR,
TRIM(L.ORGANIZATION_NUMBER ) AS ORGANIZATION_NBR,
COALESCE(CASE WHEN MOVMT_TYPE IN ('251','931','941','943','951','955','957','959','961','963','965','967') THEN M.TRX_QTY    ELSE (M.TRX_QTY * -1)  END,0.00) QTY,
COALESCE(CASE WHEN MOVMT_TYPE IN ('251','931','941','943','951','955','957','959','961','963','965','967') THEN CASE WHEN M.TTL_VAL_STK_POSG = 0 THEN 0 ELSE M.TRX_QTY*(M.TTL_VAL_POSG/M.TTL_VAL_STK_POSG) END  ELSE (CASE WHEN M.TTL_VAL_STK_POSG = 0 THEN 0 ELSE M.TRX_QTY*(M.TTL_VAL_POSG/M.TTL_VAL_STK_POSG) END * -1)   END,0.00)  COST,
0 AS SPEED_DAYS,
POSTING_DT,
10 AS SUPPLY_CHAIN_NODE_TYPE_ID
FROM SCCT_DB.INVENTORY_MOVEMENT M 
JOIN  analytics_view.location L
ON M.LOC_NBR = L.location_number
JOIN analytics_view.ITEM X
ON M.ITM_NBR = X.item_number
WHERE M.MOVMT_TYPE IN ('251','252','931','932','941','942','943','944','951','952','955','956','957','958','959','960','961','962','963','964','965','966','967')
AND X.item_type_code ='FERT'
AND L.location_type_code IN ('04')
AND  (L.location_number BETWEEN '0000000001'AND '0000002899'
     OR L.location_number BETWEEN '0000003000'AND '0000007699'
     OR L.location_number BETWEEN '0000007900'AND '0000009999')
AND M.POSTING_DT IN (SELECT JOB_CONTROL_DATE FROM SCCT_DB.SCCT_JOB_CONTROL WHERE RUN_DATE = CURRENT_DATE AND DRIVER_TABLE ='MSEG')
) T
GROUP BY T.LOCATION_NBR, 
T.ITEM_NBR, 
T.ORGANIZATION_NBR, 
T.SPEED_DAYS, 
T.POSTING_DT, 
T.SUPPLY_CHAIN_NODE_TYPE_ID
;

! echo Completed Loading into SCCT_DB.SUPPLY_CHAIN_NODE_DAY_LOC_ITEM ;

 -------------------------
 ---end script
 -------------------------
