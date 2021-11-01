---jobname:scct-E2E_Dashboard-hive-stg1-scct_db.prepack_component
--------------------------------------------------------------------------
---Target: STAGE1 table: SCCT_DB.PREPACK_COMPONENT 
---Source: SCCT_DB  table : BOM_ITEM, BOM_DETAIL, analytics_view table : item
---Load Type: Full
---Back posting: No
---Author: ryadav
---Last modified date: 12/18/2018
--------------------------------------------------------------------------

-------------------------
---Loading PREPACK_COMPONENT stage 1 base data
-------------------------

! echo Started Loading into SCCT_DB.PREPACK_COMPONENT ;

INSERT OVERWRITE TABLE SCCT_DB.PREPACK_COMPONENT
SELECT 
COALESCE((CASE WHEN TRIM(A.BOM_ITEM_NBR)='' THEN NULL ELSE TRIM(A.BOM_ITEM_NBR) END),'N/A') AS PPCK_ITM_NBR,
COALESCE((CASE WHEN TRIM(B.BOM_CPNT_ITEM_NBR)='' THEN NULL ELSE TRIM(B.BOM_CPNT_ITEM_NBR) END),'N/A') AS PPCK_CPNT_ITM_NBR,
COALESCE((CASE WHEN B.BOM_CPNT_QTY='' THEN NULL ELSE B.BOM_CPNT_QTY END),0.00) AS PPCK_CPNT_ITEM_QTY,
COALESCE((CASE WHEN trim(B.BOM_CPNT_UNT_OF_MEAS_CD)='' THEN NULL ELSE trim(B.BOM_CPNT_UNT_OF_MEAS_CD) END),'N/A') AS PPCK_CPNT_UNT_OF_MEAS
FROM SCCT_DB.BOM_ITEM A 
INNER JOIN SCCT_DB.BOM_DETAIL B 
ON A.BOM_NBR = B.BOM_NBR
INNER JOIN analytics_view.item C 
ON A.BOM_ITEM_NBR  = C.item_number
WHERE C.item_type_code = 'ZPRE'
GROUP BY A.BOM_ITEM_NBR, 
B.BOM_CPNT_ITEM_NBR,
B.BOM_CPNT_QTY,
B.BOM_CPNT_UNT_OF_MEAS_CD;


! echo Completed Loading into SCCT_DB.PREPACK_COMPONENT ;

 -------------------------
 ---end script
 -------------------------

