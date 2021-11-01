! echo completed loading into scct_work_db.rockport_cfs_shipment_work;
! echo started loading into scct_db.rockport_cfs_shipment;
! echo completed loading into scct_db.rockport_cfs_shipment;
---jobname:scct-rockport-hive-stg1-scct_db.rockport_freight_cost -------------------------------------------------------------------------- ---target: stage1 table: scct_db.rockport_freight_cost ---source: rockport_stage0 table : trader_srvc_invoice_hdr,trader_srvc_invoice_dtl ---load type: full ---back posting: no ---author - created/updated date - jira ticket ---ssoma - 03/05/2019 -  ---ssoma - 14/06/2019 - scctp4-2012/13/14 ---ssoma - 09/07/2019 - scctp4-2089 --------------------------------------------------------------------------  ------------------------- ---loading work table scct_work_db.trader_srvc_invoice_dtl_work  -------------------------  ! echo started loading into scct_work_db.trader_srvc_invoice_dtl_work;
INSERT OVERWRITE INTO SCCT_WORK_DB.TRADER_SRVC_INVOICE_DTL_WORK
(SELECT R.OWNER, R.INVOICE_REF_NO, R.REC_ID, R.EXPENSE_CODE, R.EXPENSE_CATEGORY, R.CONTRACT_NO, R.ORDER_NO, R.RATE_PER_UNIT, R.UNIT_PRC_BASIS, R.CHARGEABLE_QTY, R.ROE, R.PAYABLE_CURRENCY, R.OTHER_COSTS_AMT, R.ADJUSTMENT, R.BL_REF_NO, R.SHIPMENT_NO, R.ACCOUNT_CODE, R.EQUIP_ID, R.EQUIP_SIZE, R.TAX_INDICATOR, R.ORIGIN_COUNTRY, R.TAX_TYPE, R.TAX_AMOUNT, R.TAX_INVOICE_NO, R.P_LINK_KEY_VAL, R.FCR_NO, R.LAST_USER, R.LAST_UPDATE, R.LAST_ACTIVITY, R.CURRENCY
FROM (SELECT TRIM( OWNER) AS OWNER, TRIM( INVOICE_REF_NO) AS INVOICE_REF_NO, TRIM( REC_ID) AS REC_ID, EXPENSE_CODE, EXPENSE_CATEGORY, CONTRACT_NO, ORDER_NO, RATE_PER_UNIT, UNIT_PRC_BASIS, CHARGEABLE_QTY, ROE, PAYABLE_CURRENCY, OTHER_COSTS_AMT, ADJUSTMENT, BL_REF_NO, SHIPMENT_NO, ACCOUNT_CODE, EQUIP_ID, EQUIP_SIZE, TAX_INDICATOR, ORIGIN_COUNTRY, TAX_TYPE, TAX_AMOUNT, TAX_INVOICE_NO, P_LINK_KEY_VAL, FCR_NO, LAST_USER, LAST_UPDATE, LAST_ACTIVITY, CURRENCY, RANK() OVER (PARTITION BY TRIM( OWNER), TRIM( INVOICE_REF_NO), TRIM( REC_ID) ORDER BY LAST_UPDATE DESC) AS RK
FROM ROCKPORT_STAGE0.TRADER_SRVC_INVOICE_DTL) AS R
WHERE RK = 1);
! echo completed loading into scct_work_db.rockport_freight_cost_work;
! echo started loading into scct_db.rockport_freight_cost;
INSERT OVERWRITE INTO SCCT_DB.ROCKPORT_FREIGHT_COST
(SELECT COALESCE(RFCW.OWNER, RFC.OWNER), COALESCE(RFCW.INVOICE_REF_NBR, RFC.INVOICE_REF_NBR), COALESCE(RFCW.INVOICE_TYPE, RFC.INVOICE_TYPE), COALESCE(RFCW.INVOICE_NBR, RFC.INVOICE_NBR), COALESCE(RFCW.CUSTOMER, RFC.CUSTOMER), COALESCE(RFCW.BILL_TO, RFC.BILL_TO), COALESCE(RFCW.ORIGIN_OFFICE, RFC.ORIGIN_OFFICE), COALESCE(RFCW.INVOICE_DT, RFC.INVOICE_DT), COALESCE(RFCW.SHIP_DT, RFC.SHIP_DT), COALESCE(RFCW.CREATE_DT, RFC.CREATE_DT), COALESCE(RFCW.HDR_CURRENCY, RFC.HDR_CURRENCY), COALESCE(RFCW.PAYMENT_TYPE, RFC.PAYMENT_TYPE), COALESCE(RFCW.PAYMENT_TERMS, RFC.PAYMENT_TERMS), COALESCE(RFCW.LOAD_POINT, RFC.LOAD_POINT), COALESCE(RFCW.FINAL_DEST, RFC.FINAL_DEST), COALESCE(RFCW.CARRIER, RFC.CARRIER), COALESCE(RFCW.TRANSP_ID, RFC.TRANSP_ID), COALESCE(RFCW.VOYAGE_NBR, RFC.VOYAGE_NBR), COALESCE(RFCW.TOTAL_GROSS_AMOUNT, RFC.TOTAL_GROSS_AMOUNT), COALESCE(RFCW.TOTAL_ADJUST_AMOUNT, RFC.TOTAL_ADJUST_AMOUNT), COALESCE(RFCW.TOTAL_TAX_AMOUNT, RFC.TOTAL_TAX_AMOUNT), COALESCE(RFCW.TOTAL_NET_AMOUNT, RFC.TOTAL_NET_AMOUNT), COALESCE(RFCW.EDI_IND, RFC.EDI_IND), COALESCE(RFCW.HDR_LAST_USER, RFC.HDR_LAST_USER), COALESCE(RFCW.HDR_LAST_UPDATE_DTM, RFC.HDR_LAST_UPDATE_DTM), COALESCE(RFCW.HDR_LAST_ACTIVITY, RFC.HDR_LAST_ACTIVITY), COALESCE(RFCW.REC_ID, RFC.REC_ID), COALESCE(RFCW.EXPENSE_CD, RFC.EXPENSE_CD), COALESCE(RFCW.EXPENSE_CATEGORY, RFC.EXPENSE_CATEGORY), COALESCE(RFCW.CONTRACT_NBR, RFC.CONTRACT_NBR), COALESCE(RFCW.PO_NBR, RFC.PO_NBR), COALESCE(RFCW.RATE_PER_UNIT, RFC.RATE_PER_UNIT), COALESCE(RFCW.UNIT_PRC_BASIS, RFC.UNIT_PRC_BASIS), COALESCE(RFCW.CHARGEABLE_QTY, RFC.CHARGEABLE_QTY), COALESCE(RFCW.ROE, RFC.ROE), COALESCE(RFCW.PAYABLE_CURRENCY, RFC.PAYABLE_CURRENCY), COALESCE(RFCW.OTHER_COSTS_AMT, RFC.OTHER_COSTS_AMT), COALESCE(RFCW.ADJUSTMENT, RFC.ADJUSTMENT), COALESCE(RFCW.BL_REF_NBR, RFC.BL_REF_NBR), COALESCE(RFCW.SHIPMENT_NBR, RFC.SHIPMENT_NBR), COALESCE(RFCW.ACCOUNT_CD, RFC.ACCOUNT_CD), COALESCE(RFCW.EQUIP_ID, RFC.EQUIP_ID), COALESCE(RFCW.EQUIP_SIZE, RFC.EQUIP_SIZE), COALESCE(RFCW.TAX_IND, RFC.TAX_IND), COALESCE(RFCW.ORIGIN_COUNTRY, RFC.ORIGIN_COUNTRY), COALESCE(RFCW.TAX_TYPE, RFC.TAX_TYPE), COALESCE(RFCW.TAX_AMOUNT, RFC.TAX_AMOUNT), COALESCE(RFCW.TAX_INVOICE_NBR, RFC.TAX_INVOICE_NBR), COALESCE(RFCW.P_LINK_KEY_VAL, RFC.P_LINK_KEY_VAL), COALESCE(RFCW.FCR_NBR, RFC.FCR_NBR), COALESCE(RFCW.DTL_LAST_USER, RFC.DTL_LAST_USER), COALESCE(RFCW.DTL_LAST_UPD_DTM, RFC.DTL_LAST_UPD_DTM), COALESCE(RFCW.DTL_LAST_ACTIVITY, RFC.DTL_LAST_ACTIVITY), COALESCE(RFCW.DTL_CURRENCY, RFC.DTL_CURRENCY)
FROM SCCT_WORK_DB.ROCKPORT_FREIGHT_COST_WORK AS RFCW
FULL JOIN SCCT_DB.ROCKPORT_FREIGHT_COST AS RFC ON RFCW.OWNER = RFC.OWNER AND RFCW.INVOICE_REF_NBR = RFC.INVOICE_REF_NBR AND RFCW.REC_ID = RFC.REC_ID);
! echo completed loading into scct_work_db.rockport_cfs_shipment_work;
! echo started loading into scct_db.rockport_cfs_shipment;
! echo completed loading into scct_db.rockport_cfs_shipment;
---jobname:scct-rockport-hive-stg1-scct_db.rockport_freight_cost -------------------------------------------------------------------------- ---target: stage1 table: scct_db.rockport_freight_cost ---source: rockport_stage0 table : trader_srvc_invoice_hdr,trader_srvc_invoice_dtl ---load type: full ---back posting: no ---author - created/updated date - jira ticket ---ssoma - 03/05/2019 -  ---ssoma - 14/06/2019 - scctp4-2012/13/14 ---ssoma - 09/07/2019 - scctp4-2089 --------------------------------------------------------------------------  ------------------------- ---loading work table scct_work_db.trader_srvc_invoice_dtl_work  -------------------------  ! echo started loading into scct_work_db.trader_srvc_invoice_dtl_work;
INSERT OVERWRITE INTO SCCT_WORK_DB.TRADER_SRVC_INVOICE_DTL_WORK
(SELECT R.OWNER, R.INVOICE_REF_NO, R.REC_ID, R.EXPENSE_CODE, R.EXPENSE_CATEGORY, R.CONTRACT_NO, R.ORDER_NO, R.RATE_PER_UNIT, R.UNIT_PRC_BASIS, R.CHARGEABLE_QTY, R.ROE, R.PAYABLE_CURRENCY, R.OTHER_COSTS_AMT, R.ADJUSTMENT, R.BL_REF_NO, R.SHIPMENT_NO, R.ACCOUNT_CODE, R.EQUIP_ID, R.EQUIP_SIZE, R.TAX_INDICATOR, R.ORIGIN_COUNTRY, R.TAX_TYPE, R.TAX_AMOUNT, R.TAX_INVOICE_NO, R.P_LINK_KEY_VAL, R.FCR_NO, R.LAST_USER, R.LAST_UPDATE, R.LAST_ACTIVITY, R.CURRENCY
FROM (SELECT TRIM( OWNER) AS OWNER, TRIM( INVOICE_REF_NO) AS INVOICE_REF_NO, TRIM( REC_ID) AS REC_ID, EXPENSE_CODE, EXPENSE_CATEGORY, CONTRACT_NO, ORDER_NO, RATE_PER_UNIT, UNIT_PRC_BASIS, CHARGEABLE_QTY, ROE, PAYABLE_CURRENCY, OTHER_COSTS_AMT, ADJUSTMENT, BL_REF_NO, SHIPMENT_NO, ACCOUNT_CODE, EQUIP_ID, EQUIP_SIZE, TAX_INDICATOR, ORIGIN_COUNTRY, TAX_TYPE, TAX_AMOUNT, TAX_INVOICE_NO, P_LINK_KEY_VAL, FCR_NO, LAST_USER, LAST_UPDATE, LAST_ACTIVITY, CURRENCY, RANK() OVER (PARTITION BY TRIM( OWNER), TRIM( INVOICE_REF_NO), TRIM( REC_ID) ORDER BY LAST_UPDATE DESC) AS RK
FROM ROCKPORT_STAGE0.TRADER_SRVC_INVOICE_DTL) AS R
WHERE RK = 1);
! echo completed loading into scct_work_db.rockport_freight_cost_work;
! echo started loading into scct_db.rockport_freight_cost;
INSERT OVERWRITE INTO SCCT_DB.ROCKPORT_FREIGHT_COST
(SELECT COALESCE(RFCW.OWNER, RFC.OWNER), COALESCE(RFCW.INVOICE_REF_NBR, RFC.INVOICE_REF_NBR), COALESCE(RFCW.INVOICE_TYPE, RFC.INVOICE_TYPE), COALESCE(RFCW.INVOICE_NBR, RFC.INVOICE_NBR), COALESCE(RFCW.CUSTOMER, RFC.CUSTOMER), COALESCE(RFCW.BILL_TO, RFC.BILL_TO), COALESCE(RFCW.ORIGIN_OFFICE, RFC.ORIGIN_OFFICE), COALESCE(RFCW.INVOICE_DT, RFC.INVOICE_DT), COALESCE(RFCW.SHIP_DT, RFC.SHIP_DT), COALESCE(RFCW.CREATE_DT, RFC.CREATE_DT), COALESCE(RFCW.HDR_CURRENCY, RFC.HDR_CURRENCY), COALESCE(RFCW.PAYMENT_TYPE, RFC.PAYMENT_TYPE), COALESCE(RFCW.PAYMENT_TERMS, RFC.PAYMENT_TERMS), COALESCE(RFCW.LOAD_POINT, RFC.LOAD_POINT), COALESCE(RFCW.FINAL_DEST, RFC.FINAL_DEST), COALESCE(RFCW.CARRIER, RFC.CARRIER), COALESCE(RFCW.TRANSP_ID, RFC.TRANSP_ID), COALESCE(RFCW.VOYAGE_NBR, RFC.VOYAGE_NBR), COALESCE(RFCW.TOTAL_GROSS_AMOUNT, RFC.TOTAL_GROSS_AMOUNT), COALESCE(RFCW.TOTAL_ADJUST_AMOUNT, RFC.TOTAL_ADJUST_AMOUNT), COALESCE(RFCW.TOTAL_TAX_AMOUNT, RFC.TOTAL_TAX_AMOUNT), COALESCE(RFCW.TOTAL_NET_AMOUNT, RFC.TOTAL_NET_AMOUNT), COALESCE(RFCW.EDI_IND, RFC.EDI_IND), COALESCE(RFCW.HDR_LAST_USER, RFC.HDR_LAST_USER), COALESCE(RFCW.HDR_LAST_UPDATE_DTM, RFC.HDR_LAST_UPDATE_DTM), COALESCE(RFCW.HDR_LAST_ACTIVITY, RFC.HDR_LAST_ACTIVITY), COALESCE(RFCW.REC_ID, RFC.REC_ID), COALESCE(RFCW.EXPENSE_CD, RFC.EXPENSE_CD), COALESCE(RFCW.EXPENSE_CATEGORY, RFC.EXPENSE_CATEGORY), COALESCE(RFCW.CONTRACT_NBR, RFC.CONTRACT_NBR), COALESCE(RFCW.PO_NBR, RFC.PO_NBR), COALESCE(RFCW.RATE_PER_UNIT, RFC.RATE_PER_UNIT), COALESCE(RFCW.UNIT_PRC_BASIS, RFC.UNIT_PRC_BASIS), COALESCE(RFCW.CHARGEABLE_QTY, RFC.CHARGEABLE_QTY), COALESCE(RFCW.ROE, RFC.ROE), COALESCE(RFCW.PAYABLE_CURRENCY, RFC.PAYABLE_CURRENCY), COALESCE(RFCW.OTHER_COSTS_AMT, RFC.OTHER_COSTS_AMT), COALESCE(RFCW.ADJUSTMENT, RFC.ADJUSTMENT), COALESCE(RFCW.BL_REF_NBR, RFC.BL_REF_NBR), COALESCE(RFCW.SHIPMENT_NBR, RFC.SHIPMENT_NBR), COALESCE(RFCW.ACCOUNT_CD, RFC.ACCOUNT_CD), COALESCE(RFCW.EQUIP_ID, RFC.EQUIP_ID), COALESCE(RFCW.EQUIP_SIZE, RFC.EQUIP_SIZE), COALESCE(RFCW.TAX_IND, RFC.TAX_IND), COALESCE(RFCW.ORIGIN_COUNTRY, RFC.ORIGIN_COUNTRY), COALESCE(RFCW.TAX_TYPE, RFC.TAX_TYPE), COALESCE(RFCW.TAX_AMOUNT, RFC.TAX_AMOUNT), COALESCE(RFCW.TAX_INVOICE_NBR, RFC.TAX_INVOICE_NBR), COALESCE(RFCW.P_LINK_KEY_VAL, RFC.P_LINK_KEY_VAL), COALESCE(RFCW.FCR_NBR, RFC.FCR_NBR), COALESCE(RFCW.DTL_LAST_USER, RFC.DTL_LAST_USER), COALESCE(RFCW.DTL_LAST_UPD_DTM, RFC.DTL_LAST_UPD_DTM), COALESCE(RFCW.DTL_LAST_ACTIVITY, RFC.DTL_LAST_ACTIVITY), COALESCE(RFCW.DTL_CURRENCY, RFC.DTL_CURRENCY)
FROM SCCT_WORK_DB.ROCKPORT_FREIGHT_COST_WORK AS RFCW
FULL JOIN SCCT_DB.ROCKPORT_FREIGHT_COST AS RFC ON RFCW.OWNER = RFC.OWNER AND RFCW.INVOICE_REF_NBR = RFC.INVOICE_REF_NBR AND RFCW.REC_ID = RFC.REC_ID);
! echo completed loading into scct_work_db.rockport_cfs_shipment_work;
! echo started loading into scct_db.rockport_cfs_shipment;
! echo completed loading into scct_db.rockport_cfs_shipment;
---jobname:scct-rockport-hive-stg1-scct_db.rockport_freight_cost -------------------------------------------------------------------------- ---target: stage1 table: scct_db.rockport_freight_cost ---source: rockport_stage0 table : trader_srvc_invoice_hdr,trader_srvc_invoice_dtl ---load type: full ---back posting: no ---author - created/updated date - jira ticket ---ssoma - 03/05/2019 -  ---ssoma - 14/06/2019 - scctp4-2012/13/14 ---ssoma - 09/07/2019 - scctp4-2089 --------------------------------------------------------------------------  ------------------------- ---loading work table scct_work_db.trader_srvc_invoice_dtl_work  -------------------------  ! echo started loading into scct_work_db.trader_srvc_invoice_dtl_work;
INSERT OVERWRITE INTO SCCT_WORK_DB.TRADER_SRVC_INVOICE_DTL_WORK
(SELECT R.OWNER, R.INVOICE_REF_NO, R.REC_ID, R.EXPENSE_CODE, R.EXPENSE_CATEGORY, R.CONTRACT_NO, R.ORDER_NO, R.RATE_PER_UNIT, R.UNIT_PRC_BASIS, R.CHARGEABLE_QTY, R.ROE, R.PAYABLE_CURRENCY, R.OTHER_COSTS_AMT, R.ADJUSTMENT, R.BL_REF_NO, R.SHIPMENT_NO, R.ACCOUNT_CODE, R.EQUIP_ID, R.EQUIP_SIZE, R.TAX_INDICATOR, R.ORIGIN_COUNTRY, R.TAX_TYPE, R.TAX_AMOUNT, R.TAX_INVOICE_NO, R.P_LINK_KEY_VAL, R.FCR_NO, R.LAST_USER, R.LAST_UPDATE, R.LAST_ACTIVITY, R.CURRENCY
FROM (SELECT TRIM( OWNER) AS OWNER, TRIM( INVOICE_REF_NO) AS INVOICE_REF_NO, TRIM( REC_ID) AS REC_ID, EXPENSE_CODE, EXPENSE_CATEGORY, CONTRACT_NO, ORDER_NO, RATE_PER_UNIT, UNIT_PRC_BASIS, CHARGEABLE_QTY, ROE, PAYABLE_CURRENCY, OTHER_COSTS_AMT, ADJUSTMENT, BL_REF_NO, SHIPMENT_NO, ACCOUNT_CODE, EQUIP_ID, EQUIP_SIZE, TAX_INDICATOR, ORIGIN_COUNTRY, TAX_TYPE, TAX_AMOUNT, TAX_INVOICE_NO, P_LINK_KEY_VAL, FCR_NO, LAST_USER, LAST_UPDATE, LAST_ACTIVITY, CURRENCY, RANK() OVER (PARTITION BY TRIM( OWNER), TRIM( INVOICE_REF_NO), TRIM( REC_ID) ORDER BY LAST_UPDATE DESC) AS RK
FROM ROCKPORT_STAGE0.TRADER_SRVC_INVOICE_DTL) AS R
WHERE RK = 1);
! echo completed loading into scct_work_db.rockport_freight_cost_work;
! echo started loading into scct_db.rockport_freight_cost;
INSERT OVERWRITE INTO SCCT_DB.ROCKPORT_FREIGHT_COST
(SELECT COALESCE(RFCW.OWNER, RFC.OWNER), COALESCE(RFCW.INVOICE_REF_NBR, RFC.INVOICE_REF_NBR), COALESCE(RFCW.INVOICE_TYPE, RFC.INVOICE_TYPE), COALESCE(RFCW.INVOICE_NBR, RFC.INVOICE_NBR), COALESCE(RFCW.CUSTOMER, RFC.CUSTOMER), COALESCE(RFCW.BILL_TO, RFC.BILL_TO), COALESCE(RFCW.ORIGIN_OFFICE, RFC.ORIGIN_OFFICE), COALESCE(RFCW.INVOICE_DT, RFC.INVOICE_DT), COALESCE(RFCW.SHIP_DT, RFC.SHIP_DT), COALESCE(RFCW.CREATE_DT, RFC.CREATE_DT), COALESCE(RFCW.HDR_CURRENCY, RFC.HDR_CURRENCY), COALESCE(RFCW.PAYMENT_TYPE, RFC.PAYMENT_TYPE), COALESCE(RFCW.PAYMENT_TERMS, RFC.PAYMENT_TERMS), COALESCE(RFCW.LOAD_POINT, RFC.LOAD_POINT), COALESCE(RFCW.FINAL_DEST, RFC.FINAL_DEST), COALESCE(RFCW.CARRIER, RFC.CARRIER), COALESCE(RFCW.TRANSP_ID, RFC.TRANSP_ID), COALESCE(RFCW.VOYAGE_NBR, RFC.VOYAGE_NBR), COALESCE(RFCW.TOTAL_GROSS_AMOUNT, RFC.TOTAL_GROSS_AMOUNT), COALESCE(RFCW.TOTAL_ADJUST_AMOUNT, RFC.TOTAL_ADJUST_AMOUNT), COALESCE(RFCW.TOTAL_TAX_AMOUNT, RFC.TOTAL_TAX_AMOUNT), COALESCE(RFCW.TOTAL_NET_AMOUNT, RFC.TOTAL_NET_AMOUNT), COALESCE(RFCW.EDI_IND, RFC.EDI_IND), COALESCE(RFCW.HDR_LAST_USER, RFC.HDR_LAST_USER), COALESCE(RFCW.HDR_LAST_UPDATE_DTM, RFC.HDR_LAST_UPDATE_DTM), COALESCE(RFCW.HDR_LAST_ACTIVITY, RFC.HDR_LAST_ACTIVITY), COALESCE(RFCW.REC_ID, RFC.REC_ID), COALESCE(RFCW.EXPENSE_CD, RFC.EXPENSE_CD), COALESCE(RFCW.EXPENSE_CATEGORY, RFC.EXPENSE_CATEGORY), COALESCE(RFCW.CONTRACT_NBR, RFC.CONTRACT_NBR), COALESCE(RFCW.PO_NBR, RFC.PO_NBR), COALESCE(RFCW.RATE_PER_UNIT, RFC.RATE_PER_UNIT), COALESCE(RFCW.UNIT_PRC_BASIS, RFC.UNIT_PRC_BASIS), COALESCE(RFCW.CHARGEABLE_QTY, RFC.CHARGEABLE_QTY), COALESCE(RFCW.ROE, RFC.ROE), COALESCE(RFCW.PAYABLE_CURRENCY, RFC.PAYABLE_CURRENCY), COALESCE(RFCW.OTHER_COSTS_AMT, RFC.OTHER_COSTS_AMT), COALESCE(RFCW.ADJUSTMENT, RFC.ADJUSTMENT), COALESCE(RFCW.BL_REF_NBR, RFC.BL_REF_NBR), COALESCE(RFCW.SHIPMENT_NBR, RFC.SHIPMENT_NBR), COALESCE(RFCW.ACCOUNT_CD, RFC.ACCOUNT_CD), COALESCE(RFCW.EQUIP_ID, RFC.EQUIP_ID), COALESCE(RFCW.EQUIP_SIZE, RFC.EQUIP_SIZE), COALESCE(RFCW.TAX_IND, RFC.TAX_IND), COALESCE(RFCW.ORIGIN_COUNTRY, RFC.ORIGIN_COUNTRY), COALESCE(RFCW.TAX_TYPE, RFC.TAX_TYPE), COALESCE(RFCW.TAX_AMOUNT, RFC.TAX_AMOUNT), COALESCE(RFCW.TAX_INVOICE_NBR, RFC.TAX_INVOICE_NBR), COALESCE(RFCW.P_LINK_KEY_VAL, RFC.P_LINK_KEY_VAL), COALESCE(RFCW.FCR_NBR, RFC.FCR_NBR), COALESCE(RFCW.DTL_LAST_USER, RFC.DTL_LAST_USER), COALESCE(RFCW.DTL_LAST_UPD_DTM, RFC.DTL_LAST_UPD_DTM), COALESCE(RFCW.DTL_LAST_ACTIVITY, RFC.DTL_LAST_ACTIVITY), COALESCE(RFCW.DTL_CURRENCY, RFC.DTL_CURRENCY)
FROM SCCT_WORK_DB.ROCKPORT_FREIGHT_COST_WORK AS RFCW
FULL JOIN SCCT_DB.ROCKPORT_FREIGHT_COST AS RFC ON RFCW.OWNER = RFC.OWNER AND RFCW.INVOICE_REF_NBR = RFC.INVOICE_REF_NBR AND RFCW.REC_ID = RFC.REC_ID);
! echo completed loading into scct_work_db.rockport_cfs_shipment_work;
! echo started loading into scct_db.rockport_cfs_shipment;
! echo completed loading into scct_db.rockport_cfs_shipment;
---jobname:scct-rockport-hive-stg1-scct_db.rockport_freight_cost -------------------------------------------------------------------------- ---target: stage1 table: scct_db.rockport_freight_cost ---source: rockport_stage0 table : trader_srvc_invoice_hdr,trader_srvc_invoice_dtl ---load type: full ---back posting: no ---author - created/updated date - jira ticket ---ssoma - 03/05/2019 -  ---ssoma - 14/06/2019 - scctp4-2012/13/14 ---ssoma - 09/07/2019 - scctp4-2089 --------------------------------------------------------------------------  ------------------------- ---loading work table scct_work_db.trader_srvc_invoice_dtl_work  -------------------------  ! echo started loading into scct_work_db.trader_srvc_invoice_dtl_work;
INSERT OVERWRITE INTO SCCT_WORK_DB.TRADER_SRVC_INVOICE_DTL_WORK
(SELECT R.OWNER, R.INVOICE_REF_NO, R.REC_ID, R.EXPENSE_CODE, R.EXPENSE_CATEGORY, R.CONTRACT_NO, R.ORDER_NO, R.RATE_PER_UNIT, R.UNIT_PRC_BASIS, R.CHARGEABLE_QTY, R.ROE, R.PAYABLE_CURRENCY, R.OTHER_COSTS_AMT, R.ADJUSTMENT, R.BL_REF_NO, R.SHIPMENT_NO, R.ACCOUNT_CODE, R.EQUIP_ID, R.EQUIP_SIZE, R.TAX_INDICATOR, R.ORIGIN_COUNTRY, R.TAX_TYPE, R.TAX_AMOUNT, R.TAX_INVOICE_NO, R.P_LINK_KEY_VAL, R.FCR_NO, R.LAST_USER, R.LAST_UPDATE, R.LAST_ACTIVITY, R.CURRENCY
FROM (SELECT TRIM( OWNER) AS OWNER, TRIM( INVOICE_REF_NO) AS INVOICE_REF_NO, TRIM( REC_ID) AS REC_ID, EXPENSE_CODE, EXPENSE_CATEGORY, CONTRACT_NO, ORDER_NO, RATE_PER_UNIT, UNIT_PRC_BASIS, CHARGEABLE_QTY, ROE, PAYABLE_CURRENCY, OTHER_COSTS_AMT, ADJUSTMENT, BL_REF_NO, SHIPMENT_NO, ACCOUNT_CODE, EQUIP_ID, EQUIP_SIZE, TAX_INDICATOR, ORIGIN_COUNTRY, TAX_TYPE, TAX_AMOUNT, TAX_INVOICE_NO, P_LINK_KEY_VAL, FCR_NO, LAST_USER, LAST_UPDATE, LAST_ACTIVITY, CURRENCY, RANK() OVER (PARTITION BY TRIM( OWNER), TRIM( INVOICE_REF_NO), TRIM( REC_ID) ORDER BY LAST_UPDATE DESC) AS RK
FROM ROCKPORT_STAGE0.TRADER_SRVC_INVOICE_DTL) AS R
WHERE RK = 1);
! echo completed loading into scct_work_db.rockport_freight_cost_work;
! echo started loading into scct_db.rockport_freight_cost;
INSERT OVERWRITE INTO SCCT_DB.ROCKPORT_FREIGHT_COST
(SELECT COALESCE(RFCW.OWNER, RFC.OWNER), COALESCE(RFCW.INVOICE_REF_NBR, RFC.INVOICE_REF_NBR), COALESCE(RFCW.INVOICE_TYPE, RFC.INVOICE_TYPE), COALESCE(RFCW.INVOICE_NBR, RFC.INVOICE_NBR), COALESCE(RFCW.CUSTOMER, RFC.CUSTOMER), COALESCE(RFCW.BILL_TO, RFC.BILL_TO), COALESCE(RFCW.ORIGIN_OFFICE, RFC.ORIGIN_OFFICE), COALESCE(RFCW.INVOICE_DT, RFC.INVOICE_DT), COALESCE(RFCW.SHIP_DT, RFC.SHIP_DT), COALESCE(RFCW.CREATE_DT, RFC.CREATE_DT), COALESCE(RFCW.HDR_CURRENCY, RFC.HDR_CURRENCY), COALESCE(RFCW.PAYMENT_TYPE, RFC.PAYMENT_TYPE), COALESCE(RFCW.PAYMENT_TERMS, RFC.PAYMENT_TERMS), COALESCE(RFCW.LOAD_POINT, RFC.LOAD_POINT), COALESCE(RFCW.FINAL_DEST, RFC.FINAL_DEST), COALESCE(RFCW.CARRIER, RFC.CARRIER), COALESCE(RFCW.TRANSP_ID, RFC.TRANSP_ID), COALESCE(RFCW.VOYAGE_NBR, RFC.VOYAGE_NBR), COALESCE(RFCW.TOTAL_GROSS_AMOUNT, RFC.TOTAL_GROSS_AMOUNT), COALESCE(RFCW.TOTAL_ADJUST_AMOUNT, RFC.TOTAL_ADJUST_AMOUNT), COALESCE(RFCW.TOTAL_TAX_AMOUNT, RFC.TOTAL_TAX_AMOUNT), COALESCE(RFCW.TOTAL_NET_AMOUNT, RFC.TOTAL_NET_AMOUNT), COALESCE(RFCW.EDI_IND, RFC.EDI_IND), COALESCE(RFCW.HDR_LAST_USER, RFC.HDR_LAST_USER), COALESCE(RFCW.HDR_LAST_UPDATE_DTM, RFC.HDR_LAST_UPDATE_DTM), COALESCE(RFCW.HDR_LAST_ACTIVITY, RFC.HDR_LAST_ACTIVITY), COALESCE(RFCW.REC_ID, RFC.REC_ID), COALESCE(RFCW.EXPENSE_CD, RFC.EXPENSE_CD), COALESCE(RFCW.EXPENSE_CATEGORY, RFC.EXPENSE_CATEGORY), COALESCE(RFCW.CONTRACT_NBR, RFC.CONTRACT_NBR), COALESCE(RFCW.PO_NBR, RFC.PO_NBR), COALESCE(RFCW.RATE_PER_UNIT, RFC.RATE_PER_UNIT), COALESCE(RFCW.UNIT_PRC_BASIS, RFC.UNIT_PRC_BASIS), COALESCE(RFCW.CHARGEABLE_QTY, RFC.CHARGEABLE_QTY), COALESCE(RFCW.ROE, RFC.ROE), COALESCE(RFCW.PAYABLE_CURRENCY, RFC.PAYABLE_CURRENCY), COALESCE(RFCW.OTHER_COSTS_AMT, RFC.OTHER_COSTS_AMT), COALESCE(RFCW.ADJUSTMENT, RFC.ADJUSTMENT), COALESCE(RFCW.BL_REF_NBR, RFC.BL_REF_NBR), COALESCE(RFCW.SHIPMENT_NBR, RFC.SHIPMENT_NBR), COALESCE(RFCW.ACCOUNT_CD, RFC.ACCOUNT_CD), COALESCE(RFCW.EQUIP_ID, RFC.EQUIP_ID), COALESCE(RFCW.EQUIP_SIZE, RFC.EQUIP_SIZE), COALESCE(RFCW.TAX_IND, RFC.TAX_IND), COALESCE(RFCW.ORIGIN_COUNTRY, RFC.ORIGIN_COUNTRY), COALESCE(RFCW.TAX_TYPE, RFC.TAX_TYPE), COALESCE(RFCW.TAX_AMOUNT, RFC.TAX_AMOUNT), COALESCE(RFCW.TAX_INVOICE_NBR, RFC.TAX_INVOICE_NBR), COALESCE(RFCW.P_LINK_KEY_VAL, RFC.P_LINK_KEY_VAL), COALESCE(RFCW.FCR_NBR, RFC.FCR_NBR), COALESCE(RFCW.DTL_LAST_USER, RFC.DTL_LAST_USER), COALESCE(RFCW.DTL_LAST_UPD_DTM, RFC.DTL_LAST_UPD_DTM), COALESCE(RFCW.DTL_LAST_ACTIVITY, RFC.DTL_LAST_ACTIVITY), COALESCE(RFCW.DTL_CURRENCY, RFC.DTL_CURRENCY)
FROM SCCT_WORK_DB.ROCKPORT_FREIGHT_COST_WORK AS RFCW
FULL JOIN SCCT_DB.ROCKPORT_FREIGHT_COST AS RFC ON RFCW.OWNER = RFC.OWNER AND RFCW.INVOICE_REF_NBR = RFC.INVOICE_REF_NBR AND RFCW.REC_ID = RFC.REC_ID);
