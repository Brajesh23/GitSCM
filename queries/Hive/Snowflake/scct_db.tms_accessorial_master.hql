------------------------- ---loading staging data into final staging table -------------------------  set tez.job.name=staging:tms_accessorial_master_tmp1 :load target table from tms_accessorial_master_tmp;
! echo started loading into scct_work_db.tms_accessorial_master_tmp1;
INSERT OVERWRITE INTO SCCT_WORK_DB.TMS_ACCESSORIAL_MASTER_TMP1
(SELECT COALESCE(TMP.ACCESSORIAL_TYPE_DESCRIPTION, T.ACCESSORIAL_TYPE_DESCRIPTION), COALESCE(TMP.TC_COMPANY_ID, T.TC_COMPANY_ID), COALESCE(TMP.ACCESSORIAL_CD, T.ACCESSORIAL_CD), COALESCE(TMP.ACCESSORIAL_TYPE, T.ACCESSORIAL_TYPE), COALESCE(TMP.DATA_SOURCE, T.DATA_SOURCE), COALESCE(TMP.DISTANCE_UOM, T.DISTANCE_UOM), COALESCE(TMP.ACCESSORIAL_DESCRIPTION, T.ACCESSORIAL_DESCRIPTION), COALESCE(TMP.LAST_UPDATED_DTTM, T.LAST_UPDATED_DTTM), COALESCE(TMP.IS_HAZMAT, T.IS_HAZMAT), COALESCE(TMP.IS_DEFAULT_FOR_INVOICE, T.IS_DEFAULT_FOR_INVOICE), COALESCE(TMP.IS_FUEL_ACCESSORIAL, T.IS_FUEL_ACCESSORIAL), COALESCE(TMP.ACCESSORIAL_ID, T.ACCESSORIAL_ID), COALESCE(TMP.IS_TAX_ACCESSORIAL, T.IS_TAX_ACCESSORIAL), COALESCE(TMP.IS_ADD_TO_SPOT_CHARGE, T.IS_ADD_TO_SPOT_CHARGE), COALESCE(TMP.IS_ADD_STOP_OFF_CHARGE, T.IS_ADD_STOP_OFF_CHARGE), COALESCE(TMP.CALCULATION_OPTION_LEVEL, T.CALCULATION_OPTION_LEVEL), COALESCE(TMP.DELIVERY_LEVEL, T.DELIVERY_LEVEL), COALESCE(TMP.IS_REQUESTED, T.IS_REQUESTED), COALESCE(TMP.IS_SHIP_VIA, T.IS_SHIP_VIA), COALESCE(TMP.CALCULATION_FACTOR, T.CALCULATION_FACTOR), COALESCE(TMP.IS_OVERSIZE, T.IS_OVERSIZE), COALESCE(TMP.IS_APPLY_ONCE_PER_SHIP, T.IS_APPLY_ONCE_PER_SHIP), COALESCE(TMP.FUEL_INDEX_ID, T.FUEL_INDEX_ID), COALESCE(TMP.IS_COST_PLUS_ACCESSORIAL, T.IS_COST_PLUS_ACCESSORIAL), COALESCE(TMP.ACCESSORIAL_PARAM_SET_ID, T.ACCESSORIAL_PARAM_SET_ID), COALESCE(TMP.ACCESSORIAL_RATE_ID, T.ACCESSORIAL_RATE_ID), COALESCE(TMP.RATE, T.RATE), COALESCE(TMP.MINIMUM_RATE, T.MINIMUM_RATE), COALESCE(TMP.CURRENCY_CD, T.CURRENCY_CD), COALESCE(TMP.IS_AUTO_APPROVE, T.IS_AUTO_APPROVE), COALESCE(TMP.EFFECTIVE_DT, T.EFFECTIVE_DT), COALESCE(TMP.EXPIRATION_DT, T.EXPIRATION_DT), COALESCE(TMP.EFFECTIVE_SEQ, T.EFFECTIVE_SEQ), COALESCE(TMP.IS_SHIPMENT_ACCESSORIAL, T.IS_SHIPMENT_ACCESSORIAL), COALESCE(TMP.MOT_ID, T.MOT_ID), COALESCE(TMP.EQUIPMENT_ID, T.EQUIPMENT_ID), COALESCE(TMP.SERVICE_LEVEL_ID, T.SERVICE_LEVEL_ID), COALESCE(TMP.PROTECTION_LEVEL_ID, T.PROTECTION_LEVEL_ID)
FROM SCCT_DB.TMS_ACCESSORIAL_MASTER AS T
FULL JOIN SCCT_WORK_DB.TMS_ACCESSORIAL_MASTER_TMP AS TMP ON T.ACCESSORIAL_ID = TMP.ACCESSORIAL_ID AND T.ACCESSORIAL_PARAM_SET_ID = TMP.ACCESSORIAL_PARAM_SET_ID AND T.ACCESSORIAL_RATE_ID = TMP.ACCESSORIAL_RATE_ID AND T.ACCESSORIAL_TYPE = TMP.ACCESSORIAL_TYPE);
------------------------- ---loading staging data into final staging table -------------------------  set tez.job.name=staging:tms_accessorial_master_tmp1 :load target table from tms_accessorial_master_tmp;
! echo started loading into scct_work_db.tms_accessorial_master_tmp1;
------------------------- ---loading staging data into target table -------------------------  set tez.job.name=staging:tms_accessorial_master :load target table from tms_accessorial_master_tmp1;
! echo started loading into scct_db.tms_accessorial_master;
INSERT OVERWRITE INTO SCCT_DB.TMS_ACCESSORIAL_MASTER
(SELECT ACCESSORIAL_TYPE_DESCRIPTION, TC_COMPANY_ID, ACCESSORIAL_CD, ACCESSORIAL_TYPE, DATA_SOURCE, DISTANCE_UOM, ACCESSORIAL_DESCRIPTION, LAST_UPDATED_DTTM, IS_HAZMAT, IS_DEFAULT_FOR_INVOICE, IS_FUEL_ACCESSORIAL, ACCESSORIAL_ID, IS_TAX_ACCESSORIAL, IS_ADD_TO_SPOT_CHARGE, IS_ADD_STOP_OFF_CHARGE, CALCULATION_OPTION_LEVEL, DELIVERY_LEVEL, IS_REQUESTED, IS_SHIP_VIA, CALCULATION_FACTOR, IS_OVERSIZE, IS_APPLY_ONCE_PER_SHIP, FUEL_INDEX_ID, IS_COST_PLUS_ACCESSORIAL, ACCESSORIAL_PARAM_SET_ID, ACCESSORIAL_RATE_ID, RATE, MINIMUM_RATE, CURRENCY_CD, IS_AUTO_APPROVE, EFFECTIVE_DT, EXPIRATION_DT, EFFECTIVE_SEQ, IS_SHIPMENT_ACCESSORIAL, MOT_ID, EQUIPMENT_ID, SERVICE_LEVEL_ID, PROTECTION_LEVEL_ID
FROM SCCT_WORK_DB.TMS_ACCESSORIAL_MASTER_TMP1);
------------------------- ---loading staging data into final staging table -------------------------  set tez.job.name=staging:tms_accessorial_master_tmp1 :load target table from tms_accessorial_master_tmp;
! echo started loading into scct_work_db.tms_accessorial_master_tmp1;
INSERT OVERWRITE INTO SCCT_WORK_DB.TMS_ACCESSORIAL_MASTER_TMP1
(SELECT COALESCE(TMP.ACCESSORIAL_TYPE_DESCRIPTION, T.ACCESSORIAL_TYPE_DESCRIPTION), COALESCE(TMP.TC_COMPANY_ID, T.TC_COMPANY_ID), COALESCE(TMP.ACCESSORIAL_CD, T.ACCESSORIAL_CD), COALESCE(TMP.ACCESSORIAL_TYPE, T.ACCESSORIAL_TYPE), COALESCE(TMP.DATA_SOURCE, T.DATA_SOURCE), COALESCE(TMP.DISTANCE_UOM, T.DISTANCE_UOM), COALESCE(TMP.ACCESSORIAL_DESCRIPTION, T.ACCESSORIAL_DESCRIPTION), COALESCE(TMP.LAST_UPDATED_DTTM, T.LAST_UPDATED_DTTM), COALESCE(TMP.IS_HAZMAT, T.IS_HAZMAT), COALESCE(TMP.IS_DEFAULT_FOR_INVOICE, T.IS_DEFAULT_FOR_INVOICE), COALESCE(TMP.IS_FUEL_ACCESSORIAL, T.IS_FUEL_ACCESSORIAL), COALESCE(TMP.ACCESSORIAL_ID, T.ACCESSORIAL_ID), COALESCE(TMP.IS_TAX_ACCESSORIAL, T.IS_TAX_ACCESSORIAL), COALESCE(TMP.IS_ADD_TO_SPOT_CHARGE, T.IS_ADD_TO_SPOT_CHARGE), COALESCE(TMP.IS_ADD_STOP_OFF_CHARGE, T.IS_ADD_STOP_OFF_CHARGE), COALESCE(TMP.CALCULATION_OPTION_LEVEL, T.CALCULATION_OPTION_LEVEL), COALESCE(TMP.DELIVERY_LEVEL, T.DELIVERY_LEVEL), COALESCE(TMP.IS_REQUESTED, T.IS_REQUESTED), COALESCE(TMP.IS_SHIP_VIA, T.IS_SHIP_VIA), COALESCE(TMP.CALCULATION_FACTOR, T.CALCULATION_FACTOR), COALESCE(TMP.IS_OVERSIZE, T.IS_OVERSIZE), COALESCE(TMP.IS_APPLY_ONCE_PER_SHIP, T.IS_APPLY_ONCE_PER_SHIP), COALESCE(TMP.FUEL_INDEX_ID, T.FUEL_INDEX_ID), COALESCE(TMP.IS_COST_PLUS_ACCESSORIAL, T.IS_COST_PLUS_ACCESSORIAL), COALESCE(TMP.ACCESSORIAL_PARAM_SET_ID, T.ACCESSORIAL_PARAM_SET_ID), COALESCE(TMP.ACCESSORIAL_RATE_ID, T.ACCESSORIAL_RATE_ID), COALESCE(TMP.RATE, T.RATE), COALESCE(TMP.MINIMUM_RATE, T.MINIMUM_RATE), COALESCE(TMP.CURRENCY_CD, T.CURRENCY_CD), COALESCE(TMP.IS_AUTO_APPROVE, T.IS_AUTO_APPROVE), COALESCE(TMP.EFFECTIVE_DT, T.EFFECTIVE_DT), COALESCE(TMP.EXPIRATION_DT, T.EXPIRATION_DT), COALESCE(TMP.EFFECTIVE_SEQ, T.EFFECTIVE_SEQ), COALESCE(TMP.IS_SHIPMENT_ACCESSORIAL, T.IS_SHIPMENT_ACCESSORIAL), COALESCE(TMP.MOT_ID, T.MOT_ID), COALESCE(TMP.EQUIPMENT_ID, T.EQUIPMENT_ID), COALESCE(TMP.SERVICE_LEVEL_ID, T.SERVICE_LEVEL_ID), COALESCE(TMP.PROTECTION_LEVEL_ID, T.PROTECTION_LEVEL_ID)
FROM SCCT_DB.TMS_ACCESSORIAL_MASTER AS T
FULL JOIN SCCT_WORK_DB.TMS_ACCESSORIAL_MASTER_TMP AS TMP ON T.ACCESSORIAL_ID = TMP.ACCESSORIAL_ID AND T.ACCESSORIAL_PARAM_SET_ID = TMP.ACCESSORIAL_PARAM_SET_ID AND T.ACCESSORIAL_RATE_ID = TMP.ACCESSORIAL_RATE_ID AND T.ACCESSORIAL_TYPE = TMP.ACCESSORIAL_TYPE);
------------------------- ---loading staging data into final staging table -------------------------  set tez.job.name=staging:tms_accessorial_master_tmp1 :load target table from tms_accessorial_master_tmp;
! echo started loading into scct_work_db.tms_accessorial_master_tmp1;
------------------------- ---loading staging data into target table -------------------------  set tez.job.name=staging:tms_accessorial_master :load target table from tms_accessorial_master_tmp1;
! echo started loading into scct_db.tms_accessorial_master;
INSERT OVERWRITE INTO SCCT_DB.TMS_ACCESSORIAL_MASTER
(SELECT ACCESSORIAL_TYPE_DESCRIPTION, TC_COMPANY_ID, ACCESSORIAL_CD, ACCESSORIAL_TYPE, DATA_SOURCE, DISTANCE_UOM, ACCESSORIAL_DESCRIPTION, LAST_UPDATED_DTTM, IS_HAZMAT, IS_DEFAULT_FOR_INVOICE, IS_FUEL_ACCESSORIAL, ACCESSORIAL_ID, IS_TAX_ACCESSORIAL, IS_ADD_TO_SPOT_CHARGE, IS_ADD_STOP_OFF_CHARGE, CALCULATION_OPTION_LEVEL, DELIVERY_LEVEL, IS_REQUESTED, IS_SHIP_VIA, CALCULATION_FACTOR, IS_OVERSIZE, IS_APPLY_ONCE_PER_SHIP, FUEL_INDEX_ID, IS_COST_PLUS_ACCESSORIAL, ACCESSORIAL_PARAM_SET_ID, ACCESSORIAL_RATE_ID, RATE, MINIMUM_RATE, CURRENCY_CD, IS_AUTO_APPROVE, EFFECTIVE_DT, EXPIRATION_DT, EFFECTIVE_SEQ, IS_SHIPMENT_ACCESSORIAL, MOT_ID, EQUIPMENT_ID, SERVICE_LEVEL_ID, PROTECTION_LEVEL_ID
FROM SCCT_WORK_DB.TMS_ACCESSORIAL_MASTER_TMP1);
------------------------- ---loading staging data into final staging table -------------------------  set tez.job.name=staging:tms_accessorial_master_tmp1 :load target table from tms_accessorial_master_tmp;
! echo started loading into scct_work_db.tms_accessorial_master_tmp1;
INSERT OVERWRITE INTO SCCT_WORK_DB.TMS_ACCESSORIAL_MASTER_TMP1
(SELECT COALESCE(TMP.ACCESSORIAL_TYPE_DESCRIPTION, T.ACCESSORIAL_TYPE_DESCRIPTION), COALESCE(TMP.TC_COMPANY_ID, T.TC_COMPANY_ID), COALESCE(TMP.ACCESSORIAL_CD, T.ACCESSORIAL_CD), COALESCE(TMP.ACCESSORIAL_TYPE, T.ACCESSORIAL_TYPE), COALESCE(TMP.DATA_SOURCE, T.DATA_SOURCE), COALESCE(TMP.DISTANCE_UOM, T.DISTANCE_UOM), COALESCE(TMP.ACCESSORIAL_DESCRIPTION, T.ACCESSORIAL_DESCRIPTION), COALESCE(TMP.LAST_UPDATED_DTTM, T.LAST_UPDATED_DTTM), COALESCE(TMP.IS_HAZMAT, T.IS_HAZMAT), COALESCE(TMP.IS_DEFAULT_FOR_INVOICE, T.IS_DEFAULT_FOR_INVOICE), COALESCE(TMP.IS_FUEL_ACCESSORIAL, T.IS_FUEL_ACCESSORIAL), COALESCE(TMP.ACCESSORIAL_ID, T.ACCESSORIAL_ID), COALESCE(TMP.IS_TAX_ACCESSORIAL, T.IS_TAX_ACCESSORIAL), COALESCE(TMP.IS_ADD_TO_SPOT_CHARGE, T.IS_ADD_TO_SPOT_CHARGE), COALESCE(TMP.IS_ADD_STOP_OFF_CHARGE, T.IS_ADD_STOP_OFF_CHARGE), COALESCE(TMP.CALCULATION_OPTION_LEVEL, T.CALCULATION_OPTION_LEVEL), COALESCE(TMP.DELIVERY_LEVEL, T.DELIVERY_LEVEL), COALESCE(TMP.IS_REQUESTED, T.IS_REQUESTED), COALESCE(TMP.IS_SHIP_VIA, T.IS_SHIP_VIA), COALESCE(TMP.CALCULATION_FACTOR, T.CALCULATION_FACTOR), COALESCE(TMP.IS_OVERSIZE, T.IS_OVERSIZE), COALESCE(TMP.IS_APPLY_ONCE_PER_SHIP, T.IS_APPLY_ONCE_PER_SHIP), COALESCE(TMP.FUEL_INDEX_ID, T.FUEL_INDEX_ID), COALESCE(TMP.IS_COST_PLUS_ACCESSORIAL, T.IS_COST_PLUS_ACCESSORIAL), COALESCE(TMP.ACCESSORIAL_PARAM_SET_ID, T.ACCESSORIAL_PARAM_SET_ID), COALESCE(TMP.ACCESSORIAL_RATE_ID, T.ACCESSORIAL_RATE_ID), COALESCE(TMP.RATE, T.RATE), COALESCE(TMP.MINIMUM_RATE, T.MINIMUM_RATE), COALESCE(TMP.CURRENCY_CD, T.CURRENCY_CD), COALESCE(TMP.IS_AUTO_APPROVE, T.IS_AUTO_APPROVE), COALESCE(TMP.EFFECTIVE_DT, T.EFFECTIVE_DT), COALESCE(TMP.EXPIRATION_DT, T.EXPIRATION_DT), COALESCE(TMP.EFFECTIVE_SEQ, T.EFFECTIVE_SEQ), COALESCE(TMP.IS_SHIPMENT_ACCESSORIAL, T.IS_SHIPMENT_ACCESSORIAL), COALESCE(TMP.MOT_ID, T.MOT_ID), COALESCE(TMP.EQUIPMENT_ID, T.EQUIPMENT_ID), COALESCE(TMP.SERVICE_LEVEL_ID, T.SERVICE_LEVEL_ID), COALESCE(TMP.PROTECTION_LEVEL_ID, T.PROTECTION_LEVEL_ID)
FROM SCCT_DB.TMS_ACCESSORIAL_MASTER AS T
FULL JOIN SCCT_WORK_DB.TMS_ACCESSORIAL_MASTER_TMP AS TMP ON T.ACCESSORIAL_ID = TMP.ACCESSORIAL_ID AND T.ACCESSORIAL_PARAM_SET_ID = TMP.ACCESSORIAL_PARAM_SET_ID AND T.ACCESSORIAL_RATE_ID = TMP.ACCESSORIAL_RATE_ID AND T.ACCESSORIAL_TYPE = TMP.ACCESSORIAL_TYPE);
------------------------- ---loading staging data into final staging table -------------------------  set tez.job.name=staging:tms_accessorial_master_tmp1 :load target table from tms_accessorial_master_tmp;
! echo started loading into scct_work_db.tms_accessorial_master_tmp1;
------------------------- ---loading staging data into target table -------------------------  set tez.job.name=staging:tms_accessorial_master :load target table from tms_accessorial_master_tmp1;
! echo started loading into scct_db.tms_accessorial_master;
INSERT OVERWRITE INTO SCCT_DB.TMS_ACCESSORIAL_MASTER
(SELECT ACCESSORIAL_TYPE_DESCRIPTION, TC_COMPANY_ID, ACCESSORIAL_CD, ACCESSORIAL_TYPE, DATA_SOURCE, DISTANCE_UOM, ACCESSORIAL_DESCRIPTION, LAST_UPDATED_DTTM, IS_HAZMAT, IS_DEFAULT_FOR_INVOICE, IS_FUEL_ACCESSORIAL, ACCESSORIAL_ID, IS_TAX_ACCESSORIAL, IS_ADD_TO_SPOT_CHARGE, IS_ADD_STOP_OFF_CHARGE, CALCULATION_OPTION_LEVEL, DELIVERY_LEVEL, IS_REQUESTED, IS_SHIP_VIA, CALCULATION_FACTOR, IS_OVERSIZE, IS_APPLY_ONCE_PER_SHIP, FUEL_INDEX_ID, IS_COST_PLUS_ACCESSORIAL, ACCESSORIAL_PARAM_SET_ID, ACCESSORIAL_RATE_ID, RATE, MINIMUM_RATE, CURRENCY_CD, IS_AUTO_APPROVE, EFFECTIVE_DT, EXPIRATION_DT, EFFECTIVE_SEQ, IS_SHIPMENT_ACCESSORIAL, MOT_ID, EQUIPMENT_ID, SERVICE_LEVEL_ID, PROTECTION_LEVEL_ID
FROM SCCT_WORK_DB.TMS_ACCESSORIAL_MASTER_TMP1);
------------------------- ---loading staging data into final staging table -------------------------  set tez.job.name=staging:tms_accessorial_master_tmp1 :load target table from tms_accessorial_master_tmp;
! echo started loading into scct_work_db.tms_accessorial_master_tmp1;
INSERT OVERWRITE INTO SCCT_WORK_DB.TMS_ACCESSORIAL_MASTER_TMP1
(SELECT COALESCE(TMP.ACCESSORIAL_TYPE_DESCRIPTION, T.ACCESSORIAL_TYPE_DESCRIPTION), COALESCE(TMP.TC_COMPANY_ID, T.TC_COMPANY_ID), COALESCE(TMP.ACCESSORIAL_CD, T.ACCESSORIAL_CD), COALESCE(TMP.ACCESSORIAL_TYPE, T.ACCESSORIAL_TYPE), COALESCE(TMP.DATA_SOURCE, T.DATA_SOURCE), COALESCE(TMP.DISTANCE_UOM, T.DISTANCE_UOM), COALESCE(TMP.ACCESSORIAL_DESCRIPTION, T.ACCESSORIAL_DESCRIPTION), COALESCE(TMP.LAST_UPDATED_DTTM, T.LAST_UPDATED_DTTM), COALESCE(TMP.IS_HAZMAT, T.IS_HAZMAT), COALESCE(TMP.IS_DEFAULT_FOR_INVOICE, T.IS_DEFAULT_FOR_INVOICE), COALESCE(TMP.IS_FUEL_ACCESSORIAL, T.IS_FUEL_ACCESSORIAL), COALESCE(TMP.ACCESSORIAL_ID, T.ACCESSORIAL_ID), COALESCE(TMP.IS_TAX_ACCESSORIAL, T.IS_TAX_ACCESSORIAL), COALESCE(TMP.IS_ADD_TO_SPOT_CHARGE, T.IS_ADD_TO_SPOT_CHARGE), COALESCE(TMP.IS_ADD_STOP_OFF_CHARGE, T.IS_ADD_STOP_OFF_CHARGE), COALESCE(TMP.CALCULATION_OPTION_LEVEL, T.CALCULATION_OPTION_LEVEL), COALESCE(TMP.DELIVERY_LEVEL, T.DELIVERY_LEVEL), COALESCE(TMP.IS_REQUESTED, T.IS_REQUESTED), COALESCE(TMP.IS_SHIP_VIA, T.IS_SHIP_VIA), COALESCE(TMP.CALCULATION_FACTOR, T.CALCULATION_FACTOR), COALESCE(TMP.IS_OVERSIZE, T.IS_OVERSIZE), COALESCE(TMP.IS_APPLY_ONCE_PER_SHIP, T.IS_APPLY_ONCE_PER_SHIP), COALESCE(TMP.FUEL_INDEX_ID, T.FUEL_INDEX_ID), COALESCE(TMP.IS_COST_PLUS_ACCESSORIAL, T.IS_COST_PLUS_ACCESSORIAL), COALESCE(TMP.ACCESSORIAL_PARAM_SET_ID, T.ACCESSORIAL_PARAM_SET_ID), COALESCE(TMP.ACCESSORIAL_RATE_ID, T.ACCESSORIAL_RATE_ID), COALESCE(TMP.RATE, T.RATE), COALESCE(TMP.MINIMUM_RATE, T.MINIMUM_RATE), COALESCE(TMP.CURRENCY_CD, T.CURRENCY_CD), COALESCE(TMP.IS_AUTO_APPROVE, T.IS_AUTO_APPROVE), COALESCE(TMP.EFFECTIVE_DT, T.EFFECTIVE_DT), COALESCE(TMP.EXPIRATION_DT, T.EXPIRATION_DT), COALESCE(TMP.EFFECTIVE_SEQ, T.EFFECTIVE_SEQ), COALESCE(TMP.IS_SHIPMENT_ACCESSORIAL, T.IS_SHIPMENT_ACCESSORIAL), COALESCE(TMP.MOT_ID, T.MOT_ID), COALESCE(TMP.EQUIPMENT_ID, T.EQUIPMENT_ID), COALESCE(TMP.SERVICE_LEVEL_ID, T.SERVICE_LEVEL_ID), COALESCE(TMP.PROTECTION_LEVEL_ID, T.PROTECTION_LEVEL_ID)
FROM SCCT_DB.TMS_ACCESSORIAL_MASTER AS T
FULL JOIN SCCT_WORK_DB.TMS_ACCESSORIAL_MASTER_TMP AS TMP ON T.ACCESSORIAL_ID = TMP.ACCESSORIAL_ID AND T.ACCESSORIAL_PARAM_SET_ID = TMP.ACCESSORIAL_PARAM_SET_ID AND T.ACCESSORIAL_RATE_ID = TMP.ACCESSORIAL_RATE_ID AND T.ACCESSORIAL_TYPE = TMP.ACCESSORIAL_TYPE);
------------------------- ---loading staging data into final staging table -------------------------  set tez.job.name=staging:tms_accessorial_master_tmp1 :load target table from tms_accessorial_master_tmp;
! echo started loading into scct_work_db.tms_accessorial_master_tmp1;
------------------------- ---loading staging data into target table -------------------------  set tez.job.name=staging:tms_accessorial_master :load target table from tms_accessorial_master_tmp1;
! echo started loading into scct_db.tms_accessorial_master;
INSERT OVERWRITE INTO SCCT_DB.TMS_ACCESSORIAL_MASTER
(SELECT ACCESSORIAL_TYPE_DESCRIPTION, TC_COMPANY_ID, ACCESSORIAL_CD, ACCESSORIAL_TYPE, DATA_SOURCE, DISTANCE_UOM, ACCESSORIAL_DESCRIPTION, LAST_UPDATED_DTTM, IS_HAZMAT, IS_DEFAULT_FOR_INVOICE, IS_FUEL_ACCESSORIAL, ACCESSORIAL_ID, IS_TAX_ACCESSORIAL, IS_ADD_TO_SPOT_CHARGE, IS_ADD_STOP_OFF_CHARGE, CALCULATION_OPTION_LEVEL, DELIVERY_LEVEL, IS_REQUESTED, IS_SHIP_VIA, CALCULATION_FACTOR, IS_OVERSIZE, IS_APPLY_ONCE_PER_SHIP, FUEL_INDEX_ID, IS_COST_PLUS_ACCESSORIAL, ACCESSORIAL_PARAM_SET_ID, ACCESSORIAL_RATE_ID, RATE, MINIMUM_RATE, CURRENCY_CD, IS_AUTO_APPROVE, EFFECTIVE_DT, EXPIRATION_DT, EFFECTIVE_SEQ, IS_SHIPMENT_ACCESSORIAL, MOT_ID, EQUIPMENT_ID, SERVICE_LEVEL_ID, PROTECTION_LEVEL_ID
FROM SCCT_WORK_DB.TMS_ACCESSORIAL_MASTER_TMP1);
