------------------------- ---loading staging data into final stagnig table -------------------------  set tez.job.name=staging:tms_carrier_contact_master_tmp1 :load target table from tms_carrier_contact_master_tmp;
! echo started loading into scct_work_db.tms_carrier_contact_master_tmp1;
INSERT OVERWRITE INTO SCCT_WORK_DB.TMS_CARRIER_CONTACT_MASTER_TMP1
(SELECT COALESCE(TMP.COMPANY_ID, T.COMPANY_ID), COALESCE(TMP.CARRIER_CD, T.CARRIER_CD), COALESCE(TMP.CARRIER_COMPANY_ID, T.CARRIER_COMPANY_ID), COALESCE(TMP.CARRIER_COMPANY_NAME, T.CARRIER_COMPANY_NAME), COALESCE(TMP.CARRIER_NAME, T.CARRIER_NAME), COALESCE(TMP.CARRIER_ADDRESS, T.CARRIER_ADDRESS), COALESCE(TMP.CARRIER_CITY, T.CARRIER_CITY), COALESCE(TMP.CARRIER_STATE_PROV, T.CARRIER_STATE_PROV), COALESCE(TMP.CARRIER_POSTAL_CD, T.CARRIER_POSTAL_CD), COALESCE(TMP.CARRIER_COUNTRY_CD, T.CARRIER_COUNTRY_CD), COALESCE(TMP.CARRIER_STATUS, T.CARRIER_STATUS), COALESCE(TMP.CARRIER_MARK_FOR_DELETION, T.CARRIER_MARK_FOR_DELETION), COALESCE(TMP.CARRIER_COMMUNICATION_METHOD, T.CARRIER_COMMUNICATION_METHOD), COALESCE(TMP.CARRIER_CREATED_DTTM, T.CARRIER_CREATED_DTTM), COALESCE(TMP.CARRIER_LAST_UPDATED_DTTM, T.CARRIER_LAST_UPDATED_DTTM), COALESCE(TMP.CARRIER_PAYMENT_REFERENCE_NBR, T.CARRIER_PAYMENT_REFERENCE_NBR), COALESCE(TMP.CARRIER_TARIFF_ID, T.CARRIER_TARIFF_ID), COALESCE(TMP.CARRIER_INVOICE_PAYMENT_TERMS, T.CARRIER_INVOICE_PAYMENT_TERMS), COALESCE(TMP.CARRIER_PAYMENT_METHOD, T.CARRIER_PAYMENT_METHOD), COALESCE(TMP.CARRIER_ID, T.CARRIER_ID), COALESCE(TMP.CARRIER_HAS_HAZMAT, T.CARRIER_HAS_HAZMAT), COALESCE(TMP.CONTACT_ID, T.CONTACT_ID), COALESCE(TMP.CONTACT_FIRST_NAME, T.CONTACT_FIRST_NAME), COALESCE(TMP.CONTACT_MIDDLE_NAME, T.CONTACT_MIDDLE_NAME), COALESCE(TMP.CONTACT_SURNAME, T.CONTACT_SURNAME), COALESCE(TMP.CONTACT_PREFIX, T.CONTACT_PREFIX), COALESCE(TMP.CONTACT_TITLE, T.CONTACT_TITLE), COALESCE(TMP.CONTACT_PHONE, T.CONTACT_PHONE), COALESCE(TMP.CONTACT_MOBILE_PHONE, T.CONTACT_MOBILE_PHONE), COALESCE(TMP.CONTACT_PAGER, T.CONTACT_PAGER), COALESCE(TMP.CONTACT_FAX, T.CONTACT_FAX), COALESCE(TMP.CONTACT_EMAIL, T.CONTACT_EMAIL), COALESCE(TMP.CONTACT_ADDRESS, T.CONTACT_ADDRESS), COALESCE(TMP.CONTACT_CITY, T.CONTACT_CITY), COALESCE(TMP.CONTACT_STATE_PROV, T.CONTACT_STATE_PROV), COALESCE(TMP.CONTACT_POSTAL_CD, T.CONTACT_POSTAL_CD), COALESCE(TMP.CONTACT_COUNTRY_CD, T.CONTACT_COUNTRY_CD), COALESCE(TMP.CONTACT_COMMENTS, T.CONTACT_COMMENTS), COALESCE(TMP.CONTACT_IS_PRIMARY, T.CONTACT_IS_PRIMARY), COALESCE(TMP.CONTACT_TYPE, T.CONTACT_TYPE), COALESCE(TMP.CONTACT_CREATED_DTTM, T.CONTACT_CREATED_DTTM), COALESCE(TMP.CONTACT_LAST_UPDATED_DTTM, T.CONTACT_LAST_UPDATED_DTTM), COALESCE(TMP.CARRIER_NOTE_CARRIER_TYPE, T.CARRIER_NOTE_CARRIER_TYPE), COALESCE(TMP.CARRIER_NOTE_CANADIAN_CARRIER, T.CARRIER_NOTE_CANADIAN_CARRIER)
FROM SCCT_DB.TMS_CARRIER_CONTACT_MASTER AS T
FULL JOIN SCCT_WORK_DB.TMS_CARRIER_CONTACT_MASTER_TMP AS TMP ON T.CARRIER_ID = TMP.CARRIER_ID AND T.CONTACT_ID = TMP.CONTACT_ID);
------------------------- ---loading staging data into final stagnig table -------------------------  set tez.job.name=staging:tms_carrier_contact_master_tmp1 :load target table from tms_carrier_contact_master_tmp;
! echo started loading into scct_work_db.tms_carrier_contact_master_tmp1;
------------------------- ---loading staging data into target table -------------------------  set tez.job.name=staging:tms_carrier_contact_master :load target table from tms_carrier_contact_master_tmp1;
! echo started loading into scct_db.tms_carrier_contact_master;
INSERT OVERWRITE INTO SCCT_DB.TMS_CARRIER_CONTACT_MASTER
(SELECT COMPANY_ID, CARRIER_CD, CARRIER_COMPANY_ID, CARRIER_COMPANY_NAME, CARRIER_NAME, CARRIER_ADDRESS, CARRIER_CITY, CARRIER_STATE_PROV, CARRIER_POSTAL_CD, CARRIER_COUNTRY_CD, CARRIER_STATUS, CARRIER_MARK_FOR_DELETION, CARRIER_COMMUNICATION_METHOD, CARRIER_CREATED_DTTM, CARRIER_LAST_UPDATED_DTTM, CARRIER_PAYMENT_REFERENCE_NBR, CARRIER_TARIFF_ID, CARRIER_INVOICE_PAYMENT_TERMS, CARRIER_PAYMENT_METHOD, CARRIER_ID, CARRIER_HAS_HAZMAT, CONTACT_ID, CONTACT_FIRST_NAME, CONTACT_MIDDLE_NAME, CONTACT_SURNAME, CONTACT_PREFIX, CONTACT_TITLE, CONTACT_PHONE, CONTACT_MOBILE_PHONE, CONTACT_PAGER, CONTACT_FAX, CONTACT_EMAIL, CONTACT_ADDRESS, CONTACT_CITY, CONTACT_STATE_PROV, CONTACT_POSTAL_CD, CONTACT_COUNTRY_CD, CONTACT_COMMENTS, CONTACT_IS_PRIMARY, CONTACT_TYPE, CONTACT_CREATED_DTTM, CONTACT_LAST_UPDATED_DTTM, CARRIER_NOTE_CARRIER_TYPE, CARRIER_NOTE_CANADIAN_CARRIER
FROM SCCT_WORK_DB.TMS_CARRIER_CONTACT_MASTER_TMP1);
------------------------- ---loading staging data into final stagnig table -------------------------  set tez.job.name=staging:tms_carrier_contact_master_tmp1 :load target table from tms_carrier_contact_master_tmp;
! echo started loading into scct_work_db.tms_carrier_contact_master_tmp1;
INSERT OVERWRITE INTO SCCT_WORK_DB.TMS_CARRIER_CONTACT_MASTER_TMP1
(SELECT COALESCE(TMP.COMPANY_ID, T.COMPANY_ID), COALESCE(TMP.CARRIER_CD, T.CARRIER_CD), COALESCE(TMP.CARRIER_COMPANY_ID, T.CARRIER_COMPANY_ID), COALESCE(TMP.CARRIER_COMPANY_NAME, T.CARRIER_COMPANY_NAME), COALESCE(TMP.CARRIER_NAME, T.CARRIER_NAME), COALESCE(TMP.CARRIER_ADDRESS, T.CARRIER_ADDRESS), COALESCE(TMP.CARRIER_CITY, T.CARRIER_CITY), COALESCE(TMP.CARRIER_STATE_PROV, T.CARRIER_STATE_PROV), COALESCE(TMP.CARRIER_POSTAL_CD, T.CARRIER_POSTAL_CD), COALESCE(TMP.CARRIER_COUNTRY_CD, T.CARRIER_COUNTRY_CD), COALESCE(TMP.CARRIER_STATUS, T.CARRIER_STATUS), COALESCE(TMP.CARRIER_MARK_FOR_DELETION, T.CARRIER_MARK_FOR_DELETION), COALESCE(TMP.CARRIER_COMMUNICATION_METHOD, T.CARRIER_COMMUNICATION_METHOD), COALESCE(TMP.CARRIER_CREATED_DTTM, T.CARRIER_CREATED_DTTM), COALESCE(TMP.CARRIER_LAST_UPDATED_DTTM, T.CARRIER_LAST_UPDATED_DTTM), COALESCE(TMP.CARRIER_PAYMENT_REFERENCE_NBR, T.CARRIER_PAYMENT_REFERENCE_NBR), COALESCE(TMP.CARRIER_TARIFF_ID, T.CARRIER_TARIFF_ID), COALESCE(TMP.CARRIER_INVOICE_PAYMENT_TERMS, T.CARRIER_INVOICE_PAYMENT_TERMS), COALESCE(TMP.CARRIER_PAYMENT_METHOD, T.CARRIER_PAYMENT_METHOD), COALESCE(TMP.CARRIER_ID, T.CARRIER_ID), COALESCE(TMP.CARRIER_HAS_HAZMAT, T.CARRIER_HAS_HAZMAT), COALESCE(TMP.CONTACT_ID, T.CONTACT_ID), COALESCE(TMP.CONTACT_FIRST_NAME, T.CONTACT_FIRST_NAME), COALESCE(TMP.CONTACT_MIDDLE_NAME, T.CONTACT_MIDDLE_NAME), COALESCE(TMP.CONTACT_SURNAME, T.CONTACT_SURNAME), COALESCE(TMP.CONTACT_PREFIX, T.CONTACT_PREFIX), COALESCE(TMP.CONTACT_TITLE, T.CONTACT_TITLE), COALESCE(TMP.CONTACT_PHONE, T.CONTACT_PHONE), COALESCE(TMP.CONTACT_MOBILE_PHONE, T.CONTACT_MOBILE_PHONE), COALESCE(TMP.CONTACT_PAGER, T.CONTACT_PAGER), COALESCE(TMP.CONTACT_FAX, T.CONTACT_FAX), COALESCE(TMP.CONTACT_EMAIL, T.CONTACT_EMAIL), COALESCE(TMP.CONTACT_ADDRESS, T.CONTACT_ADDRESS), COALESCE(TMP.CONTACT_CITY, T.CONTACT_CITY), COALESCE(TMP.CONTACT_STATE_PROV, T.CONTACT_STATE_PROV), COALESCE(TMP.CONTACT_POSTAL_CD, T.CONTACT_POSTAL_CD), COALESCE(TMP.CONTACT_COUNTRY_CD, T.CONTACT_COUNTRY_CD), COALESCE(TMP.CONTACT_COMMENTS, T.CONTACT_COMMENTS), COALESCE(TMP.CONTACT_IS_PRIMARY, T.CONTACT_IS_PRIMARY), COALESCE(TMP.CONTACT_TYPE, T.CONTACT_TYPE), COALESCE(TMP.CONTACT_CREATED_DTTM, T.CONTACT_CREATED_DTTM), COALESCE(TMP.CONTACT_LAST_UPDATED_DTTM, T.CONTACT_LAST_UPDATED_DTTM), COALESCE(TMP.CARRIER_NOTE_CARRIER_TYPE, T.CARRIER_NOTE_CARRIER_TYPE), COALESCE(TMP.CARRIER_NOTE_CANADIAN_CARRIER, T.CARRIER_NOTE_CANADIAN_CARRIER)
FROM SCCT_DB.TMS_CARRIER_CONTACT_MASTER AS T
FULL JOIN SCCT_WORK_DB.TMS_CARRIER_CONTACT_MASTER_TMP AS TMP ON T.CARRIER_ID = TMP.CARRIER_ID AND T.CONTACT_ID = TMP.CONTACT_ID);
------------------------- ---loading staging data into final stagnig table -------------------------  set tez.job.name=staging:tms_carrier_contact_master_tmp1 :load target table from tms_carrier_contact_master_tmp;
! echo started loading into scct_work_db.tms_carrier_contact_master_tmp1;
------------------------- ---loading staging data into target table -------------------------  set tez.job.name=staging:tms_carrier_contact_master :load target table from tms_carrier_contact_master_tmp1;
! echo started loading into scct_db.tms_carrier_contact_master;
INSERT OVERWRITE INTO SCCT_DB.TMS_CARRIER_CONTACT_MASTER
(SELECT COMPANY_ID, CARRIER_CD, CARRIER_COMPANY_ID, CARRIER_COMPANY_NAME, CARRIER_NAME, CARRIER_ADDRESS, CARRIER_CITY, CARRIER_STATE_PROV, CARRIER_POSTAL_CD, CARRIER_COUNTRY_CD, CARRIER_STATUS, CARRIER_MARK_FOR_DELETION, CARRIER_COMMUNICATION_METHOD, CARRIER_CREATED_DTTM, CARRIER_LAST_UPDATED_DTTM, CARRIER_PAYMENT_REFERENCE_NBR, CARRIER_TARIFF_ID, CARRIER_INVOICE_PAYMENT_TERMS, CARRIER_PAYMENT_METHOD, CARRIER_ID, CARRIER_HAS_HAZMAT, CONTACT_ID, CONTACT_FIRST_NAME, CONTACT_MIDDLE_NAME, CONTACT_SURNAME, CONTACT_PREFIX, CONTACT_TITLE, CONTACT_PHONE, CONTACT_MOBILE_PHONE, CONTACT_PAGER, CONTACT_FAX, CONTACT_EMAIL, CONTACT_ADDRESS, CONTACT_CITY, CONTACT_STATE_PROV, CONTACT_POSTAL_CD, CONTACT_COUNTRY_CD, CONTACT_COMMENTS, CONTACT_IS_PRIMARY, CONTACT_TYPE, CONTACT_CREATED_DTTM, CONTACT_LAST_UPDATED_DTTM, CARRIER_NOTE_CARRIER_TYPE, CARRIER_NOTE_CANADIAN_CARRIER
FROM SCCT_WORK_DB.TMS_CARRIER_CONTACT_MASTER_TMP1);
------------------------- ---loading staging data into final stagnig table -------------------------  set tez.job.name=staging:tms_carrier_contact_master_tmp1 :load target table from tms_carrier_contact_master_tmp;
! echo started loading into scct_work_db.tms_carrier_contact_master_tmp1;
INSERT OVERWRITE INTO SCCT_WORK_DB.TMS_CARRIER_CONTACT_MASTER_TMP1
(SELECT COALESCE(TMP.COMPANY_ID, T.COMPANY_ID), COALESCE(TMP.CARRIER_CD, T.CARRIER_CD), COALESCE(TMP.CARRIER_COMPANY_ID, T.CARRIER_COMPANY_ID), COALESCE(TMP.CARRIER_COMPANY_NAME, T.CARRIER_COMPANY_NAME), COALESCE(TMP.CARRIER_NAME, T.CARRIER_NAME), COALESCE(TMP.CARRIER_ADDRESS, T.CARRIER_ADDRESS), COALESCE(TMP.CARRIER_CITY, T.CARRIER_CITY), COALESCE(TMP.CARRIER_STATE_PROV, T.CARRIER_STATE_PROV), COALESCE(TMP.CARRIER_POSTAL_CD, T.CARRIER_POSTAL_CD), COALESCE(TMP.CARRIER_COUNTRY_CD, T.CARRIER_COUNTRY_CD), COALESCE(TMP.CARRIER_STATUS, T.CARRIER_STATUS), COALESCE(TMP.CARRIER_MARK_FOR_DELETION, T.CARRIER_MARK_FOR_DELETION), COALESCE(TMP.CARRIER_COMMUNICATION_METHOD, T.CARRIER_COMMUNICATION_METHOD), COALESCE(TMP.CARRIER_CREATED_DTTM, T.CARRIER_CREATED_DTTM), COALESCE(TMP.CARRIER_LAST_UPDATED_DTTM, T.CARRIER_LAST_UPDATED_DTTM), COALESCE(TMP.CARRIER_PAYMENT_REFERENCE_NBR, T.CARRIER_PAYMENT_REFERENCE_NBR), COALESCE(TMP.CARRIER_TARIFF_ID, T.CARRIER_TARIFF_ID), COALESCE(TMP.CARRIER_INVOICE_PAYMENT_TERMS, T.CARRIER_INVOICE_PAYMENT_TERMS), COALESCE(TMP.CARRIER_PAYMENT_METHOD, T.CARRIER_PAYMENT_METHOD), COALESCE(TMP.CARRIER_ID, T.CARRIER_ID), COALESCE(TMP.CARRIER_HAS_HAZMAT, T.CARRIER_HAS_HAZMAT), COALESCE(TMP.CONTACT_ID, T.CONTACT_ID), COALESCE(TMP.CONTACT_FIRST_NAME, T.CONTACT_FIRST_NAME), COALESCE(TMP.CONTACT_MIDDLE_NAME, T.CONTACT_MIDDLE_NAME), COALESCE(TMP.CONTACT_SURNAME, T.CONTACT_SURNAME), COALESCE(TMP.CONTACT_PREFIX, T.CONTACT_PREFIX), COALESCE(TMP.CONTACT_TITLE, T.CONTACT_TITLE), COALESCE(TMP.CONTACT_PHONE, T.CONTACT_PHONE), COALESCE(TMP.CONTACT_MOBILE_PHONE, T.CONTACT_MOBILE_PHONE), COALESCE(TMP.CONTACT_PAGER, T.CONTACT_PAGER), COALESCE(TMP.CONTACT_FAX, T.CONTACT_FAX), COALESCE(TMP.CONTACT_EMAIL, T.CONTACT_EMAIL), COALESCE(TMP.CONTACT_ADDRESS, T.CONTACT_ADDRESS), COALESCE(TMP.CONTACT_CITY, T.CONTACT_CITY), COALESCE(TMP.CONTACT_STATE_PROV, T.CONTACT_STATE_PROV), COALESCE(TMP.CONTACT_POSTAL_CD, T.CONTACT_POSTAL_CD), COALESCE(TMP.CONTACT_COUNTRY_CD, T.CONTACT_COUNTRY_CD), COALESCE(TMP.CONTACT_COMMENTS, T.CONTACT_COMMENTS), COALESCE(TMP.CONTACT_IS_PRIMARY, T.CONTACT_IS_PRIMARY), COALESCE(TMP.CONTACT_TYPE, T.CONTACT_TYPE), COALESCE(TMP.CONTACT_CREATED_DTTM, T.CONTACT_CREATED_DTTM), COALESCE(TMP.CONTACT_LAST_UPDATED_DTTM, T.CONTACT_LAST_UPDATED_DTTM), COALESCE(TMP.CARRIER_NOTE_CARRIER_TYPE, T.CARRIER_NOTE_CARRIER_TYPE), COALESCE(TMP.CARRIER_NOTE_CANADIAN_CARRIER, T.CARRIER_NOTE_CANADIAN_CARRIER)
FROM SCCT_DB.TMS_CARRIER_CONTACT_MASTER AS T
FULL JOIN SCCT_WORK_DB.TMS_CARRIER_CONTACT_MASTER_TMP AS TMP ON T.CARRIER_ID = TMP.CARRIER_ID AND T.CONTACT_ID = TMP.CONTACT_ID);
------------------------- ---loading staging data into final stagnig table -------------------------  set tez.job.name=staging:tms_carrier_contact_master_tmp1 :load target table from tms_carrier_contact_master_tmp;
! echo started loading into scct_work_db.tms_carrier_contact_master_tmp1;
------------------------- ---loading staging data into target table -------------------------  set tez.job.name=staging:tms_carrier_contact_master :load target table from tms_carrier_contact_master_tmp1;
! echo started loading into scct_db.tms_carrier_contact_master;
INSERT OVERWRITE INTO SCCT_DB.TMS_CARRIER_CONTACT_MASTER
(SELECT COMPANY_ID, CARRIER_CD, CARRIER_COMPANY_ID, CARRIER_COMPANY_NAME, CARRIER_NAME, CARRIER_ADDRESS, CARRIER_CITY, CARRIER_STATE_PROV, CARRIER_POSTAL_CD, CARRIER_COUNTRY_CD, CARRIER_STATUS, CARRIER_MARK_FOR_DELETION, CARRIER_COMMUNICATION_METHOD, CARRIER_CREATED_DTTM, CARRIER_LAST_UPDATED_DTTM, CARRIER_PAYMENT_REFERENCE_NBR, CARRIER_TARIFF_ID, CARRIER_INVOICE_PAYMENT_TERMS, CARRIER_PAYMENT_METHOD, CARRIER_ID, CARRIER_HAS_HAZMAT, CONTACT_ID, CONTACT_FIRST_NAME, CONTACT_MIDDLE_NAME, CONTACT_SURNAME, CONTACT_PREFIX, CONTACT_TITLE, CONTACT_PHONE, CONTACT_MOBILE_PHONE, CONTACT_PAGER, CONTACT_FAX, CONTACT_EMAIL, CONTACT_ADDRESS, CONTACT_CITY, CONTACT_STATE_PROV, CONTACT_POSTAL_CD, CONTACT_COUNTRY_CD, CONTACT_COMMENTS, CONTACT_IS_PRIMARY, CONTACT_TYPE, CONTACT_CREATED_DTTM, CONTACT_LAST_UPDATED_DTTM, CARRIER_NOTE_CARRIER_TYPE, CARRIER_NOTE_CANADIAN_CARRIER
FROM SCCT_WORK_DB.TMS_CARRIER_CONTACT_MASTER_TMP1);
------------------------- ---loading staging data into final stagnig table -------------------------  set tez.job.name=staging:tms_carrier_contact_master_tmp1 :load target table from tms_carrier_contact_master_tmp;
! echo started loading into scct_work_db.tms_carrier_contact_master_tmp1;
INSERT OVERWRITE INTO SCCT_WORK_DB.TMS_CARRIER_CONTACT_MASTER_TMP1
(SELECT COALESCE(TMP.COMPANY_ID, T.COMPANY_ID), COALESCE(TMP.CARRIER_CD, T.CARRIER_CD), COALESCE(TMP.CARRIER_COMPANY_ID, T.CARRIER_COMPANY_ID), COALESCE(TMP.CARRIER_COMPANY_NAME, T.CARRIER_COMPANY_NAME), COALESCE(TMP.CARRIER_NAME, T.CARRIER_NAME), COALESCE(TMP.CARRIER_ADDRESS, T.CARRIER_ADDRESS), COALESCE(TMP.CARRIER_CITY, T.CARRIER_CITY), COALESCE(TMP.CARRIER_STATE_PROV, T.CARRIER_STATE_PROV), COALESCE(TMP.CARRIER_POSTAL_CD, T.CARRIER_POSTAL_CD), COALESCE(TMP.CARRIER_COUNTRY_CD, T.CARRIER_COUNTRY_CD), COALESCE(TMP.CARRIER_STATUS, T.CARRIER_STATUS), COALESCE(TMP.CARRIER_MARK_FOR_DELETION, T.CARRIER_MARK_FOR_DELETION), COALESCE(TMP.CARRIER_COMMUNICATION_METHOD, T.CARRIER_COMMUNICATION_METHOD), COALESCE(TMP.CARRIER_CREATED_DTTM, T.CARRIER_CREATED_DTTM), COALESCE(TMP.CARRIER_LAST_UPDATED_DTTM, T.CARRIER_LAST_UPDATED_DTTM), COALESCE(TMP.CARRIER_PAYMENT_REFERENCE_NBR, T.CARRIER_PAYMENT_REFERENCE_NBR), COALESCE(TMP.CARRIER_TARIFF_ID, T.CARRIER_TARIFF_ID), COALESCE(TMP.CARRIER_INVOICE_PAYMENT_TERMS, T.CARRIER_INVOICE_PAYMENT_TERMS), COALESCE(TMP.CARRIER_PAYMENT_METHOD, T.CARRIER_PAYMENT_METHOD), COALESCE(TMP.CARRIER_ID, T.CARRIER_ID), COALESCE(TMP.CARRIER_HAS_HAZMAT, T.CARRIER_HAS_HAZMAT), COALESCE(TMP.CONTACT_ID, T.CONTACT_ID), COALESCE(TMP.CONTACT_FIRST_NAME, T.CONTACT_FIRST_NAME), COALESCE(TMP.CONTACT_MIDDLE_NAME, T.CONTACT_MIDDLE_NAME), COALESCE(TMP.CONTACT_SURNAME, T.CONTACT_SURNAME), COALESCE(TMP.CONTACT_PREFIX, T.CONTACT_PREFIX), COALESCE(TMP.CONTACT_TITLE, T.CONTACT_TITLE), COALESCE(TMP.CONTACT_PHONE, T.CONTACT_PHONE), COALESCE(TMP.CONTACT_MOBILE_PHONE, T.CONTACT_MOBILE_PHONE), COALESCE(TMP.CONTACT_PAGER, T.CONTACT_PAGER), COALESCE(TMP.CONTACT_FAX, T.CONTACT_FAX), COALESCE(TMP.CONTACT_EMAIL, T.CONTACT_EMAIL), COALESCE(TMP.CONTACT_ADDRESS, T.CONTACT_ADDRESS), COALESCE(TMP.CONTACT_CITY, T.CONTACT_CITY), COALESCE(TMP.CONTACT_STATE_PROV, T.CONTACT_STATE_PROV), COALESCE(TMP.CONTACT_POSTAL_CD, T.CONTACT_POSTAL_CD), COALESCE(TMP.CONTACT_COUNTRY_CD, T.CONTACT_COUNTRY_CD), COALESCE(TMP.CONTACT_COMMENTS, T.CONTACT_COMMENTS), COALESCE(TMP.CONTACT_IS_PRIMARY, T.CONTACT_IS_PRIMARY), COALESCE(TMP.CONTACT_TYPE, T.CONTACT_TYPE), COALESCE(TMP.CONTACT_CREATED_DTTM, T.CONTACT_CREATED_DTTM), COALESCE(TMP.CONTACT_LAST_UPDATED_DTTM, T.CONTACT_LAST_UPDATED_DTTM), COALESCE(TMP.CARRIER_NOTE_CARRIER_TYPE, T.CARRIER_NOTE_CARRIER_TYPE), COALESCE(TMP.CARRIER_NOTE_CANADIAN_CARRIER, T.CARRIER_NOTE_CANADIAN_CARRIER)
FROM SCCT_DB.TMS_CARRIER_CONTACT_MASTER AS T
FULL JOIN SCCT_WORK_DB.TMS_CARRIER_CONTACT_MASTER_TMP AS TMP ON T.CARRIER_ID = TMP.CARRIER_ID AND T.CONTACT_ID = TMP.CONTACT_ID);
------------------------- ---loading staging data into final stagnig table -------------------------  set tez.job.name=staging:tms_carrier_contact_master_tmp1 :load target table from tms_carrier_contact_master_tmp;
! echo started loading into scct_work_db.tms_carrier_contact_master_tmp1;
------------------------- ---loading staging data into target table -------------------------  set tez.job.name=staging:tms_carrier_contact_master :load target table from tms_carrier_contact_master_tmp1;
! echo started loading into scct_db.tms_carrier_contact_master;
INSERT OVERWRITE INTO SCCT_DB.TMS_CARRIER_CONTACT_MASTER
(SELECT COMPANY_ID, CARRIER_CD, CARRIER_COMPANY_ID, CARRIER_COMPANY_NAME, CARRIER_NAME, CARRIER_ADDRESS, CARRIER_CITY, CARRIER_STATE_PROV, CARRIER_POSTAL_CD, CARRIER_COUNTRY_CD, CARRIER_STATUS, CARRIER_MARK_FOR_DELETION, CARRIER_COMMUNICATION_METHOD, CARRIER_CREATED_DTTM, CARRIER_LAST_UPDATED_DTTM, CARRIER_PAYMENT_REFERENCE_NBR, CARRIER_TARIFF_ID, CARRIER_INVOICE_PAYMENT_TERMS, CARRIER_PAYMENT_METHOD, CARRIER_ID, CARRIER_HAS_HAZMAT, CONTACT_ID, CONTACT_FIRST_NAME, CONTACT_MIDDLE_NAME, CONTACT_SURNAME, CONTACT_PREFIX, CONTACT_TITLE, CONTACT_PHONE, CONTACT_MOBILE_PHONE, CONTACT_PAGER, CONTACT_FAX, CONTACT_EMAIL, CONTACT_ADDRESS, CONTACT_CITY, CONTACT_STATE_PROV, CONTACT_POSTAL_CD, CONTACT_COUNTRY_CD, CONTACT_COMMENTS, CONTACT_IS_PRIMARY, CONTACT_TYPE, CONTACT_CREATED_DTTM, CONTACT_LAST_UPDATED_DTTM, CARRIER_NOTE_CARRIER_TYPE, CARRIER_NOTE_CANADIAN_CARRIER
FROM SCCT_WORK_DB.TMS_CARRIER_CONTACT_MASTER_TMP1);
