! echo completed loading into scct_db.scem_expected_events;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_events -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_events  ---source: sap_stage0 table : scem_tracking_events_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_events ---load type: full ---back posting: no --------------------------------------------------------------------------  -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 23jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti changed patition key to eh_created_date and added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic --  ------------------------- ---job related hive settings to merge files for target table ------------------------- set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_events target table data -------------------------  set tez.job.name=stage2:scem_tracking_events :load target table scem_tracking_events ;
! echo started loading into scct_db.scem_tracking_events ;
INSERT OVERWRITE INTO scct_db.scem_tracking_events
(SELECT EH_GUID, BW_PROFILE, EVENT_CODE, EVT_MSG_GUID, SERVPROV_CD, LOC_CD_1, INFO_TYPE, EH_TYPE, SERVPROV_CS, LOC_CS, LOC_CD_2, EVENT_GROUP, EVENT_STATUS, MSG_STATUS, RSN_CS_STATUS, RSN_CD_STATUS, SENDER_CS, SENDER_CD, RECIPIENT_CS, RECIPIENT_CD, MSG_SRCTYPE, MSG_UTC_DATE, MSG_UTC_TIME, MSG_EXP_TZONE, MSG_EXP_DATE, MSG_EXP_TIME, MSG_ORG_EXP_DATE, MSG_ORG_EXP_TIME, MSG_EAR_EXP_DATE, MSG_EAR_EXP_TIME, MSG_LAT_EXP_DATE, MSG_LAT_EXP_TIME, EVT_UTC_DATE, EVT_UTC_TIME, EVT_EST_DATE, EVT_EST_TIME, EVT_EST_DATE_TIME, EVT_EXP_TZONE, EVT_EXP_DATE, EVT_EXP_TIME, EVT_ORG_EXP_DATE, EVT_ORG_EXP_TIME, EVT_EAR_EXP_DATE, EVT_EAR_EXP_TIME, EVT_LAT_EXP_DATE, EVT_LAT_EXP_TIME, EVT_AO_EXP_DATE, EVT_AO_EXP_TIME, EVT_DURATION_01, EVT_DURATION_02, EVT_DURATION_03, EVT_DURATION_04, EVT_DURATION_05, MSG_EXP_DURATION, EVT_EXP_DURATION, PLANNED_TOT_DUR, PLANNED_EFF_DUR, EVT_MSG_CNTR, EM_CLIENT, ROCANCEL, DATE1, TIME1, TZONE1, DATE2, TIME2, TZONE2, DATE3, TIME3, TZONE3, DATE4, TIME4, TZONE4, DATE5, TIME5, TZONE5, DATACS, DATAID, CARRARRIVAL_DATE, CARRARRIVAL_TIME, HUBARRIVAL_DATE, HUBARRIVAL_TIME, HUBDEPART_DATE, HUBDEPART_TIME, SRCTX, EVENT_REASONTEXT, EVENT_SENDERNAME, USERNAME
FROM SCCT_WORK_DB.SCEM_TRACKING_EVENTS_TMP
WHERE UPPER(DI_OPERATION_TYPE) IN ('i', 'u', 'e'));
! echo completed loading into scct_db.scem_expected_events;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_events -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_events  ---source: sap_stage0 table : scem_tracking_events_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_events ---load type: full ---back posting: no --------------------------------------------------------------------------  -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 23jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti changed patition key to eh_created_date and added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic --  ------------------------- ---job related hive settings to merge files for target table ------------------------- set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_events target table data -------------------------  set tez.job.name=stage2:scem_tracking_events :load target table scem_tracking_events ;
! echo started loading into scct_db.scem_tracking_events ;
INSERT OVERWRITE INTO scct_db.scem_tracking_events
(SELECT EH_GUID, BW_PROFILE, EVENT_CODE, EVT_MSG_GUID, SERVPROV_CD, LOC_CD_1, INFO_TYPE, EH_TYPE, SERVPROV_CS, LOC_CS, LOC_CD_2, EVENT_GROUP, EVENT_STATUS, MSG_STATUS, RSN_CS_STATUS, RSN_CD_STATUS, SENDER_CS, SENDER_CD, RECIPIENT_CS, RECIPIENT_CD, MSG_SRCTYPE, MSG_UTC_DATE, MSG_UTC_TIME, MSG_EXP_TZONE, MSG_EXP_DATE, MSG_EXP_TIME, MSG_ORG_EXP_DATE, MSG_ORG_EXP_TIME, MSG_EAR_EXP_DATE, MSG_EAR_EXP_TIME, MSG_LAT_EXP_DATE, MSG_LAT_EXP_TIME, EVT_UTC_DATE, EVT_UTC_TIME, EVT_EST_DATE, EVT_EST_TIME, EVT_EST_DATE_TIME, EVT_EXP_TZONE, EVT_EXP_DATE, EVT_EXP_TIME, EVT_ORG_EXP_DATE, EVT_ORG_EXP_TIME, EVT_EAR_EXP_DATE, EVT_EAR_EXP_TIME, EVT_LAT_EXP_DATE, EVT_LAT_EXP_TIME, EVT_AO_EXP_DATE, EVT_AO_EXP_TIME, EVT_DURATION_01, EVT_DURATION_02, EVT_DURATION_03, EVT_DURATION_04, EVT_DURATION_05, MSG_EXP_DURATION, EVT_EXP_DURATION, PLANNED_TOT_DUR, PLANNED_EFF_DUR, EVT_MSG_CNTR, EM_CLIENT, ROCANCEL, DATE1, TIME1, TZONE1, DATE2, TIME2, TZONE2, DATE3, TIME3, TZONE3, DATE4, TIME4, TZONE4, DATE5, TIME5, TZONE5, DATACS, DATAID, CARRARRIVAL_DATE, CARRARRIVAL_TIME, HUBARRIVAL_DATE, HUBARRIVAL_TIME, HUBDEPART_DATE, HUBDEPART_TIME, SRCTX, EVENT_REASONTEXT, EVENT_SENDERNAME, USERNAME
FROM SCCT_WORK_DB.SCEM_TRACKING_EVENTS_TMP
WHERE UPPER(DI_OPERATION_TYPE) IN ('i', 'u', 'e'));
! echo completed loading into scct_db.scem_expected_events;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_events -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_events  ---source: sap_stage0 table : scem_tracking_events_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_events ---load type: full ---back posting: no --------------------------------------------------------------------------  -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 23jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti changed patition key to eh_created_date and added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic --  ------------------------- ---job related hive settings to merge files for target table ------------------------- set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_events target table data -------------------------  set tez.job.name=stage2:scem_tracking_events :load target table scem_tracking_events ;
! echo started loading into scct_db.scem_tracking_events ;
INSERT OVERWRITE INTO scct_db.scem_tracking_events
(SELECT EH_GUID, BW_PROFILE, EVENT_CODE, EVT_MSG_GUID, SERVPROV_CD, LOC_CD_1, INFO_TYPE, EH_TYPE, SERVPROV_CS, LOC_CS, LOC_CD_2, EVENT_GROUP, EVENT_STATUS, MSG_STATUS, RSN_CS_STATUS, RSN_CD_STATUS, SENDER_CS, SENDER_CD, RECIPIENT_CS, RECIPIENT_CD, MSG_SRCTYPE, MSG_UTC_DATE, MSG_UTC_TIME, MSG_EXP_TZONE, MSG_EXP_DATE, MSG_EXP_TIME, MSG_ORG_EXP_DATE, MSG_ORG_EXP_TIME, MSG_EAR_EXP_DATE, MSG_EAR_EXP_TIME, MSG_LAT_EXP_DATE, MSG_LAT_EXP_TIME, EVT_UTC_DATE, EVT_UTC_TIME, EVT_EST_DATE, EVT_EST_TIME, EVT_EST_DATE_TIME, EVT_EXP_TZONE, EVT_EXP_DATE, EVT_EXP_TIME, EVT_ORG_EXP_DATE, EVT_ORG_EXP_TIME, EVT_EAR_EXP_DATE, EVT_EAR_EXP_TIME, EVT_LAT_EXP_DATE, EVT_LAT_EXP_TIME, EVT_AO_EXP_DATE, EVT_AO_EXP_TIME, EVT_DURATION_01, EVT_DURATION_02, EVT_DURATION_03, EVT_DURATION_04, EVT_DURATION_05, MSG_EXP_DURATION, EVT_EXP_DURATION, PLANNED_TOT_DUR, PLANNED_EFF_DUR, EVT_MSG_CNTR, EM_CLIENT, ROCANCEL, DATE1, TIME1, TZONE1, DATE2, TIME2, TZONE2, DATE3, TIME3, TZONE3, DATE4, TIME4, TZONE4, DATE5, TIME5, TZONE5, DATACS, DATAID, CARRARRIVAL_DATE, CARRARRIVAL_TIME, HUBARRIVAL_DATE, HUBARRIVAL_TIME, HUBDEPART_DATE, HUBDEPART_TIME, SRCTX, EVENT_REASONTEXT, EVENT_SENDERNAME, USERNAME
FROM SCCT_WORK_DB.SCEM_TRACKING_EVENTS_TMP
WHERE UPPER(DI_OPERATION_TYPE) IN ('i', 'u', 'e'));
! echo completed loading into scct_db.scem_expected_events;
---jobname:scct-scem-hive-stg1-scct_db.scem_tracking_events -------------------------------------------------------------------------- ---target: stage1 table: scct_db.scem_tracking_events  ---source: sap_stage0 table : scem_tracking_events_tmp ---description: this script will load the delta data to target table scct_db.scem_tracking_events ---load type: full ---back posting: no --------------------------------------------------------------------------  -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 23jul19 sdhal initial version -- 01.00.00.02 06sep19 scctp4-2187 akoti changed patition key to eh_created_date and added logic to ignore records where di_operation_type <> 'd' -- 01.00.00.03 29nov19 scctp4-2623 akoti removed partition column & settings, changed di_operation_type logic --  ------------------------- ---job related hive settings to merge files for target table ------------------------- set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading scem_tracking_events target table data -------------------------  set tez.job.name=stage2:scem_tracking_events :load target table scem_tracking_events ;
! echo started loading into scct_db.scem_tracking_events ;
INSERT OVERWRITE INTO scct_db.scem_tracking_events
(SELECT EH_GUID, BW_PROFILE, EVENT_CODE, EVT_MSG_GUID, SERVPROV_CD, LOC_CD_1, INFO_TYPE, EH_TYPE, SERVPROV_CS, LOC_CS, LOC_CD_2, EVENT_GROUP, EVENT_STATUS, MSG_STATUS, RSN_CS_STATUS, RSN_CD_STATUS, SENDER_CS, SENDER_CD, RECIPIENT_CS, RECIPIENT_CD, MSG_SRCTYPE, MSG_UTC_DATE, MSG_UTC_TIME, MSG_EXP_TZONE, MSG_EXP_DATE, MSG_EXP_TIME, MSG_ORG_EXP_DATE, MSG_ORG_EXP_TIME, MSG_EAR_EXP_DATE, MSG_EAR_EXP_TIME, MSG_LAT_EXP_DATE, MSG_LAT_EXP_TIME, EVT_UTC_DATE, EVT_UTC_TIME, EVT_EST_DATE, EVT_EST_TIME, EVT_EST_DATE_TIME, EVT_EXP_TZONE, EVT_EXP_DATE, EVT_EXP_TIME, EVT_ORG_EXP_DATE, EVT_ORG_EXP_TIME, EVT_EAR_EXP_DATE, EVT_EAR_EXP_TIME, EVT_LAT_EXP_DATE, EVT_LAT_EXP_TIME, EVT_AO_EXP_DATE, EVT_AO_EXP_TIME, EVT_DURATION_01, EVT_DURATION_02, EVT_DURATION_03, EVT_DURATION_04, EVT_DURATION_05, MSG_EXP_DURATION, EVT_EXP_DURATION, PLANNED_TOT_DUR, PLANNED_EFF_DUR, EVT_MSG_CNTR, EM_CLIENT, ROCANCEL, DATE1, TIME1, TZONE1, DATE2, TIME2, TZONE2, DATE3, TIME3, TZONE3, DATE4, TIME4, TZONE4, DATE5, TIME5, TZONE5, DATACS, DATAID, CARRARRIVAL_DATE, CARRARRIVAL_TIME, HUBARRIVAL_DATE, HUBARRIVAL_TIME, HUBDEPART_DATE, HUBDEPART_TIME, SRCTX, EVENT_REASONTEXT, EVENT_SENDERNAME, USERNAME
FROM SCCT_WORK_DB.SCEM_TRACKING_EVENTS_TMP
WHERE UPPER(DI_OPERATION_TYPE) IN ('i', 'u', 'e'));