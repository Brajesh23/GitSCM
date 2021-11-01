---jobname:scct-scem-hive-stg1-scct_db.scem_expected_events
--------------------------------------------------------------------------
---Target: WORK table: scct_db.scem_expected_events
---Source: sap_stage0 table : saptrx_eh_hdr and saptrx_eh_expev ;
---Load Type: Full
---Back posting: No
---Author: ssoma
---Created Date: 02/04/2019
--------------------------------------------------------------------------

set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.dynamic.partitions.pernode=20000;
set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

-------------------------
---loading stage1 table scct_db.scem_expected_events 
-------------------------

SET tez.job.name=stage1:scem_expected_events:load target table scem_expected_events;

! echo Started Loading into scct_db.scem_expected_events;

insert overwrite table scct_db.scem_expected_events partition(created_dt)  
select 
coalesce(from_unixtime(unix_timestamp(substring(cast(ehhdr.active_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as active_dt_tm
,coalesce(case when trim(ehhdr.ao_client)='' then null else trim(ehhdr.ao_client) end, 0) as ao_client
,coalesce(case when trim(ehhdr.ao_id)='' then null else trim(ehhdr.ao_id) end, 'N/A') as ao_id
,coalesce(case when trim(ehhdr.ao_system)='' then null else trim(ehhdr.ao_system) end, 'N/A') as ao_system
,coalesce(case when trim(ehhdr.ao_type)='' then null else trim(ehhdr.ao_type) end, 'N/A') as ao_type
,coalesce(case when trim(ehhdr.app_log_flag)='' then null else trim(ehhdr.app_log_flag) end, 'N/A') as app_log_flg
,coalesce(case when trim(ehhdr.bus_proc_type)='' then null else trim(ehhdr.bus_proc_type) end, 'N/A') as bus_proc_typ
,coalesce(case when trim(ehhdr.bw_posted)='' then null else trim(ehhdr.bw_posted) end, 'N/A') as bw_posted
,coalesce(case when trim(ehhdr.chk_unexpected)='' then null else trim(ehhdr.chk_unexpected) end, 'N/A') as chk_unexpected
,coalesce(case when trim(ehhdr.client)='' then null else trim(ehhdr.client) end, 0) as client_hdr
,coalesce(case when trim(ehhdr.created_by)='' then null else trim(ehhdr.created_by) end, 'N/A') as created_by
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehhdr.created_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as created_dt_tm
,coalesce(case when trim(ehhdr.df_exist)='' then null else trim(ehhdr.df_exist) end, 'N/A') as df_exist
,coalesce(case when trim(ehhdr.ee_eval_proc)='' then null else trim(ehhdr.ee_eval_proc) end, 'N/A') as ee_eval_proc
,coalesce(case when trim(ehhdr.ee_monitor_proc)='' then null else trim(ehhdr.ee_monitor_proc) end, 'N/A') as ee_monitor_proc
,coalesce(case when trim(ehhdr.ee_profile)='' then null else trim(ehhdr.ee_profile) end, 'N/A') as ee_profile
,coalesce(case when trim(ehhdr.eh_active)='' then null else trim(ehhdr.eh_active) end, 'N/A') as eh_active
,coalesce(case when trim(ehhdr.eh_archived)='' then null else trim(ehhdr.eh_archived) end, 'N/A') as eh_archived
,coalesce(case when trim(ehhdr.eh_changeable)='' then null else trim(ehhdr.eh_changeable) end, 'N/A') as eh_changeable
,coalesce(case when trim(ehhdr.eh_deleted)='' then null else trim(ehhdr.eh_deleted) end, 'N/A') as eh_deleted
,coalesce(case when trim(ehhdr.eh_guid)='' then null else trim(ehhdr.eh_guid) end, 'N/A') as eh_guid_hdr
,coalesce(case when trim(ehhdr.eh_type)='' then null else trim(ehhdr.eh_type) end, 'N/A') as eh_type
,coalesce(case when trim(ehhdr.em_version)='' then null else trim(ehhdr.em_version) end, 'N/A') as em_version
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehhdr.end_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as end_dt_tm
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehhdr.inactive_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as inactive_dt_tm
,coalesce(ehhdr.last_cntrl_nbr, 0) as last_cntrl_nbr
,coalesce(ehhdr.last_eehst_nbr, 0) as last_eehst_nbr
,coalesce(ehhdr.last_ermsg_nbr, 0) as last_ermsg_nbr
,coalesce(ehhdr.last_evmsg_nbr, 0) as last_evmsg_nbr
,coalesce(case when trim(ehhdr.last_ev_msg)='' then null else trim(ehhdr.last_ev_msg) end, 'N/A') as last_ev_msg
,coalesce(ehhdr.last_expev_nbr, 0) as last_expev_nbr
,coalesce(ehhdr.last_hier_nbr , 0) as last_hier_nbr
,coalesce(ehhdr.last_info_nbr , 0) as last_info_nbr
,coalesce(ehhdr.last_measr_nbr, 0) as last_measr_nbr
,coalesce(ehhdr.last_mehst_nbr, 0) as last_mehst_nbr
,coalesce(ehhdr.last_qryid_nbr, 0) as last_qryid_nbr
,coalesce(ehhdr.last_sthst_nbr, 0) as last_sthst_nbr
,coalesce(ehhdr.last_task_nbr , 0) as last_task_nbr
,coalesce(ehhdr.last_trkid_nbr, 0) as last_trkid_nbr
,coalesce(case when trim(ehhdr.loglvl_ehpost )='' then null else trim(ehhdr.loglvl_ehpost ) end, 'N/A') as loglvl_ehpost
,coalesce(case when trim(ehhdr.loglvl_extupd )='' then null else trim(ehhdr.loglvl_extupd ) end, 'N/A') as loglvl_extupd
,coalesce(case when trim(ehhdr.loglvl_msgprc )='' then null else trim(ehhdr.loglvl_msgprc ) end, 'N/A') as loglvl_msgprc
,coalesce(case when trim(ehhdr.match_ee_stored)='' then null else trim(ehhdr.match_ee_stored) end, 'N/A') as match_ee_stored
,coalesce(case when trim(ehhdr.ruleset_id)='' then null else trim(ehhdr.ruleset_id) end, 'N/A') as ruleset_id
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehhdr.start_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as start_dt_tm
,coalesce(case when trim(ehhdr.status_prof)='' then null else trim(ehhdr.status_prof) end, 'N/A') as status_prof
,coalesce(case when trim(ehhdr.trackingid )='' then null else trim(ehhdr.trackingid) end, 'N/A') as tracking_id
,coalesce(case when trim(ehhdr.trackingidtype)='' then null else trim(ehhdr.trackingidtype) end, 'N/A') as tracking_id_typ
,coalesce(case when trim(ehhdr.updated_by)='' then null else trim(ehhdr.updated_by) end, 'N/A') as updated_by
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehhdr.updated_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as updated_dt_tm
,coalesce(case when trim(ehhdr.update_proc_1)='' then null else trim(ehhdr.update_proc_1) end, 'N/A') as update_proc_1
,coalesce(case when trim(ehhdr.update_proc_2)='' then null else trim(ehhdr.update_proc_2) end, 'N/A') as update_proc_2
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.ao_ev_exp_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as ao_ev_exp_dt_tm
,coalesce(case when trim(ehdtl.chk_data_func)='' then null else trim(ehdtl.chk_data_func) end, 'N/A') as chk_data_func
,coalesce(case when trim(ehdtl.chk_loc_func)='' then null else trim(ehdtl.chk_loc_func) end, 'N/A') as chk_loc_func
,coalesce(case when trim(ehdtl.chk_partner_func)='' then null else trim(ehdtl.chk_partner_func) end, 'N/A') as chk_partner_func
,coalesce(case when trim(ehdtl.city)='' then null else trim(ehdtl.city) end, 'N/A') as city
,coalesce(case when trim(ehdtl.client)='' then null else trim(ehdtl.client) end, 0) as client_dtl
,coalesce(case when trim(ehdtl.country)='' then null else trim(ehdtl.country) end, 'N/A') as country
,coalesce(case when trim(ehdtl.datacs)='' then null else trim(ehdtl.datacs) end, 'N/A') as datacs
,coalesce(case when trim(ehdtl.dataid)='' then null else trim(ehdtl.dataid) end, 'N/A') as dataid
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.datetime1 as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as dt_tm_1
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.datetime2 as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as dt_tm_2
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.datetime3 as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as dt_tm_3
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.datetime4 as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as dt_tm_4
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.datetime5 as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as dt_tm_5
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.earliest_ev_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as earliest_ev_dt_tm
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.earliest_msg_dte as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as earliest_msg_dt_tm
,coalesce(case when trim(ehdtl.eh_guid)='' then null else trim(ehdtl.eh_guid) end, 'N/A') as eh_guid_dtl
,coalesce(case when trim(ehdtl.event_code)='' then null else trim(ehdtl.event_code) end, 'N/A') as event_cd
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.event_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as event_dt_tm
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.event_date_utc as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as event_utc_dt_tm
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.event_exp_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as event_exp_dt_tm
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.event_exp_index as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as event_exp_index_dt_tm
,coalesce(case when trim(ehdtl.event_exp_tzone)='' then null else trim(ehdtl.event_exp_tzone) end, 'N/A') as event_exp_tzone
,coalesce(case when trim(ehdtl.event_group)='' then null else trim(ehdtl.event_group) end, 'N/A') as event_group
,coalesce(case when trim(ehdtl.event_status)='' then null else trim(ehdtl.event_status) end, 'N/A') as event_status
,coalesce(case when trim(ehdtl.event_tzone)='' then null else trim(ehdtl.event_tzone) end, 'N/A') as event_tzone
,coalesce(case when trim(ehdtl.ev_exp_calc_rule)='' then null else trim(ehdtl.ev_exp_calc_rule) end, 'N/A') as ev_exp_calc_rule
,coalesce(ehdtl.ev_exp_date_seq, 0) as ev_exp_date_seq
,coalesce(ehdtl.ev_exp_dur, 0) as ev_exp_dur
,coalesce(case when trim(ehdtl.ev_exp_dur_sign)='' then null else trim(ehdtl.ev_exp_dur_sign) end, 'N/A') as ev_exp_dur_sign
,coalesce(ehdtl.ev_tolerance, 0) as ev_tolerance
,coalesce(case when trim(ehdtl.ev_tol_rule)='' then null else trim(ehdtl.ev_tol_rule) end, 'N/A') as ev_tol_rule
,coalesce(case when trim(ehdtl.first_ev_evcd)='' then null else trim(ehdtl.first_ev_evcd) end, 'N/A') as first_ev_evcd
,coalesce(case when trim(ehdtl.forecast)='' then null else trim(ehdtl.forecast) end, 'N/A') as forecast
,coalesce(ehdtl.group_nbr, 0) as group_nbr
,coalesce(case when trim(ehdtl.itemident)='' then null else trim(ehdtl.itemident) end, 'N/A') as item_id_ent
,coalesce(lpad(ehdtl.item_nbr,18,0), 0) as itm_nbr
,coalesce(case when trim(ehdtl.language)='' then null else trim(ehdtl.language) end, 0) as language
,coalesce(case when trim(ehdtl.last_ev_evcd)='' then null else trim(ehdtl.last_ev_evcd) end, 'N/A') as last_ev_evcd
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.latest_ev_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as latest_ev_dt_tm
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.latest_msg_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as latest_msg_dt_tm
,coalesce(case when trim(ehdtl.locat_desc)='' then null else trim(ehdtl.locat_desc) end, 'N/A') as locat_desc
,coalesce(case when trim(ehdtl.loc_id_1)='' then null else trim(ehdtl.loc_id_1) end, 'N/A') as loc_id_1
,coalesce(case when trim(ehdtl.loc_id_2)='' then null else trim(ehdtl.loc_id_2) end, 'N/A') as loc_id_2
,coalesce(case when trim(ehdtl.loc_id_type)='' then null else trim(ehdtl.loc_id_type) end, 'N/A') as loc_id_type
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.msg_date_utc as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as msg_dt_tm_utc
,coalesce(case when trim(ehdtl.msg_exp_calc_rul)='' then null else trim(ehdtl.msg_exp_calc_rul) end, 'N/A') as msg_exp_calc_rul
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.msg_exp_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as msg_exp_dt_tm
,coalesce(ehdtl.msg_exp_date_seq, 0) as msg_exp_date_seq
,coalesce(ehdtl.msg_exp_dur, 0) as msg_exp_dur
,coalesce(case when trim(ehdtl.msg_exp_dur_sign)='' then null else trim(ehdtl.msg_exp_dur_sign) end, 'N/A') as msg_exp_dur_sign
,coalesce(case when trim(ehdtl.msg_exp_index)='' then null else trim(ehdtl.msg_exp_index) end, 'N/A') as msg_exp_index
,coalesce(case when trim(ehdtl.msg_exp_tzone)='' then null else trim(ehdtl.msg_exp_tzone) end, 'N/A') as msg_exp_tzone
,coalesce(case when trim(ehdtl.msg_guid)='' then null else trim(ehdtl.msg_guid) end, 'N/A') as msg_guid
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.msg_rcvd_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as msg_rcvd_dt_tm
,coalesce(case when trim(ehdtl.msg_rcvd_tzone)='' then null else trim(ehdtl.msg_rcvd_tzone) end, 'N/A') as msg_rcvd_tzone
,coalesce(case when trim(ehdtl.msg_status)='' then null else trim(ehdtl.msg_status) end, 'N/A') as msg_status
,coalesce(ehdtl.msg_tolerance, 0) as msg_tolerance
,coalesce(case when trim(ehdtl.msg_tol_rule)='' then null else trim(ehdtl.msg_tol_rule) end, 'N/A') as msg_tol_rule
,coalesce(case when trim(ehdtl.no_data_val)='' then null else trim(ehdtl.no_data_val) end, 'N/A') as no_data_val
,coalesce(case when trim(ehdtl.no_loc_val)='' then null else trim(ehdtl.no_loc_val) end, 'N/A') as no_loc_val
,coalesce(case when trim(ehdtl.no_partner_val)='' then null else trim(ehdtl.no_partner_val) end, 'N/A') as no_partner_val
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.orig_ev_exp_date as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as orig_ev_exp_dt_tm
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehdtl.orig_msg_exp_dte as bigint),1,14),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss'), '9999-12-31 00:00:00') as orig_msg_exp_dt_tm
,coalesce(case when trim(ehdtl.partner_id)='' then null else trim(ehdtl.partner_id) end, 'N/A') as partner_id
,coalesce(case when trim(ehdtl.partner_id_type)='' then null else trim(ehdtl.partner_id_type) end, 'N/A') as partner_id_typ
,coalesce(case when trim(ehdtl.postal_cd)='' then null else trim(ehdtl.postal_cd) end, 'N/A') as postal_cd
,coalesce(case when trim(ehdtl.pred_flag)='' then null else trim(ehdtl.pred_flag) end, 'N/A') as pred_flg
,coalesce(ehdtl.priority, 0) as priority
,coalesce(ehdtl.refdistance, 0) as refdistance
,coalesce(case when trim(ehdtl.refdistunit)='' then null else trim(ehdtl.refdistunit) end, 0) as refdistunit
,coalesce(ehdtl.refeffdurat, 0) as refeffdurat
,coalesce(ehdtl.refmilestone, 0) as refmilestone
,coalesce(ehdtl.reftotdurat, 0) as reftotdurat
,coalesce(case when trim(ehdtl.region)='' then null else trim(ehdtl.region) end, 'N/A') as region
,coalesce(case when trim(ehdtl.reprocess_flag)='' then null else trim(ehdtl.reprocess_flag) end, 'N/A') as reprocess_flg
,coalesce(ehdtl.req_set_nbr, 0) as req_set_nbr
,coalesce(case when trim(ehdtl.req_type)='' then null else trim(ehdtl.req_type) end, 'N/A') as req_typ
,coalesce(ehdtl.seq_nbr, 0) as seq_nbr
,coalesce(case when trim(ehdtl.tzone1)='' then null else trim(ehdtl.tzone1) end, 'N/A') as tzone1
,coalesce(case when trim(ehdtl.tzone2)='' then null else trim(ehdtl.tzone2) end, 'N/A') as tzone2
,coalesce(case when trim(ehdtl.tzone3)='' then null else trim(ehdtl.tzone3) end, 'N/A') as tzone3
,coalesce(case when trim(ehdtl.tzone4)='' then null else trim(ehdtl.tzone4) end, 'N/A') as tzone4
,coalesce(case when trim(ehdtl.tzone5)='' then null else trim(ehdtl.tzone5) end, 'N/A') as tzone5
,coalesce(case when trim(ehdtl.user_def_ind)='' then null else trim(ehdtl.user_def_ind) end, 'N/A') as user_def_ind
,coalesce(from_unixtime(unix_timestamp(substring(cast(ehhdr.created_date as bigint),1,8),'yyyyMMdd'),'yyyy-MM-dd'), '9999-12-31') as created_dt
from sap_stage0.saptrx_eh_hdr ehhdr inner join sap_stage0.saptrx_eh_expev ehdtl on
ehhdr.eh_guid = ehdtl.eh_guid
where from_unixtime(unix_timestamp(substring(cast(ehhdr.created_date as bigint),1,8),'yyyyMMdd'),'yyyy-MM-dd') between date_sub(current_date,90) and current_date;

! echo Completed Loading into scct_db.scem_expected_events;

 -------------------------
 ---end script
 -------------------------