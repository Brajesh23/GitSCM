---jobname:scct-tms-hive-stg1-scct_db.tms_carrier_contact_master
--------------------------------------------------------------------------
---target: stage1 table: scct_db.tms_carrier_contact_master
---source: tms_stage0 table : ca2015_carrier_code_contact, ca2015_carrier_code_note, ca2015_carrier_code
---load type: incremental
---back posting: no
--
-- History Information
--
-- Revision    Date     	JIRA       	Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 18SEP19		SCCTP4-2424		pnaayan			Initial Version
-- 01.00.00.02 31OCT19		SCCTP4-2517		mgohain			Primary key set changed to carrier_id and contact_id
-- 01.00.00.02 13NOV19		SCCTP4-2695		akoti			Converted timestamp to EST format & removed trim on integer columns
--------------------------------------------------------------------------

-------------------------
---job related hive settings
-------------------------

set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

-------------------------
---loading stage0 data into staging table
-------------------------

set tez.job.name=staging:tms_carrier_contact_master_tmp :load target from ca2015_carrier_code_contact ca2015_carrier_code_note ca2015_carrier_code;

! echo started loading into scct_work_db.tms_carrier_contact_master_tmp;

insert overwrite table scct_work_db.tms_carrier_contact_master_tmp 
select
coalesce(carr_cd.tc_company_id, 0) as company_id,
coalesce(case when trim(carr_cd.carrier_code)='' then NULL else trim(carr_cd.carrier_code) end, 'N/A') as carrier_cd,
coalesce(carr_cd.tp_company_id, 0) as carrier_company_id,
coalesce(case when trim(carr_cd.tp_company_name)='' then NULL else trim(carr_cd.tp_company_name) end, 'N/A') as carrier_company_name,
coalesce(case when trim(carr_cd.carrier_code_name)='' then NULL else trim(carr_cd.carrier_code_name) end, 'N/A') as carrier_name,
coalesce(case when trim(carr_cd.address)='' then NULL else trim(carr_cd.address) end, 'N/A') as carrier_address,
coalesce(case when trim(carr_cd.city)='' then NULL else trim(carr_cd.city) end, 'N/A') as carrier_city,
coalesce(case when trim(carr_cd.state_prov)='' then NULL else trim(carr_cd.state_prov) end, 'N/A') as carrier_state_prov,
coalesce(case when trim(carr_cd.postal_code)='' then NULL else trim(carr_cd.postal_code) end, 'N/A') as carrier_postal_cd,
coalesce(case when trim(carr_cd.country_code)='' then NULL else trim(carr_cd.country_code) end, 'N/A') as carrier_country_cd,
coalesce(carr_cd.carrier_code_status, 0) as carrier_status,
coalesce(carr_cd.mark_for_deletion, 0) as carrier_mark_for_deletion,
coalesce(case when trim(carr_cd.comm_method)='' then NULL else trim(carr_cd.comm_method) end, 'N/A') as carrier_communication_method,
coalesce(to_utc_timestamp(FROM_UNIXTIME(carr_cd.created_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as carrier_created_dttm,
coalesce(to_utc_timestamp(FROM_UNIXTIME(carr_cd.last_updated_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), to_utc_timestamp(FROM_UNIXTIME(carr_cd.created_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as carrier_last_updated_dttm,
coalesce(case when trim(carr_cd.payment_reference_number)='' then NULL else trim(carr_cd.payment_reference_number) end, 'N/A') as carrier_payment_reference_nbr,
coalesce(case when trim(carr_cd.tariff_id)='' then NULL else trim(carr_cd.tariff_id) end, 'N/A') as carrier_tariff_id,
coalesce(case when trim(carr_cd.invoice_payment_terms)='' then NULL else trim(carr_cd.invoice_payment_terms) end, 'N/A') as carrier_invoice_payment_terms,
coalesce(case when trim(carr_cd.payment_method)='' then NULL else trim(carr_cd.payment_method) end, 'N/A') as carrier_payment_method,
coalesce(carr_cd.carrier_id, 0) as carrier_id,
coalesce(carr_cd.has_hazmat, 0) as carrier_has_hazmat,
coalesce(carr_cd_cnct.cc_contact_id, 0) as contact_id,
coalesce(case when trim(carr_cd_cnct.first_name)='' then NULL else trim(carr_cd_cnct.first_name) end, 'N/A') as contact_first_name,
coalesce(case when trim(carr_cd_cnct.middle_name)='' then NULL else trim(carr_cd_cnct.middle_name) end, 'N/A') as contact_middle_name,
coalesce(case when trim(carr_cd_cnct.surname)='' then NULL else trim(carr_cd_cnct.surname) end, 'N/A') as contact_surname,
coalesce(case when trim(carr_cd_cnct.contact_prefix)='' then NULL else trim(carr_cd_cnct.contact_prefix) end, 'N/A') as contact_prefix,
coalesce(case when trim(carr_cd_cnct.contact_title)='' then NULL else trim(carr_cd_cnct.contact_title) end, 'N/A') as contact_title,
coalesce(case when trim(carr_cd_cnct.telephone_number)='' then NULL else trim(carr_cd_cnct.telephone_number) end, 'N/A') as contact_phone,
coalesce(case when trim(carr_cd_cnct.mobil_phone_number)='' then NULL else trim(carr_cd_cnct.mobil_phone_number) end, 'N/A') as contact_mobile_phone,
coalesce(case when trim(carr_cd_cnct.pager_number)='' then NULL else trim(carr_cd_cnct.pager_number) end, 'N/A') as contact_pager,
coalesce(case when trim(carr_cd_cnct.fax_number)='' then NULL else trim(carr_cd_cnct.fax_number) end, 'N/A') as contact_fax,
coalesce(case when trim(carr_cd_cnct.email_address)='' then NULL else trim(carr_cd_cnct.email_address) end, 'N/A') as contact_email,
coalesce(case when trim(carr_cd_cnct.address)='' then NULL else trim(carr_cd_cnct.address) end, 'N/A') as contact_address,
coalesce(case when trim(carr_cd_cnct.city)='' then NULL else trim(carr_cd_cnct.city) end, 'N/A') as contact_city,
coalesce(case when trim(carr_cd_cnct.state_prov)='' then NULL else trim(carr_cd_cnct.state_prov) end, 'N/A') as contact_state_prov,
coalesce(case when trim(carr_cd_cnct.postal_code)='' then NULL else trim(carr_cd_cnct.postal_code) end, 'N/A') as contact_postal_cd,
coalesce(case when trim(carr_cd_cnct.country_code)='' then NULL else trim(carr_cd_cnct.country_code) end, 'N/A') as contact_country_cd,
coalesce(case when trim(carr_cd_cnct.comments)='' then NULL else trim(carr_cd_cnct.comments) end, 'N/A') as contact_comments,
coalesce(carr_cd_cnct.is_primary_contact, 0) as contact_is_primary,
coalesce(carr_cd_cnct.contact_type, 0) as contact_type,
coalesce(to_utc_timestamp(FROM_UNIXTIME(carr_cd_cnct.created_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as contact_created_dttm,
coalesce(to_utc_timestamp(FROM_UNIXTIME(carr_cd_cnct.last_updated_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), to_utc_timestamp(FROM_UNIXTIME(carr_cd_cnct.created_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as contact_last_updated_dttm,
coalesce(trim(carr_cd_nte1.note), 'N/A') as carrier_note_carrier_type,
coalesce(trim(carr_cd_nte2.note), 'N/A') as carrier_note_canadian_carrier
from tms_stage0.ca2015_carrier_code carr_cd
left outer join tms_stage0.ca2015_carrier_code_contact carr_cd_cnct on carr_cd.carrier_id=carr_cd_cnct.carrier_id
left outer join (select carrier_id,note from tms_stage0.ca2015_carrier_code_note where carrier_note_type = '1' group by carrier_id,note) carr_cd_nte1
on carr_cd.carrier_id=carr_cd_nte1.carrier_id
left outer join (select carrier_id,note from tms_stage0.ca2015_carrier_code_note where carrier_note_type = '2' group by carrier_id,note) carr_cd_nte2
on carr_cd.carrier_id=carr_cd_nte2.carrier_id;

-------------------------
---loading staging data into final stagnig table
-------------------------

set tez.job.name=staging:tms_carrier_contact_master_tmp1 :load target table from tms_carrier_contact_master_tmp;

! echo started loading into scct_work_db.tms_carrier_contact_master_tmp1;

insert overwrite table scct_work_db.tms_carrier_contact_master_tmp1 
select
coalesce(tmp.company_id, t.company_id),
coalesce(tmp.carrier_cd, t.carrier_cd),
coalesce(tmp.carrier_company_id, t.carrier_company_id),
coalesce(tmp.carrier_company_name, t.carrier_company_name),
coalesce(tmp.carrier_name, t.carrier_name),
coalesce(tmp.carrier_address, t.carrier_address),
coalesce(tmp.carrier_city, t.carrier_city),
coalesce(tmp.carrier_state_prov, t.carrier_state_prov),
coalesce(tmp.carrier_postal_cd, t.carrier_postal_cd),
coalesce(tmp.carrier_country_cd, t.carrier_country_cd),
coalesce(tmp.carrier_status, t.carrier_status),
coalesce(tmp.carrier_mark_for_deletion, t.carrier_mark_for_deletion),
coalesce(tmp.carrier_communication_method, t.carrier_communication_method),
coalesce(tmp.carrier_created_dttm, t.carrier_created_dttm),
coalesce(tmp.carrier_last_updated_dttm, t.carrier_last_updated_dttm),
coalesce(tmp.carrier_payment_reference_nbr, t.carrier_payment_reference_nbr),
coalesce(tmp.carrier_tariff_id, t.carrier_tariff_id),
coalesce(tmp.carrier_invoice_payment_terms, t.carrier_invoice_payment_terms),
coalesce(tmp.carrier_payment_method, t.carrier_payment_method),
coalesce(tmp.carrier_id, t.carrier_id),
coalesce(tmp.carrier_has_hazmat, t.carrier_has_hazmat),
coalesce(tmp.contact_id, t.contact_id),
coalesce(tmp.contact_first_name, t.contact_first_name),
coalesce(tmp.contact_middle_name, t.contact_middle_name),
coalesce(tmp.contact_surname, t.contact_surname),
coalesce(tmp.contact_prefix, t.contact_prefix),
coalesce(tmp.contact_title, t.contact_title),
coalesce(tmp.contact_phone, t.contact_phone),
coalesce(tmp.contact_mobile_phone, t.contact_mobile_phone),
coalesce(tmp.contact_pager, t.contact_pager),
coalesce(tmp.contact_fax, t.contact_fax),
coalesce(tmp.contact_email, t.contact_email),
coalesce(tmp.contact_address, t.contact_address),
coalesce(tmp.contact_city, t.contact_city),
coalesce(tmp.contact_state_prov, t.contact_state_prov),
coalesce(tmp.contact_postal_cd, t.contact_postal_cd),
coalesce(tmp.contact_country_cd, t.contact_country_cd),
coalesce(tmp.contact_comments, t.contact_comments),
coalesce(tmp.contact_is_primary, t.contact_is_primary),
coalesce(tmp.contact_type, t.contact_type),
coalesce(tmp.contact_created_dttm, t.contact_created_dttm),
coalesce(tmp.contact_last_updated_dttm, t.contact_last_updated_dttm),
coalesce(tmp.carrier_note_carrier_type, t.carrier_note_carrier_type),
coalesce(tmp.carrier_note_canadian_carrier, t.carrier_note_canadian_carrier)
from scct_db.tms_carrier_contact_master t
full outer join scct_work_db.tms_carrier_contact_master_tmp tmp on t.carrier_id=tmp.carrier_id and t.contact_id=tmp.contact_id;

-------------------------
---loading staging data into target table
-------------------------

set tez.job.name=staging:tms_carrier_contact_master :load target table from tms_carrier_contact_master_tmp1;

! echo started loading into scct_db.tms_carrier_contact_master;

insert overwrite table scct_db.tms_carrier_contact_master
select
company_id,
carrier_cd,
carrier_company_id,
carrier_company_name,
carrier_name,
carrier_address,
carrier_city,
carrier_state_prov,
carrier_postal_cd,
carrier_country_cd,
carrier_status,
carrier_mark_for_deletion,
carrier_communication_method,
carrier_created_dttm,
carrier_last_updated_dttm,
carrier_payment_reference_nbr,
carrier_tariff_id,
carrier_invoice_payment_terms,
carrier_payment_method,
carrier_id,
carrier_has_hazmat,
contact_id,
contact_first_name,
contact_middle_name,
contact_surname,
contact_prefix,
contact_title,
contact_phone,
contact_mobile_phone,
contact_pager,
contact_fax,
contact_email,
contact_address,
contact_city,
contact_state_prov,
contact_postal_cd,
contact_country_cd,
contact_comments,
contact_is_primary,
contact_type,
contact_created_dttm,
contact_last_updated_dttm,
carrier_note_carrier_type,
carrier_note_canadian_carrier
from scct_work_db.tms_carrier_contact_master_tmp1
;


! echo data load is completed;

-------------------------
---end script
-------------------------