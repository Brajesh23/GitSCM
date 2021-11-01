---jobname:scct-tms-hive-stg1-scct_db.tms_facility_master
--------------------------------------------------------------------------
---target: stage1 table: scct_db.tms_facility_master
---source: tms_stage0 table : ca2015_facility, ca2015_facility_alias, ca2015_facility_contact
--ca2015_facility_contact_type and ca2015_facility_zone
---load type: incremental
---back posting: no
--
-- History Information
--
-- Revision    Date     	JIRA       	Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 11NOV19		SCCTP4-2560		ssoma			Initial Version
-- 01.00.00.02 17DEC19		SCCTP4-2728		ssoma			Added filter to avoid null values from source
-- 01.00.00.03 18FEB20		SCCTP4-3143		akoti			Upadted the timezone conversion logic for date fields
-- 01.00.00.03 18NOV20		MBDS-1404		ssoma			Primary_key issue Fix
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

set tez.job.name=staging:tms_facility_master_tmp :load target from facility tables;

! echo started loading into scct_work_db.tms_facility_master_tmp;


insert overwrite table scct_db.tms_facility_master
select
coalesce(case when trim(fa.facility_alias_id)='' then NULL else trim(fa.facility_alias_id) end , 'N/A') as facility_alias_id
,coalesce(fa.facility_id, 0) as facility_id
,coalesce(fa.tc_company_id, 0) as company_id
,coalesce(case when trim(fa.facility_name)='' then NULL else trim(fa.facility_name) end , 'N/A') as facility_name
,coalesce(fa.is_primary, 0) as is_primary_facility
,coalesce(fa.mark_for_deletion, 0) as facility_alias_marked_for_deletion
,coalesce(to_utc_timestamp(fa.created_dttm, "EST5EDT"), '9999-12-31 00:00:00') as facility_alias_created_dttm
,coalesce(to_utc_timestamp(fa.last_updated_dttm, "EST5EDT"),to_utc_timestamp(fa.created_dttm, "EST5EDT"),'9999-12-31 00:00:00') as facility_alias_last_updated_dttm
,coalesce(fa.facility_seq, 0) as facility_seq
,coalesce(case when trim(fa.address_1)='' then NULL else trim(fa.address_1) end , 'N/A') as facility_alias_address_1
,coalesce(case when trim(fa.address_2)='' then NULL else trim(fa.address_2) end , 'N/A') as facility_alias_facility_address_2
,coalesce(case when trim(fa.address_3)='' then NULL else trim(fa.address_3) end , 'N/A') as facility_alias_facility_address_3
,coalesce(case when trim(fa.city)='' then NULL else trim(fa.city) end , 'N/A') as facility_alias_city
,coalesce(case when trim(fa.state_prov)='' then NULL else trim(fa.state_prov) end , 'N/A') as facility_alias_state
,coalesce(case when trim(fa.postal_code)='' then NULL else trim(fa.postal_code) end , 'N/A') as facility_alias_postal_code
,coalesce(case when trim(fa.country_code)='' then NULL else trim(fa.country_code) end , 'N/A') as facility_alias_country_code
,coalesce(fc.facility_contact_id, 0) as facility_contact_id
,coalesce(case when trim(fc.first_name)='' then NULL else trim(fc.first_name) end , 'N/A') as first_name
,coalesce(case when trim(fc.middle_name)='' then NULL else trim(fc.middle_name) end , 'N/A') as middle_name
,coalesce(case when trim(fc.surname)='' then NULL else trim(fc.surname) end , 'N/A') as surname
,coalesce(case when trim(fc.contact_prefix)='' then NULL else trim(fc.contact_prefix) end , 'N/A') as contact_prefix
,coalesce(case when trim(fc.contact_title)='' then NULL else trim(fc.contact_title) end , 'N/A') as contact_title
,coalesce(case when trim(fc.telephone_number)='' then NULL else trim(fc.telephone_number) end , 'N/A') as contact_telephone_number
,coalesce(case when trim(fc.mobil_phone_number)='' then NULL else trim(fc.mobil_phone_number) end , 'N/A') as contact_mobil_phone_number
,coalesce(case when trim(fc.pager_number)='' then NULL else trim(fc.pager_number) end , 'N/A') as contact_pager_number
,coalesce(case when trim(fc.fax_number)='' then NULL else trim(fc.fax_number) end , 'N/A') as contact_fax_number
,coalesce(case when trim(fc.email_address)='' then NULL else trim(fc.email_address) end , 'N/A') as contact_email_address
,coalesce(case when trim(fc.address_1)='' then NULL else trim(fc.address_1) end , 'N/A') as contact_address_1
,coalesce(case when trim(fc.address_2)='' then NULL else trim(fc.address_2) end , 'N/A') as contact_address_2
,coalesce(case when trim(fc.address_3)='' then NULL else trim(fc.address_3) end , 'N/A') as contact_address_3
,coalesce(case when trim(fc.city)='' then NULL else trim(fc.city) end , 'N/A') as contact_city
,coalesce(case when trim(fc.state_prov)='' then NULL else trim(fc.state_prov) end , 'N/A') as contact_state
,coalesce(case when trim(fc.postal_code)='' then NULL else trim(fc.postal_code) end , 'N/A') as contact_postal_code
,coalesce(case when trim(fc.country_code)='' then NULL else trim(fc.country_code) end , 'N/A') as contact_country_code
,coalesce(case when trim(fc.comments)='' then NULL else trim(fc.comments) end , 'N/A') as comments
,coalesce(fc.mark_for_deletion, 0) as contact_marked_for_deletion
,coalesce(fc.is_primary, 0) as is_primary_contact
,coalesce(fc.contact_type, 0) as contact_type
,coalesce(to_utc_timestamp(fc.created_dttm, "EST5EDT"), '9999-12-31 00:00:00') as contact_created_dttm
,coalesce(to_utc_timestamp(fc.last_updated_dttm, "EST5EDT"),to_utc_timestamp(fc.created_dttm, "EST5EDT"),'9999-12-31 00:00:00') as contact_last_updated_dttm
,coalesce(case when trim(f.business_partner_id)='' then NULL else trim(f.business_partner_id) end , 'N/A') as business_partner_id
,coalesce(case when trim(f.address_1)='' then NULL else trim(f.address_1) end , 'N/A') as facility_address_1
,coalesce(case when trim(f.address_2)='' then NULL else trim(f.address_2) end , 'N/A') as facility_address_2
,coalesce(case when trim(f.address_3)='' then NULL else trim(f.address_3) end , 'N/A') as facility_address_3
,coalesce(case when trim(f.address_key_1)='' then NULL else trim(f.address_key_1) end , 'N/A') as facility_address_key_1
,coalesce(case when trim(f.city)='' then NULL else trim(f.city) end , 'N/A') as facility_city
,coalesce(case when trim(f.state_prov)='' then NULL else trim(f.state_prov) end , 'N/A') as facility_state_prov
,coalesce(case when trim(f.postal_code)='' then NULL else trim(f.postal_code) end , 'N/A') as facility_postal_code
,coalesce(case when trim(f.country_code)='' then NULL else trim(f.country_code) end , 'N/A') as facility_country_code
,coalesce(f.longitude, 0.000000) as longitude
,coalesce(f.latitude, 0.000000) as latitude
,coalesce(f.inbound_region_id, 0) as inbound_region_id
,coalesce(f.outbound_region_id, 0) as outbound_region_id
,coalesce(f.facility_tz, 0) as facility_tz
,coalesce(case when trim(f.ontime_perf_method)='' then NULL else trim(f.ontime_perf_method) end , 'N/A') as ontime_perf_method
,coalesce(f.load_factor_size_value, 0.00) as load_factor_size_value
,coalesce(f.mark_for_deletion, 0) as mark_for_deletion
,coalesce(to_utc_timestamp(f.created_dttm, "EST5EDT"), '9999-12-31 00:00:00') as facility_created_dttm
,coalesce(to_utc_timestamp(f.last_updated_dttm, "EST5EDT"),to_utc_timestamp(f.created_dttm, "EST5EDT"),'9999-12-31 00:00:00') as facility_last_updated_dttm
,coalesce(f.store_type, 0) as store_type
,coalesce(to_utc_timestamp(f.cutoff_dttm, "EST5EDT"), '9999-12-31 00:00:00') as cutoff_dttm
,coalesce(f.load_rate, 0.0000) as load_rate
,coalesce(f.unload_rate, 0.0000) as unload_rate
,coalesce(f.direct_delivery_allowed, 0) as direct_delivery_allowed
,coalesce(f.re_compute_feasible_equipment, 0) as re_compute_feasible_equipment
,coalesce(f.ship_to_store, 0) as ship_to_store
,coalesce(f.ship_from_facility, 0) as ship_from_facility
,coalesce(f.maintain_on_hand, 0) as maintain_on_hand
,coalesce(f.maintain_in_transit, 0) as maintain_in_transit
,coalesce(f.maintain_on_order, 0) as maintain_on_order
,coalesce(case when trim(f.ref_field_1)='' then NULL else trim(f.ref_field_1) end , 'N/A') as ref_field_1
,coalesce(case when trim(f.ref_field_2)='' then NULL else trim(f.ref_field_2) end , 'N/A') as ref_field_2
,coalesce(case when trim(f.ref_field_3)='' then NULL else trim(f.ref_field_3) end , 'N/A') as ref_field_3
,coalesce(case when trim(f.ref_field_4)='' then NULL else trim(f.ref_field_4) end , 'N/A') as ref_field_4
,coalesce(case when trim(f.ref_field_5)='' then NULL else trim(f.ref_field_5) end , 'N/A') as ref_field_5
,coalesce(case when trim(f.ref_field_6)='' then NULL else trim(f.ref_field_6) end , 'N/A') as ref_field_6
,coalesce(case when trim(f.ref_field_7)='' then NULL else trim(f.ref_field_7) end , 'N/A') as ref_field_7
,coalesce(case when trim(f.ref_field_8)='' then NULL else trim(f.ref_field_8) end , 'N/A') as ref_field_8
,coalesce(case when trim(f.ref_field_9)='' then NULL else trim(f.ref_field_9) end , 'N/A') as ref_field_9
,coalesce(case when trim(f.ref_field_10)='' then NULL else trim(f.ref_field_10) end , 'N/A') as ref_field_10
,coalesce(f.ref_num1, 0.00000) as ref_num1
,coalesce(f.ref_num2, 0.00000) as ref_num2
,coalesce(f.ref_num3, 0.00000) as ref_num3
,coalesce(f.ref_num4, 0.00000) as ref_num4
,coalesce(f.ref_num5, 0.00000) as ref_num5
,coalesce(case when trim(fct.description)='' then NULL else trim(fct.description) end , 'N/A') as description
from tms_stage0.ca2015_facility f 
inner join tms_stage0.ca2015_facility_alias fa on f.facility_id=fa.facility_id
left outer join (select * from tms_stage0.ca2015_facility_contact where is_primary=1) fc on f.facility_id=fc.facility_id
left outer join tms_stage0.ca2015_facility_contact_type fct on fc.contact_type=fct.contact_type
where f.facility_id is not null;

! echo data load is Completed;

-------------------------
---end script
-------------------------