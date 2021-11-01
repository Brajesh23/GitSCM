---jobname:scct-tms-hive-stg1-scct_db.tms_accessorial_master
--------------------------------------------------------------------------
---target: stage1 table: scct_db.tms_accessorial_master
---source: tms_stage0 table : ca2015_accessorial_code, ca2015_accessorial_type and ca2015_accessorial_rate
---load type: incremental
---back posting: no
--
-- History Information
--
-- Revision    Date     	JIRA       	Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 07NOV19		SCCTP4-2485		mgohain			Initial Version
-- 01.00.00.02 11NOV19		SCCTP4-2660		mgohain			Primary key changed to accessorial_id,accessorial_param_set_id,accessorial_rate_id and accessorial_type
-- 01.00.00.03 11NOV19		SCCTP4-2740		mgohain			Source Datatype changed for last_updated_dttm in table ca2015_accessorial_code
-- 01.00.00.04 10DEC19		SCCTP4-2740		mgohain			Target date columns converted to EST
--------------------------------------------------------------------------

-------------------------
---job related hive settings
-------------------------

-------------------------
---loading stage0 data into staging table
-------------------------

! echo started loading into scct_work_db.tms_accessorial_master_tmp;

insert overwrite table scct_work_db.tms_accessorial_master_tmp
select distinct
coalesce(case when trim(acc_type.description)='' then NULL else trim(acc_type.description) end, 'N/A') as accessorial_type_description,
coalesce(acc_cd.tc_company_id, 0) as tc_company_id,
coalesce(case when trim(acc_cd.accessorial_code)='' then NULL else trim(acc_cd.accessorial_code) end, 'N/A') as accessorial_cd,
coalesce(case when trim(acc_cd.accessorial_type)='' then NULL else trim(acc_cd.accessorial_type) end, 'N/A') as accessorial_type,
coalesce(case when trim(acc_cd.data_source)='' then NULL else trim(acc_cd.data_source) end, 'N/A') as data_source,
coalesce(case when trim(acc_cd.distance_uom)='' then NULL else trim(acc_cd.distance_uom) end, 'N/A') as distance_uom,
coalesce(case when trim(acc_cd.description)='' then NULL else trim(acc_cd.description) end, 'N/A') as accessorial_description,
coalesce(to_utc_timestamp(from_unixtime(acc_cd.last_updated_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as last_updated_dttm,
coalesce(acc_cd.is_hazmat, 0) as is_hazmat,
coalesce(acc_cd.is_default_for_invoice, 0) as is_default_for_invoice,
coalesce(acc_cd.is_fuel_accessorial, 0) as is_fuel_accessorial,
coalesce(acc_cd.accessorial_id , 0) as accessorial_id,
coalesce(acc_cd.is_tax_accessorial, 0) as is_tax_accessorial,
coalesce(acc_cd.is_add_to_spot_charge, 0) as is_add_to_spot_charge,
coalesce(acc_cd.is_add_stop_off_charge, 0) as is_add_stop_off_charge,
coalesce(acc_cd.calculation_option_level, 0) as calculation_option_level,
coalesce(acc_cd.delivery_level, 0) as delivery_level,
coalesce(acc_cd.is_requested, 0) as is_requested,
coalesce(acc_cd.is_ship_via, 0) as is_ship_via,
coalesce(acc_cd.calculation_factor, 0) as calculation_factor,
coalesce(acc_cd.is_oversize, 0) as is_oversize,
coalesce(acc_cd.is_apply_once_per_ship, 0) as is_apply_once_per_ship,
coalesce(acc_cd.fuel_index_id, 0) as fuel_index_id,
coalesce(acc_cd.is_cost_plus_accessorial, 0) as is_cost_plus_accessorial,
coalesce(acc_rate.accessorial_param_set_id, 0) as accessorial_param_set_id,
coalesce(acc_rate.accessorial_rate_id, 0) as accessorial_rate_id,
coalesce(acc_rate.rate, 0.0) as rate,
coalesce(acc_rate.minimum_rate, 0.0) as minimum_rate,
coalesce(case when trim(acc_rate.currency_code)='' then NULL else trim(acc_rate.currency_code) end, 'N/A') as currency_cd,
coalesce(acc_rate.is_auto_approve, 0) as is_auto_approve,
coalesce(to_date(to_utc_timestamp(from_unixtime(acc_rate.effective_dt DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT")), '9999-12-31') as effective_dt,
coalesce(to_date(to_utc_timestamp(from_unixtime(acc_rate.expiration_dt DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT")), '9999-12-31') as expiration_dt,
coalesce(acc_rate.effective_seq, 0) as effective_seq,
coalesce(acc_rate.is_shipment_accessorial, 0) as is_shipment_accessorial,
coalesce(acc_rate.mot_id, 0) as mot_id,
coalesce(acc_rate.equipment_id, 0) as equipment_id,
coalesce(acc_rate.service_level_id, 0) as service_level_id,
coalesce(acc_rate.protection_level_id, 0) as protection_level_id
from tms_stage0.ca2015_accessorial_code acc_cd
left outer join tms_stage0.ca2015_accessorial_type acc_type on acc_cd.accessorial_type=acc_type.accessorial_type
left outer join tms_stage0.ca2015_accessorial_rate acc_rate on acc_cd.accessorial_id=acc_rate.accessorial_id;

-------------------------
---loading staging data into final staging table
-------------------------

set tez.job.name=staging:tms_accessorial_master_tmp1 :load target table from tms_accessorial_master_tmp;

! echo started loading into scct_work_db.tms_accessorial_master_tmp1;

insert overwrite table scct_work_db.tms_accessorial_master_tmp1 select
coalesce(tmp.accessorial_type_description, t.accessorial_type_description),
coalesce(tmp.tc_company_id, t.tc_company_id),
coalesce(tmp.accessorial_cd, t.accessorial_cd),
coalesce(tmp.accessorial_type, t.accessorial_type),
coalesce(tmp.data_source, t.data_source),
coalesce(tmp.distance_uom, t.distance_uom),
coalesce(tmp.accessorial_description, t.accessorial_description),
coalesce(tmp.last_updated_dttm, t.last_updated_dttm),
coalesce(tmp.is_hazmat, t.is_hazmat),
coalesce(tmp.is_default_for_invoice, t.is_default_for_invoice),
coalesce(tmp.is_fuel_accessorial, t.is_fuel_accessorial),
coalesce(tmp.accessorial_id, t.accessorial_id),
coalesce(tmp.is_tax_accessorial, t.is_tax_accessorial),
coalesce(tmp.is_add_to_spot_charge, t.is_add_to_spot_charge),
coalesce(tmp.is_add_stop_off_charge, t.is_add_stop_off_charge),
coalesce(tmp.calculation_option_level, t.calculation_option_level),
coalesce(tmp.delivery_level, t.delivery_level),
coalesce(tmp.is_requested, t.is_requested),
coalesce(tmp.is_ship_via, t.is_ship_via),
coalesce(tmp.calculation_factor, t.calculation_factor),
coalesce(tmp.is_oversize, t.is_oversize),
coalesce(tmp.is_apply_once_per_ship, t.is_apply_once_per_ship),
coalesce(tmp.fuel_index_id, t.fuel_index_id),
coalesce(tmp.is_cost_plus_accessorial, t.is_cost_plus_accessorial),
coalesce(tmp.accessorial_param_set_id, t.accessorial_param_set_id),
coalesce(tmp.accessorial_rate_id, t.accessorial_rate_id),
coalesce(tmp.rate, t.rate),
coalesce(tmp.minimum_rate, t.minimum_rate),
coalesce(tmp.currency_cd, t.currency_cd),
coalesce(tmp.is_auto_approve, t.is_auto_approve),
coalesce(tmp.effective_dt, t.effective_dt),
coalesce(tmp.expiration_dt, t.expiration_dt),
coalesce(tmp.effective_seq, t.effective_seq),
coalesce(tmp.is_shipment_accessorial, t.is_shipment_accessorial),
coalesce(tmp.mot_id, t.mot_id),
coalesce(tmp.equipment_id, t.equipment_id),
coalesce(tmp.service_level_id, t.service_level_id),
coalesce(tmp.protection_level_id, t.protection_level_id)
from scct_db.tms_accessorial_master t
full outer join scct_work_db.tms_accessorial_master_tmp tmp
on t.accessorial_id=tmp.accessorial_id
and t.accessorial_param_set_id=tmp.accessorial_param_set_id
and t.accessorial_rate_id=tmp.accessorial_rate_id
and t.accessorial_type=tmp.accessorial_type;


-------------------------
---loading staging data into target table
-------------------------

set tez.job.name=staging:tms_accessorial_master :load target table from tms_accessorial_master_tmp1;

! echo started loading into scct_db.tms_accessorial_master;

insert overwrite table scct_db.tms_accessorial_master select
accessorial_type_description,
tc_company_id,
accessorial_cd,
accessorial_type,
data_source,
distance_uom,
accessorial_description,
last_updated_dttm,
is_hazmat,
is_default_for_invoice,
is_fuel_accessorial,
accessorial_id,
is_tax_accessorial,
is_add_to_spot_charge,
is_add_stop_off_charge,
calculation_option_level,
delivery_level,
is_requested,
is_ship_via,
calculation_factor,
is_oversize,
is_apply_once_per_ship,
fuel_index_id,
is_cost_plus_accessorial,
accessorial_param_set_id,
accessorial_rate_id,
rate,
minimum_rate,
currency_cd,
is_auto_approve,
effective_dt,
expiration_dt,
effective_seq,
is_shipment_accessorial,
mot_id,
equipment_id,
service_level_id,
protection_level_id
from scct_work_db.tms_accessorial_master_tmp1;
! echo data load is completed;

-------------------------
---end script
-------------------------
