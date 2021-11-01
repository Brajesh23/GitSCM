---jobname:scct-tms-hive-stg1-scct_db.tms_fuel_master
--------------------------------------------------------------------------
---target: stage1 table: scct_db.tms_fuel_master
---source: tms_stage0 table : ca2015_fuel_index, ca2015_fuel_rate, ca2015_fuel_surcharge, ca2015_fuel_region, ca2015_standard_uom and ca2015_accessorial_code
---load type: incremental
---back posting: no
--
-- History Information
--
-- Revision    Date     	JIRA       	Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 18SEP19		SCCTP4-2424		pnaayan			Initial Version
-- 01.00.00.02 31OCT19		SCCTP4-2516		mgohain			Primary key set changed to index_id, effective_dt, expiration_dt, fuel_surcharge_id, lower_price_limit and upper_price_limit
-- 01.00.00.03 13NOV19		SCCTP4-2697		mgohain			Column accessorial_code_accessorial_id removed as value is same as accessorial_id && date format changed to EST && inner join changed to left outer with table ca2015_accessorial_code
-- 01.00.00.04 13NOV19		SCCTP4-2697		mgohain			datatype changed for rate, lower_price_limit, upper_price_limit & surcharge_amount
-- 01.00.00.05 19NOV19		SCCTP4-2723		mgohain			datatype changed for column last_updated_dttm in source table ca2015_accessorial_code
--------------------------------------------------------------------------

-------------------------
---job related hive settings
-------------------------

-------------------------
---loading stage0 data into staging table
-------------------------

! echo started loading into scct_work_db.tms_fuel_master_tmp;

insert overwrite table scct_work_db.tms_fuel_master_tmp
select distinct
coalesce(f_indx.company_id, 0) as company_id,
coalesce(f_indx.index_id, 0) as index_id,
coalesce(case when trim(f_indx.index_name)='' then NULL else trim(f_indx.index_name) end, 'N/A') as name,
coalesce(case when trim(f_indx.index_desc)='' then NULL else trim(f_indx.index_desc) end, 'N/A') as fuel_index_description,
coalesce(f_indx.fuel_type_id, 0) as fuel_type_id,
coalesce(case when trim(f_indx.country_code)='' then NULL else trim(f_indx.country_code) end, 'N/A') as country_cd,
coalesce(to_utc_timestamp(from_unixtime(f_indx.created_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as index_created_dttm,
coalesce(to_utc_timestamp(from_unixtime(f_indx.last_updated_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), to_utc_timestamp(from_unixtime(f_indx.created_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as index_updated_dttm,
coalesce(f_rate.fuel_rate_id, 0) as rate_id,
coalesce(f_rate.fuel_rate, 0.0000) as rate,
coalesce(case when trim(f_rate.currency_code)='' then NULL else trim(f_rate.currency_code) end, 'N/A') as fuel_rate_currency_cd,
coalesce(to_utc_timestamp(from_unixtime(f_rate.effective_dt DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31') as effective_dt,
coalesce(to_utc_timestamp(from_unixtime(f_rate.expiration_dt DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31') as expiration_dt,
coalesce(to_utc_timestamp(from_unixtime(f_rate.created_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as rate_created_dttm,
coalesce(to_utc_timestamp(from_unixtime(f_rate.last_updated_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), to_utc_timestamp(from_unixtime(f_rate.created_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as rate_updated_dttm,
coalesce(case when trim(std_uom.abbreviation)='' then NULL else trim(std_uom.abbreviation) end, 'N/A') as rate_uom_abbreviation,
coalesce(case when trim(std_uom.description)='' then NULL else trim(std_uom.description) end, 'N/A') as rate_uom_description,
coalesce(f_surch.fuel_surcharge_id, 0) as fuel_surcharge_id,
coalesce(f_surch.lower_price_limit, 0.0000) as lower_price_limit,
coalesce(f_surch.upper_price_limit, 0.0000) as upper_price_limit,
coalesce(f_surch.surcharge_amount, 0.0000) as surcharge_amount,
coalesce(case when trim(f_surch.currency_code)='' then NULL else trim(f_surch.currency_code) end, 'N/A') as fuel_surcharge_currency_cd,
coalesce(f_surch.accessorial_id, 0) as fuel_surcharge_accessorial_id,
coalesce(f_surch.surcharge_percent, 0) as surcharge_percent,
coalesce(to_utc_timestamp(from_unixtime(f_surch.created_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as surcharge_created_dttm,
coalesce(to_utc_timestamp(from_unixtime(f_surch.last_updated_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), to_utc_timestamp(from_unixtime(f_surch.created_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as surcharge_last_updated_dttm,
coalesce(case when trim(acc_cd.accessorial_code)='' then NULL else trim(acc_cd.accessorial_code) end, 'N/A') as accessorial_cd,
coalesce(case when trim(acc_cd.accessorial_type)='' then NULL else trim(acc_cd.accessorial_type) end, 'N/A') as accessorial_type,
coalesce(case when trim(acc_cd.distance_uom)='' then NULL else trim(acc_cd.distance_uom) end, 'N/A') as distance_uom,
coalesce(case when trim(acc_cd.description)='' then NULL else trim(acc_cd.description) end, 'N/A') as accessorial_code_description,
coalesce(to_utc_timestamp(from_unixtime(acc_cd.last_updated_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as accessorial_last_updated_dttm,
coalesce(acc_cd.is_hazmat, 0) as is_hazmat,
coalesce(acc_cd.is_default_for_invoice, 0) as is_default_for_invoice,
coalesce(acc_cd.is_fuel_accessorial, 0) as is_fuel_accessorial,
coalesce(acc_cd.is_tax_accessorial, 0) as is_tax_accessorial,
coalesce(acc_cd.is_add_to_spot_charge, 0) as is_add_to_spot_charge,
coalesce(case when trim(f_regn.fuel_region)='' then NULL else trim(f_regn.fuel_region) end, 'N/A') as fuel_region
from tms_stage0.ca2015_fuel_index f_indx
left outer join tms_stage0.ca2015_fuel_region f_regn on f_regn.fuel_region_id=f_indx.fuel_region_id
left outer join tms_stage0.ca2015_fuel_rate f_rate on f_rate.fuel_index_id=f_indx.index_id
left outer join tms_stage0.ca2015_standard_uom std_uom on std_uom.standard_uom=f_rate.rate_standard_uom
left outer join tms_stage0.ca2015_fuel_surcharge f_surch on (f_rate.fuel_rate>=f_surch.lower_price_limit and f_rate.fuel_rate<=f_surch.upper_price_limit)
left outer join tms_stage0.ca2015_accessorial_code acc_cd on acc_cd.accessorial_id=f_surch.accessorial_id;
-------------------------
---loading staging data into final stagnig table
-------------------------

set tez.job.name=staging:tms_fuel_master_tmp1 :load target table from tms_fuel_master_tmp;

! echo started loading into scct_work_db.tms_fuel_master_tmp1;

insert overwrite table scct_work_db.tms_fuel_master_tmp1 select
coalesce(tmp.company_id, t.company_id),
coalesce(tmp.index_id, t.index_id),
coalesce(tmp.name, t.name),
coalesce(tmp.fuel_index_description, t.fuel_index_description),
coalesce(tmp.fuel_type_id, t.fuel_type_id),
coalesce(tmp.country_cd, t.country_cd),
coalesce(tmp.index_created_dttm, t.index_created_dttm),
coalesce(tmp.index_updated_dttm, t.index_updated_dttm),
coalesce(tmp.rate_id, t.rate_id),
coalesce(tmp.rate, t.rate),
coalesce(tmp.fuel_rate_currency_cd, t.fuel_rate_currency_cd),
coalesce(tmp.effective_dt, t.effective_dt),
coalesce(tmp.expiration_dt, t.expiration_dt),
coalesce(tmp.rate_created_dttm, t.rate_created_dttm),
coalesce(tmp.rate_updated_dttm, t.rate_updated_dttm),
coalesce(tmp.rate_uom_abbreviation, t.rate_uom_abbreviation),
coalesce(tmp.rate_uom_description, t.rate_uom_description),
coalesce(tmp.fuel_surcharge_id, t.fuel_surcharge_id),
coalesce(tmp.lower_price_limit, t.lower_price_limit),
coalesce(tmp.upper_price_limit, t.upper_price_limit),
coalesce(tmp.surcharge_amount, t.surcharge_amount),
coalesce(tmp.fuel_surcharge_currency_cd, t.fuel_surcharge_currency_cd),
coalesce(tmp.fuel_surcharge_accessorial_id, t.fuel_surcharge_accessorial_id),
coalesce(tmp.surcharge_percent, t.surcharge_percent),
coalesce(tmp.surcharge_created_dttm, t.surcharge_created_dttm),
coalesce(tmp.surcharge_last_updated_dttm, t.surcharge_last_updated_dttm),
coalesce(tmp.accessorial_cd, t.accessorial_cd),
coalesce(tmp.accessorial_type, t.accessorial_type),
coalesce(tmp.distance_uom, t.distance_uom),
coalesce(tmp.accessorial_code_description, t.accessorial_code_description),
coalesce(tmp.accessorial_last_updated_dttm, t.accessorial_last_updated_dttm),
coalesce(tmp.is_hazmat, t.is_hazmat),
coalesce(tmp.is_default_for_invoice, t.is_default_for_invoice),
coalesce(tmp.is_fuel_accessorial, t.is_fuel_accessorial),
coalesce(tmp.is_tax_accessorial, t.is_tax_accessorial),
coalesce(tmp.is_add_to_spot_charge, t.is_add_to_spot_charge),
coalesce(tmp.fuel_region, t.fuel_region)
from scct_db.tms_fuel_master t
full outer join scct_work_db.tms_fuel_master_tmp tmp
on t.index_id=tmp.index_id
and t.lower_price_limit=tmp.lower_price_limit
and t.upper_price_limit=tmp.upper_price_limit
and t.effective_dt = tmp.effective_dt
and t.expiration_dt = tmp.expiration_dt
and t.fuel_surcharge_id = tmp.fuel_surcharge_id;


-------------------------
---loading staging data into target table
-------------------------

set tez.job.name=staging:tms_fuel_master :load target table from tms_fuel_master_tmp1;

! echo started loading into scct_db.tms_fuel_master;

insert overwrite table scct_db.tms_fuel_master
select
company_id,
index_id,
name,
fuel_index_description,
fuel_type_id,
country_cd,
index_created_dttm,
index_updated_dttm,
rate_id,
rate,
fuel_rate_currency_cd,
effective_dt,
expiration_dt,
rate_created_dttm,
rate_updated_dttm,
rate_uom_abbreviation,
rate_uom_description,
fuel_surcharge_id,
lower_price_limit,
upper_price_limit,
surcharge_amount,
fuel_surcharge_currency_cd,
fuel_surcharge_accessorial_id,
surcharge_percent,
surcharge_created_dttm,
surcharge_last_updated_dttm,
accessorial_cd,
accessorial_type,
distance_uom,
accessorial_code_description,
accessorial_last_updated_dttm,
is_hazmat,
is_default_for_invoice,
is_fuel_accessorial,
is_tax_accessorial,
is_add_to_spot_charge,
fuel_region
from scct_work_db.tms_fuel_master_tmp1;

! echo data load is completed;

-------------------------
---end script
-------------------------