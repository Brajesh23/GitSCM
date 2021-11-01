---jobname:scct-tms-hive-stg1-scct_db.tms_comb_lane_master
--------------------------------------------------------------------------
---target: stage1 table: scct_db.tms_comb_lane_master
---source: tms_stage0 table : ca2015_comb_lane, ca2015_comb_lane_dtl, ca2015_mgt_lane_od and ca2015_lane_detail_view
---load type: incremental
---back posting: no
--
-- History Information
--
-- Revision       Date     	    JIRA       	    Developer-ID 	    Comments
-- -----------   ------- 		----------      ------------ 	   --------------------
-- 01.00.00.01    12NOV19		SCCTP4-2561		mgohain			    Initial Version
-- 01.00.00.02    19NOV19		SCCTP4-2561		mgohain			    Join type changed
-- 01.00.00.03    10DEC19		SCCTP4-2823		mgohain			    date/TS est conversion, added new key columns(effective_dt & expiration_dt) and data type changed for rate & minimum_rate
----------------------------------------------------------------------------------------

-------------------------
---job related hive settings
-------------------------


-------------------------
---loading stage0 data into staging table
-------------------------

! echo started loading into scct_work_db.tms_comb_lane_master_tmp;

insert overwrite table scct_work_db.tms_comb_lane_master_tmp select
coalesce(c_lane.tc_company_id, 0) as company_id,
coalesce(c_lane.lane_id, 0) as lane_id,
coalesce(case when trim(c_lane.o_search_location)='' then NULL else trim(c_lane.o_search_location) end, 'N/A') as origin_search_location,
coalesce(case when trim(c_lane.d_search_location)='' then NULL else trim(c_lane.d_search_location) end, 'N/A') as destination_search_location,
coalesce(c_lane.lane_unique_id, 0) as lane_unique_id,
coalesce(c_lane.is_rating, 0) as lane_is_rating,
coalesce(c_lane.is_routing, 0) as lane_is_routing,
coalesce(c_lane.is_sailing, 0) as lane_is_sailing,
coalesce(to_utc_timestamp(from_unixtime(c_lane.created_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as lane_created_dttm,
coalesce(to_utc_timestamp(from_unixtime(c_lane.last_updated_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), to_utc_timestamp(FROM_UNIXTIME(c_lane.created_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as lane_updated_dttm,
coalesce(c_lane.lane_hierarchy, 0) as lane_hierarchy,
coalesce(case when trim(c_lane.o_loc_type)='' then NULL else trim(c_lane.o_loc_type) end, 'N/A') as origin_location_type,
coalesce(c_lane.o_facility_id, 0) as origin_falcility_id,
coalesce(case when trim(c_lane.o_facility_alias_id)='' then NULL else trim(c_lane.o_facility_alias_id) end, 'N/A') as origin_facility_alias_id,
coalesce(case when trim(c_lane.o_city)='' then NULL else trim(c_lane.o_city) end, 'N/A') as origin_city,
coalesce(case when trim(c_lane.o_state_prov)='' then NULL else trim(c_lane.o_state_prov) end, 'N/A') as origin_state,
coalesce(case when trim(c_lane.o_postal_code)='' then NULL else trim(c_lane.o_postal_code) end, 'N/A') as origin_postal_cd,
coalesce(case when trim(c_lane.o_country_code)='' then NULL else trim(c_lane.o_country_code) end, 'N/A') as origin_country_cd,
coalesce(case when trim(c_lane.d_loc_type)='' then NULL else trim(c_lane.d_loc_type) end, 'N/A') as destination_location_type,
coalesce(c_lane.d_facility_id, 0) as destination_facility_id,
coalesce(case when trim(c_lane.d_facility_alias_id)='' then NULL else trim(c_lane.d_facility_alias_id) end, 'N/A') as destination_facility_alias_id,
coalesce(case when trim(c_lane.d_city)='' then NULL else trim(c_lane.d_city) end, 'N/A') as destination_city,
coalesce(case when trim(c_lane.d_state_prov)='' then NULL else trim(c_lane.d_state_prov) end, 'N/A') as destination_state,
coalesce(case when trim(c_lane.d_postal_code)='' then NULL else trim(c_lane.d_postal_code) end, 'N/A') as destination_postal_cd,
coalesce(case when trim(c_lane.d_country_code)='' then NULL else trim(c_lane.d_country_code) end, 'N/A') as destination_country_cd,
coalesce(case when trim(c_lane.frequency)='' then NULL else trim(c_lane.frequency) end, 'N/A') as frequency,
coalesce(c_lane.o_zone_id, 0) as origin_zone_id,
coalesce(c_lane.d_zone_id, 0) as destination_zone_id,
coalesce(case when trim(c_lane.lane_name)='' then NULL else trim(c_lane.lane_name) end, 'N/A') as lane_name,
coalesce(c_lane_dtl.lane_dtl_seq, 0) as lane_detail_sequence,
coalesce(c_lane_dtl.lane_dtl_status, 0) as lane_detail_status,
coalesce(c_lane_dtl.is_rating, 0) as lane_detail_is_rating,
coalesce(c_lane_dtl.is_routing, 0) as lane_detail_is_routing,
coalesce(c_lane_dtl.is_sailing, 0) as lane_detail_is_sailing,
coalesce(c_lane_dtl.is_budgeted, 0) as is_budgeted,
coalesce(c_lane_dtl.is_preferred, 0) as is_preferred,
coalesce(to_utc_timestamp(from_unixtime(c_lane_dtl.last_updated_dttm DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as lane_detail_last_updated_dttm,
coalesce(to_date(to_utc_timestamp(from_unixtime(c_lane_dtl.effective_dt DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT")), '9999-12-31') as effective_dt,
coalesce(to_date(to_utc_timestamp(from_unixtime(c_lane_dtl.expiration_dt DIV 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT")), '9999-12-31') as expiration_dt,
coalesce(c_lane_dtl.rank, 0) as peak_capacity,
coalesce(c_lane_dtl.weekly_capacity, 0) as weekly_capacity,
coalesce(c_lane_dtl.daily_capacity, 0) as daily_capacity,
coalesce(c_lane_dtl.capacity_sun, 0) as capacity_sun,
coalesce(c_lane_dtl.capacity_mon, 0) as capacity_mon,
coalesce(c_lane_dtl.capacity_tue, 0) as capacity_tue,
coalesce(c_lane_dtl.capacity_wed, 0) as capacity_wed,
coalesce(c_lane_dtl.capacity_thu, 0) as capacity_thu,
coalesce(c_lane_dtl.capacity_fri, 0) as capacity_fri,
coalesce(c_lane_dtl.capacity_sat, 0) as capacity_sat,
coalesce(c_lane_dtl.weekly_commitment, 0) as weekly_commitment,
coalesce(c_lane_dtl.daily_commitment, 0) as daily_commitment,
coalesce(c_lane_dtl.commitment_sun, 0) as commitment_sun,
coalesce(c_lane_dtl.commitment_mon, 0) as commitment_mon,
coalesce(c_lane_dtl.commitment_tue, 0) as commitment_tue,
coalesce(c_lane_dtl.commitment_wed, 0) as commitment_wed,
coalesce(c_lane_dtl.commitment_thu, 0) as commitment_thu,
coalesce(c_lane_dtl.commitment_fri, 0) as commitment_fri,
coalesce(c_lane_dtl.commitment_sat, 0) as commitment_sat,
coalesce(c_lane_dtl.weekly_commit_pct, 0.0) as weekly_commitment_percent,
coalesce(c_lane_dtl.daily_commit_pct, 0.0) as daily_commitment_percent,
coalesce(c_lane_dtl.commit_pct_sun, 0.0) as commitment_percent_sun,
coalesce(c_lane_dtl.commit_pct_mon, 0.0) as commitment_percent_mon,
coalesce(c_lane_dtl.commit_pct_tue, 0.0) as commitment_percent_tue,
coalesce(c_lane_dtl.commit_pct_wed, 0.0) as commitment_percent_wed,
coalesce(c_lane_dtl.commit_pct_thu, 0.0) as commitment_percent_thu,
coalesce(c_lane_dtl.commit_pct_fri, 0.0) as commitment_percent_fri,
coalesce(c_lane_dtl.commit_pct_sat, 0.0) as commitment_percent_sat,
coalesce(case when trim(c_lane_dtl.custom_text5)='' then NULL else trim(c_lane_dtl.custom_text5) end, 'N/A') as 2017_non_peak_awards,
coalesce(c_lane_dtl.montly_capacity, 0) as montly_capacity,
coalesce(c_lane_dtl.yearly_capacity, 0) as yearly_capacity,
coalesce(c_lane_dtl.montly_commitment, 0) as montly_commitment,
coalesce(c_lane_dtl.yearly_commitment, 0) as yearly_commitment,
coalesce(c_lane_dtl.montly_commit_pct, 0.0) as montly_commitment_percent,
coalesce(c_lane_dtl.yearly_commit_pct, 0.0) as yearly_commitment_percent,
coalesce(c_lane_dtl.carrier_id, 0) as carrier_id,
coalesce(c_lane_dtl.equipment_id, 0) as equipment_id,
coalesce(c_lane_dtl.mot_id, 0) as mot_id,
coalesce(c_lane_dtl.protection_level_id, 0) as protection_level_id,
coalesce(c_lane_dtl.service_level_id, 0) as service_level_id,
coalesce(c_lane_dtl.no_rating, 0) as no_rating,
coalesce(case when trim(c_lane_dtl.contract_number)='' then NULL else trim(c_lane_dtl.contract_number) end, 'N/A') as contract_number,
coalesce(lane_dtl_view.rate, 0.0000) as rate,
coalesce(case when trim(lane_dtl_view.currency_code)='' then NULL else trim(lane_dtl_view.currency_code) end, 'N/A') as currency_cd,
coalesce(case when trim(lane_dtl_view.rate_calc_method)='' then NULL else trim(lane_dtl_view.rate_calc_method) end, 'N/A') as rate_calc_method,
coalesce(case when trim(lane_dtl_view.rate_uom)='' then NULL else trim(lane_dtl_view.rate_uom) end, 'N/A') as rate_uom,
coalesce(lane_dtl_view.minimum_rate, 0.0000) as minimum_rate,
coalesce(mgt_od.mileage, 0.0) as mileage
from tms_stage0.ca2015_comb_lane c_lane
inner join tms_stage0.ca2015_comb_lane_dtl c_lane_dtl on c_lane.tc_company_id=c_lane_dtl.tc_company_id and c_lane.lane_id=c_lane_dtl.lane_id
left outer join tms_stage0.ca2015_mgt_lane_od mgt_od on c_lane.lane_name=mgt_od.lane_name
inner join tms_stage0.ca2015_lane_detail_view lane_dtl_view on c_lane_dtl.lane_id=lane_dtl_view.lane_id and c_lane_dtl.lane_dtl_seq=lane_dtl_view.lane_dtl_seq;

-------------------------
---loading staging data into final stagnig table
-------------------------

set tez.job.name=staging:tms_comb_lane_master_tmp1 :load target table from tms_comb_lane_master_tmp;

! echo started loading into scct_work_db.tms_comb_lane_master_tmp1;

insert overwrite table scct_work_db.tms_comb_lane_master_tmp1 select
coalesce(tmp.company_id, t.company_id),
coalesce(tmp.lane_id, t.lane_id),
coalesce(tmp.origin_search_location, t.origin_search_location),
coalesce(tmp.destination_search_location, t.destination_search_location),
coalesce(tmp.lane_unique_id, t.lane_unique_id),
coalesce(tmp.lane_is_rating, t.lane_is_rating),
coalesce(tmp.lane_is_routing, t.lane_is_routing),
coalesce(tmp.lane_is_sailing, t.lane_is_sailing),
coalesce(tmp.lane_created_dttm, t.lane_created_dttm),
coalesce(tmp.lane_updated_dttm, t.lane_updated_dttm),
coalesce(tmp.lane_hierarchy, t.lane_hierarchy),
coalesce(tmp.origin_location_type, t.origin_location_type),
coalesce(tmp.origin_falcility_id, t.origin_falcility_id),
coalesce(tmp.origin_facility_alias_id, t.origin_facility_alias_id),
coalesce(tmp.origin_city, t.origin_city),
coalesce(tmp.origin_state, t.origin_state),
coalesce(tmp.origin_postal_cd, t.origin_postal_cd),
coalesce(tmp.origin_country_cd, t.origin_country_cd),
coalesce(tmp.destination_location_type, t.destination_location_type),
coalesce(tmp.destination_facility_id, t.destination_facility_id),
coalesce(tmp.destination_facility_alias_id, t.destination_facility_alias_id),
coalesce(tmp.destination_city, t.destination_city),
coalesce(tmp.destination_state, t.destination_state),
coalesce(tmp.destination_postal_cd, t.destination_postal_cd),
coalesce(tmp.destination_country_cd, t.destination_country_cd),
coalesce(tmp.frequency, t.frequency),
coalesce(tmp.origin_zone_id, t.origin_zone_id),
coalesce(tmp.destination_zone_id, t.destination_zone_id),
coalesce(tmp.lane_name, t.lane_name),
coalesce(tmp.lane_detail_sequence, t.lane_detail_sequence),
coalesce(tmp.lane_detail_status, t.lane_detail_status),
coalesce(tmp.lane_detail_is_rating, t.lane_detail_is_rating),
coalesce(tmp.lane_detail_is_routing, t.lane_detail_is_routing),
coalesce(tmp.lane_detail_is_sailing, t.lane_detail_is_sailing),
coalesce(tmp.is_budgeted, t.is_budgeted),
coalesce(tmp.is_preferred, t.is_preferred),
coalesce(tmp.lane_detail_last_updated_dttm, t.lane_detail_last_updated_dttm),
coalesce(tmp.effective_dt, t.effective_dt),
coalesce(tmp.expiration_dt, t.expiration_dt),
coalesce(tmp.peak_capacity, t.peak_capacity),
coalesce(tmp.weekly_capacity, t.weekly_capacity),
coalesce(tmp.daily_capacity, t.daily_capacity),
coalesce(tmp.capacity_sun, t.capacity_sun),
coalesce(tmp.capacity_mon, t.capacity_mon),
coalesce(tmp.capacity_tue, t.capacity_tue),
coalesce(tmp.capacity_wed, t.capacity_wed),
coalesce(tmp.capacity_thu, t.capacity_thu),
coalesce(tmp.capacity_fri, t.capacity_fri),
coalesce(tmp.capacity_sat, t.capacity_sat),
coalesce(tmp.weekly_commitment, t.weekly_commitment),
coalesce(tmp.daily_commitment, t.daily_commitment),
coalesce(tmp.commitment_sun, t.commitment_sun),
coalesce(tmp.commitment_mon, t.commitment_mon),
coalesce(tmp.commitment_tue, t.commitment_tue),
coalesce(tmp.commitment_wed, t.commitment_wed),
coalesce(tmp.commitment_thu, t.commitment_thu),
coalesce(tmp.commitment_fri, t.commitment_fri),
coalesce(tmp.commitment_sat, t.commitment_sat),
coalesce(tmp.weekly_commitment_percent, t.weekly_commitment_percent),
coalesce(tmp.daily_commitment_percent, t.daily_commitment_percent),
coalesce(tmp.commitment_percent_sun, t.commitment_percent_sun),
coalesce(tmp.commitment_percent_mon, t.commitment_percent_mon),
coalesce(tmp.commitment_percent_tue, t.commitment_percent_tue),
coalesce(tmp.commitment_percent_wed, t.commitment_percent_wed),
coalesce(tmp.commitment_percent_thu, t.commitment_percent_thu),
coalesce(tmp.commitment_percent_fri, t.commitment_percent_fri),
coalesce(tmp.commitment_percent_sat, t.commitment_percent_sat),
coalesce(tmp.2017_non_peak_awards, t.2017_non_peak_awards),
coalesce(tmp.montly_capacity, t.montly_capacity),
coalesce(tmp.yearly_capacity, t.yearly_capacity),
coalesce(tmp.montly_commitment, t.montly_commitment),
coalesce(tmp.yearly_commitment, t.yearly_commitment),
coalesce(tmp.montly_commitment_percent, t.montly_commitment_percent),
coalesce(tmp.yearly_commitment_percent, t.yearly_commitment_percent),
coalesce(tmp.carrier_id, t.carrier_id),
coalesce(tmp.equipment_id, t.equipment_id),
coalesce(tmp.mot_id, t.mot_id),
coalesce(tmp.protection_level_id, t.protection_level_id),
coalesce(tmp.service_level_id, t.service_level_id),
coalesce(tmp.no_rating, t.no_rating),
coalesce(tmp.contract_number, t.contract_number),
coalesce(tmp.rate, t.rate),
coalesce(tmp.currency_cd, t.currency_cd),
coalesce(tmp.rate_calc_method, t.rate_calc_method),
coalesce(tmp.rate_uom, t.rate_uom),
coalesce(tmp.minimum_rate, t.minimum_rate),
coalesce(tmp.mileage, t.mileage)
from scct_db.tms_comb_lane_master t
full outer join scct_work_db.tms_comb_lane_master_tmp tmp
on t.lane_id=tmp.lane_id
and t.lane_detail_sequence=tmp.lane_detail_sequence
and t.carrier_id=tmp.carrier_id
and t.effective_dt=tmp.effective_dt
and t.expiration_dt=tmp.expiration_dt;


-------------------------
---loading staging data into target table
-------------------------

set tez.job.name=staging:tms_comb_lane_master :load target table from tms_comb_lane_master_tmp1;

! echo started loading into scct_db.tms_comb_lane_master;



insert overwrite table scct_db.tms_comb_lane_master select
company_id,
lane_id,
origin_search_location,
destination_search_location,
lane_unique_id,
lane_is_rating,
lane_is_routing,
lane_is_sailing,
lane_created_dttm,
lane_updated_dttm,
lane_hierarchy,
origin_location_type,
origin_falcility_id,
origin_facility_alias_id,
origin_city,
origin_state,
origin_postal_cd,
origin_country_cd,
destination_location_type,
destination_facility_id,
destination_facility_alias_id,
destination_city,
destination_state,
destination_postal_cd,
destination_country_cd,
frequency,
origin_zone_id,
destination_zone_id,
lane_name,
lane_detail_sequence,
lane_detail_status,
lane_detail_is_rating,
lane_detail_is_routing,
lane_detail_is_sailing,
is_budgeted,
is_preferred,
lane_detail_last_updated_dttm,
effective_dt,
expiration_dt,
peak_capacity,
weekly_capacity,
daily_capacity,
capacity_sun,
capacity_mon,
capacity_tue,
capacity_wed,
capacity_thu,
capacity_fri,
capacity_sat,
weekly_commitment,
daily_commitment,
commitment_sun,
commitment_mon,
commitment_tue,
commitment_wed,
commitment_thu,
commitment_fri,
commitment_sat,
weekly_commitment_percent,
daily_commitment_percent,
commitment_percent_sun,
commitment_percent_mon,
commitment_percent_tue,
commitment_percent_wed,
commitment_percent_thu,
commitment_percent_fri,
commitment_percent_sat,
2017_non_peak_awards,
montly_capacity,
yearly_capacity,
montly_commitment,
yearly_commitment,
montly_commitment_percent,
yearly_commitment_percent,
carrier_id,
equipment_id,
mot_id,
protection_level_id,
service_level_id,
no_rating,
contract_number,
rate,
currency_cd,
rate_calc_method,
rate_uom,
minimum_rate,
mileage
from scct_work_db.tms_comb_lane_master_tmp1;

! echo data load is completed;

-------------------------
---end script
-------------------------