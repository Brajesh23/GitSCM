---jobname:scct-E2E_Outbound_Visibility-hive-stg1-scct_db.purge_wms_outbound_e2e
--------------------------------------------------------------------------
---Target: STAGE1 table: scct_db.wms_outbound_e2e
---Source: scct_db table : scct_db.wms_outbound_e2e and operations_db.purge_tables 
---Load Type: incrimental
---Back posting: No
---Author: ssoma
---Created Date: 12/02/2019
--------------------------------------------------------------------------

-------------------------
---loading worker table without STR rows 
-------------------------


insert overwrite table scct_work_db.wms_retail_warehouse_shipping_link_work
select
rs.retail_warehouse
,rs.retail_distro_nbr
,rs.retail_store_nbr
,rs.retail_season_nbr
,rs.retail_season_year_nbr
,rs.retail_style_nbr
,rs.retail_style_sfx_nbr
,rs.retail_sector
,rs.retail_sap_style
,rs.retail_sap_choice
,rs.retail_sap_size
,rs.retail_ticket_typ
,rs.retail_instore_dt
,rs.retail_distro_total_qty
,rs.retail_distro_allocated_qty
,rs.retail_distro_remaining_qty
,rs.retail_dtstor_status_flag
,rs.retail_record_expansion_fld
,rs.retail_delv_doc_nbr
,rs.retail_delv_doc_itm_nbr
,rs.retail_distro_last_modified_datetime
,rs.retail_distro_create_datetime
,rs.retail_pickticket_control_nbr
,rs.retail_pdpick_pkt_line_nbr
,rs.retail_pdpick_pick_wave_nbr
,rs.retail_wave_processing_typ
,rs.retail_inv_allocation_typ
,rs.retail_pick_determination_typ
,rs.retail_pick_loc_assignment_typ
,rs.retail_pickticket_pick_location
,rs.retail_original_pick_order_qty
,rs.retail_original_pick_qty
,rs.retail_pick_qty
,rs.retail_pick_shortage_qty
,rs.retail_pick_cancelled_qty
,rs.retail_pick_packed_qty
,rs.retail_pdpick_status_flg
,rs.retail_specl_instr_cd_1
,rs.retail_pickticket_last_modified_datetime
,rs.retail_pdpick_user_id
,rs.retail_district
,rs.retail_shipto_name
,rs.retail_shipto_state
,rs.retail_shipto_zip
,rs.retail_shipto_country
,rs.retail_pickticket_header_created_datetime
,rs.retail_pickticket_header_last_modified_datetime
,rs.retail_phpick_user_id
,rs.retail_carton_nbr
,rs.retail_carton_line_nbr
,rs.retail_package_barcd
,rs.retail_cdcart_pkt_line_nbr
,rs.retail_cdcart_pick_wave_nbr
,rs.retail_carton_qty_to_be_packed
,rs.retail_carton_qty_packed
,rs.retail_carton_delivered_qty
,rs.retail_carton_cancelled_qty
,rs.retail_carton_pick_location
,rs.retail_cdcart_status_flag
,rs.retail_packer_id
,rs.retail_carton_line_create_datetime
,rs.retail_carton_line_last_modified_datetime
,rs.retail_cdcart_user_id
,rs.retail_misc_nbr_1
,rs.retail_chcart_pick_wave_nbr
,rs.retail_chcart_status_flg
,rs.retail_pack_cd
,rs.retail_pack_typ
,rs.retail_single_sku_flg
,rs.retail_first_pick_location
,rs.retail_carton_typ
,rs.retail_carton_sz
,rs.retail_carton_volume
,rs.retail_estimated_weight
,rs.retail_actual_weight
,rs.retail_pallet_id
,rs.retail_product_value
,rs.retail_bill_of_lading
,rs.retail_shipment_nbr
,rs.retail_load_nbr
,rs.retail_path_nbr
,rs.retail_ship_via
,rs.retail_special_ins_cd_1
,rs.retail_special_ins_cd_2
,rs.retail_special_ins_cd_3
,rs.retail_misc_10_byte_1
,rs.retail_misc_20_byte_1
,rs.retail_misc_20_byte_2
,rs.retail_misc_20_byte_4
,rs.retail_misc_20_byte_5
,rs.retail_reference_case_nbr
,rs.retail_carton_nbr_x_of_y
,rs.retail_audit_status
,rs.retail_tms_parcel_nbr
,rs.retail_carton_length
,rs.retail_carton_width
,rs.retail_carton_height
,rs.retail_recipient_first_name
,rs.retail_carton_last_modified_datetime
,rs.retail_chcart_user_id
,rs.retail_pickticket_create_datetime
,rs.retail_carton_create_datetime
,rs.retail_carton_invoiced_datetime
,rs.retail_distro_to_wave_time_duration
,rs.retail_wave_to_invoiced_duration
,rs.retail_distro_total_reaches
,rs.retail_distro_allocated_reaches
,rs.retail_distro_remaining_reaches
,rs.retail_original_pick_order_reaches
,rs.retail_original_pick_reaches
,rs.retail_pick_reaches
,rs.retail_pick_shortage_reaches
,rs.retail_pick_cancelled_reaches
,rs.retail_pick_packed_reaches
,rs.retail_carton_reaches_to_be_packed
,rs.retail_carton_reaches_packed
,rs.retail_carton_delivered_reaches
,rs.retail_carton_cancelled_reaches
,rs.retail_fc_ltfc
,rs.retail_processing_area
,rs.shipping_misc_20_byte_3
,rs.shipping_carton_nbr
,rs.shipping_warehouse
,rs.shipping_load_nbr
,rs.shipping_destination_typ
,rs.shipping_ship_via
,rs.shipping_master_bill_of_lading
,rs.shipping_trailer_nbr
,rs.shipping_tractor_nbr
,rs.shipping_driver
,rs.shipping_dock
,rs.shipping_door
,rs.shipping_dock_door_locn_brcd
,rs.shipping_tms_plan_nbr
,rs.shipping_status_flg
,rs.shipping_status_dt
,rs.shipping_load_closed_dt
,rs.shipping_load_closed_ts
,rs.shipping_tc_shipment_id
,rs.shipping_tpe_created_load
,rs.shipping_tpe_processing_status
,rs.shipping_misc_instruction_1
,rs.shipping_created_dt
,rs.shipping_created_ts
,rs.shipping_last_modified_dt
,rs.shipping_last_modified_ts
,rs.shipping_user_id
,rs.shipping_record_type
,rs.shipping_trip_nbr
,rs.shipping_trip_type
,rs.shipping_dispatch_dt
,rs.shipping_dispatch_ts
,rs.shipping_carton_limit
,rs.shipping_run_nbr
,rs.shipping_thtrip_ship_via
,rs.shipping_thtrip_status_flg
,rs.shipping_delay_code
,rs.shipping_cartons_loaded
,rs.shipping_seal_nbr
,rs.shipping_date_billed
,rs.shipping_time_billed
,rs.shipping_thtrip_seal_nbr
,rs.shipping_misc1_numeric_fld
,rs.shipping_miscellaneous_20
,rs.shipping_thtrip_created_dt
,rs.shipping_thtrip_created_ts
,rs.shipping_thtrip_last_modified_dt
,rs.shipping_thtrip_last_modified_ts
,rs.shipping_thtrip_user_id
,rs.shipping_load_create_dt_ts
,rs.shipping_load_closed_dt_ts
,rs.shipping_load_create_to_closed_duration
,rs.distro_create_date
,rs.batch_code
from scct_db.wms_outbound_e2e rs join (SELECT
purge_min_dt,
purge_max_dt
from operations_db.purge_tables p join 
(select date_sub(min(to_date(distro_create_datetime)),1) purge_max_dt from scct_db.wms_outbound_retail) r on 1=1
where p.run_ind='N') pt on 1=1
where rs.distro_create_date between pt.purge_min_dt and pt.purge_max_dt and rs.retail_pickticket_control_nbr not like 'STR%';

! echo worker table load is completed;

-------------------------
---Insert back to base table only not STR retail_pickticket_control_nbr rows
-------------------------


insert overwrite table scct_db.wms_outbound_e2e partition(distro_create_date)
select
retail_warehouse
,retail_distro_nbr
,retail_store_nbr
,retail_season_nbr
,retail_season_year_nbr
,retail_style_nbr
,retail_style_sfx_nbr
,retail_sector
,retail_sap_style
,retail_sap_choice
,retail_sap_size
,retail_ticket_typ
,retail_instore_dt
,retail_distro_total_qty
,retail_distro_allocated_qty
,retail_distro_remaining_qty
,retail_dtstor_status_flag
,retail_record_expansion_fld
,retail_delv_doc_nbr
,retail_delv_doc_itm_nbr
,retail_distro_last_modified_datetime
,retail_distro_create_datetime
,retail_pickticket_control_nbr
,retail_pdpick_pkt_line_nbr
,retail_pdpick_pick_wave_nbr
,retail_wave_processing_typ
,retail_inv_allocation_typ
,retail_pick_determination_typ
,retail_pick_loc_assignment_typ
,retail_pickticket_pick_location
,retail_original_pick_order_qty
,retail_original_pick_qty
,retail_pick_qty
,retail_pick_shortage_qty
,retail_pick_cancelled_qty
,retail_pick_packed_qty
,retail_pdpick_status_flg
,retail_specl_instr_cd_1
,retail_pickticket_last_modified_datetime
,retail_pdpick_user_id
,retail_district
,retail_shipto_name
,retail_shipto_state
,retail_shipto_zip
,retail_shipto_country
,retail_pickticket_header_created_datetime
,retail_pickticket_header_last_modified_datetime
,retail_phpick_user_id
,retail_carton_nbr
,retail_carton_line_nbr
,retail_package_barcd
,retail_cdcart_pkt_line_nbr
,retail_cdcart_pick_wave_nbr
,retail_carton_qty_to_be_packed
,retail_carton_qty_packed
,retail_carton_delivered_qty
,retail_carton_cancelled_qty
,retail_carton_pick_location
,retail_cdcart_status_flag
,retail_packer_id
,retail_carton_line_create_datetime
,retail_carton_line_last_modified_datetime
,retail_cdcart_user_id
,retail_misc_nbr_1
,retail_chcart_pick_wave_nbr
,retail_chcart_status_flg
,retail_pack_cd
,retail_pack_typ
,retail_single_sku_flg
,retail_first_pick_location
,retail_carton_typ
,retail_carton_sz
,retail_carton_volume
,retail_estimated_weight
,retail_actual_weight
,retail_pallet_id
,retail_product_value
,retail_bill_of_lading
,retail_shipment_nbr
,retail_load_nbr
,retail_path_nbr
,retail_ship_via
,retail_special_ins_cd_1
,retail_special_ins_cd_2
,retail_special_ins_cd_3
,retail_misc_10_byte_1
,retail_misc_20_byte_1
,retail_misc_20_byte_2
,retail_misc_20_byte_4
,retail_misc_20_byte_5
,retail_reference_case_nbr
,retail_carton_nbr_x_of_y
,retail_audit_status
,retail_tms_parcel_nbr
,retail_carton_length
,retail_carton_width
,retail_carton_height
,retail_recipient_first_name
,retail_carton_last_modified_datetime
,retail_chcart_user_id
,retail_pickticket_create_datetime
,retail_carton_create_datetime
,retail_carton_invoiced_datetime
,retail_distro_to_wave_time_duration
,retail_wave_to_invoiced_duration
,retail_distro_total_reaches
,retail_distro_allocated_reaches
,retail_distro_remaining_reaches
,retail_original_pick_order_reaches
,retail_original_pick_reaches
,retail_pick_reaches
,retail_pick_shortage_reaches
,retail_pick_cancelled_reaches
,retail_pick_packed_reaches
,retail_carton_reaches_to_be_packed
,retail_carton_reaches_packed
,retail_carton_delivered_reaches
,retail_carton_cancelled_reaches
,retail_fc_ltfc
,retail_processing_area
,shipping_misc_20_byte_3
,shipping_carton_nbr
,shipping_warehouse
,shipping_load_nbr
,shipping_destination_typ
,shipping_ship_via
,shipping_master_bill_of_lading
,shipping_trailer_nbr
,shipping_tractor_nbr
,shipping_driver
,shipping_dock
,shipping_door
,shipping_dock_door_locn_brcd
,shipping_tms_plan_nbr
,shipping_status_flg
,shipping_status_dt
,shipping_load_closed_dt
,shipping_load_closed_ts
,shipping_tc_shipment_id
,shipping_tpe_created_load
,shipping_tpe_processing_status
,shipping_misc_instruction_1
,shipping_created_dt
,shipping_created_ts
,shipping_last_modified_dt
,shipping_last_modified_ts
,shipping_user_id
,shipping_record_type
,shipping_trip_nbr
,shipping_trip_type
,shipping_dispatch_dt
,shipping_dispatch_ts
,shipping_carton_limit
,shipping_run_nbr
,shipping_thtrip_ship_via
,shipping_thtrip_status_flg
,shipping_delay_code
,shipping_cartons_loaded
,shipping_seal_nbr
,shipping_date_billed
,shipping_time_billed
,shipping_thtrip_seal_nbr
,shipping_misc1_numeric_fld
,shipping_miscellaneous_20
,shipping_thtrip_created_dt
,shipping_thtrip_created_ts
,shipping_thtrip_last_modified_dt
,shipping_thtrip_last_modified_ts
,shipping_thtrip_user_id
,shipping_load_create_dt_ts
,shipping_load_closed_dt_ts
,shipping_load_create_to_closed_duration
,batch_code
,distro_create_date
from scct_work_db.wms_retail_warehouse_shipping_link_work;

! echo Purge STR rows completed;

-------------------------
---Update Record into control table to purge STR rows
-------------------------


! echo Started entry into job control table;

insert into operations_db.purge_tables 
select 
concat('wms_retail_warehouse_shipping_link_',CURRENT_DATE) as key,
CURRENT_DATE as job_start_dt,
purge_min_dt, 
'Y' as run_ind
from 
(select min(purge_min_dt) as purge_min_dt from operations_db.purge_tables where run_ind='N') A;

-------------------------
---Insert Record into control table to purge STR rows
-------------------------

! echo Started entry into job control table;

insert into operations_db.purge_tables 
select 
concat('wms_retail_warehouse_shipping_link_',date_add(CURRENT_DATE,1)) as key,
CURRENT_DATE as job_start_dt,
distro_create_dt as purge_min_dt, 
'N' as run_ind
from 
(select min(to_date(distro_create_datetime)) as distro_create_dt from scct_db.wms_outbound_retail) A;


! echo Entry into job control table completed;

-------------------------
---end script
-------------------------
