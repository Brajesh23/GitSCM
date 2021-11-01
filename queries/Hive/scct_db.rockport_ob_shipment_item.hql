---jobname:scct-rockport-hive-stg1-scct_db.rockport_ob_shipment_item
--------------------------------------------------------------------------
---Target: STAGE1 table: scct_db.rockport_ob_shipment_item 
---Source: SCCT_WORK_DB table : scct_work_db.rockport_ob_shipment_item_temp1, scct_work_db.rockmstr_ob_event_info_temp3
---Description: This script loads stage 1 table using scct_work_db.rockport_ob_shipment_item_temp1 and scct_work_db.rockmstr_ob_event_info_temp3 work tables
---Load Type: Incremental
---Back posting: No
---Author: ryadav
--------------------------------------------------------------------------

-- History Information
--
-- Revision    Date     JIRA       Developer-ID Comments
-- ----------- ------- ----------  ------------ ----------------------------
-- 01.00.00.01 02AUG19 SCCTP4-2119  RYADAV       Initial Version
----------------------------------------------------------------------------

-------------------------
---Loading scct_db.rockport_ob_shipment_item stage 1 base data
-------------------------

! echo Started Loading into scct_db.rockport_ob_shipment_item ;

insert overwrite table scct_db.rockport_ob_shipment_item
select primary_agent_id
,primary_agent_name
,secondary_agent_id
,shipment_nbr
,origin_dc
,shipper_name
,shipper_address1
,shipper_address2
,shipper_city
,shipper_state
,shipper_zip
,shipper_cc
,seal_nbr
,seal_intact
,linehaul_carrier_cd
,departure_dttm
,est_arrival_dttm
,total_gross_wgt
,total_gross_weight_uom
,total_net_wgt
,total_net_weight_uom
,consignee_name
,consignee_address_1
,consignee_address_2
,consignee_city
,consignee_state
,consignee_zip
,consignee_cc
,commercial_invoice_nbr
,total_asn_cartons
,total_origin_cargo_receipt_cartons
,total_booked_cartons
,total_shipped_cartons
,total_destination_cargo_receipt_cartons
,total_pod_cartons
,shipment_update_dttm
,agent_shipment_nbr
,cargo_receipt_nbr
,mawb_obol
,hawb_container
,transportation_mode
,book_dttm
,destination_cc
,flight_voyage_nbr
,booking_gross_wgt
,booking_gross_weight_uom
,booking_net_wgt
,booking_net_weight_uom
,est_ship_dttm
,booking_booked_cartons
,booking_shipped_cartons
,est_delivery_dttm
,booking_destination_cargo_receipt_cartons
,booking_pod_cartons
,booking_update_dttm
,bol_nbr
,owner
,store_nbr
,store_name
,store_address1
,store_address2
,store_address3
,store_city
,store_state
,store_zip
,store_cc
,bol_asn_cartons
,bol_origin_cargo_receipt_cartons
,bol_shipped_cartons
,bol_destination_cargo_receipt_cartons
,bol_pod_cartons
,signee
,bol_update_dttm
,carton_barcode
,carton_nbr
,carton_class
,merch_category_cd
,carton_process_tm
,carton_gross_wgt
,carton_gross_weight_uom
,carton_net_wgt
,carton_net_weight_uom
,total_qty
,origin_cargo_receipt_dttm
,ship_dttm
,destination_cargo_receipt_dttm
,destination_cargo_receipt_damage_cd
,pod_delivery_dttm
,pod_damage_cd
,carton_update_dttm
,vas_start_dttm
,vas_end_dttm
,ciq_approval_dttm
,inventory_approval_dttm
,total_adj_qty
,order_nbr
,delivery_nbr
,site
,sku
,sku_description
,sku_barcode
,generic_article_number
,origin_cc
,haz
,batch_cd
,qty
,item_update_dttm
,adjusted_qty
,item_adjusted_update_dttm
,adjusted_reason_cd
,coalesce((case when trim(ei.315af_departed_pickup_location_agent_id)='' then null else trim(ei.315af_departed_pickup_location_agent_id) end),'N/A') as 315af_departed_pickup_location_agent_id
,coalesce((case when trim(ei.315af_departed_pickup_location_agent_name)='' then null else trim(ei.315af_departed_pickup_location_agent_name) end),'N/A') as 315af_departed_pickup_location_agent_name
,coalesce((case when ei.315af_departed_pickup_location_event_dttm='' then null else ei.315af_departed_pickup_location_event_dttm end),'9999-12-31 00:00:00') as 315af_departed_pickup_location_event_dttm
,coalesce((case when ei.315af_departed_pickup_location_received_dttm=0 then null else ei.315af_departed_pickup_location_received_dttm end),'9999-12-31 00:00:00') as 315af_departed_pickup_location_received_dttm
,coalesce((case when trim(ei.315f_actual_departure_agent_id)='' then null else trim(ei.315f_actual_departure_agent_id) end),'N/A') as 315f_actual_departure_agent_id
,coalesce((case when trim(ei.315f_actual_departure_agent_name)='' then null else trim(ei.315f_actual_departure_agent_name) end),'N/A') as 315f_actual_departure_agent_name
,coalesce((case when ei.315f_actual_departure_event_dttm='' then null else ei.315f_actual_departure_event_dttm end),'9999-12-31 00:00:00') as 315f_actual_departure_event_dttm
,coalesce((case when ei.315f_actual_departure_received_dttm=0 then null else ei.315f_actual_departure_received_dttm end),'9999-12-31 00:00:00') as 315f_actual_departure_received_dttm
,coalesce((case when trim(ei.315a_port_arrival_agent_id)='' then null else trim(ei.315a_port_arrival_agent_id) end),'N/A') as 315a_port_arrival_agent_id
,coalesce((case when trim(ei.315a_port_arrival_agent_name)='' then null else trim(ei.315a_port_arrival_agent_name) end),'N/A') as 315a_port_arrival_agent_name
,coalesce((case when ei.315a_port_arrival_event_dttm='' then null else ei.315a_port_arrival_event_dttm end),'9999-12-31 00:00:00') as 315a_port_arrival_event_dttm
,coalesce((case when ei.315a_port_arrival_received_dttm=0 then null else ei.315a_port_arrival_received_dttm end),'9999-12-31 00:00:00') as 315a_port_arrival_received_dttm
,coalesce((case when trim(ei.315co_customs_submit_agent_id)='' then null else trim(ei.315co_customs_submit_agent_id) end),'N/A') as 315co_customs_submit_agent_id
,coalesce((case when trim(ei.315co_customs_submit_agent_name)='' then null else trim(ei.315co_customs_submit_agent_name) end),'N/A') as 315co_customs_submit_agent_name
,coalesce((case when ei.315co_customs_submit_event_dttm='' then null else ei.315co_customs_submit_event_dttm end),'9999-12-31 00:00:00') as 315co_customs_submit_event_dttm
,coalesce((case when ei.315co_customs_submit_received_dttm=0 then null else ei.315co_customs_submit_received_dttm end),'9999-12-31 00:00:00') as 315co_customs_submit_received_dttm
,coalesce((case when trim(ei.315co_unique_document_reference_nbr)='' then null else trim(ei.315co_unique_document_reference_nbr) end),'N/A') as 315co_unique_document_reference_nbr
,coalesce((case when trim(ei.315ct_customs_release_agent_id)='' then null else trim(ei.315ct_customs_release_agent_id) end),'N/A') as 315ct_customs_release_agent_id
,coalesce((case when trim(ei.315ct_customs_release_agent_name)='' then null else trim(ei.315ct_customs_release_agent_name) end),'N/A') as 315ct_customs_release_agent_name
,coalesce((case when ei.315ct_customs_release_event_dttm='' then null else ei.315ct_customs_release_event_dttm end),'9999-12-31 00:00:00') as 315ct_customs_release_event_dttm
,coalesce((case when ei.315ct_customs_release_received_dttm=0 then null else ei.315ct_customs_release_received_dttm end),'9999-12-31 00:00:00') as 315ct_customs_release_received_dttm
,coalesce((case when trim(ei.315r_terminal_pickup_agent_id)='' then null else trim(ei.315r_terminal_pickup_agent_id) end),'N/A') as 315r_terminal_pickup_agent_id
,coalesce((case when trim(ei.315r_terminal_pickup_agent_name)='' then null else trim(ei.315r_terminal_pickup_agent_name) end),'N/A') as 315r_terminal_pickup_agent_name
,coalesce((case when ei.315r_terminal_pickup_event_dttm='' then null else ei.315r_terminal_pickup_event_dttm end),'9999-12-31 00:00:00') as 315r_terminal_pickup_event_dttm
,coalesce((case when ei.315r_terminal_pickup_received_dttm=0 then null else ei.315r_terminal_pickup_received_dttm end),'9999-12-31 00:00:00') as 315r_terminal_pickup_received_dttm
,coalesce((case when trim(ei.315x1_cross_dock_agent_id)='' then null else trim(ei.315x1_cross_dock_agent_id) end),'N/A') as 315x1_cross_dock_agent_id
,coalesce((case when trim(ei.315x1_cross_dock_agent_name)='' then null else trim(ei.315x1_cross_dock_agent_name) end),'N/A') as 315x1_cross_dock_agent_name
,coalesce((case when ei.315x1_cross_dock_event_dttm='' then null else ei.315x1_cross_dock_event_dttm end),'9999-12-31 00:00:00') as 315x1_cross_dock_event_dttm
,coalesce((case when ei.315x1_cross_dock_received_dttm=0 then null else ei.315x1_cross_dock_received_dttm end),'9999-12-31 00:00:00') as 315x1_cross_dock_received_dttm
,coalesce((case when trim(ei.315ri_bonded_dc_arrival_agent_id)='' then null else trim(ei.315ri_bonded_dc_arrival_agent_id) end),'N/A') as 315ri_bonded_dc_arrival_agent_id
,coalesce((case when trim(ei.315ri_bonded_dc_arrival_agent_name)='' then null else trim(ei.315ri_bonded_dc_arrival_agent_name) end),'N/A') as 315ri_bonded_dc_arrival_agent_name
,coalesce((case when ei.315ri_bonded_dc_arrival_event_dttm='' then null else ei.315ri_bonded_dc_arrival_event_dttm end),'9999-12-31 00:00:00') as 315ri_bonded_dc_arrival_event_dttm
,coalesce((case when ei.315ri_bonded_dc_arrival_received_dttm=0 then null else ei.315ri_bonded_dc_arrival_received_dttm end),'9999-12-31 00:00:00') as 315ri_bonded_dc_arrival_received_dttm
,coalesce((case when trim(ei.856tt_vas_start_agent_id)='' then null else trim(ei.856tt_vas_start_agent_id) end),'N/A') as 856tt_vas_start_agent_id
,coalesce((case when trim(ei.856tt_vas_start_agent_name)='' then null else trim(ei.856tt_vas_start_agent_name) end),'N/A') as 856tt_vas_start_agent_name
,coalesce((case when ei.856tt_vas_start_event_dttm='' then null else ei.856tt_vas_start_event_dttm end),'9999-12-31 00:00:00') as 856tt_vas_start_event_dttm
,coalesce((case when ei.856tt_vas_start_received_dttm=0 then null else ei.856tt_vas_start_received_dttm end),'9999-12-31 00:00:00') as 856tt_vas_start_received_dttm
,coalesce((case when trim(ei.856u4_ciq_approval_agent_id)='' then null else trim(ei.856u4_ciq_approval_agent_id) end),'N/A') as 856u4_ciq_approval_agent_id
,coalesce((case when trim(ei.856u4_ciq_approval_agent_name)='' then null else trim(ei.856u4_ciq_approval_agent_name) end),'N/A') as 856u4_ciq_approval_agent_name
,coalesce((case when ei.856u4_ciq_approval_event_dttm='' then null else ei.856u4_ciq_approval_event_dttm end),'9999-12-31 00:00:00') as 856u4_ciq_approval_event_dttm
,coalesce((case when ei.856u4_ciq_approval_received_dttm=0 then null else ei.856u4_ciq_approval_received_dttm end),'9999-12-31 00:00:00') as 856u4_ciq_approval_received_dttm
,coalesce((case when trim(ei.315xb_pickup_nonbonded_agent_id)='' then null else trim(ei.315xb_pickup_nonbonded_agent_id) end),'N/A') as 315xb_pickup_nonbonded_agent_id
,coalesce((case when trim(ei.315xb_pickup_nonbonded_agent_name)='' then null else trim(ei.315xb_pickup_nonbonded_agent_name) end),'N/A') as 315xb_pickup_nonbonded_agent_name
,coalesce((case when ei.315xb_pickup_nonbonded_event_dttm='' then null else ei.315xb_pickup_nonbonded_event_dttm end),'9999-12-31 00:00:00') as 315xb_pickup_nonbonded_event_dttm
,coalesce((case when ei.315xb_pickup_nonbonded_received_dttm=0 then null else ei.315xb_pickup_nonbonded_received_dttm end),'9999-12-31 00:00:00') as 315xb_pickup_nonbonded_received_dttm
,coalesce((case when trim(ei.315zz_store_departure_agent_id)='' then null else trim(ei.315zz_store_departure_agent_id) end),'N/A') as 315zz_store_departure_agent_id
,coalesce((case when trim(ei.315zz_store_departure_agent_name)='' then null else trim(ei.315zz_store_departure_agent_name) end),'N/A') as 315zz_store_departure_agent_name
,coalesce((case when ei.315zz_store_departure_event_dttm='' then null else ei.315zz_store_departure_event_dttm end),'9999-12-31 00:00:00') as 315zz_store_departure_event_dttm
,coalesce((case when ei.315zz_store_departure_received_dttm=0 then null else ei.315zz_store_departure_received_dttm end),'9999-12-31 00:00:00') as 315zz_store_departure_received_dttm
,coalesce((case when trim(ei.85630_store_delivery_agent_id)='' then null else trim(ei.85630_store_delivery_agent_id) end),'N/A') as 85630_store_delivery_agent_id
,coalesce((case when trim(ei.85630_store_delivery_agent_name)='' then null else trim(ei.85630_store_delivery_agent_name) end),'N/A') as 85630_store_delivery_agent_name
,coalesce((case when ei.85630_store_delivery_event_dttm='' then null else ei.85630_store_delivery_event_dttm end),'9999-12-31 00:00:00') as 85630_store_delivery_event_dttm
,coalesce((case when ei.85630_store_delivery_received_dttm=0 then null else ei.85630_store_delivery_received_dttm end),'9999-12-31 00:00:00') as 85630_store_delivery_received_dttm
from scct_work_db.rockport_ob_shipment_item_temp1 si
left outer join scct_work_db.rockmstr_ob_event_info_temp3 ei
on si.shipment_nbr = ei.shipment_no;

! echo Completed Loading into scct_db.rockport_ob_shipment_item ;

 -------------------------
 ---end script
 -------------------------

