insert overwrite table scct_db.wms_warehouse_shipping
select 
1  as process_id
,coalesce(chrtl.carton_nbr, 'N/A')  as misc_20_byte_3
,coalesce(chrtl.path_nbr, 'N/A')  as carton_nbr
,coalesce(lhrtlshp.warehouse, 'N/A')  as warehouse
,coalesce(regexp_replace(lhrtlshp.load_nbr,"^0+(?!$)",""), 'N/A')  as load_nbr
,coalesce(lhrtlshp.destination_typ, 'N/A')  as destination_typ
,coalesce(lhrtlshp.ship_via, 'N/A')  as ship_via
,coalesce(lhrtlshp.master_bill_of_lading, 'N/A')  as master_bill_of_lading
,coalesce(lhrtlshp.trailer_nbr, 'N/A')  as trailer_nbr
,coalesce(lhrtlshp.tractor__nbr, 'N/A')  as tractor__nbr
,coalesce(lhrtlshp.driver, 'N/A')  as driver
,coalesce(lhrtlshp.dock, 'N/A')  as dock
,coalesce(lhrtlshp.door, 'N/A')  as door
,coalesce(lhrtlshp.dock_door_locn_brcd, 'N/A')  as dock_door_locn_brcd
,coalesce(lhrtlshp.tms_plan_nbr ,case  when cast(cast(now() as timestamp )+ interval 2 days as timestamp )+ interval 4 days = cast(cast(cast(now() as timestamp )+ interval 2 days as timestamp )+ interval 6 days as timestamp )+ interval 2 days then 0 when cast(cast(now() as timestamp )+ interval 2 days as timestamp )+ interval 4 days > cast(cast(cast(now() as timestamp )+ interval 2 days as timestamp )+ interval 6 days as timestamp )+ interval 2 days then 1 when cast(cast(now() as timestamp )+ interval 2 days as timestamp )+ interval 4 days < cast(cast(cast(now() as timestamp )+ interval 2 days as timestamp )+ interval 6 days as timestamp )+ interval 2 days then -1 when cast(cast(now() as timestamp )+ interval 2 days as timestamp )+ interval 4 days is null or cast(cast(cast(now() as timestamp )+ interval 2 days as timestamp )+ interval 6 days as timestamp )+ interval 2 days is null then null end)  as tms_plan_nbr
,coalesce(lhrtlshp.status_flg, 'N/A')  as status_flg
,coalesce(lhrtlshp.status_dt, '9999-12-31')  as status_dt
,coalesce(lhrtlshp.load_closed_dt ,'9999-12-31')  as load_closed_dt
,coalesce(lhrtlshp.load_closed_ts ,'00:00:00')  as  load_closed_ts
,coalesce(lhrtlshp.tc_shipment_id, 'N/A')  as tc_shipment_id
,coalesce(lhrtlshp.tpe_created_load, 'N/A')  as tpe_created_load
,coalesce(lhrtlshp.tpe_processing_status, 'N/A')  as tpe_processing_status
,coalesce(lhrtlshp.misc_instruction_1, 'N/A')  as misc_instruction_1
,coalesce(lhrtlshp.created_dt, 'N/A')  as created_dt
,coalesce(lhrtlshp.created_ts, 'N/A')  as created_ts
,coalesce(lhrtlshp.last_modified_dt ,'9999-12-31')  as last_modified_dt
,coalesce(lhrtlshp.last_modified_ts ,cast(date_format(cast(cast(cc_rec_start_date as timestamp )+ interval 2 days as timestamp )+ interval 4 days,'yyyy-MM-dd HH:mm:ss') as timestamp )- interval 3 milliseconds)  as last_modified_ts
,coalesce(lhrtlshp.user_id, 'N/A')  as user_id
,coalesce(thrtlshp.record_type, 'N/A')  as record_type
,coalesce(thrtlshp.trip_nbr, 'N/A')  as trip_nbr
,coalesce(thrtlshp.trip_type, 'N/A')  as trip_type
,coalesce(thrtlshp.dispatch_dt  ,'9999-12-31')  as dispatch_dt
,coalesce(thrtlshp.dispatch_ts  ,'00:00:00')  as dispatch_ts
,coalesce(thrtlshp.carton_limit  ,0)  as carton_limit
,coalesce(thrtlshp.run_nbr, 'N/A')  as run_nbr
,coalesce(thrtlshp.ship_via, 'N/A')  as thtrip_ship_via
,coalesce(thrtlshp.status_flg, 'N/A')  as thtrip_status_flg
,coalesce(thrtlshp.delay_code, 'N/A')  as delay_code
,coalesce(thrtlshp.cartons_loaded ,0)  as cartons_loaded
,coalesce(thrtlshp.seal_nbr, 'N/A')  as seal_nbr
,coalesce(thrtlshp.date_billed  ,0)  as date_billed
,coalesce(thrtlshp.time_billed  ,0)  as time_billed
,coalesce(thrtlshp.seal_nbr_2, 'N/A')  as seal_nbr_2
,coalesce(thrtlshp.misc1_numeric_fld, 'N/A')  as misc1_numeric_fld
,coalesce(thrtlshp.miscellaneous_20, 'N/A')  as miscellaneous_20
,coalesce(thrtlshp.created_dt,'9999-12-31')  as thtrip_created_dt
,coalesce(thrtlshp.created_ts ,'00:00:00')  as thtrip_created_ts
,coalesce(thrtlshp.last_modified_dt  ,'9999-12-31')  as thtrip_last_modified_dt
,coalesce(thrtlshp.last_modified_ts ,'00:00:00')  as thtrip_last_modified_ts
,coalesce(thrtlshp.user_id, 'N/A')  as thtrip_user_id
,coalesce(from_unixtime(unix_timestamp(concat(lhrtlshp.created_dt,' ',lhrtlshp.created_ts), "yyyy-MM-dd HH:mm:ss")),'9999-12-31 00:00:00')  as load_create_dt_ts
,coalesce(from_unixtime(unix_timestamp(concat(lhrtlshp.load_closed_dt,' ',lhrtlshp.load_closed_ts), "yyyy-MM-dd HH:mm:ss")),'9999-12-31 00:00:00') load_closed_dt_ts
,case when (lhrtlshp.created_dt='9999-12-31' or lhrtlshp.load_closed_dt='9999-12-31') then 0 else day(CAST(concat(lhrtlshp.load_closed_dt,' ',lhrtlshp.load_closed_ts)  as  timestamp) - CAST(concat(lhrtlshp.created_dt,' ',lhrtlshp.created_ts)  as  timestamp))*24*60*60 
+hour(CAST(concat(lhrtlshp.load_closed_dt,' ',lhrtlshp.load_closed_ts)  as  timestamp) - CAST(concat(lhrtlshp.created_dt,' ',lhrtlshp.created_ts)  as  timestamp))*60*60 
+date_format(CAST(concat(lhrtlshp.load_closed_dt,' ',lhrtlshp.load_closed_ts)  as  timestamp)-CAST(concat(lhrtlshp.created_dt,' ',lhrtlshp.created_ts)  as  timestamp),'m')*60 
+second(CAST(concat(lhrtlshp.load_closed_dt,' ',lhrtlshp.load_closed_ts)  as  timestamp) - CAST(concat(lhrtlshp.created_dt,' ',lhrtlshp.created_ts)  as  timestamp)) end 
 AS load_create_to_closed_duration
from scct_work_db.chcart00_retail_work chrtl
join scct_work_db.lhload00_retail_shipping_work lhrtlshp on chrtl.load_nbr=lhrtlshp.load_nbr and chrtl.warehouse=lhrtlshp.warehouse
join scct_work_db.thtrip00_retail_shipping_work thrtlshp on lhrtlshp.load_nbr=thrtlshp.load_nbr and lhrtlshp.warehouse=thrtlshp.warehouse
where chrtl.Status_flg='85'and chrtl.load_nbr<>'N/A' and thrtlshp.trip_type in ('01','02')  union all
select 
2  as process_id
,coalesce(chws.misc_20_byte_3 ,'N/A' ) as misc_20_byte_3
,coalesce(chws.carton_nbr ,'N/A' ) as carton_nbr
,coalesce(lhws.warehouse ,'N/A' ) as warehouse
,coalesce(regexp_replace(lhws.load_nbr ,"^0+(?!$)",""), 'N/A')  as load_nbr
,coalesce(lhws.destination_typ ,'N/A' ) as destination_typ
,coalesce(lhws.ship_via ,'N/A' ) as ship_via
,coalesce(lhws.master_bill_of_lading ,'N/A' ) as master_bill_of_lading
,coalesce(lhws.trailer_nbr ,'N/A' ) as trailer_nbr
,coalesce(lhws.tractor__nbr ,'N/A' ) as tractor__nbr
,coalesce(lhws.driver ,'N/A' ) as driver
,coalesce(lhws.dock ,'N/A' ) as dock
,coalesce(lhws.door ,'N/A' ) as door
,coalesce(lhws.dock_door_locn_brcd ,'N/A' ) as dock_door_locn_brcd
,coalesce(lhws.tms_plan_nbr ,0 ) as tms_plan_nbr
,coalesce(lhws.status_flg ,'N/A' ) as status_flg
,coalesce(lhws.status_dt ,'9999-12-31' ) as status_dt
,coalesce(lhws.load_closed_dt ,'9999-12-31' ) as load_closed_dt
,coalesce(lhws.load_closed_ts ,'00:00:00' ) as  load_closed_ts
,coalesce(lhws.tc_shipment_id ,'N/A' ) as tc_shipment_id
,coalesce(lhws.tpe_created_load ,'N/A' ) as tpe_created_load
,coalesce(lhws.tpe_processing_status ,'N/A' ) as tpe_processing_status
,coalesce(lhws.misc_instruction_1 ,'N/A' ) as misc_instruction_1
,coalesce(lhws.created_dt ,'N/A' ) as created_dt
,coalesce(lhws.created_ts ,'N/A' ) as created_ts
,coalesce(lhws.last_modified_dt ,'9999-12-31' ) as last_modified_dt
,coalesce(lhws.last_modified_ts ,'00:00:00' ) as last_modified_ts
,coalesce(lhws.user_id ,'N/A' ) as user_id
,coalesce(thws.record_type ,'N/A' ) as record_type
,coalesce(thws.trip_nbr ,'N/A' ) as trip_nbr
,coalesce(thws.trip_type ,'N/A' ) as trip_type
,coalesce(thws.dispatch_dt ,'9999-12-31' ) as dispatch_dt
,coalesce(thws.dispatch_ts ,'00:00:00' ) as dispatch_ts
,coalesce(thws.carton_limit ,0 ) as carton_limit
,coalesce(thws.run_nbr ,'N/A' ) as run_nbr
,coalesce(thws.ship_via ,'N/A' ) as thtrip_ship_via
,coalesce(thws.status_flg ,'N/A' ) as thtrip_status_flg
,coalesce(thws.delay_code ,'N/A' ) as delay_code
,coalesce(thws.cartons_loaded ,0 ) as cartons_loaded
,coalesce(thws.seal_nbr ,'N/A' ) as seal_nbr
,coalesce(thws.date_billed ,0 ) as date_billed
,coalesce(thws.time_billed ,0 ) as time_billed
,coalesce(thws.seal_nbr_2 ,'N/A' ) as seal_nbr_2
,coalesce(thws.misc1_numeric_fld ,'N/A' ) as misc1_numeric_fld
,coalesce(thws.miscellaneous_20 ,'N/A' ) as miscellaneous_20
,coalesce(thws.created_dt ,'9999-12-31' ) as thtrip_created_dt
,coalesce(thws.created_ts ,'00:00:00' ) as thtrip_created_ts
,coalesce(thws.last_modified_dt ,'9999-12-31' ) as thtrip_last_modified_dt
,coalesce(thws.last_modified_ts ,'00:00:00' ) as thtrip_last_modified_ts
,coalesce(thws.user_id ,'N/A' ) as thtrip_user_id
,coalesce(from_unixtime(unix_timestamp(concat(lhws.created_dt,' ',lhws.created_ts), "yyyy-MM-dd HH:mm:ss")),'9999-12-31 00:00:00')  as load_create_dt_ts
,coalesce(from_unixtime(unix_timestamp(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts), "yyyy-MM-dd HH:mm:ss")),'9999-12-31 00:00:00') load_closed_dt_ts
,case when (cast(cast(cast(lhws.created_dt as timestamp )+ interval 2 days as timestamp )+ interval 4 days as timestamp )+ interval 2 days='9999-12-31' or lhws.load_closed_dt='9999-12-31') then 0 else day(CAST(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts)  as  timestamp) - CAST(concat(lhws.created_dt,' ',lhws.created_ts)  as  timestamp))*24*60*60 
+hour(CAST(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts)  as  timestamp) - CAST(concat(lhws.created_dt,' ',lhws.created_ts)  as  timestamp))*60*60 
+date_format(CAST(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts)  as  timestamp)-CAST(concat(lhws.created_dt,' ',lhws.created_ts)  as  timestamp),'m')*60 
+second(CAST(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts)  as  timestamp) - CAST(concat(lhws.created_dt,' ',lhws.created_ts)  as  timestamp)) end 
 AS load_create_to_closed_duration
 from  (select misc_20_byte_3,carton_nbr,load_nbr,row_number() over (partition by misc_20_byte_3 order by last_modified_dt desc, last_modified_ts desc) r 
from scct_work_db.chcart00_warehouse_shipping_work where 1 | 4 & 32767 <> 'N/A' and 1 | 4 & 32767 | 4 <>'N/A') chws
JOIN scct_work_db.lhload00_warehouse_shipping_work lhws ON(lhws.load_nbr = chws.load_nbr)
JOIN scct_work_db.thtrip00_warehouse_shipping_work thws ON (thws.load_nbr =  lhws.load_nbr)
WHERE chws.r=1 AND thws.Trip_type IN ('01','02') union all
select 
3  as process_id
,coalesce(chws.misc_20_byte_3 ,'N/A' ) as misc_20_byte_3
,coalesce(chws.carton_nbr ,'N/A' ) as carton_nbr
,coalesce(lhws.warehouse ,'N/A' ) as warehouse
,coalesce(regexp_replace(lhws.load_nbr ,"^0+(?!$)",""), 'N/A')  as load_nbr
,coalesce(lhws.destination_typ ,'N/A' ) as destination_typ
,coalesce(lhws.ship_via ,'N/A' ) as ship_via
,coalesce(lhws.master_bill_of_lading ,'N/A' ) as master_bill_of_lading
,coalesce(lhws.trailer_nbr ,'N/A' ) as trailer_nbr
,coalesce(lhws.tractor__nbr ,'N/A' ) as tractor__nbr
,coalesce(lhws.driver ,'N/A' ) as driver
,coalesce(lhws.dock ,'N/A' ) as dock
,coalesce(lhws.door ,'N/A' ) as door
,coalesce(lhws.dock_door_locn_brcd ,'N/A' ) as dock_door_locn_brcd
,coalesce(lhws.tms_plan_nbr ,0 ) as tms_plan_nbr
,coalesce(lhws.status_flg ,'N/A' ) as status_flg
,coalesce(lhws.status_dt ,'9999-12-31' ) as status_dt
,coalesce(lhws.load_closed_dt ,'9999-12-31' ) as load_closed_dt
,coalesce(lhws.load_closed_ts ,'00:00:00' ) as  load_closed_ts
,coalesce(lhws.tc_shipment_id ,'N/A' ) as tc_shipment_id
,coalesce(lhws.tpe_created_load ,'N/A' ) as tpe_created_load
,coalesce(lhws.tpe_processing_status ,'N/A' ) as tpe_processing_status
,coalesce(lhws.misc_instruction_1 ,'N/A' ) as misc_instruction_1
,coalesce(lhws.created_dt ,'N/A' ) as created_dt
,coalesce(lhws.created_ts ,'N/A' ) as created_ts
,coalesce(lhws.last_modified_dt ,'9999-12-31' ) as last_modified_dt
,coalesce(lhws.last_modified_ts ,'00:00:00' ) as last_modified_ts
,coalesce(lhws.user_id ,'N/A' ) as user_id
,coalesce(thws.record_type ,'N/A' ) as record_type
,coalesce(thws.trip_nbr ,'N/A' ) as trip_nbr
,coalesce(thws.trip_type ,'N/A' ) as trip_type
,coalesce(thws.dispatch_dt ,'9999-12-31' ) as dispatch_dt
,coalesce(thws.dispatch_ts ,'00:00:00' ) as dispatch_ts
,coalesce(thws.carton_limit ,0 ) as carton_limit
,coalesce(thws.run_nbr ,'N/A' ) as run_nbr
,coalesce(thws.ship_via ,'N/A' ) as thtrip_ship_via
,coalesce(thws.status_flg ,'N/A' ) as thtrip_status_flg
,coalesce(thws.delay_code ,'N/A' ) as delay_code
,coalesce(thws.cartons_loaded ,0 ) as cartons_loaded
,coalesce(thws.seal_nbr ,'N/A' ) as seal_nbr
,coalesce(thws.date_billed ,0 ) as date_billed
,coalesce(thws.time_billed ,0 ) as time_billed
,coalesce(thws.seal_nbr_2 ,'N/A' ) as seal_nbr_2
,coalesce(thws.misc1_numeric_fld ,'N/A' ) as misc1_numeric_fld
,coalesce(thws.miscellaneous_20 ,'N/A' ) as miscellaneous_20
,coalesce(thws.created_dt ,'9999-12-31' ) as thtrip_created_dt
,coalesce(thws.created_ts ,'00:00:00' ) as thtrip_created_ts
,coalesce(thws.last_modified_dt ,'9999-12-31' ) as thtrip_last_modified_dt
,coalesce(thws.last_modified_ts ,'00:00:00' ) as thtrip_last_modified_ts
,coalesce(thws.user_id ,'N/A' ) as thtrip_user_id
,coalesce(cast(cast(cast(cc_rec_start_date as timestamp )+ interval 2 days as timestamp )+ interval 4 days as timestamp )- interval 3 Hours,'9999-12-31 00:00:00')  as load_create_dt_ts
,coalesce(from_unixtime(unix_timestamp(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts), "yyyy-MM-dd HH:mm:ss")),'9999-12-31 00:00:00') load_closed_dt_ts
,case when (lhws.created_dt='9999-12-31' or lhws.load_closed_dt='9999-12-31') then 0 else day(CAST(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts)  as  timestamp) - CAST(concat(lhws.created_dt,' ',lhws.created_ts)  as  timestamp))*24*60*60 
+hour(CAST(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts)  as  timestamp) - CAST(concat(lhws.created_dt,' ',lhws.created_ts)  as  timestamp))*60*60 
+date_format(CAST(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts)  as  timestamp)-CAST(concat(lhws.created_dt,' ',lhws.created_ts)  as  timestamp),'m')*60 
+second(CAST(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts)  as  timestamp) - CAST(concat(lhws.created_dt,' ',lhws.created_ts)  as  timestamp)) end
 AS load_create_to_closed_duration
from scct_work_db.chcart00_warehouse_shipping_work chws
JOIN scct_work_db.lhload00_warehouse_shipping_work lhws ON (lhws.load_nbr = chws.load_nbr)
JOIN scct_work_db.thtrip00_warehouse_shipping_work thws ON (thws.load_nbr =  lhws.load_nbr)
JOIN scct_work_db.chcart00_retail_work chrtl on (chws.carton_nbr = chrtl.path_nbr  AND chws.load_nbr = concat('000',chrtl.load_nbr)) 
WHERE  chws.misc_20_byte_3 = 'N/A'AND chws.load_nbr <> 'N/A'AND thws.trip_type IN ('01','02') and cast(substr('3.45555',1,instr('3.45555','.')+3) as decimal(38,3)) = '85' AND chrtl.load_nbr <> 'N/A' union all
select 
4  as process_id
,coalesce(chws.misc_20_byte_3 ,'N/A' ) as misc_20_byte_3
,coalesce(chws.carton_nbr ,'N/A' ) as carton_nbr
,coalesce(lhws.warehouse ,'N/A' ) as warehouse
,coalesce(regexp_replace(lhws.load_nbr ,"^0+(?!$)",""), 'N/A')  as load_nbr
,coalesce(lhws.destination_typ ,'N/A' ) as destination_typ
,coalesce(lhws.ship_via ,'N/A' ) as ship_via
,coalesce(lhws.master_bill_of_lading ,'N/A' ) as master_bill_of_lading
,coalesce(lhws.trailer_nbr ,'N/A' ) as trailer_nbr
,coalesce(lhws.tractor__nbr ,'N/A' ) as tractor__nbr
,coalesce(lhws.driver ,'N/A' ) as driver
,coalesce(lhws.dock ,'N/A' ) as dock
,coalesce(lhws.door ,'N/A' ) as door
,coalesce(lhws.dock_door_locn_brcd ,'N/A' ) as dock_door_locn_brcd
,coalesce(lhws.tms_plan_nbr ,0 ) as tms_plan_nbr
,coalesce(lhws.status_flg ,'N/A' ) as status_flg
,coalesce(lhws.status_dt ,'9999-12-31' ) as status_dt
,coalesce(lhws.load_closed_dt ,'9999-12-31' ) as load_closed_dt
,coalesce(lhws.load_closed_ts ,'00:00:00' ) as  load_closed_ts
,coalesce(lhws.tc_shipment_id ,'N/A' ) as tc_shipment_id
,coalesce(lhws.tpe_created_load ,'N/A' ) as tpe_created_load
,coalesce(lhws.tpe_processing_status ,'N/A' ) as tpe_processing_status
,coalesce(lhws.misc_instruction_1 ,'N/A' ) as misc_instruction_1
,coalesce(lhws.created_dt ,'N/A' ) as created_dt
,coalesce(lhws.created_ts ,'N/A' ) as created_ts
,coalesce(lhws.last_modified_dt ,'9999-12-31' ) as last_modified_dt
,coalesce(lhws.last_modified_ts ,'00:00:00' ) as last_modified_ts
,coalesce(lhws.user_id ,'N/A' ) as user_id
,coalesce(thws.record_type ,'N/A' ) as record_type
,coalesce(thws.trip_nbr ,'N/A' ) as trip_nbr
,coalesce(thws.trip_type ,'N/A' ) as trip_type
,coalesce(thws.dispatch_dt ,'9999-12-31' ) as dispatch_dt
,coalesce(thws.dispatch_ts ,'00:00:00' ) as dispatch_ts
,coalesce(thws.carton_limit ,0 ) as carton_limit
,coalesce(thws.run_nbr ,'N/A' ) as run_nbr
,coalesce(thws.ship_via ,'N/A' ) as thtrip_ship_via
,coalesce(thws.status_flg ,'N/A' ) as thtrip_status_flg
,coalesce(thws.delay_code ,'N/A' ) as delay_code
,coalesce(thws.cartons_loaded ,0 ) as cartons_loaded
,coalesce(thws.seal_nbr ,'N/A' ) as seal_nbr
,coalesce(thws.date_billed ,0 ) as date_billed
,coalesce(thws.time_billed ,0 ) as time_billed
,coalesce(thws.seal_nbr_2 ,'N/A' ) as seal_nbr_2
,coalesce(thws.misc1_numeric_fld ,'N/A' ) as misc1_numeric_fld
,coalesce(thws.miscellaneous_20 ,'N/A' ) as miscellaneous_20
,coalesce(thws.created_dt ,'9999-12-31' ) as thtrip_created_dt
,coalesce(thws.created_ts ,'00:00:00' ) as thtrip_created_ts
,coalesce(thws.last_modified_dt ,'9999-12-31' ) as thtrip_last_modified_dt
,coalesce(thws.last_modified_ts ,'00:00:00' ) as thtrip_last_modified_ts
,coalesce(thws.user_id ,'N/A' ) as thtrip_user_id
,coalesce(from_unixtime(unix_timestamp(concat(lhws.created_dt,' ',lhws.created_ts), "yyyy-MM-dd HH:mm:ss")),'9999-12-31 00:00:00')  as load_create_dt_ts
,coalesce(from_unixtime(unix_timestamp(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts), "yyyy-MM-dd HH:mm:ss")),'9999-12-31 00:00:00') load_closed_dt_ts
,case when (cast(cast(cast(cc_rec_start_date as timestamp )+ interval 2 days as timestamp )+ interval 4 days as timestamp )+interval 3 weeks='9999-12-31' or lhws.load_closed_dt='9999-12-31') then 0 else day(CAST(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts)  as  timestamp) - CAST(concat(lhws.created_dt,' ',lhws.created_ts)  as  timestamp))*24*60*60 
+hour(CAST(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts)  as  timestamp) - CAST(concat(lhws.created_dt,' ',lhws.created_ts)  as  timestamp))*60*60 
+date_format(CAST(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts)  as  timestamp)-CAST(concat(lhws.created_dt,' ',lhws.created_ts)  as  timestamp),'m')*60 
+second(CAST(concat(lhws.load_closed_dt,' ',lhws.load_closed_ts)  as  timestamp) - CAST(concat(lhws.created_dt,' ',lhws.created_ts)  as  timestamp)) end
 AS load_create_to_closed_duration
FROM scct_work_db.chcart00_warehouse_shipping_work chws
JOIN scct_work_db.lhload00_warehouse_shipping_work lhws ON (lhws.load_nbr  = chws.load_nbr) 
JOIN scct_work_db.thtrip00_warehouse_shipping_work thws ON (thws.load_nbr  =  lhws.load_nbr) 
JOIN scct_work_db.chcart00_retail_work chrtl ON  (chws.carton_nbr =  chrtl.path_nbr  AND  chws.load_nbr  <> concat('000',chrtl.load_nbr)) 
WHERE chws.misc_20_byte_3 = 'N/A' AND chws.load_nbr <> 'N/A' AND thws.trip_type IN ('01','02') AND  chrtl.status_flg = '85' AND chrtl.load_nbr <> 'N/A'
and chws.created_dt > chrtl.created_dt AND 
day(chws.created_dt-chrtl.created_dt)BETWEEN 0 AND 14;

