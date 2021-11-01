---jobname:scct-rockport-hive-stg1-scct_db.rockport_air_shipment
--------------------------------------------------------------------------
---Target: Stage1 table: scct_db.rockport_air_shipment
---Source: rockport_stage0 table : trader_shipping_hdr, trader_shipping_dtl, trader_party, trader_plci3225
--- trader_packing, trader_delivery_dtl and trader_matrix_hdr;
---Load Type: Full
---Back posting: No
---Author: ssoma
---Created Date: 02/05/2019
--------------------------------------------------------------------------

-------------------------
---loading work table scct_db.trader_plci3225_work 
-------------------------


! echo Started Loading into scct_work_db.trader_plci3225_work;

insert overwrite table scct_work_db.trader_plci3225_work 
select
owner
,place_code
,place_name from (
select
owner
,place_code
,place_name
,row_number ()
OVER (PARTITION BY TRIM(owner), TRIM(place_code) order BY from_unixtime(last_update div 1000, 'yyyy-MM-dd HH:mm:ss') desc ) row_num
from rockport_stage0.trader_plci3225 ) a where a.row_num=1;

! echo Completed Loading into scct_work_db.trader_plci3225_work;

-------------------------
---loading stage1 table scct_db.rockport_air_shipment 
-------------------------

! echo Started Loading into scct_work_db.rockport_air_shipment_work;

insert overwrite table scct_work_db.rockport_air_shipment_work
select
coalesce(case when trim(sh.bill_of_lading_no)='' then null else trim(sh.bill_of_lading_no) end, 'N/A') as mawb_nbr
,coalesce(case when trim(sd.equip_id)='' then null else trim(sd.equip_id) end, 'N/A') as hawb_nbr
,coalesce(case when trim(sd.ref_no)='' then null else trim(sd.ref_no) end, 'N/A') as ref_nbr
,coalesce(case when trim(sd.item)='' then null else trim(sd.item) end, 'N/A') as itm_nbr
,coalesce(case when trim(sh.transport_mode)='' then null else trim(sh.transport_mode) end, 'N/A') as shipping_method
,coalesce(case when trim(sh.id_means_of_transp)='' then null else trim(sh.id_means_of_transp) end, 'N/A') as flight_nbr
,coalesce(case when trim(sh.owner)='' then null else trim(sh.owner) end, 'N/A') as forwarder_id
,coalesce(case when trim(pa1.party_name_line1)='' then null else trim(pa1.party_name_line1) end, 'N/A') as forwarder_name
,coalesce(case when trim(sh.carrier)='' then null else trim(sh.carrier) end, 'N/A') as carrier_cd
,coalesce(case when trim(sh.load_point)='' then null else trim(sh.load_point) end, 'N/A') as port_of_lading_id
,coalesce(case when trim(pc1.place_name)='' then null else trim(pc1.place_name) end, 'N/A') as port_of_lading_name
,coalesce(case when trim(sh.state_province)='' then null else trim(sh.state_province) end, 'N/A') as port_of_discharge_id
,coalesce(case when trim(pc2.place_name)='' then null else trim(pc2.place_name) end, 'N/A') as port_of_discharge_name
,coalesce(case when trim(sh.final_dest)='' then null else trim(sh.final_dest) end, 'N/A') as destination_id
,coalesce(case when trim(pc3.place_name)='' then null else trim(pc3.place_name) end, 'N/A') as destination_name
,coalesce(case when trim(sd.measurement_val)='' then null else trim(sd.measurement_val) end, 0.00) as dim_wgt 
,coalesce(case when trim(sd.equip_meas_um)='' then null else trim(sd.equip_meas_um) end, 'N/A') as dim_wgt_uom
,coalesce(case when trim(sd.equip_weight_val)='' then null else trim(sd.equip_weight_val) end, 0.00) as gross_wgt
,coalesce(case when trim(sd.equip_weight_um)='' then null else trim(sd.equip_weight_um) end, 'N/A') as gross_wgt_uom
,coalesce(case when trim(p1.qty_per_pack)='' then null else trim(p1.qty_per_pack) end, 0.00) as total_cartons
,coalesce(to_utc_timestamp(from_unixtime(sh.export_date div 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as actual_depart_dtm
,coalesce(to_utc_timestamp(from_unixtime(sh.last_update div 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as transmission_dtm
,coalesce(to_utc_timestamp(from_unixtime(sh.act_arrive_date  div 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as eta_dtm
,coalesce(to_utc_timestamp(from_unixtime(dd1.recd_date div 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as cargo_receipt_dtm
,coalesce(to_utc_timestamp(from_unixtime(dd2.last_delivery_date div 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as docs_available_dtm
,coalesce(case when trim(sd.status)='' then null else trim(sd.status) end, 'N/A') as brand
,coalesce(case when trim(sd.deliver_to)='' then null else trim(sd.deliver_to) end, 'N/A') as deliver_to
,coalesce(coalesce(sd.contract_no,sd.order_no), 'N/A') as po_nbr
,coalesce(case when trim(sd.contract_no)='' then null else trim(sd.contract_no) end, 'N/A') as contract_nbr
,coalesce(case when trim(sd.order_no)='' then null else trim(sd.order_no) end, 'N/A') as order_nbr
,coalesce(case when trim(mh.matrix_total)='' then null else trim(mh.matrix_total) end, 0.00) as po_unit 
,coalesce(case when trim(p2.pack_id_qty)='' then null else trim(p2.pack_id_qty) end, 0.00) as po_cartons_qty 
,coalesce(case when trim(p2.total_wgt_pack_id)='' then null else trim(p2.total_wgt_pack_id) end, 0.00) as po_gross_wgt 
,coalesce(case when trim(p2.total_wgt_um)='' then null else trim(p2.total_wgt_um)end, 'N/A') as po_gross_wgt_uom 
,coalesce(case when trim(p2.line_wgt)='' then null else trim(p2.line_wgt) end, 0.00) as po_dim_wgt 
,coalesce(case when trim(p2.line_wgt_um)='' then null else trim(p2.line_wgt_um) end, 'N/A') as po_dim_wgt_uom 
,coalesce(case when trim(p2.net_wgt_unit)='' then null else trim(p2.net_wgt_unit) end, 0.00) as po_chargeable_wgt 
,coalesce(case when trim(p2.net_wgt_unit_um)='' then null else trim(p2.net_wgt_unit_um)end, 'N/A') as po_chargeable_wgt_uom 
,coalesce(case when trim(p2.pkg_type)='' then null else trim(p2.pkg_type) end, 'N/A') as garment_pack
,coalesce(case when trim(p3.sample_carton)='' then null else trim(p3.sample_carton) end, 'N/A') as pre_tkt
,coalesce(to_utc_timestamp(from_unixtime(sh.last_update div 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as header_last_upd_dtm
,coalesce(to_utc_timestamp(from_unixtime(sd.last_update div 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"), '9999-12-31 00:00:00') as detail_last_upd_dtm
from rockport_stage0.trader_shipping_hdr sh
left join rockport_stage0.trader_shipping_dtl sd
on sh.owner=sd.owner and sh.bill_of_lading_no=sd.bill_of_lading_no
left join rockport_stage0.trader_party pa1
on pa1.party_id=sh.owner
left join scct_work_db.trader_plci3225_work pc1
on pc1.place_code=sh.load_point
left join scct_work_db.trader_plci3225_work pc2
on pc2.place_code=sh.state_province
left join scct_work_db.trader_plci3225_work pc3
on pc3.place_code=sh.final_dest
left join rockport_stage0.trader_packing p1
on p1.owner=sh.owner and p1.link_key_val=concat(sh.bill_of_lading_no,' + ',sd.equip_id) and p1.pack_id=sd.equip_id
left join rockport_stage0.trader_packing p2
on p2.owner=sh.owner and p2.link_key_val=concat(sh.bill_of_lading_no,' + ',sd.equip_id) and p2.pack_id=coalesce(sd.contract_no,sd.order_no)
left join rockport_stage0.trader_packing p3
on p3.owner=sh.owner and p3.link_key_val=concat(sh.bill_of_lading_no,' + ',sd.equip_id,' + ',sd.item,' + ',sd.ref_no,' + ',sd.item,' + ',sd.item) and p3.pack_id=sd.item
left join rockport_stage0.trader_delivery_dtl dd1
on dd1.owner=sh.owner and dd1.link_key_val=concat(sh.bill_of_lading_no,' + ',sd.equip_id) and dd1.delivery_id=sd.equip_id and dd1.delivery_no='E1'
left join rockport_stage0.trader_delivery_dtl dd2
on dd2.owner=sh.owner and dd2.link_key_val=concat(sh.bill_of_lading_no,' + ',sd.equip_id,' + ',sd.item,' + ',sd.ref_no)
left join rockport_stage0.trader_matrix_hdr mh
on mh.owner=sd.owner and mh.link_key_val=concat(sd.bill_of_lading_no,' + ',sd.equip_id,' + ',sd.item,' + ',sd.ref_no,' + ',sd.item,' + ',sd.item,' + ',sd.item)
where sh.transport_mode='A' and sh.owner not like 'CFS%';

! echo Completed Loading into scct_work_db.rockport_air_shipment_work;

! echo Started Loading into scct_work_db.rockport_air_shipment_work1;

insert overwrite table scct_work_db.rockport_air_shipment_work1
select 
rasw1.mawb_nbr
,rasw1.hawb_nbr
,rasw1.ref_nbr
,coalesce(i.item_number,rasw1.itm_nbr)
,rasw1.shipping_method
,rasw1.flight_nbr
,rasw1.forwarder_id
,rasw1.forwarder_name
,rasw1.carrier_cd
,rasw1.port_of_lading_id
,rasw1.port_of_lading_name
,rasw1.port_of_discharge_id
,rasw1.port_of_discharge_name
,rasw1.destination_id
,rasw1.destination_name
,rasw1.dim_wgt
,rasw1.dim_wgt_uom
,rasw1.gross_wgt
,rasw1.gross_wgt_uom
,rasw1.total_cartons
,rasw1.actual_depart_dtm
,rasw1.transmission_dtm
,rasw1.eta_dtm
,rasw1.cargo_receipt_dtm
,rasw1.docs_available_dtm
,rasw1.brand
,rasw1.deliver_to
,rasw1.po_nbr
,rasw1.contract_nbr
,rasw1.order_nbr
,rasw1.po_unit
,rasw1.po_cartons_qty
,rasw1.po_gross_wgt
,rasw1.po_gross_wgt_uom
,rasw1.po_dim_wgt
,rasw1.po_dim_wgt_uom
,rasw1.po_chargeable_wgt
,rasw1.po_chargeable_wgt_uom
,rasw1.garment_pack
,rasw1.pre_tkt
,rasw1.header_last_upd_dtm
,rasw1.detail_last_upd_dtm from 
(select 
rasw.mawb_nbr
,rasw.hawb_nbr
,rasw.ref_nbr
,rasw.itm_nbr
,rasw.shipping_method
,rasw.flight_nbr
,rasw.forwarder_id
,rasw.forwarder_name
,rasw.carrier_cd
,rasw.port_of_lading_id
,rasw.port_of_lading_name
,rasw.port_of_discharge_id
,rasw.port_of_discharge_name
,rasw.destination_id
,rasw.destination_name
,rasw.dim_wgt
,rasw.dim_wgt_uom
,rasw.gross_wgt
,rasw.gross_wgt_uom
,rasw.total_cartons
,rasw.actual_depart_dtm
,rasw.transmission_dtm
,rasw.eta_dtm
,rasw.cargo_receipt_dtm
,rasw.docs_available_dtm
,rasw.brand
,rasw.deliver_to
,rasw.po_nbr
,rasw.contract_nbr
,rasw.order_nbr
,rasw.po_unit
,rasw.po_cartons_qty
,rasw.po_gross_wgt
,rasw.po_gross_wgt_uom
,rasw.po_dim_wgt
,rasw.po_dim_wgt_uom
,rasw.po_chargeable_wgt
,rasw.po_chargeable_wgt_uom
,rasw.garment_pack
,rasw.pre_tkt
,rasw.header_last_upd_dtm
,rasw.detail_last_upd_dtm from 
(select
mawb_nbr
,hawb_nbr
,ref_nbr
,itm_nbr
,shipping_method
,flight_nbr
,forwarder_id
,forwarder_name
,carrier_cd
,port_of_lading_id
,port_of_lading_name
,port_of_discharge_id
,port_of_discharge_name
,destination_id
,destination_name
,dim_wgt
,dim_wgt_uom
,gross_wgt
,gross_wgt_uom
,total_cartons
,actual_depart_dtm
,transmission_dtm
,eta_dtm
,cargo_receipt_dtm
,docs_available_dtm
,brand
,deliver_to
,po_nbr
,contract_nbr
,order_nbr
,po_unit
,po_cartons_qty
,po_gross_wgt
,po_gross_wgt_uom
,po_dim_wgt
,po_dim_wgt_uom
,po_chargeable_wgt
,po_chargeable_wgt_uom
,garment_pack
,pre_tkt
,header_last_upd_dtm
,detail_last_upd_dtm
,row_number() over(partition by mawb_nbr,hawb_nbr,forwarder_id,contract_nbr,order_nbr,ref_nbr,itm_nbr order by detail_last_upd_dtm desc ) row_num
from scct_work_db.rockport_air_shipment_work ) rasw where rasw.row_num=1)rasw1
left outer join
analytics_view.item i
on trim(rasw1.itm_nbr) = trim(SUBSTRING(i.item_number,-8)); 

! echo Completed Loading into scct_work_db.rockport_air_shipment_work1;

SET tez.job.name=stage1:rockport_air_shipment:load work table rockport_air_shipment;

! echo Started Loading into scct_db.rockport_air_shipment;

insert overwrite table scct_db.rockport_air_shipment
select
coalesce(rasw.mawb_nbr, ras.mawb_nbr)
,coalesce(rasw.hawb_nbr, ras.hawb_nbr)
,coalesce(rasw.ref_nbr, ras.ref_nbr)
,coalesce(rasw.itm_nbr, ras.itm_nbr)
,coalesce(rasw.shipping_method, ras.shipping_method)
,coalesce(rasw.flight_nbr, ras.flight_nbr)
,coalesce(rasw.forwarder_id, ras.forwarder_id)
,coalesce(rasw.forwarder_name, ras.forwarder_name)
,coalesce(rasw.carrier_cd, ras.carrier_cd)
,coalesce(rasw.port_of_lading_id, ras.port_of_lading_id)
,coalesce(rasw.port_of_lading_name, ras.port_of_lading_name)
,coalesce(rasw.port_of_discharge_id, ras.port_of_discharge_id)
,coalesce(rasw.port_of_discharge_name, ras.port_of_discharge_name)
,coalesce(rasw.destination_id, ras.destination_id)
,coalesce(rasw.destination_name, ras.destination_name)
,coalesce(rasw.dim_wgt, ras.dim_wgt)
,coalesce(rasw.dim_wgt_uom, ras.dim_wgt_uom)
,coalesce(rasw.gross_wgt, ras.gross_wgt)
,coalesce(rasw.gross_wgt_uom, ras.gross_wgt_uom)
,coalesce(rasw.total_cartons, ras.total_cartons)
,coalesce(rasw.actual_depart_dtm, ras.actual_depart_dtm)
,coalesce(rasw.transmission_dtm, ras.transmission_dtm)
,coalesce(rasw.eta_dtm, ras.eta_dtm)
,coalesce(rasw.cargo_receipt_dtm, ras.cargo_receipt_dtm)
,coalesce(rasw.docs_available_dtm, ras.docs_available_dtm)
,coalesce(rasw.brand, ras.brand)
,coalesce(rasw.deliver_to, ras.deliver_to)
,coalesce(rasw.po_nbr, ras.po_nbr)
,coalesce(rasw.contract_nbr, ras.contract_nbr)
,coalesce(rasw.order_nbr, ras.order_nbr)
,coalesce(rasw.po_unit, ras.po_unit)
,coalesce(rasw.po_cartons_qty, ras.po_cartons_qty)
,coalesce(rasw.po_gross_wgt, ras.po_gross_wgt)
,coalesce(rasw.po_gross_wgt_uom, ras.po_gross_wgt_uom)
,coalesce(rasw.po_dim_wgt, ras.po_dim_wgt)
,coalesce(rasw.po_dim_wgt_uom, ras.po_dim_wgt_uom)
,coalesce(rasw.po_chargeable_wgt, ras.po_chargeable_wgt)
,coalesce(rasw.po_chargeable_wgt_uom, ras.po_chargeable_wgt_uom)
,coalesce(rasw.garment_pack, ras.garment_pack)
,coalesce(rasw.pre_tkt, ras.pre_tkt)
,coalesce(rasw.header_last_upd_dtm, ras.header_last_upd_dtm)
,coalesce(rasw.detail_last_upd_dtm, ras.detail_last_upd_dtm)
from scct_work_db.rockport_air_shipment_work1 rasw full join
scct_db.rockport_air_shipment ras 
on rasw.mawb_nbr=ras.mawb_nbr 
and rasw.forwarder_id=ras.forwarder_id
and rasw.hawb_nbr=ras.hawb_nbr
and rasw.contract_nbr=ras.contract_nbr
and rasw.order_nbr=ras.order_nbr
and rasw.ref_nbr=ras.ref_nbr
and rasw.itm_nbr=ras.itm_nbr;

! echo Completed Loading into scct_db.rockport_air_shipment;

 -------------------------
 ---end script
 -------------------------