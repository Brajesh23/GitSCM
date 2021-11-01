---jobname:scct-rockport-hive-stg1-scct_db.rockport_cfs_shipment
--------------------------------------------------------------------------
---Target: Stage1 table: scct_db.rockport_cfs_shipment
---Source: rockport_stage0 table : trader_delivery_hdr, trader_delivery_dtl, trader_shipping_dtl, trader_shipping_hdr
---Load Type: Full
---Back posting: No
---Author: ssoma
---Created Date: 03/05/2019
---Modified By: sdhal
---Modified Date: 17/06/2019
--------------------------------------------------------------------------

-------------------------
---loading stage1 table scct_db.rockport_cfs_shipment 
-------------------------

insert overwrite table scct_work_db.trader_shipping_dtl_work
select
 owner
,bill_of_lading_no
,sum(cast(trim(qty) as int)) qty
,qty_um
,order_no
,equip_id
,seal_no
,contract_no
,last_update
,p_link_key_val
,item
,ref_no
from 
rockport_stage0.trader_shipping_dtl
group by 
 owner
,bill_of_lading_no
,qty_um
,order_no
,equip_id
,seal_no
,contract_no
,last_update
,p_link_key_val
,item
,ref_no;



SET tez.job.name=stage1:rockport_cfs_shipment_work:load work table rockport_cfs_shipment_work;

! echo Started Loading into scct_work_db.rockport_cfs_shipment_work;

insert overwrite table scct_work_db.rockport_cfs_shipment_work
select
coalesce(case when trim(dh.owner)='' then null else trim(dh.owner) end, 'N/A') as owner_id
,coalesce(substr(trim(dh.owner),4), 'N/A') as cfs_id
,coalesce(case when trim(sh.bill_of_lading_no)='' then null else trim(sh.bill_of_lading_no) end, 'N/A') as cfs_manifest_nbr
,coalesce(case when trim(dd1.proof_of_del_no)='' then null else trim(dd1.proof_of_del_no) end, 'N/A') as mawb_nbr
,coalesce(case when trim(dd2.proof_of_del_no)='' then null else trim(dd2.proof_of_del_no) end, 'N/A') as hawb_nbr
,coalesce(case when trim(dh.status)='' then null else trim(dh.status) end, 'N/A') as sample_pulled_flg
,coalesce(to_utc_timestamp(from_unixtime(sh.import_date div 1000, 'yyyy-MM-dd HH:mm:ss'),"EST5EDT"),'9999-12-31 00:00:00') as cfs_ship_dtm
,coalesce(to_utc_timestamp(from_unixtime(sh.last_update div 1000, 'yyyy-MM-dd HH:mm:ss'),"EST5EDT"),'9999-12-31 00:00:00') as last_update_dtm
,coalesce(sd.contract_no,sd.order_no,'N/A') as po_nbr
,coalesce(case when trim(sd.contract_no)='' then null else trim(sd.contract_no) end, 'N/A') as contract_nbr
,coalesce(case when trim(sd.order_no)='' then null else trim(sd.order_no) end, 'N/A') as order_nbr
,coalesce(case when (sd.qty)='' then null else (sd.qty) end, 0) as shipped_qty
,coalesce(case when trim(sd.qty_um)='' then null else trim(sd.qty_um) end, 'N/A') as shipped_qty_uom
,coalesce(case when trim(sh.carrier)='' then null else trim(sh.carrier) end, 'N/A') as carrier
,coalesce(case when trim(sd.equip_id)='' then null else trim(sd.equip_id) end, 'N/A') as trailer_nbr
,coalesce(case when trim(sd.seal_no)='' then null else trim(sd.seal_no) end, 'N/A') as seal_nbr
,coalesce(case when trim(sd.ITM_NBR)='' then null else trim(sd.ITM_NBR) end, 'N/A') as ITM_NBR
,coalesce(case when trim(sd.REF_NBR)='' then null else trim(sd.REF_NBR) end, 'N/A') as REF_NBR
,coalesce(to_utc_timestamp(from_unixtime(sh.last_update div 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"),'9999-12-31 00:00:00') as hdr_last_upd_dtm
,coalesce(to_utc_timestamp(from_unixtime(sd.last_update div 1000, 'yyyy-MM-dd HH:mm:ss'), "EST5EDT"),'9999-12-31 00:00:00') as dtl_last_upd_dtm
from rockport_stage0.trader_delivery_hdr dh
join rockport_stage0.trader_delivery_dtl dd1
on dh.owner = dd1.owner and dh.link_tbl_name = dd1.link_tbl_name and dh.link_key_val = dd1.link_key_val and dd1.delivery_no = 'MWB'
join rockport_stage0.trader_delivery_dtl dd2
on dh.owner = dd2.owner and dh.link_tbl_name = dd2.link_tbl_name and dh.link_key_val = dd2.link_key_val and dd2.delivery_no = 'HWB'
join SCCT_WORK_DB.trader_shipping_dtl_work sd
on sd.owner = dh.owner and dh.link_tbl_name = concat('SHIPPING - ',sd.bill_of_lading_no) and dh.link_key_val = sd.p_link_key_val
join rockport_stage0.trader_shipping_hdr sh
on sh.owner = sd.owner and sh.bill_of_lading_no = sd.bill_of_lading_no
left outer join analytics_view.item i
on trim(itm_nbr) = trim(SUBSTRING(i.item_number,-8))
where dh.owner like 'CFS%' and dh.last_user = 'EVENT9';

! echo Completed Loading into scct_work_db.rockport_cfs_shipment_work;

SET tez.job.name=stage1:rockport_cfs_shipment:load work table rockport_cfs_shipment;

! echo Started Loading into scct_db.rockport_cfs_shipment;

insert overwrite table scct_db.rockport_cfs_shipment
select
coalesce(rsw.owner_id, rs.owner_id)
,coalesce(rsw.cfs_id, rs.cfs_id)
,coalesce(rsw.cfs_manifest_nbr, rs.cfs_manifest_nbr)
,coalesce(rsw.mawb_nbr, rs.mawb_nbr)
,coalesce(rsw.hawb_nbr, rs.hawb_nbr)
,coalesce(rsw.sample_pulled_flg, rs.sample_pulled_flg)
,coalesce(rsw.cfs_ship_dtm, rs.cfs_ship_dtm)
,coalesce(rsw.last_update_dtm, rs.last_update_dtm)
,coalesce(rsw.po_nbr, rs.po_nbr)
,coalesce(rsw.contract_nbr, rs.contract_nbr)
,coalesce(rsw.order_nbr, rs.order_nbr)
,coalesce(rsw.shipped_qty, rs.shipped_qty)
,coalesce(rsw.shipped_qty_uom, rs.shipped_qty_uom)
,coalesce(rsw.carrier, rs.carrier)
,coalesce(rsw.trailer_nbr, rs.trailer_nbr)
,coalesce(rsw.seal_nbr, rs.seal_nbr)
,coalesce(rsw.itm_nbr, rs.itm_nbr)
,coalesce(rsw.ref_nbr, rs.ref_nbr)
,coalesce(rsw.hdr_last_upd_dtm, rs.hdr_last_upd_dtm)
,coalesce(rsw.dtl_last_upd_dtm, rs.dtl_last_upd_dtm)
from scct_work_db.rockport_cfs_shipment_work rsw full join
scct_db.rockport_cfs_shipment rs 
on rsw.cfs_manifest_nbr=rs.cfs_manifest_nbr 
and rsw.cfs_id=rs.cfs_id
and rsw.mawb_nbr=rs.mawb_nbr
and rsw.hawb_nbr=rs.hawb_nbr
and rsw.contract_nbr=rs.contract_nbr
and rsw.order_nbr=rs.order_nbr
and rsw.trailer_nbr=rs.trailer_nbr
and rsw.itm_nbr=rs.itm_nbr
and rsw.ref_nbr=rs.ref_nbr;

! echo Completed Loading into scct_db.rockport_cfs_shipment;

 -------------------------
 ---end script
 -------------------------