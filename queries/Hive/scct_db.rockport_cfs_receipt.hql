---jobname:scct-rockport-hive-stg1-scct_db.rockport_cfs_receipt
--------------------------------------------------------------------------
---Target: Stage1 table: scct_db.rockport_cfs_receipt
---Source: rockport_stage0 table : trader_shipping_hdr, trader_shipping_dtl, trader_delivery_dtl, trader_packing
---Load Type: Full
---Back posting: No
---Author	-	Created/Updated Date	-	Jira Ticket
---ssoma	-	03/05/2019				- 
---ssoma	-	14/06/2019				- SCCTP4-2004
--------------------------------------------------------------------------

---loading stage1 table scct_db.rockport_cfs_receipt 
-------------------------

! echo Started Loading into scct_work_db.rockport_cfs_receipt_work;

insert overwrite table scct_work_db.rockport_cfs_receipt_work
select
coalesce(case when trim(sh.owner)='' then null else trim(sh.owner) end, 'N/A') as owner_id
,coalesce(substr(sh.owner,4), 'N/A') as cfs_id
,coalesce(case when trim(sh.transport_mode)='' then null else trim(sh.transport_mode) end, 'N/A') as shipping_method
,coalesce(case when trim(dd.delivery_no)='' then null else trim(dd.delivery_no) end, 'N/A') as delivery_nbr
,coalesce(case when trim(sh.bill_of_lading_no)='' then null else trim(sh.bill_of_lading_no) end, 'N/A') as mawb_obol_nbr
,coalesce(case when trim(sd.equip_id)='' then null else trim(sd.equip_id) end, 'N/A') as hawb_container
,coalesce(case when trim(sd.ref_no)='' then null else trim(sd.ref_no) end, 'N/A') as fcr_nbr
,coalesce(case when trim(sd.status)='' then null else trim(sd.status) end, 'N/A') as brand
,coalesce(sd.contract_no,sd.order_no, 'N/A') as po_nbr
,coalesce(case when trim(sd.contract_no)='' then null else trim(sd.contract_no) end, 'N/A') as contract_nbr
,coalesce(case when trim(sd.order_no)='' then null else trim(sd.order_no) end, 'N/A') as order_nbr
,coalesce(case when trim(sd.qty)='' then null else trim(sd.qty) end, 0.00) as expected_qty
,coalesce(case when trim(sd.qty_um)='' then null else trim(sd.qty_um) end, 'N/A') as expected_qty_uom
,coalesce(dd.qty_recd, 0.00) as received_qty
,coalesce(case when trim(dd.qty_recd_um)='' then null else trim(dd.qty_recd_um) end, 'N/A') as received_qty_uom
,coalesce(p.gross_wgt_ctn, 0.00) as weight
,coalesce(case when trim(p.gross_wgt_ctn_um)='' then null else trim(p.gross_wgt_ctn_um) end, 'N/A') as weight_uom
,coalesce(to_utc_timestamp(dd.begin_date, "EST5EDT"), '9999-12-31 00:00:00') as actual_arrive_dtm
,coalesce(to_utc_timestamp(dd.recd_date, "EST5EDT"), '9999-12-31 00:00:00') as cfs_receipt_dtm
,coalesce(to_utc_timestamp(sd.last_update, "EST5EDT"), '9999-12-31 00:00:00') as last_update_dtm
,coalesce(sd.movement_type, 'N/A') as hold_flg
,coalesce(dd.transport_mode, 'N/A') as warehouse_location_nbr
,coalesce(to_utc_timestamp(sh.last_update, "EST5EDT"), '9999-12-31 00:00:00') as hdr_last_upd_dtm
,coalesce(to_utc_timestamp(sd.last_update , "EST5EDT"), '9999-12-31 00:00:00') as dtl_last_upd_dtm
from rockport_stage0.trader_shipping_hdr sh
left join rockport_stage0.trader_shipping_dtl sd
on sh.owner = sd.owner and sh.bill_of_lading_no = sd.bill_of_lading_no
left join (select owner,link_tbl_name,link_key_val,p_link_key_val,delivery_no,begin_date,recd_date,qty_recd_um,transport_mode,sum(cast(qty_recd as int)) qty_recd
FROM rockport_stage0.trader_delivery_dtl
where p_link_key_val is not null
group by owner,link_tbl_name,link_key_val,p_link_key_val,delivery_no,begin_date,recd_date,qty_recd_um,transport_mode) dd
on dd.owner = sd.owner and dd.link_tbl_name = concat('SHIPPING - ',sd.bill_of_lading_no) and dd.link_key_val = sd.p_link_key_val
left join (select owner,link_tbl_name,link_key_val,gross_wgt_ctn_um,sum(cast(gross_wgt_ctn as int))  gross_wgt_ctn
FROM rockport_stage0.trader_packing
group by owner,link_tbl_name,link_key_val,gross_wgt_ctn_um) p
on p.owner = dd.owner and p.link_tbl_name = dd.link_tbl_name and p.link_key_val = dd.p_link_key_val
where sh.id_means_of_transp = 'RECEIPT' and sh.owner like 'CFS%';


! echo Completed Loading into scct_work_db.rockport_cfs_receipt_work;

SET tez.job.name=stage1:rockport_cfs_receipt:load work table rockport_cfs_receipt;

! echo Started Loading into scct_db.rockport_cfs_receipt;

insert overwrite table scct_db.rockport_cfs_receipt
select
coalesce(rrw.owner_id, rr.owner_id)
,coalesce(rrw.cfs_id, rr.cfs_id)
,coalesce(rrw.shipping_method, rr.shipping_method)
,coalesce(rrw.delivery_nbr, rr.delivery_nbr)
,coalesce(rrw.mawb_obol_nbr, rr.mawb_obol_nbr)
,coalesce(rrw.hawb_container, rr.hawb_container)
,coalesce(rrw.fcr_nbr, rr.fcr_nbr)
,coalesce(rrw.brand, rr.brand)
,coalesce(rrw.po_nbr, rr.po_nbr)
,coalesce(rrw.contract_nbr, rr.contract_nbr)
,coalesce(rrw.order_nbr, rr.order_nbr)
,coalesce(rrw.expected_qty, rr.expected_qty)
,coalesce(rrw.expected_qty_uom, rr.expected_qty_uom)
,coalesce(rrw.received_qty, rr.received_qty)
,coalesce(rrw.received_qty_uom, rr.received_qty_uom)
,coalesce(rrw.weight, rr.weight)
,coalesce(rrw.weight_uom, rr.weight_uom)
,coalesce(rrw.actual_arrive_dtm, rr.actual_arrive_dtm)
,coalesce(rrw.cfs_receipt_dtm, rr.cfs_receipt_dtm)
,coalesce(rrw.last_update_dtm, rr.last_update_dtm)
,coalesce(rrw.hold_flg, rr.hold_flg)
,coalesce(rrw.warehouse_location_nbr, rr.warehouse_location_nbr)
,coalesce(rrw.hdr_last_upd_dtm, rr.hdr_last_upd_dtm)
,coalesce(rrw.dtl_last_upd_dtm, rr.dtl_last_upd_dtm)
from scct_work_db.rockport_cfs_receipt_work rrw full join
scct_db.rockport_cfs_receipt rr 
on rrw.owner_id=rr.owner_id
and rrw.delivery_nbr=rr.delivery_nbr
and rrw.mawb_obol_nbr=rr.mawb_obol_nbr 
and rrw.hawb_container=rr.hawb_container
and rrw.fcr_nbr=rr.fcr_nbr
and rrw.contract_nbr=rr.contract_nbr
and rrw.order_nbr=rr.order_nbr;

! echo Completed Loading into scct_db.rockport_cfs_receipt;

 -------------------------
 ---end script
 -------------------------