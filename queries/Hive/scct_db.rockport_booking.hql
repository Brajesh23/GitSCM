---jobname:scct-rockport-hive-stg1-scct_db.rockport_booking
--------------------------------------------------------------------------
---Target: Stage1 table: scct_db.rockport_booking
---Source: rockport_stage0 table : rockmstr_booking
---Load Type: Full
---Back posting: No
---Author	-	Created/Updated Date	-	Jira Ticket
---ssoma	-	03/05/2019				- 
---ssoma	-	14/06/2019				- SCCTP4-2010/09/11
--------------------------------------------------------------------------

-------------------------
---loading stage1 table scct_db.rockport_booking 
-------------------------

! echo Started Loading into scct_work_db.rockport_booking_work;

insert overwrite table scct_work_db.rockport_booking_work
select
coalesce(case when trim(seq_no)='' then null else trim(seq_no) end , 0) as seq_nbr
,coalesce(case when trim(splitid)='' then null else trim(splitid) end , 'N/A') as split_id
,coalesce(case when trim(booking_no)='' then null else trim(booking_no) end , 'N/A') as booking_nbr
,coalesce(to_date(to_utc_timestamp(booking_date, "EST5EDT")), '9999-12-31') as booking_dt
,coalesce(to_date(to_utc_timestamp(book_notice_date, "EST5EDT")), '9999-12-31') as book_notice_dt
,coalesce(case when trim(div_code)='' then null else trim(div_code) end , 'N/A') as div_cd
,coalesce(case when trim(order_no)='' then null else trim(order_no) end , 'N/A') as po_nbr
,coalesce(case when trim(ff_code)='' then null else trim(ff_code) end , 'N/A') as ff_cd
,coalesce(case when trim(dock_receipt)='' then null else trim(dock_receipt) end , 'N/A') as dock_receipt
,coalesce(case when trim(hawb_cntr)='' then null else trim(hawb_cntr) end , 'N/A') as hawb_cntr
,coalesce(case when trim(mawb_obol)='' then null else trim(mawb_obol) end , 'N/A') as mawb_obol_nbr
,coalesce(case when trim(flight_no)='' then null else trim(flight_no) end , 'N/A') as flight_nbr
,coalesce(case when trim(country_orig)='' then null else trim(country_orig) end , 'N/A') as country_origin
,coalesce(case when trim(export_port)='' then null else trim(export_port) end , 'N/A') as export_port
,coalesce(case when trim(country_export)='' then null else trim(country_export) end , 'N/A') as country_export
,coalesce(case when trim(cartons)='' then null else trim(cartons) end , 0) as cartons
,coalesce(case when trim(quantity)='' then null else trim(quantity) end , 0.00) as qty
,coalesce(case when trim(weight)='' then null else trim(weight) end , 0.00) as wgt
,coalesce(case when trim(weight_qlf)='' then null else trim(weight_qlf) end , 'N/A') as wgt_qlf
,coalesce(case when trim(cubic)='' then null else trim(cubic) end , 0.00) as cubic
,coalesce(case when trim(cubic_qlf)='' then null else trim(cubic_qlf) end , 'N/A') as cubic_qlf
,coalesce(case when trim(ff_cartons)='' then null else trim(ff_cartons) end , 0.00) as ff_cartons
,coalesce(case when trim(ff_quantity)='' then null else trim(ff_quantity) end , 0.00) as ff_qty
,coalesce(case when trim(ff_weight)='' then null else trim(ff_weight) end , 0.00) as ff_wgt
,coalesce(case when trim(ff_weight_qlf)='' then null else trim(ff_weight_qlf) end , 'N/A') as ff_wgt_qlf
,coalesce(case when trim(ff_cubic)='' then null else trim(ff_cubic) end , 0.00) as ff_cubic
,coalesce(case when trim(ff_cubic_qlf)='' then null else trim(ff_cubic_qlf) end , 'N/A') as ff_cubic_qlf
,coalesce(to_date(to_utc_timestamp(xfactory_date, "EST5EDT")), '9999-12-31') as xfactory_dt
,coalesce(to_date(to_utc_timestamp(receipt_date, "EST5EDT")), '9999-12-31') as receipt_dt
,coalesce(to_date(to_utc_timestamp(flt_sail_date, "EST5EDT")), '9999-12-31') as flt_sail_dt
,coalesce(case when trim(shipper_name)='' then null else trim(shipper_name) end , 'N/A') as shipper_name
,coalesce(case when trim(vendor)='' then null else trim(vendor) end , 'N/A') as vendor
,coalesce(case when trim(factory)='' then null else trim(factory) end , 'N/A') as factory
,coalesce(case when trim(terms)='' then null else trim(terms) end , 'N/A') as terms
,coalesce(case when trim(goh)='' then null else trim(goh) end , 'N/A') as goh
,coalesce(case when trim(trans_mode)='' then null else trim(trans_mode) end , 'N/A') as trans_mode
,coalesce(case when trim(priority_code)='' then null else trim(priority_code) end , 'N/A') as priority_cd
,coalesce(to_date(to_utc_timestamp(planned_ship_date, "EST5EDT")), '9999-12-31') as planned_ship_dt
,coalesce(to_date(to_utc_timestamp(gac_date, "EST5EDT")), '9999-12-31') as gac_dt
,coalesce(to_date(to_utc_timestamp(current_ndc_date, "EST5EDT")), '9999-12-31') as current_ndc_dt
,coalesce(to_date(to_utc_timestamp(cust_cancel_date, "EST5EDT")), '9999-12-31') as cust_cancel_dt
,coalesce(case when trim(customs_review_flag)='' then null else trim(customs_review_flag) end , 'N/A') as customs_review_flg
,coalesce(case when trim(comments_1)='' then null else trim(comments_1) end , 'N/A') as comments_1
,coalesce(case when trim(comments_2)='' then null else trim(comments_2) end , 'N/A') as comments_2
,coalesce(case when trim(comments_3)='' then null else trim(comments_3) end , 'N/A') as comments_3
,coalesce(case when trim(last_user)='' then null else trim(last_user) end , 'N/A') as last_user
,coalesce(to_utc_timestamp(last_update, "EST5EDT"), '9999-12-31 00:00:00') as last_upd_dt
,coalesce(case when trim(last_activity)='' then null else trim(last_activity) end , 'N/A') as last_activity
,coalesce(case when trim(shipto)='' then null else trim(shipto) end , 'N/A') as ship_to
,coalesce(case when trim(secondary_owner)='' then null else trim(secondary_owner) end , 'N/A') as secondary_owner
from rockport_stage0.rockmstr_booking;

! echo Completed Loading into scct_work_db.rockport_booking_work;

SET tez.job.name=stage1:rockport_booking:load work table rockport_booking;

! echo Started Loading into scct_db.rockport_booking;

insert overwrite table scct_db.rockport_booking
select
coalesce(rbw.seq_nbr, rb.seq_nbr)
,coalesce(rbw.split_id, rb.split_id)
,coalesce(rbw.booking_nbr, rb.booking_nbr)
,coalesce(rbw.booking_dt, rb.booking_dt)
,coalesce(rbw.book_notice_dt, rb.book_notice_dt)
,coalesce(rbw.div_cd, rb.div_cd)
,coalesce(rbw.po_nbr, rb.po_nbr)
,coalesce(rbw.ff_cd, rb.ff_cd)
,coalesce(rbw.dock_receipt, rb.dock_receipt)
,coalesce(rbw.hawb_cntr, rb.hawb_cntr)
,coalesce(rbw.mawb_obol_nbr, rb.mawb_obol_nbr)
,coalesce(rbw.flight_nbr, rb.flight_nbr)
,coalesce(rbw.country_origin, rb.country_origin)
,coalesce(rbw.export_port, rb.export_port)
,coalesce(rbw.country_export, rb.country_export)
,coalesce(rbw.cartons, rb.cartons)
,coalesce(rbw.qty, rb.qty)
,coalesce(rbw.wgt, rb.wgt)
,coalesce(rbw.wgt_qlf, rb.wgt_qlf)
,coalesce(rbw.cubic, rb.cubic)
,coalesce(rbw.cubic_qlf, rb.cubic_qlf)
,coalesce(rbw.ff_cartons, rb.ff_cartons)
,coalesce(rbw.ff_qty, rb.ff_qty)
,coalesce(rbw.ff_wgt, rb.ff_wgt)
,coalesce(rbw.ff_wgt_qlf, rb.ff_wgt_qlf)
,coalesce(rbw.ff_cubic, rb.ff_cubic)
,coalesce(rbw.ff_cubic_qlf, rb.ff_cubic_qlf)
,coalesce(rbw.xfactory_dt, rb.xfactory_dt)
,coalesce(rbw.receipt_dt, rb.receipt_dt)
,coalesce(rbw.flt_sail_dt, rb.flt_sail_dt)
,coalesce(rbw.shipper_name, rb.shipper_name)
,coalesce(rbw.vendor, rb.vendor)
,coalesce(rbw.factory, rb.factory)
,coalesce(rbw.terms, rb.terms)
,coalesce(rbw.goh, rb.goh)
,coalesce(rbw.trans_mode, rb.trans_mode)
,coalesce(rbw.priority_cd, rb.priority_cd)
,coalesce(rbw.planned_ship_dt, rb.planned_ship_dt)
,coalesce(rbw.gac_dt, rb.gac_dt)
,coalesce(rbw.current_ndc_dt, rb.current_ndc_dt)
,coalesce(rbw.cust_cancel_dt, rb.cust_cancel_dt)
,coalesce(rbw.customs_review_flg, rb.customs_review_flg)
,coalesce(rbw.comments_1, rb.comments_1)
,coalesce(rbw.comments_2, rb.comments_2)
,coalesce(rbw.comments_3, rb.comments_3)
,coalesce(rbw.last_user, rb.last_user)
,coalesce(rbw.last_upd_dt, rb.last_upd_dt)
,coalesce(rbw.last_activity, rb.last_activity)
,coalesce(rbw.ship_to, rb.ship_to)
,coalesce(rbw.secondary_owner, rb.secondary_owner)
from scct_work_db.rockport_booking_work rbw full join
scct_db.rockport_booking rb 
on rbw.seq_nbr = rb.seq_nbr
and rbw.div_cd = rb.div_cd
and rbw.po_nbr = rb.po_nbr
and rbw.split_id = rb.split_id
and rbw.ff_cd = rb.ff_cd;

! echo Completed Loading into scct_db.rockport_booking;

 -------------------------
 ---end script
 -------------------------