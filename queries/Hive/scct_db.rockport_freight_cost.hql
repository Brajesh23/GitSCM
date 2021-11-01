---jobname:scct-rockport-hive-stg1-scct_db.rockport_freight_cost
--------------------------------------------------------------------------
---Target: Stage1 table: scct_db.rockport_freight_cost
---Source: rockport_stage0 table : trader_srvc_invoice_hdr,trader_srvc_invoice_dtl
---Load Type: Full
---Back posting: No
---Author	-	Created/Updated Date	-	Jira Ticket
---ssoma	-	03/05/2019				- 
---ssoma	-	14/06/2019				- SCCTP4-2012/13/14
---ssoma	-	09/07/2019				- SCCTP4-2089
--------------------------------------------------------------------------

-------------------------
---loading work table scct_work_db.trader_srvc_invoice_dtl_work 
-------------------------

! echo Started Loading into scct_work_db.trader_srvc_invoice_dtl_work;

insert overwrite table scct_work_db.trader_srvc_invoice_dtl_work
select 
r.owner
,r.invoice_ref_no
,r.rec_id
,r.expense_code
,r.expense_category
,r.contract_no
,r.order_no
,r.rate_per_unit
,r.unit_prc_basis
,r.chargeable_qty
,r.roe
,r.payable_currency
,r.other_costs_amt
,r.adjustment
,r.bl_ref_no
,r.shipment_no
,r.account_code
,r.equip_id
,r.equip_size
,r.tax_indicator
,r.origin_country
,r.tax_type
,r.tax_amount
,r.tax_invoice_no
,r.p_link_key_val
,r.fcr_no
,r.last_user
,r.last_update
,r.last_activity
,r.currency from
(select
trim(owner) as owner
,trim(invoice_ref_no) as invoice_ref_no
,trim(rec_id) as rec_id
,expense_code
,expense_category
,contract_no
,order_no
,rate_per_unit
,unit_prc_basis
,chargeable_qty
,roe
,payable_currency
,other_costs_amt
,adjustment
,bl_ref_no
,shipment_no
,account_code
,equip_id
,equip_size
,tax_indicator
,origin_country
,tax_type
,tax_amount
,tax_invoice_no
,p_link_key_val
,fcr_no
,last_user
,last_update
,last_activity
,currency
,rank() over (partition by trim(owner),trim(invoice_ref_no),trim(rec_id) order by last_update desc ) as rk
from rockport_stage0.trader_srvc_invoice_dtl ) r where rk=1;

! echo Completed Loading into scct_work_db.trader_srvc_invoice_dtl_work;

-------------------------
---loading stage1 table scct_db.rockport_freight_cost 
-------------------------

SET tez.job.name=stage1:rockport_freight_cost_work:load work table rockport_freight_cost_work;

! echo Started Loading into scct_work_db.rockport_freight_cost_work;

insert overwrite table scct_work_db.rockport_freight_cost_work
select
coalesce(case when trim(ih.owner)='' then null else trim(ih.owner) end , 'N/A') as owner
,coalesce(case when trim(ih.invoice_ref_no)='' then null else trim(ih.invoice_ref_no) end , 'N/A') as invoice_ref_nbr
,coalesce(case when trim(ih.invoice_type)='' then null else trim(ih.invoice_type) end , 'N/A') as invoice_type
,coalesce(case when trim(ih.invoice_no)='' then null else trim(ih.invoice_no) end , 'N/A') as invoice_nbr
,coalesce(case when trim(ih.customer)='' then null else trim(ih.customer) end , 'N/A') as customer
,coalesce(case when trim(ih.bill_to)='' then null else trim(ih.bill_to) end , 'N/A') as bill_to
,coalesce(case when trim(ih.origin_office)='' then null else trim(ih.origin_office) end , 'N/A') as origin_office
,coalesce(to_utc_timestamp(ih.invoice_date, "EST5EDT"), '9999-12-31 00:00:00') as invoice_dt
,coalesce(to_utc_timestamp(ih.ship_date, "EST5EDT"), '9999-12-31 00:00:00') as ship_dt
,coalesce(to_utc_timestamp(ih.create_date, "EST5EDT"), '9999-12-31 00:00:00') as create_dt
,coalesce(case when trim(ih.currency)='' then null else trim(ih.currency) end , 'N/A') as hdr_currency
,coalesce(case when trim(ih.payment_type)='' then null else trim(ih.payment_type) end , 'N/A') as payment_type
,coalesce(case when trim(ih.payment_terms)='' then null else trim(ih.payment_terms) end , 'N/A') as payment_terms
,coalesce(case when trim(ih.load_point)='' then null else trim(ih.load_point) end , 'N/A') as load_point
,coalesce(case when trim(ih.final_dest)='' then null else trim(ih.final_dest) end , 'N/A') as final_dest
,coalesce(case when trim(ih.carrier)='' then null else trim(ih.carrier) end , 'N/A') as carrier
,coalesce(case when trim(ih.transp_id)='' then null else trim(ih.transp_id) end , 'N/A') as transp_id
,coalesce(case when trim(ih.voyage_no)='' then null else trim(ih.voyage_no) end , 'N/A') as voyage_nbr
,coalesce(case when trim(ih.total_gross_amount)='' then null else trim(ih.total_gross_amount) end , 0.00) as total_gross_amount
,coalesce(case when trim(ih.total_adjust_amount)='' then null else trim(ih.total_adjust_amount) end , 0.00) as total_adjust_amount
,coalesce(case when trim(ih.total_tax_amount)='' then null else trim(ih.total_tax_amount) end , 0.00) as total_tax_amount
,coalesce(case when trim(ih.total_net_amount)='' then null else trim(ih.total_net_amount) end , 0.00) as total_net_amount
,coalesce(case when trim(ih.edi_ind)='' then null else trim(ih.edi_ind) end , 'N/A') as edi_ind
,coalesce(case when trim(ih.last_user)='' then null else trim(ih.last_user) end , 'N/A') as hdr_last_user
,coalesce(to_utc_timestamp(ih.last_update, "EST5EDT"), '9999-12-31 00:00:00') as hdr_last_update_dtm
,coalesce(case when trim(ih.last_activity)='' then null else trim(ih.last_activity) end , 'N/A') as hdr_last_activity
,coalesce(case when trim(id.rec_id)='' then null else trim(id.rec_id) end , 0) as rec_id
,coalesce(case when trim(id.expense_code)='' then null else trim(id.expense_code) end , 'N/A') as expense_cd
,coalesce(case when trim(id.expense_category)='' then null else trim(id.expense_category) end , 'N/A') as expense_category
,coalesce(case when trim(id.contract_no)='' then null else trim(id.contract_no) end , 'N/A') as contract_nbr
,coalesce(case when trim(id.order_no)='' then null else trim(id.order_no) end , 'N/A') as po_nbr
,coalesce(case when trim(id.rate_per_unit)='' then null else trim(id.rate_per_unit) end , 0.00) as rate_per_unit
,coalesce(case when trim(id.unit_prc_basis)='' then null else trim(id.unit_prc_basis) end , 'N/A') as unit_prc_basis
,coalesce(case when trim(id.chargeable_qty)='' then null else trim(id.chargeable_qty) end , 0.00) as chargeable_qty
,coalesce(case when trim(id.roe)='' then null else trim(id.roe) end , 0.00) as roe
,coalesce(case when trim(id.payable_currency)='' then null else trim(id.payable_currency) end , 'N/A') as payable_currency
,coalesce(case when trim(id.other_costs_amt)='' then null else trim(id.other_costs_amt) end , 0.00) as other_costs_amt
,coalesce(case when trim(id.adjustment)='' then null else trim(id.adjustment) end , 0.00) as adjustment
,coalesce(case when trim(id.bl_ref_no)='' then null else trim(id.bl_ref_no) end , 'N/A') as bl_ref_nbr
,coalesce(case when trim(id.shipment_no)='' then null else trim(id.shipment_no) end , 'N/A') as shipment_nbr
,coalesce(case when trim(id.account_code)='' then null else trim(id.account_code) end , 'N/A') as account_cd
,coalesce(case when trim(id.equip_id)='' then null else trim(id.equip_id) end , 'N/A') as equip_id
,coalesce(case when trim(id.equip_size)='' then null else trim(id.equip_size) end , 'N/A') as equip_size
,coalesce(case when trim(id.tax_indicator)='' then null else trim(id.tax_indicator) end , 'N/A') as tax_ind
,coalesce(case when trim(id.origin_country)='' then null else trim(id.origin_country) end , 'N/A') as origin_country
,coalesce(case when trim(id.tax_type)='' then null else trim(id.tax_type) end , 'N/A') as tax_type
,coalesce(case when trim(id.tax_amount)='' then null else trim(id.tax_amount) end , 0.00) as tax_amount
,coalesce(case when trim(id.tax_invoice_no)='' then null else trim(id.tax_invoice_no) end , 'N/A') as tax_invoice_nbr
,coalesce(case when trim(id.p_link_key_val)='' then null else trim(id.p_link_key_val) end , 'N/A') as p_link_key_val
,coalesce(case when trim(id.fcr_no)='' then null else trim(id.fcr_no) end , 'N/A') as fcr_nbr
,coalesce(case when trim(id.last_user)='' then null else trim(id.last_user) end , 'N/A') as dtl_last_user
,coalesce(to_utc_timestamp(id.last_update, "EST5EDT"), '9999-12-31 00:00:00') as dtl_last_upd_dtm
,coalesce(case when trim(id.last_activity)='' then null else trim(id.last_activity) end , 'N/A') as dtl_last_activity
,coalesce(case when trim(id.currency)='' then null else trim(id.currency) end , 'N/A') as dtl_currency
from rockport_stage0.trader_srvc_invoice_hdr ih
inner join scct_work_db.trader_srvc_invoice_dtl_work id
on ih.owner=id.owner and ih.invoice_ref_no=id.invoice_ref_no;

! echo Completed Loading into scct_work_db.rockport_freight_cost_work;

SET tez.job.name=stage1:rockport_freight_cost:load work table rockport_freight_cost;

! echo Started Loading into scct_db.rockport_freight_cost;

insert overwrite table scct_db.rockport_freight_cost
select
coalesce(rfcw.owner, rfc.owner)
,coalesce(rfcw.invoice_ref_nbr, rfc.invoice_ref_nbr)
,coalesce(rfcw.invoice_type, rfc.invoice_type)
,coalesce(rfcw.invoice_nbr, rfc.invoice_nbr)
,coalesce(rfcw.customer, rfc.customer)
,coalesce(rfcw.bill_to, rfc.bill_to)
,coalesce(rfcw.origin_office, rfc.origin_office)
,coalesce(rfcw.invoice_dt, rfc.invoice_dt)
,coalesce(rfcw.ship_dt, rfc.ship_dt)
,coalesce(rfcw.create_dt, rfc.create_dt)
,coalesce(rfcw.hdr_currency, rfc.hdr_currency)
,coalesce(rfcw.payment_type, rfc.payment_type)
,coalesce(rfcw.payment_terms, rfc.payment_terms)
,coalesce(rfcw.load_point, rfc.load_point)
,coalesce(rfcw.final_dest, rfc.final_dest)
,coalesce(rfcw.carrier, rfc.carrier)
,coalesce(rfcw.transp_id, rfc.transp_id)
,coalesce(rfcw.voyage_nbr, rfc.voyage_nbr)
,coalesce(rfcw.total_gross_amount, rfc.total_gross_amount)
,coalesce(rfcw.total_adjust_amount, rfc.total_adjust_amount)
,coalesce(rfcw.total_tax_amount, rfc.total_tax_amount)
,coalesce(rfcw.total_net_amount, rfc.total_net_amount)
,coalesce(rfcw.edi_ind, rfc.edi_ind)
,coalesce(rfcw.hdr_last_user, rfc.hdr_last_user)
,coalesce(rfcw.hdr_last_update_dtm, rfc.hdr_last_update_dtm)
,coalesce(rfcw.hdr_last_activity, rfc.hdr_last_activity)
,coalesce(rfcw.rec_id, rfc.rec_id)
,coalesce(rfcw.expense_cd, rfc.expense_cd)
,coalesce(rfcw.expense_category, rfc.expense_category)
,coalesce(rfcw.contract_nbr, rfc.contract_nbr)
,coalesce(rfcw.po_nbr, rfc.po_nbr)
,coalesce(rfcw.rate_per_unit, rfc.rate_per_unit)
,coalesce(rfcw.unit_prc_basis, rfc.unit_prc_basis)
,coalesce(rfcw.chargeable_qty, rfc.chargeable_qty)
,coalesce(rfcw.roe, rfc.roe)
,coalesce(rfcw.payable_currency, rfc.payable_currency)
,coalesce(rfcw.other_costs_amt, rfc.other_costs_amt)
,coalesce(rfcw.adjustment, rfc.adjustment)
,coalesce(rfcw.bl_ref_nbr, rfc.bl_ref_nbr)
,coalesce(rfcw.shipment_nbr, rfc.shipment_nbr)
,coalesce(rfcw.account_cd, rfc.account_cd)
,coalesce(rfcw.equip_id, rfc.equip_id)
,coalesce(rfcw.equip_size, rfc.equip_size)
,coalesce(rfcw.tax_ind, rfc.tax_ind)
,coalesce(rfcw.origin_country, rfc.origin_country)
,coalesce(rfcw.tax_type, rfc.tax_type)
,coalesce(rfcw.tax_amount, rfc.tax_amount)
,coalesce(rfcw.tax_invoice_nbr, rfc.tax_invoice_nbr)
,coalesce(rfcw.p_link_key_val, rfc.p_link_key_val)
,coalesce(rfcw.fcr_nbr, rfc.fcr_nbr)
,coalesce(rfcw.dtl_last_user, rfc.dtl_last_user)
,coalesce(rfcw.dtl_last_upd_dtm, rfc.dtl_last_upd_dtm)
,coalesce(rfcw.dtl_last_activity, rfc.dtl_last_activity)
,coalesce(rfcw.dtl_currency, rfc.dtl_currency)
from scct_work_db.rockport_freight_cost_work rfcw full join
scct_db.rockport_freight_cost rfc 
on rfcw.owner=rfc.owner
and rfcw.invoice_ref_nbr=rfc.invoice_ref_nbr
and rfcw.rec_id=rfc.rec_id;

! echo Completed Loading into scct_db.rockport_freight_cost;

 -------------------------
 ---end script
 -------------------------