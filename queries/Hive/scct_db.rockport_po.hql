---jobname:scct-rockport-hive-stg1-scct_db.rockport_po
--------------------------------------------------------------------------
---Target: Stage1 table: scct_db.rockport_po
---Source: rockport_stage0 table : trader_contract_dtl,trader_contract_hdr,trader_delivery_dtl,trader_plci3225,
---trader_order_dtl,trader_delivery_dtl,trader_order_hdr,trader_party,trader_costing_hdr,trader_packing,
---Load Type: Full
---Back posting: No
---Author	-	Created/Updated Date	-	Jira Ticket
---ssoma	-	06/05/2019				- 
---ssoma	-	14/06/2019				- SCCTP4-2006/07/08
--------------------------------------------------------------------------

set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

-------------------------
---loading stage1 table scct_db.rockport_po 
-------------------------

SET tez.job.name=stage1:rockport_po_work:load work table rockport_po_work;

! echo Started Loading into scct_work_db.rockport_po_work;

insert overwrite table scct_work_db.rockport_po_work
select
'CONTRACT' as po_type
,coalesce(case when trim(ch.OWNER)='' then NULL else trim(ch.OWNER) end , 'N/A') as owner
,coalesce(case when trim(cd.CONTRACT_NO)='' then NULL else trim(cd.CONTRACT_NO) end , 'N/A') as po_nbr
,coalesce(case when trim(cd.ITEM)='' then NULL when length(trim(cd.ITEM))=8 then lpad(trim(cd.ITEM),18,0) else trim(cd.ITEM) end , '0') as itm_nbr
,coalesce(case when trim(cd.LINE_NO)='' then NULL else trim(cd.LINE_NO) end , 'N/A') as line_nbr
,coalesce(case when trim(ch.DEPT)='' then NULL else trim(ch.DEPT) end , 'N/A') as department
,coalesce(case when trim(dd.PROOF_OF_DEL_NO)='' then NULL else trim(dd.PROOF_OF_DEL_NO) end , 'N/A') as inco_terms
,coalesce(case when trim(cd.QTY_ORDERED)='' then NULL else trim(cd.QTY_ORDERED) end , 0.00) as qty_ordered
,coalesce(case when trim(dd.TRANSPORT_MODE)='' then NULL else trim(dd.TRANSPORT_MODE) end , 'N/A') as shipping_method
,coalesce(case when trim(cd.ORIGIN_COUNTRY)='' then NULL else trim(cd.ORIGIN_COUNTRY) end , 'N/A') as origin_country
,coalesce(case when trim(dd.STATUS)='' then NULL else trim(dd.STATUS) end , 'N/A') as port
,coalesce(case when trim(pc.PLACE_NAME)='' then NULL else trim(pc.PLACE_NAME) end , 'N/A') as port_name
,coalesce(case when trim(pc.COUNTRY)='' then NULL else trim(pc.COUNTRY) end , 'N/A') as export_country
,coalesce(to_date(to_utc_timestamp(cd.ORDER_DATE, "EST5EDT")), '9999-12-31') as order_dt
,coalesce(case when trim(cd.MANUFACTURER)='' then NULL else trim(cd.MANUFACTURER) end , 'N/A') as factory_id
,coalesce(case when trim(pa.PARTY_NAME_LINE1)='' then NULL else trim(pa.PARTY_NAME_LINE1) end , 'N/A') as factory_name
,coalesce(case when trim(pa.CITY_NAME_1)='' then NULL else trim(pa.CITY_NAME_1) end , 'N/A') as factory_city
,coalesce(case when trim(pa.COUNTRY_1)='' then NULL else trim(pa.COUNTRY_1) end , 'N/A') as factory_country
,coalesce(case when trim(cd.SECONDARY_OWNER)='' then NULL else trim(cd.SECONDARY_OWNER) end , 'N/A') as brand
,coalesce(to_date(to_utc_timestamp(cd.LATEST_SHIP_DATE, "EST5EDT")), '9999-12-31') as plan_gac_dt
,coalesce(to_date(to_utc_timestamp(dd.LAST_DELIVERY_DATE, "EST5EDT")), '9999-12-31') as plan_ndc_dt
,coalesce(case when trim(dd.DELIVER_TO)='' then NULL else trim(dd.DELIVER_TO) end , 'N/A') as deliver_to
,coalesce(case when trim(pa2.PARTY_NAME_LINE1)='' then NULL else trim(pa2.PARTY_NAME_LINE1) end , 'N/A') as deliver_to_name
,coalesce(case when trim(pa2.CITY_NAME_1)='' then NULL else trim(pa2.CITY_NAME_1) end , 'N/A') as deliver_to_city
,coalesce(case when trim(pa2.COUNTRY_1)='' then NULL else trim(pa2.COUNTRY_1) end , 'N/A') as deliver_to_country
,coalesce(case when trim(cd.ORDER_NO)='' then NULL else trim(cd.ORDER_NO) end , 'N/A') as cpo
,coalesce(case when trim(ch.SELLER)='' then NULL else trim(ch.SELLER) end , 'N/A') as seller_id
,coalesce(case when trim(pa3.PARTY_NAME_LINE1)='' then NULL else trim(pa3.PARTY_NAME_LINE1) end , 'N/A') as seller_name
,coalesce(case when trim(pa3.CITY_NAME_1)='' then NULL else trim(pa3.CITY_NAME_1) end , 'N/A') as seller_city
,coalesce(case when trim(pa3.COUNTRY_1)='' then NULL else trim(pa3.COUNTRY_1) end , 'N/A') as seller_country
,coalesce(case when trim(coh.DIM_FACTOR)='' then NULL else trim(coh.DIM_FACTOR) end , 0.00) as dim_factor
,coalesce(case when trim(coh.PRICE)='' then NULL else trim(coh.PRICE) end , 0.00) as price
,coalesce(case when trim(coh.CURRENCY)='' then NULL else trim(coh.CURRENCY) end , 'N/A') as currency
,coalesce(case when trim(p.GROSS_WGT_UNIT)='' then NULL else trim(p.GROSS_WGT_UNIT) end , 0.00) as gross_wgt_unit
,coalesce(case when trim(p.GROSS_WGT_UNIT_UM)='' then NULL else trim(p.GROSS_WGT_UNIT_UM) end , 'N/A') as gross_wgt_uom
,coalesce(to_utc_timestamp(ch.LAST_UPDATE, "EST5EDT"), '9999-12-31 00:00:00') as header_last_upd_dtm
,coalesce(to_utc_timestamp(cd.LAST_UPDATE, "EST5EDT"), '9999-12-31 00:00:00') as detail_last_upd_dtm
,coalesce(to_utc_timestamp(dd.LAST_UPDATE, "EST5EDT"), '9999-12-31 00:00:00') as delivery_last_upd_dtm
,coalesce(to_utc_timestamp(p.LAST_UPDATE, "EST5EDT"), '9999-12-31 00:00:00') as packing_last_upd_dtm
,coalesce(to_utc_timestamp(coh.LAST_UPDATE, "EST5EDT"), '9999-12-31 00:00:00') as costing_last_upd_dtm
from rockport_stage0.trader_contract_dtl cd
left join rockport_stage0.trader_contract_hdr ch
on ch.owner=cd.owner and ch.contract_no=cd.contract_no
left join rockport_stage0.trader_delivery_dtl dd
on dd.link_key_val=concat(cd.contract_no,' + ',cd.contract_no,' + ',cd.item,' + ',cd.line_no) and dd.owner=ch.owner
left join rockport_stage0.trader_plci3225 pc
on pc.place_code=dd.status
left join rockport_stage0.trader_party pa
on pa.party_id=cd.manufacturer
left join rockport_stage0.trader_party pa2
on pa2.party_id=dd.deliver_to
left join rockport_stage0.trader_party pa3
on pa3.party_id=ch.seller
left join rockport_stage0.trader_costing_hdr coh
on coh.owner=cd.owner and coh.link_key_val=concat(cd.contract_no,' + ',cd.contract_no,' + ',cd.item,' + ',cd.line_no)
left join rockport_stage0.trader_packing p
on p.owner=coh.owner and p.link_key_val=coh.link_key_val 
union all
select
'ORDER' as po_type
,coalesce(case when trim(oh.OWNER)='' then NULL else trim(oh.OWNER) end , 'N/A') as owner
,coalesce(case when trim(od.ORDER_NO)='' then NULL else trim(od.ORDER_NO) end , 'N/A') as po_nbr
,coalesce(case when trim(od.ITEM)='' then NULL when length(trim(od.ITEM))=8 then lpad(trim(od.ITEM),18,0) else trim(od.ITEM) end , '0') as itm_nbr
,coalesce(case when trim(od.LINE_NO)='' then NULL else trim(od.LINE_NO) end , 'N/A') as line_nbr
,coalesce(case when trim(oh.DEPT)='' then NULL else trim(oh.DEPT) end , 'N/A') as department
,coalesce(case when trim(dd.PROOF_OF_DEL_NO)='' then NULL else trim(dd.PROOF_OF_DEL_NO) end , 'N/A') as inco_terms
,coalesce(case when trim(od.QTY_ORDERED)='' then NULL else trim(od.QTY_ORDERED) end , 0.00) as qty_ordered
,coalesce(case when trim(dd.TRANSPORT_MODE)='' then NULL else trim(dd.TRANSPORT_MODE) end , 'N/A') as shipping_method
,coalesce(case when trim(od.ORIGIN_COUNTRY)='' then NULL else trim(od.ORIGIN_COUNTRY) end , 'N/A') as origin_country
,coalesce(case when trim(dd.STATUS)='' then NULL else trim(dd.STATUS) end , 'N/A') as port
,coalesce(case when trim(REPLACE(pc.PLACE_NAME,',',''))='' then NULL else trim(REPLACE(pc.PLACE_NAME,',','')) end , 'N/A') as port_name
,coalesce(case when trim(pc.COUNTRY)='' then NULL else trim(pc.COUNTRY) end , 'N/A') as export_country
,coalesce(to_date(to_utc_timestamp(oh.ORDER_DATE, "EST5EDT")), '9999-12-31') as order_dt
,coalesce(case when trim(od.MANUFACTURER)='' then NULL else trim(od.MANUFACTURER) end , 'N/A') as factory_id
,coalesce(case when trim(pa.PARTY_NAME_LINE1)='' then NULL else trim(pa.PARTY_NAME_LINE1) end , 'N/A') as factory_name
,coalesce(case when trim(pa.CITY_NAME_1)='' then NULL else trim(pa.CITY_NAME_1) end , 'N/A') as factory_city
,coalesce(case when trim(pa.COUNTRY_1)='' then NULL else trim(pa.COUNTRY_1) end , 'N/A') as factory_country
,coalesce(case when trim(od.DELIVER_TO)='' then NULL else trim(od.DELIVER_TO) end , 'N/A') as brand
,coalesce(to_date(to_utc_timestamp(od.LATEST_SHIP_DATE, "EST5EDT")), '9999-12-31') as plan_gac_dt
,coalesce(to_date(to_utc_timestamp(dd.LAST_DELIVERY_DATE, "EST5EDT")), '9999-12-31') as plan_ndc_dt
,coalesce(case when trim(dd.DELIVER_TO)='' then NULL else trim(dd.DELIVER_TO) end , 'N/A') as deliver_to
,coalesce(case when trim(pa2.PARTY_NAME_LINE1)='' then NULL else trim(pa2.PARTY_NAME_LINE1) end , 'N/A') as deliver_to_name
,coalesce(case when trim(pa2.CITY_NAME_1)='' then NULL else trim(pa2.CITY_NAME_1) end , 'N/A') as deliver_to_city
,coalesce(case when trim(pa2.COUNTRY_1)='' then NULL else trim(pa2.COUNTRY_1) end , 'N/A') as deliver_to_country
,'N/A' as cpo
,coalesce(case when trim(oh.SELLER)='' then NULL else trim(oh.SELLER) end , 'N/A') as seller_id
,coalesce(case when trim(pa2.PARTY_NAME_LINE1)='' then NULL else trim(pa2.PARTY_NAME_LINE1) end , 'N/A') as seller_name
,coalesce(case when trim(pa3.CITY_NAME_1)='' then NULL else trim(pa3.CITY_NAME_1) end , 'N/A') as seller_city
,coalesce(case when trim(pa3.COUNTRY_1)='' then NULL else trim(pa3.COUNTRY_1) end , 'N/A') as seller_country
,coalesce(case when trim(coh.DIM_FACTOR)='' then NULL else trim(coh.DIM_FACTOR) end , 0.00) as dim_factor
,coalesce(case when trim(coh.PRICE)='' then NULL else trim(coh.PRICE) end , 0.00) as price
,coalesce(case when trim(coh.CURRENCY)='' then NULL else trim(coh.CURRENCY) end , 'N/A') as currency
,coalesce(case when trim(p.GROSS_WGT_UNIT)='' then NULL else trim(p.GROSS_WGT_UNIT) end , 0.00) as gross_wgt_unit
,coalesce(case when trim(p.GROSS_WGT_UNIT_UM)='' then NULL else trim(p.GROSS_WGT_UNIT_UM) end , 'N/A') as gross_wgt_uom
,coalesce(to_utc_timestamp(oh.LAST_UPDATE, "EST5EDT"), '9999-12-31 00:00:00') as header_last_upd_dtm
,coalesce(to_utc_timestamp(od.LAST_UPDATE, "EST5EDT"), '9999-12-31 00:00:00') as detail_last_upd_dtm
,coalesce(to_utc_timestamp(dd.LAST_UPDATE, "EST5EDT"), '9999-12-31 00:00:00') as delivery_last_upd_dtm
,coalesce(to_utc_timestamp(p.LAST_UPDATE, "EST5EDT"), '9999-12-31 00:00:00') as packing_last_upd_dtm
,coalesce(to_utc_timestamp(coh.LAST_UPDATE, "EST5EDT"), '9999-12-31 00:00:00') as costing_last_upd_dtm
from rockport_stage0.trader_order_dtl od
left join rockport_stage0.trader_delivery_dtl dd
on dd.link_key_val=concat(od.order_no,' + ',od.item,' + ',od.line_no)
left join rockport_stage0.trader_plci3225 pc
on pc.place_code=dd.status
left join rockport_stage0.trader_party pa
on pa.party_id=od.manufacturer
left join rockport_stage0.trader_party pa2
on pa2.party_id=dd.deliver_to
join rockport_stage0.trader_order_hdr oh
on oh.order_no=od.order_no and oh.owner=od.owner
left join rockport_stage0.trader_party pa3
on pa3.party_id=oh.seller
left join rockport_stage0.trader_costing_hdr coh
on coh.owner=od.owner and coh.link_key_val=concat(od.order_no,' + ',od.item,' + ',od.line_no)
left join rockport_stage0.trader_packing p
on p.owner=coh.owner and p.link_key_val=coh.link_key_val;


! echo Completed Loading into scct_work_db.rockport_po_work;

SET tez.job.name=stage1:rockport_po:load work table rockport_po;

! echo Started Loading into scct_db.rockport_po;

insert overwrite table scct_db.rockport_po
select
coalesce(rpw.po_type, rp.po_type)
,coalesce(rpw.owner, rp.owner)
,coalesce(rpw.po_nbr, rp.po_nbr)
,coalesce(i.item_number, rpw.itm_nbr, rp.itm_nbr)
,coalesce(rpw.line_nbr, rp.line_nbr)
,coalesce(rpw.department, rp.department)
,coalesce(rpw.inco_terms, rp.inco_terms)
,coalesce(rpw.qty_ordered, rp.qty_ordered)
,coalesce(rpw.shipping_method, rp.shipping_method)
,coalesce(rpw.origin_country, rp.origin_country)
,coalesce(rpw.port, rp.port)
,coalesce(rpw.port_name, rp.port_name)
,coalesce(rpw.export_country, rp.export_country)
,coalesce(rpw.order_dt, rp.order_dt)
,coalesce(rpw.factory_id, rp.factory_id)
,coalesce(rpw.factory_name, rp.factory_name)
,coalesce(rpw.factory_city, rp.factory_city)
,coalesce(rpw.factory_country, rp.factory_country)
,coalesce(rpw.brand, rp.brand)
,coalesce(rpw.plan_gac_dt, rp.plan_gac_dt)
,coalesce(rpw.plan_ndc_dt, rp.plan_ndc_dt)
,coalesce(rpw.deliver_to, rp.deliver_to)
,coalesce(rpw.deliver_to_name, rp.deliver_to_name)
,coalesce(rpw.deliver_to_city, rp.deliver_to_city)
,coalesce(rpw.deliver_to_country, rp.deliver_to_country)
,coalesce(rpw.cpo, rp.cpo)
,coalesce(rpw.seller_id, rp.seller_id)
,coalesce(rpw.seller_name, rp.seller_name)
,coalesce(rpw.seller_city, rp.seller_city)
,coalesce(rpw.seller_country, rp.seller_country)
,coalesce(rpw.dim_factor, rp.dim_factor)
,coalesce(rpw.price, rp.price)
,coalesce(rpw.currency, rp.currency)
,coalesce(rpw.gross_wgt_unit, rp.gross_wgt_unit)
,coalesce(rpw.gross_wgt_uom, rp.gross_wgt_uom)
,coalesce(rpw.header_last_upd_dtm, rp.header_last_upd_dtm)
,coalesce(rpw.detail_last_upd_dtm, rp.detail_last_upd_dtm)
,coalesce(rpw.delivery_last_upd_dtm, rp.delivery_last_upd_dtm)
,coalesce(rpw.packing_last_upd_dtm, rp.packing_last_upd_dtm)
,coalesce(rpw.costing_last_upd_dtm, rp.costing_last_upd_dtm)
from scct_work_db.rockport_po_work rpw 
full join
scct_db.rockport_po rp 
on rpw.owner=rp.owner
and rpw.po_type=rp.po_type
and rpw.po_nbr=rp.po_nbr
and rpw.line_nbr=rp.line_nbr
and rpw.itm_nbr = rp.itm_nbr
left outer join
analytics_view.item i
on rpw.itm_nbr = i.item_number;

! echo Completed Loading into scct_db.rockport_po;

 -------------------------
 ---end script
 -------------------------