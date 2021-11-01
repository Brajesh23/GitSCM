---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_dc_intra
--------------------------------------------------------------------------
---target: stage1 table: scct_db.po_so_dd_hu_details 
---source: tms_stage0 table : po_master, delivery_master, inventory_movement, 
---handling_master and item_unit_measure_master
---load type: full
---back posting: no
--
-- History Information
--
-- Revision    Date     	JIRA       	Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 20AUG19		SCCTP4-3287	ssoma			DC to DC Intra Initial Version
-- 01.00.00.02 06MAR20		SCCTP4-3316	ssoma			remove reducer parameter
----------------------------------------------------------------------------

-------------------------
---loading po line kpi's data into po_so_dd_hu_details table
-------------------------

! echo started loading into scct_db.po_so_dd_hu_details;

insert overwrite table scct_db.po_so_dd_hu_details partition(business_process,to_node,po_doc_typ)
select
'DC' as spply_chn_nd
,'DC_Brand' as from_node
,po.purch_ord_nbr
,po.purch_ord_itm_nbr
,coalesce(po.del_ind,dm.del_ind,'N/A') as del_ind
,coalesce(po.itm_nbr,dm.itm_nbr,hm.itm_nbr,'0') as itm_nbr
,coalesce(po.co_cd,'N/A')
,coalesce(po.loc_nbr,'N/A')
,coalesce(po.purch_ord_qty,0.000)
,coalesce(po.purch_ord_qty_uom,'N/A')
,coalesce(po.ord_unt_nmrtr_convn_nbr,0)
,coalesce(po.base_unt_nmrtr_convn_nbr,0)
,coalesce(po.net_ord_val_curcy,0.00)
,coalesce(po.delv_compln_ind,'N/A')
,coalesce(po.base_uom_cd,'N/A')
,coalesce(po.purch_ord_qty*po.base_unt_nmrtr_convn_nbr/po.base_unt_dnmntr_convn_nbr,0.000)
,coalesce(po.mast_ownshp_dt,'9999-12-31')
,coalesce(po.orig_mast_ownshp_dt,'9999-12-31')
,coalesce(po.purch_doc_type_cd,'N/A')
,coalesce(case when trim(po.vend_acct_id)='' then null else trim(po.vend_acct_id) end,'N/A')
,coalesce(po.purchg_org_cd,'N/A')
,coalesce(po.purchg_grp_cd,'N/A')
,coalesce(po.cust_nbr,'N/A')
,coalesce(po.inctrm_1,dm.inctrm_1,'N/A')
,coalesce(po.inctrm_2,dm.inctrm_2,'N/A')
,coalesce(po.our_ref,'N/A')
,coalesce(po.transp_mode,'N/A')
,coalesce(po.sls_ord_nbr,'N/A')
,coalesce(po.sls_ord_itm_nbr,0)
,coalesce(po.coorig,'N/A')
,coalesce(po.ndc_dt,'9999-12-31')
,coalesce(po.stat_ndc_dt,'9999-12-31')
,coalesce(po.po_create_dt,'9999-12-31')
,coalesce(po.ship_to_loc_nbr,'N/A')
,coalesce(po.rec_crtd_dt,hm.rec_crtd_dt,'9999-12-31') as rec_crtd_dt
,coalesce(po.floorset_dt,'9999-12-31')
,'N/A' as so_sls_ord_nbr
,0 as so_sls_ord_itm_nbr
,'N/A' as so_itm_nbr
,0.00 as so_net_val
,0.000 as so_ord_qty_sls_unt
,'N/A' as so_recv_loc_nbr
,'9999-12-31' as so_rec_crtd_dt
,'N/A' as so_rec_crtd_id
,0.00 as so_cost
,'9999-12-31' as so_reqstd_dt
,'N/A' as so_cust_prch_nbr
,'N/A' as so_ship_to_loc_nbr
,'N/A' as so_cntry_cd
,'N/A' as so_sls_doc_type
,coalesce(dm.delv_doc_nbr,hm.delv_doc_nbr,'N/A') as delv_doc_nbr
,coalesce(dm.delv_doc_itm_nbr,hm.delv_doc_itm_nbr,0) as delv_doc_itm_nbr
,coalesce(dm.del_type,'N/A')
,coalesce(dm.act_delv_qty,0.000)
,coalesce(dm.actl_gds_movmt_dt,'9999-12-31')
,coalesce(dm.delv_prty,0)
,coalesce(dm.delv_qty,0.000)
,coalesce(dm.gds_iss_date,'9999-12-31')
,coalesce(dm.rcvg_ste_delv,'N/A')
,coalesce(dm.hdr_rec_crtd_dt,'9999-12-31')
,coalesce(dm.ref_doc_itm_nbr,0)
,coalesce(dm.ref_doc_nbr,'N/A')
,coalesce(dm.shp_rcvg_pt,'N/A')
,coalesce(dm.gds_movmt_cntl,'N/A')
,coalesce(dm.movmt_ind,'N/A')
,coalesce(dm.pod_ind,'N/A')
,coalesce(dm.pod_rlse_ind,'N/A')
,coalesce(dm.recv_loc_nbr,'N/A')
,coalesce(dm.transp_type,'N/A')
,coalesce(dm.transp_id,'N/A')
,coalesce(hm.intl_hndlg_unt_nbr,'N/A')
,coalesce(hm.ext_hndlg_unt_nbr,'N/A')
,coalesce(hm.ext_hndlg_unt_nbr_2,'N/A')
,coalesce(hm.ext_hndlg_unt_type,'N/A')
,coalesce(hm.pckg_mat_type,'N/A')
,coalesce(hm.pckd_qty,0.000)
,coalesce(hm.pckd_uom,'N/A')
,coalesce(ium.numerator,0.00)
,coalesce(hm.grs_wgt,0.000)
,coalesce(hm.wgt_unt,'N/A')
,coalesce(hm.len,0.000)
,coalesce(hm.wdt,0.000)
,coalesce(hm.hgt,0.000)
,coalesce(hm.dim_uom,'N/A')
,coalesce(hm.rec_chgd_dt,'9999-12-31')
,coalesce(hm.hndlg_unt_stat,'N/A')
,coalesce(hm.bol_dt,'9999-12-31')
,coalesce(hm.str_dt,'9999-12-31')
,coalesce(hm.da_scan_dt,'9999-12-31')
,coalesce(hm.load_nbr  ,0)
,coalesce(case when trim(po.vend_acct_id)='' then null else trim(po.vend_acct_id) end,'N/A') as supl_loc_nbr
,coalesce(po.loc_nbr,'N/A') as recv_loc_nbr
,'Stock_Transfer_Intra_Company' as business_process
,'DC_Brand' as to_node
,po.purch_doc_type_cd as po_doc_typ
from (select * from scct_db.po_master where purch_doc_type_cd='UB' AND (del_ind<>'L' OR (del_ind='L' AND gr_qty>0)) and vend_acct_id<>'N/A' and loc_nbr<>'N/A' )po 
inner join (select * from scct_db.location_master where loc_typ_cd='01') a on po.vend_acct_id=a.loc_nbr 
inner join (select * from scct_db.location_master where loc_typ_cd='01') b on po.loc_nbr=b.loc_nbr 
left outer join (select * from scct_db.delivery_master where del_ind<>'D' and del_type not in('EL','ELST') and itm_nbr<>'0') dm 
on po.purch_ord_nbr=dm.ref_doc_nbr and po.purch_ord_itm_nbr= dm.ref_doc_itm_nbr and po.itm_nbr=dm.itm_nbr 
left outer join (select * from scct_db.handling_master where itm_nbr<>'0') hm 
on dm.delv_doc_itm_nbr=hm.delv_doc_itm_nbr and dm.delv_doc_nbr=hm.delv_doc_nbr and po.itm_nbr=hm.itm_nbr 
left outer join (select * from scct_db.item_unit_measure_master where trim(itm_nbr)<>'') ium on dm.itm_nbr=ium.itm_nbr and hm.pckd_uom=ium.alt_uom ;

! echo Completed po_so_dd_hu_details data load;

-------------------------
---end script
-------------------------