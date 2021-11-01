---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_mbp_nb
--------------------------------------------------------------------------
---target: stage1 table: scct_db.po_so_dd_hu_details 
---source: tms_stage0 table : po_master, inventory_movement, 
---handling_master and item_unit_measure_master
---load type: full
---back posting: no
--
-- History Information
--
-- Revision    Date     	JIRA       	Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 20AUG19		SCCTP4-2276	ssoma			Initial Version
-- 01.00.00.02 16OCT19		SCCTP4-2523	ssoma			Changed partition columns and po for so model
-- 01.00.00.02 06MAR20		SCCTP4-3316	ssoma			remove reducer parameter
-- 01.00.00.04 05NOV20		SCCTP-456	fyuan			add fg filter
---------------------------------------------------------------------------

-------------------------
---loading po line kpis data into po_so_dd_hu_details table
-------------------------

! echo started loading into scct_db.po_so_dd_hu_details for MAST beauty Production;

insert overwrite table scct_db.po_so_dd_hu_details partition(business_process,to_node,po_doc_typ)
select
'Vendor' as spply_chn_nd
,'Vendor_Turnkey' as from_node
,po.purch_ord_nbr
,po.purch_ord_itm_nbr
,coalesce(po.del_ind,'N/A') as del_ind
,coalesce(po.itm_nbr,'0') as itm_nbr
,coalesce(po.co_cd,'N/A')
,coalesce(po.loc_nbr,'N/A')
,coalesce(po.purch_ord_qty,0.000)
,coalesce(po.purch_ord_qty_uom,'N/A')
,coalesce(po.ord_unt_nmrtr_convn_nbr,0)
,coalesce(po.base_unt_nmrtr_convn_nbr,0)
,coalesce(po.net_ord_val_curcy,0.00)
,coalesce(po.delv_compln_ind,'N/A')
,coalesce(po.base_uom_cd,'N/A')
--purch_ord_qty_units
,coalesce(po.purch_ord_qty*po.base_unt_nmrtr_convn_nbr/po.base_unt_dnmntr_convn_nbr,0.000)
,coalesce(po.mast_ownshp_dt,'9999-12-31')
,coalesce(po.orig_mast_ownshp_dt,'9999-12-31')
,coalesce(po.purch_doc_type_cd,'N/A')
,coalesce(case when trim(po.vend_acct_id)='' then null else trim(po.vend_acct_id) end,'N/A')
,coalesce(po.purchg_org_cd,'N/A')
,coalesce(po.purchg_grp_cd,'N/A')
,coalesce(po.cust_nbr,'N/A')
,coalesce(po.inctrm_1,'N/A')
,coalesce(po.inctrm_2,'N/A')
,coalesce(po.our_ref,'N/A')
,coalesce(po.transp_mode,'N/A')
,coalesce(po.sls_ord_nbr,'N/A')
,coalesce(po.sls_ord_itm_nbr,0)
,coalesce(po.coorig,'N/A')
,coalesce(po.ndc_dt,'9999-12-31')
,coalesce(po.stat_ndc_dt,'9999-12-31')
,coalesce(po.po_create_dt,'9999-12-31')
,coalesce(po.ship_to_loc_nbr,'N/A')
,coalesce(po.rec_crtd_dt,'9999-12-31') as rec_crtd_dt
,coalesce(po.floorset_dt,'9999-12-31')
,coalesce(so.sls_ord_nbr,'N/A') as so_sls_ord_nbr
,coalesce(so.sls_ord_itm_nbr,0) as so_sls_ord_itm_nbr
,coalesce(so.itm_nbr,'N/A') as so_itm_nbr
,coalesce(so.net_val,0.00) as so_net_val
,coalesce(so.ord_qty_sls_unt,0.000) as so_ord_qty_sls_unt
,coalesce(so.recv_loc_nbr,'N/A') as so_recv_loc_nbr
,coalesce(so.rec_crtd_dt,'9999-12-31') as so_rec_crtd_dt
,coalesce(so.rec_crtd_id,'N/A') as so_rec_crtd_id
,coalesce(so.cost,0.00) as so_cost
,coalesce(so.reqstd_dt,'9999-12-31') as so_reqstd_dt
,coalesce(so.cust_prch_nbr,'N/A') as so_cust_prch_nbr
,coalesce(so.ship_to_loc_nbr,'N/A') as so_ship_to_loc_nbr
,coalesce(so.cntry_cd,'N/A') as so_cntry_cd
,coalesce(so.sls_doc_type,'N/A') as so_sls_doc_type
,'N/A' as dm_delv_doc_nbr
,0  as dm_delv_doc_itm_nbr
,'N/A' as dm_del_type
,0.000 as dm_act_delv_qty
,'9999-12-31' as dm_actl_gds_movmt_dt
,0  as dm_delv_prty
,0.000 as dm_delv_qty
,'9999-12-31' as dm_gds_iss_date
,'N/A' as dm_rcvg_ste_delv
,'9999-12-31' as dm_hdr_rec_crtd_dt
,0 as dm_ref_doc_itm_nbr
,'N/A' as dm_ref_doc_nbr
,'N/A' as dm_shp_rcvg_pt
,'N/A' as dm_gds_movmt_cntl
,'N/A' as dm_movmt_ind
,'N/A' as dm_pod_ind
,'N/A' as dm_pod_rlse_ind
,'N/A' as dm_recv_loc_nbr
,'N/A' as dm_transp_type
,'N/A' as dm_transp_id
,'N/A' as hm_intl_hndlg_unt_nbr
,'N/A' as hm_ext_hndlg_unt_nbr
,'N/A' as hm_ext_hndlg_unt_nbr_2
,'N/A' as hm_ext_hndlg_unt_type
,'N/A' as hm_pckg_mat_type
,0.000 as hm_pckd_qty
,'N/A' as hm_pckd_uom
,0.00 as ium_numerator
,0.000 as hm_grs_wgt
,'N/A' as hm_wgt_unt
,0.000 as hm_len
,0.000 as hm_wdt
,0.000 as hm_hgt
,'N/A' as hm_dim_uom
,'9999-12-31' as hm_rec_chgd_dt
,'N/A' as hm_hndlg_unt_stat
,'9999-12-31' as hm_bol_dt
,'9999-12-31' as hm_str_dt
,'9999-12-31' as hm_da_scan_dt
,0 as hm_load_nbr
,coalesce(case when trim(po.vend_acct_id)='' then null else trim(po.vend_acct_id) end,'N/A') as supl_loc_nbr
,coalesce(po.loc_nbr,'N/A') as recv_loc_nbr
,'Managed_Production_Mast' as business_process
,'Vendor_Managed_Mast' as to_node
,po.purch_doc_type_cd as po_doc_typ
from (select * from scct_db.po_master where purch_doc_type_cd='NB' AND (del_ind<>'L' OR (del_ind='L' AND gr_qty>0)) 
and vend_acct_id<>'N/A' and loc_nbr<>'N/A') po 
inner join (select * from scct_db.location_master where loc_typ_cd='99') a ON po.vend_acct_id=a.loc_nbr 
inner join (select * from scct_db.location_master where loc_typ_cd='05') b ON po.loc_nbr=b.loc_nbr
left outer join (select * from scct_db.so_master where sls_doc_type='ZOR4') so on po.sls_ord_nbr=so.sls_ord_nbr and po.sls_ord_itm_nbr=so.sls_ord_itm_nbr
inner join (select * from analytics_view.item_scct where itm_typ_cd <> 'ROH') c ON po.itm_nbr = c.itm_nbr; 

! echo Completed po_so_dd_hu_details data load;

-------------------------
---end script
-------------------------