---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_kpi_details_po
--------------------------------------------------------------------------
---target: stage1 table: scct_db.po_so_dd_hu_kpi_details 
---source: tms_stage0 table : po_master, delivery_master, inventory_movement, 
---handling_master and item_unit_measure_master
---load type: full
---back posting: no
--
-- History Information
--
-- Revision    Date     	JIRA       	Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 20AUG19		SCCTP4-2276	ssoma			Initial Version
-- 01.00.00.02 09OCT19		SCCTP4-2523	ssoma			Changed partition columns and added new column
-- 01.00.00.02 06MAR20		SCCTP4-3316	ssoma			remove reducer parameter
-- 01.00.00.04 05NOV20		SCCTP-456	fyuan			add component filter 
--------------------------------------------------------------------------

-------------------------
---loading po line kpi's data into po_so_dd_hu_kpi_details table
-------------------------

! echo started loading into scct_db.po_so_dd_hu_kpi_details;

insert overwrite table scct_db.po_so_dd_hu_kpi_details  partition(business_process,to_node,po_doc_typ)
select
dtl.spply_chn_nd
,dtl.from_node
,dtl.po_purch_ord_nbr
,dtl.po_purch_ord_itm_nbr
,dtl.po_itm_nbr
,coalesce(kpi.po_frst_shp_dt,'9999-12-31')
,coalesce(kpi.po_lst_shp_dt,'9999-12-31')
,coalesce(kpi.po_tot_shp_units,0)
,coalesce(kpi.po_ontm_to_shp_pln_dt_pct,0)
,coalesce(kpi.po_ontm_to_shp_pln_dt_and_cmplt_pct,0)
,coalesce(kpi.po_ontm_to_shp_stat_dt_pct,0)
,coalesce(kpi.po_ontm_to_shp_stat_dt_and_cmplt_pct,0)
,coalesce(kpi.po_crt_to_frst_spmt_days,0)
,coalesce(kpi.po_crt_to_lst_spmt_days,0)
,coalesce(kpi.po_frst_rcpt_dt,'9999-12-31')
,coalesce(kpi.po_lst_rcpt_dt,'9999-12-31')
,coalesce(kpi.po_tot_rcpt_units,0)
,coalesce(kpi.po_ontm_to_rcpt_pln_dt_pct,0)
,coalesce(kpi.po_ontm_to_rcpt_pln_dt_and_cmplt_pct,0)
,coalesce(kpi.po_ontm_to_rcpt_stat_dt_pct,0)
,coalesce(kpi.po_ontm_to_rcpt_stat_dt_and_cmplt_pct,0)
,coalesce(kpi.po_nbr_of_spmts,0)
,coalesce(kpi.po_spmt_instncs,'N/A')
,coalesce(kpi.po_nbr_of_rcpt,0)
,coalesce(kpi.po_rcpt_instncs,'N/A')
,coalesce(kpi.po_frst_spmt_to_frst_rcpt_days,0)
,coalesce(kpi.po_frst_spmt_to_lst_rcpt_days,0)
,coalesce(kpi.po_lst_spmt_to_frst_rcpt_days,0)
,coalesce(kpi.po_lst_spmt_to_lst_rcpt_days,0)
,coalesce(kpi.po_crt_to_frst_rcpt_days,0)
,coalesce(kpi.po_crt_to_lst_rcpt_days,0)
,coalesce(kpi.po_crtn_tot_cnt,0)
,coalesce(kpi.po_crtn_tot_pckd_units,0)
,coalesce(kpi.po_crtn_frst_spmt_dt,'9999-12-31')
,coalesce(kpi.po_crtn_frst_rcpt_dt,'9999-12-31')
,coalesce(kpi.po_crtn_lst_spmt_dt,'9999-12-31')
,coalesce(kpi.po_crtn_lst_rcpt_dt,'9999-12-31')
,'9999-12-31'
,'9999-12-31' 
,0
,0
,0
,coalesce(dtl.po_del_ind ,'N/A') 
,coalesce(dtl.po_co_cd ,'N/A') 
,coalesce(dtl.po_loc_nbr ,'N/A') 
,coalesce(dtl.po_purch_ord_qty ,0.000)
,coalesce(dtl.po_purch_ord_qty_uom ,'N/A') 
,coalesce(dtl.po_ord_unt_nmrtr_convn_nbr , 0) 
,coalesce(dtl.po_base_unt_nmrtr_convn_nbr , 0) 
,coalesce(dtl.po_net_ord_val_curcy ,0.00)
,coalesce(dtl.po_delv_compln_ind ,'N/A') 
,coalesce(dtl.po_base_uom_cd ,'N/A') 
--po_purch_ord_qty_units
,coalesce(dtl.po_purch_ord_qty_units ,0.000)
,coalesce(dtl.po_mast_ownshp_dt, '9999-12-31') 
,coalesce(dtl.po_orig_mast_ownshp_dt, '9999-12-31') 
,coalesce(dtl.po_purch_doc_type_cd ,'N/A') 
,coalesce(dtl.po_vend_acct_id ,'N/A') 
,coalesce(dtl.po_purchg_org_cd ,'N/A') 
,coalesce(dtl.po_purchg_grp_cd ,'N/A') 
,coalesce(dtl.po_cust_nbr ,'N/A') 
,coalesce(dtl.po_inctrm ,'N/A') 
,coalesce(dtl.po_port_cd ,'N/A')
,coalesce(dtl.po_our_ref ,'N/A') 
,coalesce(dtl.po_transp_mode ,'N/A') 
,coalesce(dtl.po_sls_ord_nbr ,'N/A') 
,coalesce(dtl.po_sls_ord_itm_nbr , 0) 
,coalesce(dtl.po_coorig ,'N/A') 
,coalesce(dtl.po_ndc_dt, '9999-12-31' ) 
,coalesce(dtl.po_stat_ndc_dt, '9999-12-31' ) 
,coalesce(dtl.po_po_create_dt, '9999-12-31' ) 
,coalesce(dtl.po_ship_to_loc_nbr ,'N/A') 
,coalesce(dtl.po_rec_crtd_dt ,'N/A') 
,coalesce(dtl.po_floorset_dt, '9999-12-31' ) 
,coalesce(dtl.so_sls_ord_nbr ,'N/A') 
,coalesce(dtl.so_sls_ord_itm_nbr , 0) 
,coalesce(dtl.so_itm_nbr ,'N/A') 
,coalesce(dtl.so_net_val ,0.00)
,coalesce(dtl.so_ord_qty_sls_unt ,0.000)
,coalesce(dtl.so_recv_loc_nbr ,'N/A') 
,coalesce(dtl.so_rec_crtd_dt, '9999-12-31' ) 
,coalesce(dtl.so_rec_crtd_id ,'N/A') 
,coalesce(dtl.so_cost ,0.00)
,coalesce(dtl.so_reqstd_dt, '9999-12-31' ) 
,coalesce(dtl.so_cust_prch_nbr ,'N/A') 
,coalesce(dtl.so_ship_to_loc_nbr ,'N/A') 
,coalesce(dtl.so_cntry_cd ,'N/A') 
,coalesce(dtl.so_sls_doc_type ,'N/A') 
,coalesce(dtl.dd_delv_doc_nbr ,'N/A') 
,coalesce(dtl.dd_delv_doc_itm_nbr , 0) 
,coalesce(dtl.dd_del_type ,'N/A') 
,coalesce(dtl.dd_act_delv_qty ,0.000)
,coalesce(dtl.dd_actl_gds_movmt_dt, '9999-12-31' ) 
,coalesce(dtl.dd_delv_prty , 0) 
,coalesce(dtl.dd_delv_qty ,0.000)
,coalesce(dtl.dd_gds_iss_date, '9999-12-31' ) 
,coalesce(dtl.dd_rcvg_ste_delv ,'N/A') 
,coalesce(dtl.dd_hdr_rec_crtd_dt, '9999-12-31' ) 
,coalesce(dtl.dd_ref_doc_itm_nbr , 0) 
,coalesce(dtl.dd_ref_doc_nbr ,'N/A') 
,coalesce(dtl.dd_shp_rcvg_pt ,'N/A') 
,coalesce(dtl.dd_gds_movmt_cntl ,'N/A') 
,coalesce(dtl.dd_movmt_ind ,'N/A') 
,coalesce(dtl.dd_pod_ind ,'N/A') 
,coalesce(dtl.dd_pod_rlse_ind ,'N/A') 
,coalesce(dtl.dd_recv_loc_nbr ,'N/A') 
,coalesce(dtl.dd_transp_type ,'N/A') 
,coalesce(dtl.dd_transp_id ,'N/A') 
,coalesce(dtl.hu_intl_hndlg_unt_nbr ,'N/A') 
,coalesce(dtl.hu_ext_hndlg_unt_nbr ,'N/A') 
,coalesce(dtl.hu_ext_hndlg_unt_nbr_2 ,'N/A') 
,coalesce(dtl.hu_ext_hndlg_unt_type ,'N/A') 
,coalesce(dtl.hu_pckg_mat_type ,'N/A') 
,coalesce(dtl.hu_pckd_qty ,0.000)
,coalesce(dtl.hu_pckd_uom ,'N/A') 
,coalesce(dtl.um_numerator ,0.0)
,coalesce(dtl.hu_grs_wgt ,0.000)
,coalesce(dtl.hu_wgt_unt ,'N/A') 
,coalesce(dtl.hu_len ,0.000)
,coalesce(dtl.hu_wdt ,0.000)
,coalesce(dtl.hu_hgt ,0.000)
,coalesce(dtl.hu_dim_uom ,'N/A') 
,coalesce(dtl.hu_rec_chgd_dt, '9999-12-31' ) 
,coalesce(dtl.hu_hndlg_unt_stat ,'N/A') 
,coalesce(dtl.hu_bol_dt, '9999-12-31' ) 
,coalesce(dtl.hu_str_dt, '9999-12-31' ) 
,coalesce(dtl.hu_da_scan_dt, '9999-12-31' ) 
,coalesce(dtl.hu_load_nbr , 0) 
,coalesce(dtl.supl_loc_nbr,'N/A')
,coalesce(dtl.recv_loc_nbr,'N/A')
,dtl.business_process
,dtl.to_node
,dtl.po_doc_typ
from scct_db.po_so_dd_hu_details dtl left join scct_db.po_line_kpi kpi  
on dtl.business_process=kpi.business_process and dtl.to_node=kpi.to_node and dtl.po_doc_typ=kpi.po_doc_typ and
dtl.po_purch_ord_nbr=kpi.purch_ord_nbr and dtl.po_purch_ord_itm_nbr=kpi.purch_ord_itm_nbr and dtl.po_itm_nbr=kpi.itm_nbr
WHERE  ((dtl.business_process='Turnkey_Production' and dtl.to_node='DC_Brand' AND dtl.po_doc_typ='NB')
OR (dtl.business_process='Component_Inflow' and dtl.to_node='Vendor_Managed_Mast' AND dtl.po_doc_typ='NB')
OR (dtl.business_process='Managed_Production_Mast' and dtl.to_node='Vendor_Managed_Mast' AND dtl.po_doc_typ='NB')
OR (dtl.business_process='Stock_Transfer_Inter_Company' and dtl.to_node='DC_Brand_Int' AND dtl.po_doc_typ='NB')
OR (dtl.business_process='Stock_Transfer_Inter_Company' and dtl.to_node='FC' AND dtl.po_doc_typ='NB')
OR (dtl.business_process='Stock_Transfer_Inter_Company' and dtl.to_node='DC_Mast' AND dtl.po_doc_typ='YUB')
OR (dtl.business_process='Component_Inflow' and dtl.to_node='Vendor_Managed_Mast' AND dtl.po_doc_typ='UB')
OR (dtl.business_process='Stock_Transfer_Intra_Company' and dtl.to_node='DC_Brand' AND dtl.po_doc_typ='ZUB')
OR (dtl.business_process='Stock_Transfer_Intra_Company' and dtl.to_node='DC_Mast' AND dtl.po_doc_typ='UB')
OR (dtl.business_process='Managed_Production_Brand' and dtl.to_node='Vendor_Managed_Brand' AND dtl.po_doc_typ='NB')
OR (dtl.business_process='Stock_Transfer_Intra_Company' and dtl.to_node='Vendor_Managed_Mast' AND dtl.po_doc_typ='UB')
OR (dtl.business_process='Turnkey_Production' and dtl.to_node='DC_Rework' AND dtl.po_doc_typ='NB')
OR (dtl.business_process='VSD Inflow' and dtl.to_node='DC_VSD' AND dtl.po_doc_typ='ZBPO')
)
;

! echo Completed po_so_dd_hu_kpi_details data load;

-------------------------
---end script
-------------------------