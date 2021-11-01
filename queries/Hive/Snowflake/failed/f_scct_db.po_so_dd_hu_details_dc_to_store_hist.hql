! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_store -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-2922 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter -- 01.00.00.03 04may20 scctp4-3331 ssoma hql optimization  -- 01.00.00.04 17jan21 scctp-601 kaali m replaced handling_master table with handling_master_poso  ----------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------    set hive.auto.convert.join.noconditionaltask.size=200000000;
------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  set tez.job.name=load detail data wiht our kpis:po_so_dd_hu_details :load target from po/dm/hm/inv ;
! echo started loading into scct_db.po_so_dd_hu_details;
insert overwrite table scct_db.po_so_dd_hu_details partition(business_process,to_node,po_doc_typ) select 'dc' as spply_chn_nd ,'dc_brand' as from_node ,po.purch_ord_nbr ,po.purch_ord_itm_nbr ,coalesce(po.del_ind,dm.del_ind,'n/a') as del_ind ,coalesce(po.itm_nbr,dm.itm_nbr,hm.itm_nbr,'0') as itm_nbr ,coalesce(po.co_cd,'n/a') ,coalesce(po.loc_nbr,'n/a') ,coalesce(po.purch_ord_qty,0.000) ,coalesce(po.purch_ord_qty_uom,'n/a') ,coalesce(po.ord_unt_nmrtr_convn_nbr,0) ,coalesce(po.base_unt_nmrtr_convn_nbr,0) ,coalesce(po.net_ord_val_curcy,0.00) ,coalesce(po.delv_compln_ind,'n/a') ,coalesce(po.base_uom_cd,'n/a') ,coalesce(po.purch_ord_qty*po.base_unt_nmrtr_convn_nbr/po.base_unt_dnmntr_convn_nbr,0.000) ,coalesce(po.mast_ownshp_dt,'9999-12-31') ,coalesce(po.orig_mast_ownshp_dt,'9999-12-31') ,coalesce(po.purch_doc_type_cd,'n/a') ,coalesce(case when trim(po.vend_acct_id)='' then null else trim(po.vend_acct_id) end,'n/a') ,coalesce(po.purchg_org_cd,'n/a') ,coalesce(po.purchg_grp_cd,'n/a') ,coalesce(po.cust_nbr,'n/a') ,coalesce(po.inctrm_1,dm.inctrm_1,'n/a') ,coalesce(po.inctrm_2,dm.inctrm_2,'n/a') ,coalesce(po.our_ref,'n/a') ,coalesce(po.transp_mode,'n/a') ,coalesce(po.sls_ord_nbr,'n/a') ,coalesce(po.sls_ord_itm_nbr,0) ,coalesce(po.coorig,'n/a') ,coalesce(po.ndc_dt,'9999-12-31') ,coalesce(po.stat_ndc_dt,'9999-12-31') ,coalesce(po.po_create_dt,'9999-12-31') ,coalesce(po.ship_to_loc_nbr,'n/a') ,coalesce(po.rec_crtd_dt,hm.rec_crtd_dt,'9999-12-31') as rec_crtd_dt ,coalesce(po.floorset_dt,'9999-12-31') ,'n/a' as so_sls_ord_nbr ,0 as so_sls_ord_itm_nbr ,'n/a' as so_itm_nbr ,0.00 as so_net_val ,0.000 as so_ord_qty_sls_unt ,'n/a' as so_recv_loc_nbr ,'9999-12-31' as so_rec_crtd_dt ,'n/a' as so_rec_crtd_id ,0.00 as so_cost ,'9999-12-31' as so_reqstd_dt ,'n/a' as so_cust_prch_nbr ,'n/a' as so_ship_to_loc_nbr ,'n/a' as so_cntry_cd ,'n/a' as so_sls_doc_type ,coalesce(dm.delv_doc_nbr,hm.delv_doc_nbr,'n/a') as delv_doc_nbr ,coalesce(dm.delv_doc_itm_nbr,hm.delv_doc_itm_nbr,0) as delv_doc_itm_nbr ,coalesce(dm.del_type,'n/a') ,coalesce(dm.act_delv_qty,0.000) ,coalesce(dm.actl_gds_movmt_dt,'9999-12-31') ,coalesce(dm.delv_prty,0) ,coalesce(dm.delv_qty,0.000) ,coalesce(dm.gds_iss_date,'9999-12-31') ,coalesce(dm.rcvg_ste_delv,'n/a') ,coalesce(dm.hdr_rec_crtd_dt,'9999-12-31') ,coalesce(dm.ref_doc_itm_nbr,0) ,coalesce(dm.ref_doc_nbr,'n/a') ,coalesce(dm.shp_rcvg_pt,'n/a') ,coalesce(dm.gds_movmt_cntl,'n/a') ,coalesce(dm.movmt_ind,'n/a') ,coalesce(dm.pod_ind,'n/a') ,coalesce(dm.pod_rlse_ind,'n/a') ,coalesce(dm.recv_loc_nbr,'n/a') ,coalesce(dm.transp_type,'n/a') ,coalesce(dm.transp_id,'n/a') ,coalesce(hm.intl_hndlg_unt_nbr,'n/a') ,coalesce(hm.ext_hndlg_unt_nbr,'n/a') ,coalesce(hm.ext_hndlg_unt_nbr_2,'n/a') ,coalesce(hm.ext_hndlg_unt_type,'n/a') ,coalesce(hm.pckg_mat_type,'n/a') ,coalesce(hm.pckd_qty,0.000) ,coalesce(hm.pckd_uom,'n/a') ,coalesce(ium.numerator,0.00) ,coalesce(hm.grs_wgt,0.000) ,coalesce(hm.wgt_unt,'n/a') ,coalesce(hm.len,0.000) ,coalesce(hm.wdt,0.000) ,coalesce(hm.hgt,0.000) ,coalesce(hm.dim_uom,'n/a') ,coalesce(hm.rec_chgd_dt,'9999-12-31') ,coalesce(hm.hndlg_unt_stat,'n/a') ,coalesce(hm.bol_dt,'9999-12-31') ,coalesce(hm.str_dt,'9999-12-31') ,coalesce(hm.da_scan_dt,'9999-12-31') ,coalesce(hm.load_nbr ,0) ,coalesce(case when trim(po.vend_acct_id)='' then null else trim(po.vend_acct_id) end,'n/a') as supl_loc_nbr ,coalesce(po.loc_nbr,'n/a') as recv_loc_nbr ,'stock_transfer_inter_company' as business_process ,'store' as to_node ,po.purch_doc_type_cd as po_doc_typ from (select * from scct_db.po_master where purch_doc_type_cd='nb' and (del_ind<>'l' or (del_ind='l' and gr_qty>0)) and vend_acct_id<>'n/a' and loc_nbr<>'n/a') po  inner join (select * from scct_db.location_master where loc_typ_cd='01') a on po.vend_acct_id=a.loc_nbr  inner join (select * from scct_db.location_master where loc_typ_cd='04') b on po.loc_nbr=b.loc_nbr  left outer join (select * from scct_db.delivery_master where del_ind<>'d' and del_type not in('el','elst') and itm_nbr<>'0') dm  on po.purch_ord_nbr=dm.ref_doc_nbr and po.purch_ord_itm_nbr= dm.ref_doc_itm_nbr and po.itm_nbr=dm.itm_nbr  left outer join (select * from scct_db.handling_master_poso where itm_nbr<>'0') hm  on dm.delv_doc_itm_nbr=hm.delv_doc_itm_nbr and dm.delv_doc_nbr=hm.delv_doc_nbr and po.itm_nbr=hm.itm_nbr left outer join (select * from scct_db.item_unit_measure_master where trim(itm_nbr)<>'') ium on dm.itm_nbr=ium.itm_nbr and hm.pckd_uom=ium.alt_uom
! echo completed po_so_dd_hu_details data load;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_store -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-2922 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter -- 01.00.00.03 04may20 scctp4-3331 ssoma hql optimization  -- 01.00.00.04 17jan21 scctp-601 kaali m replaced handling_master table with handling_master_poso  ----------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------    set hive.auto.convert.join.noconditionaltask.size=200000000;
------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  set tez.job.name=load detail data wiht our kpis:po_so_dd_hu_details :load target from po/dm/hm/inv ;
! echo started loading into scct_db.po_so_dd_hu_details;
insert overwrite table scct_db.po_so_dd_hu_details partition(business_process,to_node,po_doc_typ) select 'dc' as spply_chn_nd ,'dc_brand' as from_node ,po.purch_ord_nbr ,po.purch_ord_itm_nbr ,coalesce(po.del_ind,dm.del_ind,'n/a') as del_ind ,coalesce(po.itm_nbr,dm.itm_nbr,hm.itm_nbr,'0') as itm_nbr ,coalesce(po.co_cd,'n/a') ,coalesce(po.loc_nbr,'n/a') ,coalesce(po.purch_ord_qty,0.000) ,coalesce(po.purch_ord_qty_uom,'n/a') ,coalesce(po.ord_unt_nmrtr_convn_nbr,0) ,coalesce(po.base_unt_nmrtr_convn_nbr,0) ,coalesce(po.net_ord_val_curcy,0.00) ,coalesce(po.delv_compln_ind,'n/a') ,coalesce(po.base_uom_cd,'n/a') ,coalesce(po.purch_ord_qty*po.base_unt_nmrtr_convn_nbr/po.base_unt_dnmntr_convn_nbr,0.000) ,coalesce(po.mast_ownshp_dt,'9999-12-31') ,coalesce(po.orig_mast_ownshp_dt,'9999-12-31') ,coalesce(po.purch_doc_type_cd,'n/a') ,coalesce(case when trim(po.vend_acct_id)='' then null else trim(po.vend_acct_id) end,'n/a') ,coalesce(po.purchg_org_cd,'n/a') ,coalesce(po.purchg_grp_cd,'n/a') ,coalesce(po.cust_nbr,'n/a') ,coalesce(po.inctrm_1,dm.inctrm_1,'n/a') ,coalesce(po.inctrm_2,dm.inctrm_2,'n/a') ,coalesce(po.our_ref,'n/a') ,coalesce(po.transp_mode,'n/a') ,coalesce(po.sls_ord_nbr,'n/a') ,coalesce(po.sls_ord_itm_nbr,0) ,coalesce(po.coorig,'n/a') ,coalesce(po.ndc_dt,'9999-12-31') ,coalesce(po.stat_ndc_dt,'9999-12-31') ,coalesce(po.po_create_dt,'9999-12-31') ,coalesce(po.ship_to_loc_nbr,'n/a') ,coalesce(po.rec_crtd_dt,hm.rec_crtd_dt,'9999-12-31') as rec_crtd_dt ,coalesce(po.floorset_dt,'9999-12-31') ,'n/a' as so_sls_ord_nbr ,0 as so_sls_ord_itm_nbr ,'n/a' as so_itm_nbr ,0.00 as so_net_val ,0.000 as so_ord_qty_sls_unt ,'n/a' as so_recv_loc_nbr ,'9999-12-31' as so_rec_crtd_dt ,'n/a' as so_rec_crtd_id ,0.00 as so_cost ,'9999-12-31' as so_reqstd_dt ,'n/a' as so_cust_prch_nbr ,'n/a' as so_ship_to_loc_nbr ,'n/a' as so_cntry_cd ,'n/a' as so_sls_doc_type ,coalesce(dm.delv_doc_nbr,hm.delv_doc_nbr,'n/a') as delv_doc_nbr ,coalesce(dm.delv_doc_itm_nbr,hm.delv_doc_itm_nbr,0) as delv_doc_itm_nbr ,coalesce(dm.del_type,'n/a') ,coalesce(dm.act_delv_qty,0.000) ,coalesce(dm.actl_gds_movmt_dt,'9999-12-31') ,coalesce(dm.delv_prty,0) ,coalesce(dm.delv_qty,0.000) ,coalesce(dm.gds_iss_date,'9999-12-31') ,coalesce(dm.rcvg_ste_delv,'n/a') ,coalesce(dm.hdr_rec_crtd_dt,'9999-12-31') ,coalesce(dm.ref_doc_itm_nbr,0) ,coalesce(dm.ref_doc_nbr,'n/a') ,coalesce(dm.shp_rcvg_pt,'n/a') ,coalesce(dm.gds_movmt_cntl,'n/a') ,coalesce(dm.movmt_ind,'n/a') ,coalesce(dm.pod_ind,'n/a') ,coalesce(dm.pod_rlse_ind,'n/a') ,coalesce(dm.recv_loc_nbr,'n/a') ,coalesce(dm.transp_type,'n/a') ,coalesce(dm.transp_id,'n/a') ,coalesce(hm.intl_hndlg_unt_nbr,'n/a') ,coalesce(hm.ext_hndlg_unt_nbr,'n/a') ,coalesce(hm.ext_hndlg_unt_nbr_2,'n/a') ,coalesce(hm.ext_hndlg_unt_type,'n/a') ,coalesce(hm.pckg_mat_type,'n/a') ,coalesce(hm.pckd_qty,0.000) ,coalesce(hm.pckd_uom,'n/a') ,coalesce(ium.numerator,0.00) ,coalesce(hm.grs_wgt,0.000) ,coalesce(hm.wgt_unt,'n/a') ,coalesce(hm.len,0.000) ,coalesce(hm.wdt,0.000) ,coalesce(hm.hgt,0.000) ,coalesce(hm.dim_uom,'n/a') ,coalesce(hm.rec_chgd_dt,'9999-12-31') ,coalesce(hm.hndlg_unt_stat,'n/a') ,coalesce(hm.bol_dt,'9999-12-31') ,coalesce(hm.str_dt,'9999-12-31') ,coalesce(hm.da_scan_dt,'9999-12-31') ,coalesce(hm.load_nbr ,0) ,coalesce(case when trim(po.vend_acct_id)='' then null else trim(po.vend_acct_id) end,'n/a') as supl_loc_nbr ,coalesce(po.loc_nbr,'n/a') as recv_loc_nbr ,'stock_transfer_inter_company' as business_process ,'store' as to_node ,po.purch_doc_type_cd as po_doc_typ from (select * from scct_db.po_master where purch_doc_type_cd='nb' and (del_ind<>'l' or (del_ind='l' and gr_qty>0)) and vend_acct_id<>'n/a' and loc_nbr<>'n/a') po  inner join (select * from scct_db.location_master where loc_typ_cd='01') a on po.vend_acct_id=a.loc_nbr  inner join (select * from scct_db.location_master where loc_typ_cd='04') b on po.loc_nbr=b.loc_nbr  left outer join (select * from scct_db.delivery_master where del_ind<>'d' and del_type not in('el','elst') and itm_nbr<>'0') dm  on po.purch_ord_nbr=dm.ref_doc_nbr and po.purch_ord_itm_nbr= dm.ref_doc_itm_nbr and po.itm_nbr=dm.itm_nbr  left outer join (select * from scct_db.handling_master_poso where itm_nbr<>'0') hm  on dm.delv_doc_itm_nbr=hm.delv_doc_itm_nbr and dm.delv_doc_nbr=hm.delv_doc_nbr and po.itm_nbr=hm.itm_nbr left outer join (select * from scct_db.item_unit_measure_master where trim(itm_nbr)<>'') ium on dm.itm_nbr=ium.itm_nbr and hm.pckd_uom=ium.alt_uom
! echo completed po_so_dd_hu_details data load;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_store -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-2922 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter -- 01.00.00.03 04may20 scctp4-3331 ssoma hql optimization  -- 01.00.00.04 17jan21 scctp-601 kaali m replaced handling_master table with handling_master_poso  ----------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------    set hive.auto.convert.join.noconditionaltask.size=200000000;
------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  set tez.job.name=load detail data wiht our kpis:po_so_dd_hu_details :load target from po/dm/hm/inv ;
! echo started loading into scct_db.po_so_dd_hu_details;
insert overwrite table scct_db.po_so_dd_hu_details partition(business_process,to_node,po_doc_typ) select 'dc' as spply_chn_nd ,'dc_brand' as from_node ,po.purch_ord_nbr ,po.purch_ord_itm_nbr ,coalesce(po.del_ind,dm.del_ind,'n/a') as del_ind ,coalesce(po.itm_nbr,dm.itm_nbr,hm.itm_nbr,'0') as itm_nbr ,coalesce(po.co_cd,'n/a') ,coalesce(po.loc_nbr,'n/a') ,coalesce(po.purch_ord_qty,0.000) ,coalesce(po.purch_ord_qty_uom,'n/a') ,coalesce(po.ord_unt_nmrtr_convn_nbr,0) ,coalesce(po.base_unt_nmrtr_convn_nbr,0) ,coalesce(po.net_ord_val_curcy,0.00) ,coalesce(po.delv_compln_ind,'n/a') ,coalesce(po.base_uom_cd,'n/a') ,coalesce(po.purch_ord_qty*po.base_unt_nmrtr_convn_nbr/po.base_unt_dnmntr_convn_nbr,0.000) ,coalesce(po.mast_ownshp_dt,'9999-12-31') ,coalesce(po.orig_mast_ownshp_dt,'9999-12-31') ,coalesce(po.purch_doc_type_cd,'n/a') ,coalesce(case when trim(po.vend_acct_id)='' then null else trim(po.vend_acct_id) end,'n/a') ,coalesce(po.purchg_org_cd,'n/a') ,coalesce(po.purchg_grp_cd,'n/a') ,coalesce(po.cust_nbr,'n/a') ,coalesce(po.inctrm_1,dm.inctrm_1,'n/a') ,coalesce(po.inctrm_2,dm.inctrm_2,'n/a') ,coalesce(po.our_ref,'n/a') ,coalesce(po.transp_mode,'n/a') ,coalesce(po.sls_ord_nbr,'n/a') ,coalesce(po.sls_ord_itm_nbr,0) ,coalesce(po.coorig,'n/a') ,coalesce(po.ndc_dt,'9999-12-31') ,coalesce(po.stat_ndc_dt,'9999-12-31') ,coalesce(po.po_create_dt,'9999-12-31') ,coalesce(po.ship_to_loc_nbr,'n/a') ,coalesce(po.rec_crtd_dt,hm.rec_crtd_dt,'9999-12-31') as rec_crtd_dt ,coalesce(po.floorset_dt,'9999-12-31') ,'n/a' as so_sls_ord_nbr ,0 as so_sls_ord_itm_nbr ,'n/a' as so_itm_nbr ,0.00 as so_net_val ,0.000 as so_ord_qty_sls_unt ,'n/a' as so_recv_loc_nbr ,'9999-12-31' as so_rec_crtd_dt ,'n/a' as so_rec_crtd_id ,0.00 as so_cost ,'9999-12-31' as so_reqstd_dt ,'n/a' as so_cust_prch_nbr ,'n/a' as so_ship_to_loc_nbr ,'n/a' as so_cntry_cd ,'n/a' as so_sls_doc_type ,coalesce(dm.delv_doc_nbr,hm.delv_doc_nbr,'n/a') as delv_doc_nbr ,coalesce(dm.delv_doc_itm_nbr,hm.delv_doc_itm_nbr,0) as delv_doc_itm_nbr ,coalesce(dm.del_type,'n/a') ,coalesce(dm.act_delv_qty,0.000) ,coalesce(dm.actl_gds_movmt_dt,'9999-12-31') ,coalesce(dm.delv_prty,0) ,coalesce(dm.delv_qty,0.000) ,coalesce(dm.gds_iss_date,'9999-12-31') ,coalesce(dm.rcvg_ste_delv,'n/a') ,coalesce(dm.hdr_rec_crtd_dt,'9999-12-31') ,coalesce(dm.ref_doc_itm_nbr,0) ,coalesce(dm.ref_doc_nbr,'n/a') ,coalesce(dm.shp_rcvg_pt,'n/a') ,coalesce(dm.gds_movmt_cntl,'n/a') ,coalesce(dm.movmt_ind,'n/a') ,coalesce(dm.pod_ind,'n/a') ,coalesce(dm.pod_rlse_ind,'n/a') ,coalesce(dm.recv_loc_nbr,'n/a') ,coalesce(dm.transp_type,'n/a') ,coalesce(dm.transp_id,'n/a') ,coalesce(hm.intl_hndlg_unt_nbr,'n/a') ,coalesce(hm.ext_hndlg_unt_nbr,'n/a') ,coalesce(hm.ext_hndlg_unt_nbr_2,'n/a') ,coalesce(hm.ext_hndlg_unt_type,'n/a') ,coalesce(hm.pckg_mat_type,'n/a') ,coalesce(hm.pckd_qty,0.000) ,coalesce(hm.pckd_uom,'n/a') ,coalesce(ium.numerator,0.00) ,coalesce(hm.grs_wgt,0.000) ,coalesce(hm.wgt_unt,'n/a') ,coalesce(hm.len,0.000) ,coalesce(hm.wdt,0.000) ,coalesce(hm.hgt,0.000) ,coalesce(hm.dim_uom,'n/a') ,coalesce(hm.rec_chgd_dt,'9999-12-31') ,coalesce(hm.hndlg_unt_stat,'n/a') ,coalesce(hm.bol_dt,'9999-12-31') ,coalesce(hm.str_dt,'9999-12-31') ,coalesce(hm.da_scan_dt,'9999-12-31') ,coalesce(hm.load_nbr ,0) ,coalesce(case when trim(po.vend_acct_id)='' then null else trim(po.vend_acct_id) end,'n/a') as supl_loc_nbr ,coalesce(po.loc_nbr,'n/a') as recv_loc_nbr ,'stock_transfer_inter_company' as business_process ,'store' as to_node ,po.purch_doc_type_cd as po_doc_typ from (select * from scct_db.po_master where purch_doc_type_cd='nb' and (del_ind<>'l' or (del_ind='l' and gr_qty>0)) and vend_acct_id<>'n/a' and loc_nbr<>'n/a') po  inner join (select * from scct_db.location_master where loc_typ_cd='01') a on po.vend_acct_id=a.loc_nbr  inner join (select * from scct_db.location_master where loc_typ_cd='04') b on po.loc_nbr=b.loc_nbr  left outer join (select * from scct_db.delivery_master where del_ind<>'d' and del_type not in('el','elst') and itm_nbr<>'0') dm  on po.purch_ord_nbr=dm.ref_doc_nbr and po.purch_ord_itm_nbr= dm.ref_doc_itm_nbr and po.itm_nbr=dm.itm_nbr  left outer join (select * from scct_db.handling_master_poso where itm_nbr<>'0') hm  on dm.delv_doc_itm_nbr=hm.delv_doc_itm_nbr and dm.delv_doc_nbr=hm.delv_doc_nbr and po.itm_nbr=hm.itm_nbr left outer join (select * from scct_db.item_unit_measure_master where trim(itm_nbr)<>'') ium on dm.itm_nbr=ium.itm_nbr and hm.pckd_uom=ium.alt_uom
! echo completed po_so_dd_hu_details data load;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_store -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-2922 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter -- 01.00.00.03 04may20 scctp4-3331 ssoma hql optimization  -- 01.00.00.04 17jan21 scctp-601 kaali m replaced handling_master table with handling_master_poso  ----------------------------------------------------------------------------  ------------------------- ---job related hive settings  -------------------------    set hive.auto.convert.join.noconditionaltask.size=200000000;
------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  set tez.job.name=load detail data wiht our kpis:po_so_dd_hu_details :load target from po/dm/hm/inv ;
! echo started loading into scct_db.po_so_dd_hu_details;
insert overwrite table scct_db.po_so_dd_hu_details partition(business_process,to_node,po_doc_typ) select 'dc' as spply_chn_nd ,'dc_brand' as from_node ,po.purch_ord_nbr ,po.purch_ord_itm_nbr ,coalesce(po.del_ind,dm.del_ind,'n/a') as del_ind ,coalesce(po.itm_nbr,dm.itm_nbr,hm.itm_nbr,'0') as itm_nbr ,coalesce(po.co_cd,'n/a') ,coalesce(po.loc_nbr,'n/a') ,coalesce(po.purch_ord_qty,0.000) ,coalesce(po.purch_ord_qty_uom,'n/a') ,coalesce(po.ord_unt_nmrtr_convn_nbr,0) ,coalesce(po.base_unt_nmrtr_convn_nbr,0) ,coalesce(po.net_ord_val_curcy,0.00) ,coalesce(po.delv_compln_ind,'n/a') ,coalesce(po.base_uom_cd,'n/a') ,coalesce(po.purch_ord_qty*po.base_unt_nmrtr_convn_nbr/po.base_unt_dnmntr_convn_nbr,0.000) ,coalesce(po.mast_ownshp_dt,'9999-12-31') ,coalesce(po.orig_mast_ownshp_dt,'9999-12-31') ,coalesce(po.purch_doc_type_cd,'n/a') ,coalesce(case when trim(po.vend_acct_id)='' then null else trim(po.vend_acct_id) end,'n/a') ,coalesce(po.purchg_org_cd,'n/a') ,coalesce(po.purchg_grp_cd,'n/a') ,coalesce(po.cust_nbr,'n/a') ,coalesce(po.inctrm_1,dm.inctrm_1,'n/a') ,coalesce(po.inctrm_2,dm.inctrm_2,'n/a') ,coalesce(po.our_ref,'n/a') ,coalesce(po.transp_mode,'n/a') ,coalesce(po.sls_ord_nbr,'n/a') ,coalesce(po.sls_ord_itm_nbr,0) ,coalesce(po.coorig,'n/a') ,coalesce(po.ndc_dt,'9999-12-31') ,coalesce(po.stat_ndc_dt,'9999-12-31') ,coalesce(po.po_create_dt,'9999-12-31') ,coalesce(po.ship_to_loc_nbr,'n/a') ,coalesce(po.rec_crtd_dt,hm.rec_crtd_dt,'9999-12-31') as rec_crtd_dt ,coalesce(po.floorset_dt,'9999-12-31') ,'n/a' as so_sls_ord_nbr ,0 as so_sls_ord_itm_nbr ,'n/a' as so_itm_nbr ,0.00 as so_net_val ,0.000 as so_ord_qty_sls_unt ,'n/a' as so_recv_loc_nbr ,'9999-12-31' as so_rec_crtd_dt ,'n/a' as so_rec_crtd_id ,0.00 as so_cost ,'9999-12-31' as so_reqstd_dt ,'n/a' as so_cust_prch_nbr ,'n/a' as so_ship_to_loc_nbr ,'n/a' as so_cntry_cd ,'n/a' as so_sls_doc_type ,coalesce(dm.delv_doc_nbr,hm.delv_doc_nbr,'n/a') as delv_doc_nbr ,coalesce(dm.delv_doc_itm_nbr,hm.delv_doc_itm_nbr,0) as delv_doc_itm_nbr ,coalesce(dm.del_type,'n/a') ,coalesce(dm.act_delv_qty,0.000) ,coalesce(dm.actl_gds_movmt_dt,'9999-12-31') ,coalesce(dm.delv_prty,0) ,coalesce(dm.delv_qty,0.000) ,coalesce(dm.gds_iss_date,'9999-12-31') ,coalesce(dm.rcvg_ste_delv,'n/a') ,coalesce(dm.hdr_rec_crtd_dt,'9999-12-31') ,coalesce(dm.ref_doc_itm_nbr,0) ,coalesce(dm.ref_doc_nbr,'n/a') ,coalesce(dm.shp_rcvg_pt,'n/a') ,coalesce(dm.gds_movmt_cntl,'n/a') ,coalesce(dm.movmt_ind,'n/a') ,coalesce(dm.pod_ind,'n/a') ,coalesce(dm.pod_rlse_ind,'n/a') ,coalesce(dm.recv_loc_nbr,'n/a') ,coalesce(dm.transp_type,'n/a') ,coalesce(dm.transp_id,'n/a') ,coalesce(hm.intl_hndlg_unt_nbr,'n/a') ,coalesce(hm.ext_hndlg_unt_nbr,'n/a') ,coalesce(hm.ext_hndlg_unt_nbr_2,'n/a') ,coalesce(hm.ext_hndlg_unt_type,'n/a') ,coalesce(hm.pckg_mat_type,'n/a') ,coalesce(hm.pckd_qty,0.000) ,coalesce(hm.pckd_uom,'n/a') ,coalesce(ium.numerator,0.00) ,coalesce(hm.grs_wgt,0.000) ,coalesce(hm.wgt_unt,'n/a') ,coalesce(hm.len,0.000) ,coalesce(hm.wdt,0.000) ,coalesce(hm.hgt,0.000) ,coalesce(hm.dim_uom,'n/a') ,coalesce(hm.rec_chgd_dt,'9999-12-31') ,coalesce(hm.hndlg_unt_stat,'n/a') ,coalesce(hm.bol_dt,'9999-12-31') ,coalesce(hm.str_dt,'9999-12-31') ,coalesce(hm.da_scan_dt,'9999-12-31') ,coalesce(hm.load_nbr ,0) ,coalesce(case when trim(po.vend_acct_id)='' then null else trim(po.vend_acct_id) end,'n/a') as supl_loc_nbr ,coalesce(po.loc_nbr,'n/a') as recv_loc_nbr ,'stock_transfer_inter_company' as business_process ,'store' as to_node ,po.purch_doc_type_cd as po_doc_typ from (select * from scct_db.po_master where purch_doc_type_cd='nb' and (del_ind<>'l' or (del_ind='l' and gr_qty>0)) and vend_acct_id<>'n/a' and loc_nbr<>'n/a') po  inner join (select * from scct_db.location_master where loc_typ_cd='01') a on po.vend_acct_id=a.loc_nbr  inner join (select * from scct_db.location_master where loc_typ_cd='04') b on po.loc_nbr=b.loc_nbr  left outer join (select * from scct_db.delivery_master where del_ind<>'d' and del_type not in('el','elst') and itm_nbr<>'0') dm  on po.purch_ord_nbr=dm.ref_doc_nbr and po.purch_ord_itm_nbr= dm.ref_doc_itm_nbr and po.itm_nbr=dm.itm_nbr  left outer join (select * from scct_db.handling_master_poso where itm_nbr<>'0') hm  on dm.delv_doc_itm_nbr=hm.delv_doc_itm_nbr and dm.delv_doc_nbr=hm.delv_doc_nbr and po.itm_nbr=hm.itm_nbr left outer join (select * from scct_db.item_unit_measure_master where trim(itm_nbr)<>'') ium on dm.itm_nbr=ium.itm_nbr and hm.pckd_uom=ium.alt_uom
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_dc -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-2923 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter ----------------------------------------------------------------------------  ------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  ! echo started loading into scct_db.po_so_dd_hu_details;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_dc_intra -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-3287 ssoma dc to dc intra initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter ----------------------------------------------------------------------------  ------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  ! echo started loading into scct_db.po_so_dd_hu_details;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_store -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-2922 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter -- 01.00.00.03 04may20 scctp4-3331 ssoma hql optimization  -- 01.00.00.04 17jan21 scctp-601 kaali m replaced handling_master table with handling_master_poso  ----------------------------------------------------------------------------  ------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  ! echo started loading into scct_db.po_so_dd_hu_details;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_dc -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-2923 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter ----------------------------------------------------------------------------  ------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  ! echo started loading into scct_db.po_so_dd_hu_details;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_dc_intra -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-3287 ssoma dc to dc intra initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter ----------------------------------------------------------------------------  ------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  ! echo started loading into scct_db.po_so_dd_hu_details;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_store -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-2922 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter -- 01.00.00.03 04may20 scctp4-3331 ssoma hql optimization  -- 01.00.00.04 17jan21 scctp-601 kaali m replaced handling_master table with handling_master_poso  ----------------------------------------------------------------------------  ------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  ! echo started loading into scct_db.po_so_dd_hu_details;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_dc -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-2923 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter ----------------------------------------------------------------------------  ------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  ! echo started loading into scct_db.po_so_dd_hu_details;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_dc_intra -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-3287 ssoma dc to dc intra initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter ----------------------------------------------------------------------------  ------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  ! echo started loading into scct_db.po_so_dd_hu_details;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_store -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-2922 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter -- 01.00.00.03 04may20 scctp4-3331 ssoma hql optimization  -- 01.00.00.04 17jan21 scctp-601 kaali m replaced handling_master table with handling_master_poso  ----------------------------------------------------------------------------  ------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  ! echo started loading into scct_db.po_so_dd_hu_details;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_dc -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-2923 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter ----------------------------------------------------------------------------  ------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  ! echo started loading into scct_db.po_so_dd_hu_details;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_dc_intra -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-3287 ssoma dc to dc intra initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter ----------------------------------------------------------------------------  ------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  ! echo started loading into scct_db.po_so_dd_hu_details;
! echo completed po_so_dd_hu_details data load;
---jobname:scct-ufposo-hive-stg1-po_so_dd_hu_details_dc_to_store -------------------------------------------------------------------------- ---target: stage1 table: scct_db.po_so_dd_hu_details  ---source: tms_stage0 table : po_master, delivery_master, inventory_movement,  ---handling_master and item_unit_measure_master ---load type: full ---back posting: no -- -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ -------------------- -- 01.00.00.01 20aug19 scctp4-2922 ssoma initial version -- 01.00.00.02 06mar20 scctp4-3316 ssoma remove reducer parameter -- 01.00.00.03 04may20 scctp4-3331 ssoma hql optimization  -- 01.00.00.04 17jan21 scctp-601 kaali m replaced handling_master table with handling_master_poso  ----------------------------------------------------------------------------  ------------------------- ---loading po line kpi's data into po_so_dd_hu_details table -------------------------  ! echo started loading into scct_db.po_so_dd_hu_details;
! echo completed po_so_dd_hu_details data load;
