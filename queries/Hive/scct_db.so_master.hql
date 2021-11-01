---jobname:scct-masterdata-hive-stg1-scct_db.so_master
--------------------------------------------------------------------------
---target: stage1 table: so_master
---source: scct_raw_db.maprdb_vbak_raw, scct_raw_db.vbpa_raw, scct_raw_db.maprdb_vbap_raw
---load type: delta
---back posting: no
-------------------------------------------------------------------------

-- History Information
--
-- Revision    Date     JIRA       Developer-ID Comments
-- ----------- ------- ----------  ------------ ----------------------------
-- 01.00.00.01 23MAY19             SSOMA        Initial Version
-- 01.00.00.02 23SEP19 SCCTP4-2471 RYADAV       Mapr Remediation changes for column names, item and location length
-- 01.00.00.03 26SEP19 SCCTP4-2481 RYADAV       added lpad in location join
-- 01.00.00.04 04NOV19 SCCTP4-2638 ssoma        Added cust_prch_itm_nbr(posex) column into so_master
-- 01.00.00.05 03JUN20 SCCTP4-1777 ashettap     Added chngind column into so_master
-- 01.00.00.06 03JUL20 SCCTP5-217  ashettap     Added 9 columns from sls_distr_doc_nbr through overall_sls_ord_blk_stat to so_master

-------------------------
---Job related Hive settings to read huge dataset and merge files for Target table
-------------------------

set hive.auto.convert.join.noconditionaltask.size=3000000000;
set mapreduce.input.fileinputformat.split.maxsize = 12000000;

-------------------------
---loading so_master stage 1 base table
-------------------------

set tez.job.name=stage1:load_so_master:load_so_master; 

! echo started loading into scct_db.so_master;

insert overwrite table scct_db.so_master
select
sls_ord_nbr
,sls_ord_itm_nbr
,itm_nbr
,deliv_itm_cat
,rej_rsn
,prod_hier
,doc_currcy_val
,tgt_sls_unt_qty
,tgt_qty_uom
,convn_fct_1
,convn_fct_2
,base_uom
,net_val
,currcy
,ord_qty_sls_unt
,req_deliv_qty_sls_unt
,confrmd_qty_sls_unt
,confrmd_qty_base_unt
,sls_unt
,num_convn
,denom_convn
,grs_wgt
,net_wgt
,wgt_unt
,vol
,vol_unt
,origtg_doc
,origtg_itm
,ref_itm_nbr
,deliv_prty
,coalesce(rcvl.location_number,tmp.recv_loc_nbr)
,coalesce(srl.location_number,tmp.ship_recv_loc_nbr)
,rec_crtd_dt
,rec_crtd_id
,rec_entr_tm
,net_prc
,cond_prc_unt
,cond_uom
,cost
,prc_cond_subt_1
,prc_cond_subt_2
,prc_cond_subt_3
,prc_cond_subt_4
,prc_cond_subt_5
,prc_cond_subt_6
,exchg_rt_stats
,rec_chgd_dt
,pctr
,spec_stk_ind
,prft_seg
,convn_fct
,reqms_type
,itm_cr_prc
,actv_cr_func
,cr_exchg_rt
,cnfgrn_intrnl_obj_nbr
,cust_cls
,cust_style
,style_desc
,orig_qty
,orig_uom
,orig_itm_prc
,orig_cust_itm_prc
,rev_cust_itm_prc
,cncl_dt
,dn_shp_dt
,cust_dept_cd
,doc_dt
,sls_doc_cat
,trans_grp
,sls_doc_type
,ord_rsn
,sls_doc_crcy
,sls_org
,distr_chnl
,division
,sls_grp
,sls_ofc
,nbr_doc_cond
,reqstd_dt
,prpsd_dt_type
,proc
,shp_cond
,sls_prob
,cust_prch_nbr
,cust_ord_type
,cust_prch_ord_dt
,nbr_cntc_cust
,cust_nbr
,upd_stats_grp
,stats_crcy
,cntlg_area
,cr_cntl_area
,cr_rsk
,cr_val_crcy
,rlsed_cr_val_doc
,hier_type_prc
,co_cd_billd
,ref_doc_nbr
,artcl_avail_dt
,guid_key
,tot_ord_qty
,orig_tot_prc
,rev_tot_prc
,cpo_stat
,cpo_type
,mast_nbr
,pre_tickt
,media_cd
,coalesce(shpl.location_number,tmp.ship_to_loc_nbr)
,cntry_cd
,shp_to_desc
,partn_func
,rndg_qty
,purch_ord_itm_nbr
,delv_grp
,qty_fixd
,unltd_od
,od_tolnc
,und_delv_tolnc
,btch_splt_alwd
,ref_doc_nbr_dtl
,compl_ref
,route
,intnl_prod_nbr
,cmpnt_qlty
,cust_lng_sku
,mast_style_nbr
,cust_color_cd
,cust_color_desc
,cust_sz_cd
,co_sz_desc
,pre_pack_mstr_sku
,pre_pack_sort_cd
,orig_ndc_dt
,dn_shp_erlr_dt
,cust_garment_cls
,vend_style_cd
,pre_pack_cd
,pack_type
,gac_dt
,pack_cnt
,corsp_artcl_nbr_shrd
,rec_crtd_dt_hdr
,rec_crtd_id_hdr
,delv_blk
,bllg_blk
,rec_chgd_dt_hdr
,ref_doc_nbr_hdr
,cntns_dg
,coorig_cd
,cust_prch_itm_nbr
,del_ind
,sls_ord_itm_delv_dt
,sls_ord_itm_req_dt
,sls_ord_itm_rsrvn_dt
,sls_ord_itm_pkg_rtrn_dt
,sls_ord_itm_transp_plng_dt
,sls_ord_itm_mtrl_stng_dt
,sls_ord_itm_loadg_dt
,sls_ord_itm_gds_iss_dt
,sls_ord_itm_arriv_tm
,sls_ord_itm_dt_typ
,sls_distr_doc_nbr
,sls_ord_del_stat
,overall_sls_ord_del_stat_all_itm
,overall_rejct_stat_all_sls_doc_itm
,overall_doc_prcssg_stat
,overall_cr_ck_stat
,overall_billg_blk_stat
,overall_sls_ord_del_blk_stat
,overall_sls_ord_blk_stat
from scct_work_db.so_master_tmp tmp
left outer join (select location_number from analytics_db.location where location_type_code ='04') shpl
on lpad(tmp.ship_to_loc_nbr,10,0)= shpl.location_number 
left outer join (select location_number from analytics_db.location where location_type_code ='04') rcvl
on lpad(tmp.recv_loc_nbr,10,0) = rcvl.location_number 
left outer join (select location_number from analytics_db.location where location_type_code ='04') srl
on lpad(tmp.ship_recv_loc_nbr,10,0) = srl.location_number;



! echo completed loading into scct_db.so_master;

-------------------------
---cocatenate files for target table so_master
-------------------------

SET tez.job.name=STAGE1:so_master:cocatenate files on so_master;

! echo Started cocatenate files;

alter table scct_db.so_master concatenate;

! echo Completed cocatenate files;

-------------------------
---END SCRIPT
-------------------------
