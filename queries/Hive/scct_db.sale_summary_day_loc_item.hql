---jobname:scct-car-sales-hive-scct_db.sale_summary_day_loc_item
--------------------------------------------------------------------------
---target: table: scct_db.sale_summary_day_loc_item
---source: tables: ANALYTICS_DB.SALE_SUMMARY_DAY_LOC_ITEM, analytics_db.transaction_line_summary_ref, scct_db.car_pos_xref
---load type: incremental
---back posting: yes
--
-- History Information
--
-- Revision    Date     	JIRA       	Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 13NOV19		SCCTP4-2546		mgohain			Initial Version
-- 01.00.00.02 10DEC19		SCCTP4-2836		mgohain			NULL value handled
-- 01.00.00.03 16JAN20		SCCTP4-2836		mgohain			Added crcy_exch_rt and location table and corrected logic
-- 01.00.00.04 04FEB20		SCCTP4-3071		mgohain			Added discount columns
-- 01.00.00.05 07FEB20		SCCTP4-3072		mgohain			Added metrics for clearance and giveaway
-- 01.00.00.06 26FEB20		SCCTP4-3100		mgohain			Added metrics for Promo and Regular sales
-- 01.00.00.07 09MAR20		SCCTP4-3266		Abhishek		Added the trim function
-- 01.00.00.08 18MAR20		SCCTP4-3289		ryadav			Removed redundant joins,refactored code and added job setting for optimisation
-- 01.00.00.09 24MAR20		SCCTP4-3291		ryadav			Added ORD_TYP_CD column and added round() function to metrics
-- 01.00.00.10 30MAR20		MBDP-1645		ryadav			Added LAST_UPDATED_TIMESTAMP column
-- 01.00.00.10 13MAY20		SCCTP4-3332		akoti			Removed Product_Margin and Prod_AUR columns also corrected the ref table join to inner to avoid full refresh
--------------------------------------------------------------------------

-------------------------
---Job related Hive settings to merge files for Target table
-------------------------

-------------------------
---Loading scct_db.sale_summary_day_loc_item table data
-------------------------

insert overwrite table scct_db.sale_summary_day_loc_item partition(TRXN_DT)
select
a.LOC_NBR
,a.ITM_NBR
,a.ORD_TYP_CD
,a.TOT_SLS_UNT_QTY
,a.TOT_SLS_RTL_AMT
,a.TOT_SLS_CST_AMT
,a.RTN_UNT_QTY
,a.RTN_RTL_AMT
,a.RTN_CST_AMT
,a.GRS_SLS_UNT_QTY
,a.GRS_SLS_RTL_AMT
,a.GRS_SLS_CST_AMT
,a.NET_SLS_UNT_QTY
,a.NET_SLS_RTL_AMT
,a.NET_SLS_CST_AMT
,a.NET_SLS_AVG_UNT_RTL_AMT
,a.NET_SLS_AVG_UNT_CST_AMT
,a.TOT_SLS_AVG_UNT_RTL_AMT
,a.TOT_SLS_AVG_UNT_CST_AMT
--,a.NET_SLS_PROD_MRGN_AMT
--,a.TOT_SLS_PROD_MRGN_AMT
--,a.NET_SLS_PROD_MRGN_PCT
--,a.Prod_AUR
,a.assc_disc_amt
,a.disc_unt_amt
,a.tot_clr_sls_rtl_amt
,a.tot_clr_sls_unt
,a.tot_clr_sls_cst_amt
,coalesce(round(a.tot_clr_sls_cst_amt/a.tot_clr_sls_unt,2), 0.0000) as tot_clr_sls_auc
,coalesce(round(a.tot_clr_sls_rtl_amt/a.tot_clr_sls_unt,2), 0.0000) as tot_clr_sls_aur
,coalesce(round(a.tot_clr_sls_rtl_amt - a.tot_clr_sls_cst_amt,2), 0.0000) as tot_clr_sls_prd_amt
,coalesce(round((a.tot_clr_sls_rtl_amt - a.tot_clr_sls_cst_amt)/a.tot_clr_sls_rtl_amt,2), 0.0000) as tot_clr_sls_prd_mrgn_prcntg
,a.tot_gvawy_sls_rtl_amt
,a.tot_gvawy_sls_unt
,a.tot_gvawy_sls_cst_amt
,coalesce(round(a.tot_gvawy_sls_cst_amt/a.tot_gvawy_sls_unt,2), 0.0000) as tot_gvawy_sls_auc
,0.0000 as tot_gvawy_sls_aur
,coalesce(round((a.tot_gvawy_sls_rtl_amt - a.tot_gvawy_sls_cst_amt),2), 0.0000) as tot_gvawy_sls_prd_mrgn_amt
,0.0000 as tot_gvawy_sls_prd_mrgn_prcntg
,a.tot_prmo_sls_rtl_amt
,a.tot_prmo_sls_unt
,a.tot_prmo_sls_cst_amt
,coalesce(round(a.tot_prmo_sls_cst_amt/a.tot_prmo_sls_unt,2), 0.0000) as tot_prmo_sls_auc
,coalesce(round(a.tot_prmo_sls_rtl_amt/a.tot_prmo_sls_unt,2), 0.0000) as tot_prmo_sls_aur
,coalesce(round(a.tot_prmo_sls_rtl_amt - a.tot_prmo_sls_cst_amt,2), 0.0000) as tot_prmo_sls_prd_mrgn_amt
,coalesce(round((a.tot_prmo_sls_rtl_amt - a.tot_prmo_sls_cst_amt)/a.tot_prmo_sls_rtl_amt,2), 0.0000) as tot_prmo_sls_prd_mrgn_prcntg
,a.tot_reg_sls_rtl_amt
,a.tot_reg_sls_unt
,a.tot_reg_sls_cst_amt
,coalesce(round(a.tot_reg_sls_cst_amt/a.tot_reg_sls_unt,2), 0.0000) as tot_reg_sls_auc
,coalesce(round(a.tot_reg_sls_rtl_amt/a.tot_reg_sls_unt,2), 0.0000) as tot_reg_sls_aur
,coalesce(round(a.tot_reg_sls_rtl_amt - a.tot_reg_sls_cst_amt,2), 0.0000) as tot_reg_sls_prd_mrgn_amt
,coalesce(round((a.tot_reg_sls_rtl_amt - a.tot_reg_sls_cst_amt)/a.tot_reg_sls_rtl_amt,2), 0.0000) as tot_reg_sls_prd_mrgn_prcntg
,a.LAST_UPDATED_TIMESTAMP
,a.TRXN_DT
from
(SELECT
s.LOC_NBR
,s.ITM_NBR
,s.ORD_TYP_CD
,coalesce(round(SUM(s.SLS_UNT_EXCL_VOID_QTY + s.RTN_UNT_EXCL_VOID_QTY),2), 0.0)  AS TOT_SLS_UNT_QTY
,coalesce(round(SUM((s.SLS_UNT_EXCL_VOID_AMT + s.RTN_UNT_EXCL_VOID_AMT + s.ASSC_DISC_AMT) * s.curr_exch_rt),2), 0.0000)  AS TOT_SLS_RTL_AMT
,coalesce(round(SUM((s.EXT_SLS_CST_AMT + s.EXT_RTN_CST_AMT)* s.curr_exch_rt),2), 0.0000)  AS TOT_SLS_CST_AMT
,coalesce(round(SUM(s.RTN_UNT_EXCL_VOID_QTY),2), 0.0) AS RTN_UNT_QTY
,coalesce(round(SUM(s.RTN_UNT_EXCL_VOID_AMT * s.curr_exch_rt),2), 0.0000) AS RTN_RTL_AMT
,coalesce(round(SUM(s.EXT_RTN_CST_AMT * s.curr_exch_rt),2), 0.0000) AS RTN_CST_AMT
,coalesce(round(SUM(s.SLS_UNT_EXCL_VOID_QTY),2), 0.0)  AS GRS_SLS_UNT_QTY
,coalesce(round(SUM((s.SLS_UNT_EXCL_VOID_AMT + s.ASSC_DISC_AMT) * s.curr_exch_rt),2), 0.0000)  AS GRS_SLS_RTL_AMT
,coalesce(round(SUM(s.EXT_SLS_CST_AMT * s.curr_exch_rt),2), 0.0000)  AS GRS_SLS_CST_AMT
,coalesce(round(SUM(s.SLS_UNT_EXCL_VOID_QTY + s.RTN_UNT_EXCL_VOID_QTY),2), 0.0)  AS NET_SLS_UNT_QTY
,coalesce(round(SUM((s.SLS_UNT_EXCL_VOID_AMT + s.RTN_UNT_EXCL_VOID_AMT) * s.curr_exch_rt),2), 0.0000)  AS NET_SLS_RTL_AMT
,coalesce(round(SUM((s.EXT_SLS_CST_AMT + s.EXT_RTN_CST_AMT) * s.curr_exch_rt),2), 0.0000)  AS NET_SLS_CST_AMT
,coalesce(round(SUM((s.SLS_UNT_EXCL_VOID_AMT + s.RTN_UNT_EXCL_VOID_AMT) * s.curr_exch_rt) / SUM(s.SLS_UNT_EXCL_VOID_QTY + s.RTN_UNT_EXCL_VOID_QTY),2), 0.0000)  AS NET_SLS_AVG_UNT_RTL_AMT
,coalesce(round(SUM((s.EXT_SLS_CST_AMT + s.EXT_RTN_CST_AMT) * s.curr_exch_rt) /SUM(s.SLS_UNT_EXCL_VOID_QTY + s.RTN_UNT_EXCL_VOID_QTY),2), 0.0000)  AS NET_SLS_AVG_UNT_CST_AMT
,coalesce(round(SUM((s.SLS_UNT_EXCL_VOID_AMT + s.RTN_UNT_EXCL_VOID_AMT+s.ASSC_DISC_AMT) * s.curr_exch_rt)  / SUM(s.SLS_UNT_EXCL_VOID_QTY + s.RTN_UNT_EXCL_VOID_QTY),2), 0.0000)  AS TOT_SLS_AVG_UNT_RTL_AMT
,coalesce(round(SUM((s.EXT_SLS_CST_AMT + s.EXT_RTN_CST_AMT) * s.curr_exch_rt) /SUM(s.SLS_UNT_EXCL_VOID_QTY + s.RTN_UNT_EXCL_VOID_QTY),2), 0.0000)  AS TOT_SLS_AVG_UNT_CST_AMT
--,NULL AS NET_SLS_PROD_MRGN_AMT
--,NULL AS TOT_SLS_PROD_MRGN_AMT
--,NULL AS NET_SLS_PROD_MRGN_PCT
--,NULL AS Prod_AUR
,coalesce(round(SUM(s.assc_disc_amt * s.curr_exch_rt),2), 0.0000) AS assc_disc_amt
,coalesce(round(SUM(s.disc_unt_amt * s.curr_exch_rt),2), 0.0000) AS disc_unt_amt
,coalesce(round(SUM(s.clr_sls_rtl_amt),2), 0.0000) as tot_clr_sls_rtl_amt
,coalesce(round(SUM(s.clr_sls_unt),2), 0.0) as tot_clr_sls_unt
,coalesce(round(SUM(s.unt_cst_amt * s.curr_exch_rt * s.clr_sls_unt),2), 0.0000) as tot_clr_sls_cst_amt
,coalesce(round(SUM(s.gvawy_sls_rtl_amt * s.curr_exch_rt),2), 0.0000) as tot_gvawy_sls_rtl_amt
,coalesce(round(SUM(s.gvawy_sls_unt),2), 0.0) as tot_gvawy_sls_unt
,coalesce(round(SUM(s.unt_cst_amt * s.curr_exch_rt * s.gvawy_sls_unt),2), 0.0000) as tot_gvawy_sls_cst_amt
,coalesce(round(SUM(s.prmo_sls_rtl_amt),2), 0.0000) as tot_prmo_sls_rtl_amt
,coalesce(round(SUM(s.prmo_sls_unt),2), 0.0) as tot_prmo_sls_unt
,coalesce(round(SUM(s.unt_cst_amt * s.curr_exch_rt * s.prmo_sls_unt),2), 0.0000) as tot_prmo_sls_cst_amt
,coalesce(round(SUM(s.reg_sls_rtl_amt),2), 0.0000) as tot_reg_sls_rtl_amt
,coalesce(round(SUM(s.reg_sls_unt),2), 0.0) as tot_reg_sls_unt
,coalesce(round(SUM(s.unt_cst_amt * s.curr_exch_rt * s.reg_sls_unt),2), 0.0000) as tot_reg_sls_cst_amt
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as LAST_UPDATED_TIMESTAMP
,s.TRXN_DT
FROM ANALYTICS_DB.SALE_SUMMARY_DAY_LOC_ITEM s 
INNER JOIN (select ref.transaction_date from analytics_db.transaction_line_summary_ref ref where ref.process_date=current_date group by ref.transaction_date) a 
ON s.TRXN_DT=a.transaction_date
LEFT JOIN (select coalesce(case when trim(src_appl_fld_val)='' then NULL else trim(src_appl_fld_val) end, 'N/A') src_appl_fld_val from scct_db.car_pos_xref where tgt_tbl_nme='SHPG_TRXN_CHARGE' group by coalesce(case when trim(src_appl_fld_val)='' then NULL else trim(src_appl_fld_val) end, 'N/A')) cpxref
ON s.ITM_NBR = cpxref.src_appl_fld_val
where s.GFT_CRD_SLS_IND=0 
and s.void_ind=0
AND cpxref.src_appl_fld_val is null
GROUP BY s.TRXN_DT,s.LAST_UPDATED_TIMESTAMP,s.LOC_NBR,s.ITM_NBR,s.ORD_TYP_CD) a;


 