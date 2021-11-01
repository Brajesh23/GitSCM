--! echo completed loading into scct_work_db.dy_pyrl_pos_audited_tmp ;
---jobname:scct-aces-hive-stg1-scct_work_db.dy_pyrl_pos_audited_tmp1 -------------------------------------------------------------------------- ---target: stage1 table: scct_work_db.dy_pyrl_pos_audited_tmp1 ---source: scct_work_db.dy_pyrl_pos_audited_tmp ---load type: incremental load ---back posting: yes --------------------------------------------------------------------------  -- history information -- -- revision date jira developer-id comments -- ----------- ------- ---------- ------------ ---------------------------- -- 01.00.00.01 10feb20 scctp4-3096 ryadav initial version -- 01.00.00.02 03mar20 scctp4-3258 ryadav added clk_pnch_evnt_id while generating row number  ------------------------- ---loading scct_work_db.dy_pyrl_pos_audited_tmp1 work table data -------------------------  --set tez.job.name=stage1_scct_db:dy_pyrl_pos_audited_tmp1 :load work table dy_pyrl_pos_audited_tmp1 ;
--! echo started loading into scct_work_db.dy_pyrl_pos_audited_tmp1 ;
insert overwrite into scct_work_db.dy_pyrl_pos_audited_tmp1 select lgcy_sto_nbr ,brnd_cd ,assc_ssn_nbr ,clk_pnch_in_dttm ,clk_pnch_out_dttm ,pyrl_hr_amt ,pos_pyrl_cd ,hrly_ind ,trxn_post_dt ,clk_pnch_evnt_id ,lst_mod_dttm ,lst_mod_pty_id_val ,rec_ins_dttm ,pyrl_tme_typ_nme ,prod_pyrl_ind ,tplh_flg_ind from (select lgcy_sto_nbr ,brnd_cd ,assc_ssn_nbr ,clk_pnch_in_dttm ,clk_pnch_out_dttm ,pyrl_hr_amt ,pos_pyrl_cd ,hrly_ind ,trxn_post_dt ,clk_pnch_evnt_id ,lst_mod_dttm ,lst_mod_pty_id_val ,rec_ins_dttm ,pyrl_tme_typ_nme ,prod_pyrl_ind ,tplh_flg_ind ,row_number() over (partition by assc_ssn_nbr,brnd_cd,lgcy_sto_nbr,pos_pyrl_cd,clk_pnch_evnt_id,clk_pnch_out_dttm order by lst_mod_dttm desc) rn2 from (select lgcy_sto_nbr ,brnd_cd ,assc_ssn_nbr ,clk_pnch_in_dttm ,clk_pnch_out_dttm ,pyrl_hr_amt ,pos_pyrl_cd ,hrly_ind ,trxn_post_dt ,clk_pnch_evnt_id ,lst_mod_dttm ,lst_mod_pty_id_val ,rec_ins_dttm ,pyrl_tme_typ_nme ,prod_pyrl_ind ,tplh_flg_ind ,row_number() over (partition by assc_ssn_nbr,brnd_cd,lgcy_sto_nbr,pos_pyrl_cd,clk_pnch_evnt_id,cast(clk_pnch_in_dttm as date) order by lst_mod_dttm desc) rn from scct_work_db.dy_pyrl_pos_audited_tmp)dppa where rn=1)dppa2 where rn2=1
--! echo completed loading into scct_work_db.dy_pyrl_pos_audited_tmp1 ;