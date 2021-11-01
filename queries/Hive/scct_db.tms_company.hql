---jobname:scct-tms-hive-stg1-scct_db.tms_company
--------------------------------------------------------------------------
---target: stage1 table: scct_db.tms_company
---source: tms_stage0 table : ca2015_company
---load type: full
---back posting: no
--
-- History Information
--
-- Revision    Date     	JIRA       	Developer-ID 	Comments
-- ----------- ------- 		----------  ------------ 	--------------------
-- 01.00.00.01 05AUG19		SCCTP4-2253	pnaayan			Initial Version
--------------------------------------------------------------------------

-------------------------
---job related hive settings to read data from hbase
-------------------------

set hive.auto.convert.join=false;

-------------------------
---loading stage0 data into hbase staging table
-------------------------

set tez.job.name=staging:maprdb_company_raw :load target from ca2015_company ;

! echo started loading into scct_raw_db.maprdb_company_raw ;

insert into table scct_raw_db.maprdb_company_raw
select
concat(company_id,"|",to_utc_timestamp(last_updated_dttm, "EST5EDT")) as key
,company_id
,company_name
,company_type_id
,is_active
,is_multiple_logon_restricted
,created_source_type_id
,created_source
,created_dttm
,last_updated_source_type_id
,last_updated_source
,last_updated_dttm
,company_description
,company_url
,telephone_number
,fax_number
,address_1
,address_2
,city
,state_prov
,postal_code
,country_code
,billing_address_1
,billing_address_2
,billing_city
,billing_state_prov
,billing_postal_code
,billing_country_code
,contact_title
,contact_name
,contact_telephone_number
,contact_fax_number
,contact_email
,hibernate_version
,has_logo
,business_number
,duns_number
,company_code
,invoice_compare_method
,address_3
,auto_create_batch_flag
,batch_ctrl_flag
,batch_role_id
,billing_address_3
,case_lock_code_exp_rec
,case_lock_code_held
,color_mask
,color_offset
,color_septr
,color_sfx_mask
,color_sfx_offset
,color_sfx_septr
,dflt_batch_stat_code
,dsp_item_desc_flag
,lock_code_invalid
,pick_lock_code_exp_rec
,pick_lock_code_held
,proc_whse_xfer
,qual_mask
,qual_offset
,qual_septr
,recv_batch
,season_mask
,season_offset
,season_septr
,season_yr_mask
,season_yr_offset
,season_yr_septr
,sec_dim_mask
,sec_dim_offset
,sec_dim_septr
,size_desc_mask
,size_desc_offset
,size_desc_septr
,sku_mask
,sku_offset_mask
,style_mask
,style_offset
,style_septr
,style_sfx_mask
,style_sfx_offset
,style_sfx_septr
,ucc_ean_co_pfx
,parent_company_id
,is_initialized
from tms_stage0.ca2015_company;

-------------------------
---loading staging data into stage1 tms_company table
-------------------------

set tez.job.name=stage1:tms_company :load target table from maprdb_company_raw ;

! echo started loading into scct_db.tms_company ;

insert overwrite table scct_db.tms_company
select
coalesce((case when trim(company_id)='' then NULL else trim(company_id) END),0) as company_id
,coalesce((case when trim(company_name)='' then NULL else trim(company_name) END),'N/A')   as company_name
,coalesce((case when trim(company_type_id)='' then NULL else trim(company_type_id) END),0) as company_type_id
,coalesce((case when trim(is_active)='' then NULL else trim(is_active ) END),0) as is_active
,coalesce((case when trim(is_multiple_logon_restricted)='' then NULL else trim(is_multiple_logon_restricted ) END),0) as is_multiple_logon_restricted
,coalesce((case when trim(created_source_type_id)='' then NULL else trim(created_source_type_id) END),0) as created_source_type_id
,coalesce((case when trim(created_source)='' then NULL else trim(created_source) END),'N/A')   as created_source
,coalesce(to_utc_timestamp(created_dttm, "EST5EDT"), '9999-12-31 00:00:00') as created_dttm
,coalesce((case when trim(last_updated_source_type_id)='' then NULL else trim(last_updated_source_type_id) END),0) as last_updated_source_type_id
,coalesce((case when trim(last_updated_source)='' then NULL else trim(last_updated_source) END),'N/A')   as last_updated_source
,coalesce(to_utc_timestamp(last_updated_dttm, "EST5EDT"), '9999-12-31 00:00:00') as last_updated_dttm
,coalesce((case when trim(company_description)='' then NULL else trim(company_description) END),'N/A') as company_description
,coalesce((case when trim(company_url)='' then NULL else trim(company_url ) END),'N/A')   as company_url
,coalesce((case when trim(telephone_number)='' then NULL else trim(telephone_number) END),'N/A')   as telephone_number
,coalesce((case when trim(fax_number)='' then NULL else trim(fax_number) END),'N/A')   as fax_number
,coalesce((case when trim(address_1)='' then NULL else trim(address_1) END),'N/A')   as address_1
,coalesce((case when trim(address_2)='' then NULL else trim(address_2) END),'N/A')   as address_2
,coalesce((case when trim(city)='' then NULL else trim(city) END),'N/A')   as city
,coalesce((case when trim(state_prov)='' then NULL else trim(state_prov) END),'N/A')   as state_prov
,coalesce((case when trim(postal_code)='' then NULL else trim(postal_code) END),'N/A')   as postal_code
,coalesce((case when trim(country_code)='' then NULL else trim(country_code) END),'N/A')   as country_code
,coalesce((case when trim(billing_address_1)='' then NULL else trim(billing_address_1) END),'N/A')   as billing_address_1
,coalesce((case when trim(billing_address_2)='' then NULL else trim(billing_address_2) END),'N/A')   as billing_address_2
,coalesce((case when trim(billing_city)='' then NULL else trim(billing_city) END),'N/A')   as billing_city
,coalesce((case when trim(billing_state_prov)='' then NULL else trim(billing_state_prov) END),'N/A')   as billing_state_prov
,coalesce((case when trim(billing_postal_code)='' then NULL else trim(billing_postal_code) END),'N/A')   as billing_postal_code
,coalesce((case when trim(billing_country_code)='' then NULL else trim(billing_country_code) END),'N/A')   as billing_country_code
,coalesce((case when trim(contact_title)='' then NULL else trim(contact_title) END),'N/A')   as contact_title
,coalesce((case when trim(contact_name)='' then NULL else trim(contact_name) END),'N/A')   as contact_name
,coalesce((case when trim(contact_telephone_number)='' then NULL else trim(contact_telephone_number) END),'N/A')   as contact_telephone_number
,coalesce((case when trim(contact_fax_number)='' then NULL else trim(contact_fax_number) END),'N/A')   as contact_fax_number
,coalesce((case when trim(contact_email)='' then NULL else trim(contact_email) END),'N/A')   as contact_email
,coalesce((case when trim(hibernate_version)='' then NULL else trim(hibernate_version) END),0) as hibernate_version
,coalesce((case when trim(has_logo)='' then NULL else trim(has_logo) END),0) as has_logo
,coalesce((case when trim(business_number)='' then NULL else trim(business_number) END),'N/A')   as business_number
,coalesce((case when trim(duns_number)='' then NULL else trim(duns_number) END),'N/A')   as duns_number
,coalesce((case when trim(company_code)='' then NULL else trim(company_code) END),'N/A')   as company_code
,coalesce((case when trim(invoice_compare_method)='' then NULL else trim(invoice_compare_method) END),'N/A')   as invoice_compare_method
,coalesce((case when trim(address_3)='' then NULL else trim(address_3) END),'N/A')   as address_3
,coalesce((case when trim(auto_create_batch_flag)='' then NULL else trim(auto_create_batch_flag) END),'N/A')   as auto_create_batch_flag
,coalesce((case when trim(batch_ctrl_flag)='' then NULL else trim(batch_ctrl_flag) END),'N/A')   as batch_ctrl_flag
,coalesce((case when trim(batch_role_id)='' then NULL else trim(batch_role_id) END),'N/A')   as batch_role_id
,coalesce((case when trim(billing_address_3)='' then NULL else trim(billing_address_3) END),'N/A')   as billing_address_3
,coalesce((case when trim(case_lock_code_exp_rec)='' then NULL else trim(case_lock_code_exp_rec) END),'N/A')   as case_lock_code_exp_rec
,coalesce((case when trim(case_lock_code_held)='' then NULL else trim(case_lock_code_held) END),'N/A')   as case_lock_code_held
,coalesce((case when trim(color_mask)='' then NULL else trim(color_mask) END),'N/A')   as color_mask
,coalesce((case when trim(color_offset)='' then NULL else trim(color_offset) END),0) as color_offset
,coalesce((case when trim(color_septr)='' then NULL else trim(color_septr) END),'N/A')   as color_septr
,coalesce((case when trim(color_sfx_mask)='' then NULL else trim(color_sfx_mask) END),'N/A')   as color_sfx_mask
,coalesce((case when trim(color_sfx_offset)='' then NULL else trim(color_sfx_offset) END),0) as color_sfx_offset
,coalesce((case when trim(color_sfx_septr)='' then NULL else trim(color_sfx_septr) END),'N/A')   as color_sfx_septr
,coalesce((case when trim(dflt_batch_stat_code)='' then NULL else trim(dflt_batch_stat_code) END),0) as dflt_batch_stat_code
,coalesce((case when trim(dsp_item_desc_flag)='' then NULL else trim(dsp_item_desc_flag) END),'N/A')   as dsp_item_desc_flag
,coalesce((case when trim(lock_code_invalid)='' then NULL else trim(lock_code_invalid) END),'N/A')   as lock_code_invalid
,coalesce((case when trim(pick_lock_code_exp_rec)='' then NULL else trim(pick_lock_code_exp_rec) END),'N/A')   as pick_lock_code_exp_rec
,coalesce((case when trim(pick_lock_code_held)='' then NULL else trim(pick_lock_code_held) END),'N/A')   as pick_lock_code_held
,coalesce((case when trim(proc_whse_xfer)='' then NULL else trim(proc_whse_xfer) END),'N/A')   as proc_whse_xfer
,coalesce((case when trim(qual_mask)='' then NULL else trim(qual_mask) END),'N/A')   as qual_mask
,coalesce((case when trim(qual_offset)='' then NULL else trim(qual_offset) END),0) as qual_offset
,coalesce((case when trim(qual_septr)='' then NULL else trim(qual_septr) END),'N/A')   as qual_septr
,coalesce((case when trim(recv_batch)='' then NULL else trim(recv_batch) END),'N/A')   as recv_batch
,coalesce((case when trim(season_mask)='' then NULL else trim(season_mask) END),'N/A')   as season_mask
,coalesce((case when trim(season_offset)='' then NULL else trim(season_offset) END),0) as season_offset
,coalesce((case when trim(season_septr)='' then NULL else trim(season_septr) END),'N/A')   as season_septr
,coalesce((case when trim(season_yr_mask)='' then NULL else trim(season_yr_mask) END),'N/A')   as season_yr_mask
,coalesce((case when trim(season_yr_offset)='' then NULL else trim(season_yr_offset) END),0) as season_yr_offset
,coalesce((case when trim(season_yr_septr)='' then NULL else trim(season_yr_septr) END),'N/A')   as season_yr_septr
,coalesce((case when trim(sec_dim_mask)='' then NULL else trim(sec_dim_mask) END),'N/A')   as sec_dim_mask
,coalesce((case when trim(sec_dim_offset)='' then NULL else trim(sec_dim_offset) END),0) as sec_dim_offset
,coalesce((case when trim(sec_dim_septr)='' then NULL else trim(sec_dim_septr) END),'N/A')   as sec_dim_septr
,coalesce((case when trim(size_desc_mask)='' then NULL else trim(size_desc_mask) END),'N/A')   as size_desc_mask
,coalesce((case when trim(size_desc_offset)='' then NULL else trim(size_desc_offset) END),0) as size_desc_offset
,coalesce((case when trim(size_desc_septr)='' then NULL else trim(size_desc_septr) END),'N/A')   as size_desc_septr
,coalesce((case when trim(sku_mask)='' then NULL else trim(sku_mask) END),'N/A')   as sku_mask
,coalesce((case when trim(sku_offset_mask)='' then NULL else trim(sku_offset_mask) END),'N/A')   as sku_offset_mask
,coalesce((case when trim(style_mask)='' then NULL else trim(style_mask) END),'N/A')   as style_mask
,coalesce((case when trim(style_offset)='' then NULL else trim(style_offset) END),0) as style_offset
,coalesce((case when trim(style_septr)='' then NULL else trim(style_septr) END),'N/A')   as style_septr
,coalesce((case when trim(style_sfx_mask)='' then NULL else trim(style_sfx_mask) END),'N/A')   as style_sfx_mask
,coalesce((case when trim(style_sfx_offset)='' then NULL else trim(style_sfx_offset) END),0) as style_sfx_offset
,coalesce((case when trim(style_sfx_septr)='' then NULL else trim(style_sfx_septr) END),'N/A')   as style_sfx_septr
,coalesce((case when trim(ucc_ean_co_pfx)='' then NULL else trim(ucc_ean_co_pfx) END),'N/A')   as ucc_ean_co_pfx
,coalesce((case when trim(parent_company_id)='' then NULL else trim(parent_company_id) END),0) as parent_company_id
,coalesce((case when trim(is_initialized)='' then NULL else trim(is_initialized) END),0) as is_initialized
,to_date(to_utc_timestamp(last_updated_dttm, "EST5EDT")) as strt_dt
,lag(date_sub(to_utc_timestamp(last_updated_dttm, "EST5EDT"),1),1,'9999-12-31') over(partition by company_id order by key desc) as end_dt
from scct_raw_db.maprdb_company_raw;

! echo completed loading into scct_db.tms_company ;

--------------------------
---cocatenate files for target table tms_company
-------------------------

SET tez.job.name=STAGE1:tms_company:cocatenate files on tms_company;

! echo Started cocatenate files;

alter table scct_db.tms_company concatenate;

! echo Completed cocatenate files;

-------------------------
---end script
-------------------------