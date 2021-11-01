---jobname:scct-E2E_Dashboard-hive-stg1-scct_db.scct_job_control
--------------------------------------------------------------------------
---Target: STAGE1 table: SCCT_DB.SCCT_JOB_CONTROL 
---Source: scct_raw_db  table : likp_raw, zmm_inv_bal_raw, cdpos_raw, scct_db table : delivery_master, SCCT_WORK_DB table : MSEG_WORK
---Load Type: Full
---Back posting: No
---Author: ryadav
---Last modified date: 01/02/2019
--------------------------------------------------------------------------


-------------------------
---Loading SCCT_JOB_CONTROL stage 1 base data
-------------------------

! echo Started Loading into SCCT_DB.SCCT_JOB_CONTROL ;

insert overwrite table SCCT_DB.SCCT_JOB_CONTROL PARTITION (run_date,driver_table)
select budat_mkpf,current_date,'MSEG' from SCCT_WORK_DB.MSEG_WORK
group by budat_mkpf;

insert overwrite table SCCT_DB.SCCT_JOB_CONTROL PARTITION (run_date,driver_table)
select zbaldate,current_date,'ZMM_INV_BAL'
from scct_raw_db.zmm_inv_bal_raw
group by zbaldate;

insert overwrite table SCCT_DB.SCCT_JOB_CONTROL PARTITION (run_date,driver_table)
SELECT business_date,current_date,'LIKP-LIPS' 
from analytics_db.enterprise_calendar
where  business_date between DATEADD('day',-31,current_date) and current_date
GROUP BY business_date;

! echo Completed Loading into SCCT_DB.SCCT_JOB_CONTROL ;

	
-------------------------
---Collect stats for target table
-------------------------
