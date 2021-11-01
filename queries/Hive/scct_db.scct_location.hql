---jobname:scct-E2E_Dashboard-hive-stg1-scct_db.scct_location
--------------------------------------------------------------------------
---Target: STAGE1 table: scct_db.scct_location
---Source: table : kna1 , lfa1, analytics_view.location
---Load Type: Full
---Back posting: No
---Author: ryadav
---Last modified date: 12/26/2018
--------------------------------------------------------------------------


-------------------------
---Job related Hive settings to merge files for Target table
-------------------------

-------------------------
---Loading scct_db.scct_location target table data
-------------------------
insert overwrite table scct_db.scct_location
select 
location_number,
location_name,
case when location_type_code='04' then location_display_name else location_name end as location_display_name,
brand_code,
legacy_store_number,
coalesce(latitudedegrees,0),
coalesce(longitudedegrees,0),
lzone,
ort01,
district_code,
district_description,
region_code,
region_description,
organization_number,
organization_name
from analytics_view.location l
join sap_stage0.kna1 kna1
on trim(l.location_number) = trim(kna1.kunnr)
left outer join (select facility_brand_name,store_number,latitudedegrees,longitudedegrees from accruent_stage0.accruent_prod_vw_mgt_facility_lat_long where program = 'Company Owned')c
on l.brand_code = substr(facility_brand_name,-3)
and l.legacy_store_number = c.store_number
union all  
select 
lifnr,
case when trim(name2) in('','.','' ,'@') or trim(name2) is null then trim(name1) else trim(name2) end as loc_nme,
case when trim(name2) in('','.','' ,'@') or trim(name2) is null then trim(name1) else trim(name2) end as loc_display_name,
'unk',
0,
coalesce(latitude,0),
coalesce(longitude,0),
coalesce(lzone,'n/a'),
ort01,
'n/a',
'n/a',
'n/a',
'n/a',
'n/a',
'n/a'
from
(select 
lifnr,
name1,
name2,
lzone,
ort01
from scct_raw_db.lfa1_raw 
where substr(lifnr,1,3) in ('003','004')  
and trim(lifnr) not in (select trim(kunnr) from sap_stage0.kna1 ) ) x 
left outer join bira_stage0.bods_stg_dim_longitude_latitude y 
on x.lifnr = y.loc_cd;

! echo Completed Loading into scct_db.scct_location ;

 -------------------------
 ---end script
 -------------------------