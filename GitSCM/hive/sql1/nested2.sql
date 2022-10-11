insert overwrite table DATABASENAME1.TABLENAME1
select
COLUMNNAME1
        ,COLUMNNAME2
        ,COLUMNNAME3
        ,COLUMNNAME4
        ,COLUMNNAME5
        ,round(DAY(cast(COLUMNNAME5 as timestamp) - cast(COLUMNNAME4 as timestamp))*24
+ HOUR(cast(COLUMNNAME5 as timestamp) - cast(COLUMNNAME4 as timestamp))
+ MINUTE(cast(COLUMNNAME5 as timestamp) - cast(COLUMNNAME4 as timestamp))/60
+ SECOND(cast(COLUMNNAME5 as timestamp) - cast(COLUMNNAME4 as timestamp))/(60*60),2) as COLUMNNAME6
        ,COLUMNNAME7
        ,' ' as COLUMNNAME8
        ,date_Sub(days_Add(AddDate(COLUMNNAME12,2),4), interval 3 weeks) as COLUMNNAME13
        ,COLUMNNAME14
        ,COLUMNNAME15
        ,COLUMNNAME16
        ,COLUMNNAME17
        ,COLUMNNAME18
        ,COLUMNNAME19
        ,COLUMNNAME20
FROM
(select
COLUMNNAME1
        ,COLUMNNAME2
        ,COLUMNNAME3
        ,COLUMNNAME4
        ,case when datediff(COLUMNNAME5,COLUMNNAME4) >2 then concat(date_add(COLUMNNAME4,1) ,' ','00 : 00 : 00')
        else COLUMNNAME5 end as COLUMNNAME5
        ,COLUMNNAME7
        ,COLUMNNAME14
        ,COLUMNNAME15
        ,COLUMNNAME16
        ,COLUMNNAME17
        ,COLUMNNAME18
        ,COLUMNNAME19
        ,COLUMNNAME20
FROM (
   SELECT  POS.COLUMNNAME1
 ,POS.COLUMNNAME2 AS COLUMNNAME2
        ,POS.COLUMNNAME23
        ,POS.COLUMNNAME23 as COLUMNNAME3
        ,POS.COLUMNNAME4
        ,cast(POS.COLUMNNAME4 as date) AS COLUMNNAME24
        ,substr(POS.COLUMNNAME4,11,9) AS COLUMNNAME26
        ,from_timeStamp(days_Add(AddDate(COLUMNNAME12,2),4),'yyyy-MM-dd HH : mm : ss') AS COLUMNNAME5 --+14400
        ,cast(from_unixtime(unix_timestamp(COALESCE(POS.COLUMNNAME5,POS.COLUMNNAME4))+0) as date) AS COLUMNNAME29   --+ INTERVAL '04 : 00 : 00' HOUR TO SECOND) AS DATE) AS PNCH_OUT_DT
        ,substr(from_unixtime(unix_timestamp(COALESCE(POS.COLUMNNAME5,POS.COLUMNNAME4))+0),11,9) AS COLUMNNAME30  --+ INTERVAL '04 : 00 : 00' HOUR TO SECOND) AS TIME(6)  WITH TIME ZONE) AS PNCH_OUT_TM
 ,COALESCE(POS.COLUMNNAME31,'00103') AS COLUMNNAME7
        ,POS.COLUMNNAME14                   AS COLUMNNAME14
        ,POS.COLUMNNAME15                       AS COLUMNNAME15
        ,POS.COLUMNNAME16       as COLUMNNAME32
        ,POS.COLUMNNAME16
        ,POS.COLUMNNAME17
        ,coalesce(PCTT.COLUMNNAME18,'N/A') as COLUMNNAME18
        ,coalesce(PCTT.COLUMNNAME19,'N/A') as COLUMNNAME19
        ,coalesce(PCTT.COLUMNNAME20,'N/A') as COLUMNNAME20
 FROM  (select coalesce(case when trim(COLUMNNAME2)='' then null else trim(COLUMNNAME2) end, 'N/A') as COLUMNNAME2
  ,coalesce(case when COLUMNNAME1='' then null else COLUMNNAME1 end, 0) as COLUMNNAME1
  ,coalesce(case when COLUMNNAME14='' then null else COLUMNNAME14 end, 0) as COLUMNNAME14
  ,coalesce(case when trim(COLUMNNAME23)='' then null else trim(COLUMNNAME23) end, 'N/A') as COLUMNNAME23
  ,coalesce(case when COLUMNNAME4='' then null else COLUMNNAME4 end, '9999-12-31 00 : 00 : 00') as COLUMNNAME4
  ,coalesce(case when COLUMNNAME5='' then null else COLUMNNAME5 end, '9999-12-31 00 : 00 : 00') as COLUMNNAME5
  ,coalesce(case when STRLEFT(STrRight(COLUMNNAME31 ,2) ,2)='' then null else trim(COLUMNNAME31) end, 'N/A') as COLUMNNAME31
  ,coalesce(case when trim(COLUMNNAME15)='' then null else trim(COLUMNNAME15) end, '9999-12-31 00 : 00 : 00') as COLUMNNAME15
  ,coalesce(case when trim(COLUMNNAME16)='' then null else trim(COLUMNNAME16) end, 'N/A') as COLUMNNAME16
  ,coalesce(case when COLUMNNAME17='' then null else COLUMNNAME17 end, '9999-12-31 00 : 00 : 00') as COLUMNNAME17
from   DATABASENAME2.TABLENAME2
where  timestamp_cmp(from_timeStamp(days_Add(AddDate(COLUMNNAME17,2),4),'yyyy-MM-dd HH : mm : ss'),now())
and unix_timestamp(COLUMNNAME17)>=(select unix_timestamp(COLUMNNAME36)-(5*60) from DATABASENAME1.TABLENAME3))  COLUMNNAME37
left outer join (select COLUMNNAME7, COLUMNNAME2, COLUMNNAME18, COLUMNNAME19, COLUMNNAME20 from DATABASENAME1.TABLENAME4
group by COLUMNNAME7, COLUMNNAME2, COLUMNNAME18, COLUMNNAME19, COLUMNNAME20) COLUMNNAME38
on POS.COLUMNNAME31 = PCTT.COLUMNNAME7
AND POS.COLUMNNAME2 = PCTT.COLUMNNAME2)COLUMNNAME39
)COLUMNNAME40;
