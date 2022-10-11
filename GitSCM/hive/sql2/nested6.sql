with
COLUMNNAME1 as
(
    select COLUMNNAME2 COLUMNNAME3
        , Dexp(Dfloor(COLUMNNAME6)) COLUMNNAME7
    from TABLENAME1
        , TABLENAME2
    where COLUMNNAME8 < microseconds_sub(from_timeStamp(days_Add(AddDate(COLUMNNAME12,2),4),'yyyy-MM-dd HH : mm : ss'),3)
        and COLUMNNAME13 between 1212 and 1212 + 11
    group by COLUMNNAME2
        ,COLUMNNAME6
), COLUMNNAME14 as
(
    select COLUMNNAME15 COLUMNNAME3
        , COLUMNNAME16 COLUMNNAME7
    from TABLENAME3
        , TABLENAME2
    where COLUMNNAME17 = COLUMNNAME18
        and COLUMNNAME13 between 1212 and 1212 + 11
    group by COLUMNNAME15
        ,COLUMNNAME16
)
select sum(case when SSCI.COLUMNNAME3 is not null and CSCI.COLUMNNAME3 is null then 1 else 0 end) COLUMNNAME19
    , sum(case when SSCI.COLUMNNAME3 is null and CSCI.COLUMNNAME3 is not null then 1 else 0 end) COLUMNNAME20
    , sum(case when SSCI.COLUMNNAME3 is not null and CSCI.COLUMNNAME3 is not null then 1 else 0 end) COLUMNNAME21
from TABLENAME4 full outer join
    TABLENAME5 on (SSCI.COLUMNNAME3=CSCI.COLUMNNAME3 and SSCI.COLUMNNAME7 = CSCI.COLUMNNAME7)
limit 100
;
