select
    BASE64DECODE(COLUMNNAME1),
    COLUMNNAME2,
    COLUMNNAME3,
    COLUMNNAME4,
    COLUMNNAME5,
    ucase(COLUMNNAME6)
from COLUMNNAME7,
COLUMNNAME8,
(
    select COLUMNNAME9, avg(COLUMNNAME10) as ave
    from (
        select COLUMNNAME9
            , COLUMNNAME11
            , sum(COLUMNNAME12) as COLUMNNAME10
        from TABLENAME1
            , TABLENAME2
        where COLUMNNAME13 = COLUMNNAME14
            and COLUMNNAME15 is true
        group by COLUMNNAME9 , COLUMNNAME11
    ) sa
    group by COLUMNNAME9
) sb,
(
    select
    COLUMNNAME9, COLUMNNAME11, sum(COLUMNNAME12) as COLUMNNAME10
    from TABLENAME1
        , TABLENAME2
    where COLUMNNAME13 = subdate(from_timeStamp(days_Add(AddDate(COLUMNNAME16,2),4),'yyyy-MM-dd HH:mm:ss'),4)
        and COLUMNNAME15 between 1212 and 1212 + 11
    group by COLUMNNAME9 , COLUMNNAME11
) sc
where sb.COLUMNNAME9 = sc.COLUMNNAME9
    and COLUMNNAME3 <= 0.1 * sb.ave
    and s_COLUMNNAME7_sk = sc.COLUMNNAME9
    and i_COLUMNNAME8_sk = sc.COLUMNNAME11
order by COLUMNNAME1 , COLUMNNAME2
limit 100
;
