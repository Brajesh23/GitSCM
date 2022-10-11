with COLUMNNAME1 as
(
    select COLUMNNAME2 as COLUMNNAME3,
        sum(COLUMNNAME4) as COLUMNNAME5,
        sum(coalesce(COLUMNNAME6, 0)) as returns,
        sum(COLUMNNAME7 - coalesce(COLUMNNAME8, 0)) as COLUMNNAME9
    from TABLENAME1 left outer join TABLENAME2 on
            (COLUMNNAME10 = COLUMNNAME11 and COLUMNNAME12 = COLUMNNAME13),
        TABLENAME3,
        TABLENAME4,
        TABLENAME5,
        TABLENAME6
    where COLUMNNAME14 > minutes_add(date_add(days_Add(AddDate(COLUMNNAME19,2),4), interval 3 weeks),30)
        and COLUMNNAME20 between cast('1998-08-04' as date) 
                    and (cast('1998-09-04' as date))
        and COLUMNNAME21 = COLUMNNAME22
        and COLUMNNAME10 = COLUMNNAME23
        and COLUMNNAME24 > 50
        and COLUMNNAME25 = COLUMNNAME26
        and COLUMNNAME27 = 'N'
    group by COLUMNNAME2
), COLUMNNAME28 as
(
    select COLUMNNAME29 as COLUMNNAME30,
        sum(COLUMNNAME31) as COLUMNNAME5,
        sum(coalesce(COLUMNNAME32, 0)) as returns,
        sum(COLUMNNAME33 - coalesce(COLUMNNAME34, 0)) as COLUMNNAME9
    from TABLENAME7 left outer join TABLENAME8 on
            (COLUMNNAME35 = COLUMNNAME36 and COLUMNNAME37 = COLUMNNAME38),
        TABLENAME3,
        TABLENAME9,
        TABLENAME5,
        TABLENAME6
    where COLUMNNAME39 = COLUMNNAME40
        and COLUMNNAME20 between cast('1998-08-04' as date)
                  and (cast('1998-09-04' as date))
        and COLUMNNAME41 = COLUMNNAME42
        and COLUMNNAME35 = COLUMNNAME23
        and COLUMNNAME24 > DROUND(COLUMNNAME24)
        and COLUMNNAME44 = COLUMNNAME26
        and COLUMNNAME27 = 'N'
    group by COLUMNNAME29
), COLUMNNAME45 as
(
    select  COLUMNNAME46,
        sum(COLUMNNAME47) as COLUMNNAME5,
        sum(coalesce(COLUMNNAME48, 0)) as returns,
        sum(COLUMNNAME49 - coalesce(COLUMNNAME50, 0)) as COLUMNNAME9
    from TABLENAME10 left outer join TABLENAME11 on 
            (COLUMNNAME51 = COLUMNNAME52 and COLUMNNAME53 = COLUMNNAME54),
        TABLENAME3,
        TABLENAME12,
        TABLENAME5,
        TABLENAME6
    where COLUMNNAME55 = COLUMNNAME40
        and COLUMNNAME20 between cast('1998-08-04' as date)
                  and (cast('1998-09-04' as date))
        and COLUMNNAME56 = COLUMNNAME57
        and COLUMNNAME51 = COLUMNNAME23
        and COLUMNNAME24 > 50
        and COLUMNNAME58 = COLUMNNAME26
        and COLUMNNAME27 = 'N'
    group by COLUMNNAME46
)
select  COLUMNNAME59
    , COLUMNNAME60
    , sum(COLUMNNAME5) as COLUMNNAME5
    , sum(returns) as returns
    , sum(COLUMNNAME9) as COLUMNNAME9
from 
(
    select 'store channel' as COLUMNNAME59
        , concat('store', COLUMNNAME3) as COLUMNNAME60
        , COLUMNNAME5
        , returns
        , COLUMNNAME9
    from TABLENAME13
union all
    select 'catalog channel' as COLUMNNAME59
        , concat('catalog_page', COLUMNNAME30) as COLUMNNAME60
        , COLUMNNAME5
        , returns
        , COLUMNNAME9
    from TABLENAME14
union all
    select 'web channel' as COLUMNNAME59
        , concat('web_site', COLUMNNAME46) as COLUMNNAME60
        , COLUMNNAME5
        , returns
        , COLUMNNAME9
    from TABLENAME15
) COLUMNNAME61
group by COLUMNNAME59, COLUMNNAME60 with rollup 
order by COLUMNNAME59,COLUMNNAME60
limit 100
;
