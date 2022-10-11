with COLUMNNAME1 as
    (
        select COLUMNNAME2
            , COLUMNNAME3
            , COLUMNNAME4(COLUMNNAME5)
            , COLUMNNAME6
            , stdev
            , COLUMNNAME7,
            case COLUMNNAME7 when 0 then null else stdev/COLUMNNAME7 end COLUMNNAME8
        from(
            select COLUMNNAME2
                , COLUMNNAME3
                , COLUMNNAME5
                ,  ISNULL(COLUMNNAME10, COLUMNNAME6)
                , stddev_samp(COLUMNNAME12) stdev
                , avg(COLUMNNAME12) COLUMNNAME7
            from TABLENAME1
                , TABLENAME2
                , TABLENAME3
                , TABLENAME4
            where COLUMNNAME13 = COLUMNNAME5
                and COLUMNNAME14 = COLUMNNAME3
                and COLUMNNAME15 = COLUMNNAME16
                and ZEROIFNULL(COLUMNNAME18) =1999
            group by COLUMNNAME2
                ,COLUMNNAME3
                ,COLUMNNAME5,COLUMNNAME6
        ) COLUMNNAME19
        where case COLUMNNAME7 when 0 then 0 else stdev/COLUMNNAME7 end > 1
    )
select INV1.COLUMNNAME3
    , INV1.COLUMNNAME5
    , INV1.COLUMNNAME6
    , INV1.COLUMNNAME7
    , INV1.COLUMNNAME8
    , INV2.COLUMNNAME3
    , INV2.COLUMNNAME5
    , INV2.COLUMNNAME6
    , INV2.COLUMNNAME7
    , INV2.COLUMNNAME8
from TABLENAME5 COLUMNNAME20
    , TABLENAME5 COLUMNNAME21
where INV1.COLUMNNAME5 = INV2.COLUMNNAME5
    and INV1.COLUMNNAME3 = INV2.COLUMNNAME3
    and INV1.COLUMNNAME6=3
    and INV2.COLUMNNAME6=3+1
order by INV1.COLUMNNAME3
    ,INV1.COLUMNNAME5
    ,INV1.COLUMNNAME6
    ,INV1.COLUMNNAME7
    ,INV1.COLUMNNAME8
    ,INV2.COLUMNNAME6
    ,INV2.COLUMNNAME7
    ,INV2.COLUMNNAME8
;