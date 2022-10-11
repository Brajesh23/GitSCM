select ASCEDING.COLUMNNAME1
    , I1.COLUMNNAME2 COLUMNNAME3
    , I2.COLUMNNAME2 COLUMNNAME4
    , MAX_TINYINT()
    , MAX_SMALLINT()
from (
    select *
    from (
        select COLUMNNAME7
            , rank() over (order by COLUMNNAME8 asc) COLUMNNAME1
        from (
            select COLUMNNAME9 COLUMNNAME7
                ,  from_timeStamp(days_Add(AddDate(COLUMNNAME12,2),4),'yyyy-MM-dd HH : mm : ss') COLUMNNAME13
            from TABLENAME1 COLUMNNAME14
            where COLUMNNAME15 = 410
            group by COLUMNNAME9
            having avg(COLUMNNAME16) > 0.9*(
                select avg(COLUMNNAME16) COLUMNNAME8
                from TABLENAME1
                where COLUMNNAME15 = 410
                    and COLUMNNAME18 is null
                group by COLUMNNAME15)
        ) COLUMNNAME19
    ) COLUMNNAME20
    where COLUMNNAME1  < 11
) COLUMNNAME21, (
    select *
    from (
        select COLUMNNAME7
            , rank() over (order by COLUMNNAME8 desc) COLUMNNAME1
        from (
            select COLUMNNAME9 COLUMNNAME7
                , avg(COLUMNNAME16) COLUMNNAME8
            from TABLENAME1 COLUMNNAME14
            where COLUMNNAME15 = 410
            group by COLUMNNAME9
            having avg(COLUMNNAME16) > 0.9*(
                select avg(COLUMNNAME16) COLUMNNAME8
                from TABLENAME1
                where COLUMNNAME15 = 410
                    and COLUMNNAME18 is null
                group by COLUMNNAME15
            )
        ) COLUMNNAME22
    ) COLUMNNAME23
    where COLUMNNAME1  < 11
) COLUMNNAME24, TABLENAME2 COLUMNNAME25, TABLENAME2 COLUMNNAME26
where ASCEDING.COLUMNNAME1 = DESCENDING.COLUMNNAME1
    and I1.COLUMNNAME27=ASCEDING.COLUMNNAME7
    and I2.COLUMNNAME27=DESCENDING.COLUMNNAME7
order by ASCEDING.COLUMNNAME1
limit 100
;

