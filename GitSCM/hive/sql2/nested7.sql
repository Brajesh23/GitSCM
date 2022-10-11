with COLUMNNAME1 as
(
    select COLUMNNAME2 COLUMNNAME3,
        sum(COLUMNNAME4) COLUMNNAME5
    from TABLENAME1,
        TABLENAME2,
        TABLENAME3
    where COLUMNNAME6 = COLUMNNAME7
        and COLUMNNAME8 in (
            select COLUMNNAME8
            from TABLENAME3
            where COLUMNNAME9 in (
                select COLUMNNAME9
                from TABLENAME3
                where COLUMNNAME8 = date_add(weeks_Add(AddDate(COLUMNNAME8,2),4), interval 3 weeks)
            )
        )
        and COLUMNNAME13 = years_add(COLUMNNAME15,2)
    group by COLUMNNAME2
), COLUMNNAME16 as
(
    select COLUMNNAME2 COLUMNNAME3,
        sum(COLUMNNAME17) COLUMNNAME18
    from TABLENAME4,
        TABLENAME2,
        TABLENAME3
    where COLUMNNAME19 = COLUMNNAME7
        and COLUMNNAME8 in (
            select COLUMNNAME8
            from TABLENAME3
            where COLUMNNAME9 in (
                select COLUMNNAME9
                from TABLENAME3
                where COLUMNNAME8 in ('1998-01-02','1998-10-15','1998-11-10')
            )
        )
        and COLUMNNAME20 = COLUMNNAME15
    group by COLUMNNAME2
), COLUMNNAME21 as
(
    select COLUMNNAME2 COLUMNNAME3,
        sum(COLUMNNAME22) COLUMNNAME23
    from TABLENAME5,
        TABLENAME2,
        TABLENAME3
    where COLUMNNAME24 = COLUMNNAME7
        and COLUMNNAME8 in (
            select COLUMNNAME8
            from TABLENAME3
            where COLUMNNAME9 in (
                select COLUMNNAME9
                from TABLENAME3
                where COLUMNNAME8 in ('1998-01-02','1998-10-15','1998-11-10')
            )
        )
        and COLUMNNAME25 = years_sub(COLUMNNAME15,2)
    group by COLUMNNAME2
)
select SR_ITEMS.COLUMNNAME3
    ,COLUMNNAME5
    ,COLUMNNAME5/(COLUMNNAME5+COLUMNNAME18+COLUMNNAME23)/3.COLUMNNAME27 * 100 COLUMNNAME28
    ,COLUMNNAME18
    ,COLUMNNAME18/(COLUMNNAME5+COLUMNNAME18+COLUMNNAME23)/3.COLUMNNAME27 * 100 COLUMNNAME29
    ,COLUMNNAME23
    ,COLUMNNAME23/(COLUMNNAME5+COLUMNNAME18+COLUMNNAME23)/3.COLUMNNAME27 * 100 COLUMNNAME30
    ,(COLUMNNAME5+COLUMNNAME18+COLUMNNAME23)/3.COLUMNNAME27 COLUMNNAME31
from TABLENAME6
    ,TABLENAME7
    ,TABLENAME8
where SR_ITEMS.COLUMNNAME3=CR_ITEMS.COLUMNNAME3
    and SR_ITEMS.COLUMNNAME3=WR_ITEMS.COLUMNNAME3 
order by SR_ITEMS.COLUMNNAME3
    ,COLUMNNAME5
limit 100
;
