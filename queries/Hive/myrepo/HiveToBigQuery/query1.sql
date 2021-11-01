select 
max_day.thumb_image,
max_day.title,
max_day.trend,
max_day.waist_length,
max_day.delta_hash
FROM (SELECT * FROM digitaldata_db.productreach where run_time in (select run_time from digitaldata_db.productreach where run_time >= cast(UNIX_TIMESTAMP(date_add(current_date,-1)) as string)  order by run_time desc limit 1)) max_day
LEFT JOIN
(select * from digitaldata_db.productreach where run_time in (select run_time from (select distinct run_time from digitaldata_db.productreach where run_time >= cast(UNIX_TIMESTAMP(date_add(current_date,-1)) as string)  order by run_time desc limit 2) d order by run_time limit 1) 
)second_max_day
on max_day.skuid = second_max_day.skuid and max_day.pid = second_max_day.pid and max_day.delta_hash = second_max_day.delta_hash;

