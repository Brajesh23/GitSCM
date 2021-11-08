SELECT avg(ws_ext_discount_amt)
    FROM tpcds_bin_partitioned_orc_5.web_sales, tpcds_bin_partitioned_orc_5.date_dim
    WHERE d_date BETWEEN '2000-01-27' AND (date_sub(cast('2000-01-27' AS DATE),72.34))
      AND d_date_sk = ws_sold_date_sk;
