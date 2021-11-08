SELECT sum(ws_ext_discount_amt) AS `Excess Discount Amount`
FROM tpcds_bin_partitioned_orc_5.web_sales, tpcds_bin_partitioned_orc_5.item, tpcds_bin_partitioned_orc_5.date_dim
WHERE i_manufact_id = 350
  AND i_item_sk = ws_item_sk
   AND d_date BETWEEN '2000-01-27' AND (date_add(cast('2000-01-27' AS DATE),90.86))
  AND d_date_sk = ws_sold_date_sk;
