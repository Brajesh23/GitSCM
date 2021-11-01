SELECT
      iss.i_brand_id brand_id,
      iss.i_class_id class_id,
      iss.i_category_id category_id
    FROM tpcds_bin_partitioned_orc_5.store_sales, tpcds_bin_partitioned_orc_5.item iss, tpcds_bin_partitioned_orc_5.date_dim d1
    WHERE ss_item_sk = iss.i_item_sk
      AND ss_sold_date_sk = d1.d_date_sk
      AND d1.d_year BETWEEN 1999 AND 1999 + 2
   union
    SELECT
      ics.i_brand_id,
      ics.i_class_id,
      ics.i_category_id
    FROM tpcds_bin_partitioned_orc_5.catalog_sales, tpcds_bin_partitioned_orc_5.item ics, tpcds_bin_partitioned_orc_5.date_dim d2
    WHERE cs_item_sk = ics.i_item_sk
      AND cs_sold_date_sk = d2.d_date_sk
      AND d2.d_year BETWEEN 1999 AND 1999 + 2     
    union all
    SELECT
      iws.i_brand_id,
      iws.i_class_id,
      iws.i_category_id
    FROM tpcds_bin_partitioned_orc_5.web_sales, tpcds_bin_partitioned_orc_5.item iws, tpcds_bin_partitioned_orc_5.date_dim d3
    WHERE ws_item_sk = iws.i_item_sk
      AND ws_sold_date_sk = d3.d_date_sk
      AND d3.d_year BETWEEN 1999 AND 1999 + 2;
