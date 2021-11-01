
DROP TABLE IF EXISTS IMPETUS_MAPR_S1_DAY_1.EDWDATA.WMS_CARTON_LABEL_PRINT_BARCODES;
CREATE TABLE IMPETUS_MAPR_S1_DAY_1.EDWDATA.WMS_CARTON_LABEL_PRINT_BARCODES
(  print_job_num number, 
  dt_tm_printed timestamp_ntz, 
  carton_barcode string, 
  lpn_control string, 
  user_id string, 
  po_num string, 
  item_num string, 
  description string, 
  division_num number, 
  building_num number, 
  search_stores_from number, 
  search_stores_to number, 
  search_state string, 
  search_zip string, 
  search_district_code string, 
  search_intransit_time string, 
  search_store_grouping string, 
  cartclass_num number, 
  search_sort_order string, 
  store_list string, 
  quantity_list string, 
  order_list string, 
  sequence_list string, 
  label_qty number,
  item_id number
  )
;
DROP TABLE IF EXISTS IMPETUS_MAPR_S1_DAY_0.EDLDATA.CLPSWEB_PRINT_JOB;
CREATE TABLE IMPETUS_MAPR_S1_DAY_0.EDLDATA.CLPSWEB_PRINT_JOB
(  print_job_num number COMMENT '', 
  dt_tm_printed number COMMENT '', 
  user_id string COMMENT '', 
  po_num string COMMENT '', 
  item_num string COMMENT '', 
  description string COMMENT '', 
  division_num number COMMENT '', 
  building_num number COMMENT '', 
  search_stores_from number COMMENT '', 
  search_stores_to number COMMENT '', 
  search_state string COMMENT '', 
  search_zip string COMMENT '', 
  search_district_code string COMMENT '', 
  search_intransit_time string COMMENT '', 
  search_store_grouping string COMMENT '', 
  cartclass_num number COMMENT '', 
  search_sort_order string COMMENT '', 
  store_list string COMMENT '', 
  quantity_list string COMMENT '', 
  order_list string COMMENT '', 
  sequence_list string COMMENT '', 
  label_qty number COMMENT '')
;
DROP TABLE IF EXISTS IMPETUS_MAPR_S1_DAY_0.EDLDATA.CLPSWEB_PRINT_CARTON_BARCODE;
CREATE TABLE IMPETUS_MAPR_S1_DAY_0.EDLDATA.CLPSWEB_PRINT_CARTON_BARCODE
(  print_job_num number COMMENT '', 
  dt_tm_printed number COMMENT '', 
  carton_barcode string COMMENT '', 
  lpn_control string COMMENT '')
