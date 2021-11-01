! echo completed loading into scct_db.rockport_ob_shipment_item ;
---jobname:scct-rockport-hive-stg1-scct_db.rockport_ocean_shipment -------------------------------------------------------------------------- ---target: stage1 table: scct_db.rockport_ocean_shipment  ---source: scct_work_db table : rockport_ocean_shipment_temp ---load type: upsert ---back posting: no ---author: ryadav ---last modified date: 05/08/2019 --------------------------------------------------------------------------  ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading rockport_ocean_shipment stage 1 base data -------------------------  set tez.job.name=stage1:rockport_ocean_shipment :load target table rockport_ocean_shipment ;
! echo started loading into scct_db.rockport_ocean_shipment ;
INSERT OVERWRITE INTO SCCT_DB.ROCKPORT_OCEAN_SHIPMENT
(SELECT COALESCE(SRC.SHIPPING_METHOD, DEST.SHIPPING_METHOD, 'n/a') AS SHIPPING_METHOD, COALESCE(SRC.FCR_NBR, DEST.FCR_NBR, 'n/a') AS FCR_NBR, COALESCE(SRC.CARRIER_CD, DEST.CARRIER_CD, 'n/a') AS CARRIER_CD, COALESCE(SRC.OBOL_NBR, DEST.OBOL_NBR, 'n/a') AS OBOL_NBR, COALESCE(SRC.CONTAINER_NBR, DEST.CONTAINER_NBR, 'n/a') AS CONTAINER_NBR, COALESCE(SRC.SEAL_NBR, DEST.SEAL_NBR, 'n/a') AS SEAL_NBR, COALESCE(SRC.EQUIP_SIZE, DEST.EQUIP_SIZE, 'n/a') AS EQUIP_SIZE, COALESCE(SRC.CARGO_RECEIPT_DTM, DEST.CARGO_RECEIPT_DTM, '9999-12-31 00:00:00') AS CARGO_RECEIPT_DTM, COALESCE(SRC.SCHEDULED_DEPARTURE_DTM, DEST.SCHEDULED_DEPARTURE_DTM, '9999-12-31 00:00:00') AS SCHEDULED_DEPARTURE_DTM, COALESCE(SRC.ACTUAL_DEPARTURE_DTM, DEST.ACTUAL_DEPARTURE_DTM, '9999-12-31 00:00:00') AS ACTUAL_DEPARTURE_DTM, COALESCE(SRC.ETA_DISCHARGE_PORT_DTM, DEST.ETA_DISCHARGE_PORT_DTM, '9999-12-31 00:00:00') AS ETA_DISCHARGE_PORT_DTM, COALESCE(SRC.DISCHARGE_POINT_ARRIVAL_DTM, DEST.DISCHARGE_POINT_ARRIVAL_DTM, '9999-12-31 00:00:00') AS DISCHARGE_POINT_ARRIVAL_DTM, COALESCE(SRC.VESSEL_UNLOADED_DTM, DEST.VESSEL_UNLOADED_DTM, '9999-12-31 00:00:00') AS VESSEL_UNLOADED_DTM, COALESCE(SRC.RAIL_DEPARTURE_DTM, DEST.RAIL_DEPARTURE_DTM, '9999-12-31 00:00:00') AS RAIL_DEPARTURE_DTM, COALESCE(SRC.FINAL_DEST_ARRIVAL_DTM, DEST.FINAL_DEST_ARRIVAL_DTM, '9999-12-31 00:00:00') AS FINAL_DEST_ARRIVAL_DTM, COALESCE(SRC.PORT_OF_LADING_ID, DEST.PORT_OF_LADING_ID, 'n/a') AS PORT_OF_LADING_ID, COALESCE(SRC.PORT_OF_LADING_NAME, DEST.PORT_OF_LADING_NAME, 'n/a') AS PORT_OF_LADING_NAME, COALESCE(SRC.DISCHARGE_POINT_ID, DEST.DISCHARGE_POINT_ID, 'n/a') AS DISCHARGE_POINT_ID, COALESCE(SRC.PORT_OF_DISCHARGE_NAME, DEST.PORT_OF_DISCHARGE_NAME, 'n/a') AS PORT_OF_DISCHARGE_NAME, COALESCE(SRC.DESTINATION_ID, DEST.DESTINATION_ID, 'n/a') AS DESTINATION_ID, COALESCE(SRC.DESTINATION_NAME, DEST.DESTINATION_NAME, 'n/a') AS DESTINATION_NAME, COALESCE(SRC.VESSEL_NAME, DEST.VESSEL_NAME, 'n/a') AS VESSEL_NAME, COALESCE(SRC.VOYAGE_NBR, DEST.VOYAGE_NBR, 'n/a') AS VOYAGE_NBR, COALESCE(SRC.FORWARDER_ID, DEST.FORWARDER_ID, 'n/a') AS FORWARDER_ID, COALESCE(SRC.PO_NBR, DEST.PO_NBR, 'n/a') AS PO_NBR, COALESCE(SRC.CONTRACT_NBR, DEST.CONTRACT_NBR, 'n/a') AS CONTRACT_NBR, COALESCE(SRC.ORDER_NBR, DEST.ORDER_NBR, 'n/a') AS ORDER_NBR, COALESCE(SRC.ITM_NBR, DEST.ITM_NBR, 'n/a') AS ITM_NBR, COALESCE(SRC.LOAD_SEQ, DEST.LOAD_SEQ, 'n/a') AS LOAD_SEQ, COALESCE(SRC.PACKAGE_QTY, DEST.PACKAGE_QTY, 0.00) AS PACKAGE_QTY, COALESCE(SRC.PACKAGE_QTY_UOM, DEST.PACKAGE_QTY_UOM, 'n/a') AS PACKAGE_QTY_UOM, COALESCE(SRC.VOLUME, DEST.VOLUME, 0.00) AS VOLUME, COALESCE(SRC.VOLUME_UOM, DEST.VOLUME_UOM, 'n/a') AS VOLUME_UOM, COALESCE(SRC.WGT, DEST.WGT, 0.00) AS WGT, COALESCE(SRC.WGT_UOM, DEST.WGT_UOM, 'n/a') AS WGT_UOM, COALESCE(SRC.CONTAINER_WGT, DEST.CONTAINER_WGT, 0.00) AS CONTAINER_WGT, COALESCE(SRC.CONTAINER_WGT_UOM, DEST.CONTAINER_WGT_UOM, 'n/a') AS CONTAINER_WGT_UOM, COALESCE(SRC.LOAD_TYPE_ID, DEST.LOAD_TYPE_ID, 'n/a') AS LOAD_TYPE_ID, COALESCE(SRC.DELIVERY_LOCATION, DEST.DELIVERY_LOCATION, 'n/a') AS DELIVERY_LOCATION, COALESCE(SRC.HEADER_LAST_UPD_DTM, DEST.HEADER_LAST_UPD_DTM, '9999-12-31 00:00:00') AS HEADER_LAST_UPD_DTM, COALESCE(SRC.DETAIL_LAST_UPD_DTM, DEST.DETAIL_LAST_UPD_DTM, '9999-12-31 00:00:00') AS DETAIL_LAST_UPD_DTM
FROM SCCT_WORK_DB.ROCKPORT_OCEAN_SHIPMENT_TEMP AS SRC
FULL JOIN SCCT_DB.ROCKPORT_OCEAN_SHIPMENT AS DEST ON TRIM( SRC.FCR_NBR) = TRIM( DEST.FCR_NBR) AND TRIM( SRC.OBOL_NBR) = TRIM( DEST.OBOL_NBR) AND TRIM( SRC.CONTAINER_NBR) = TRIM( DEST.CONTAINER_NBR) AND TRIM( SRC.CONTRACT_NBR) = TRIM( DEST.CONTRACT_NBR) AND TRIM( SRC.ORDER_NBR) = TRIM( DEST.ORDER_NBR) AND TRIM( SRC.ITM_NBR) = TRIM( DEST.ITM_NBR) AND TRIM( SRC.DESTINATION_NAME) = TRIM( DEST.DESTINATION_NAME) AND TRIM( SRC.PORT_OF_DISCHARGE_NAME) = TRIM( DEST.PORT_OF_DISCHARGE_NAME) AND TRIM( SRC.LOAD_SEQ) = TRIM( DEST.LOAD_SEQ) AND TRIM( SRC.FORWARDER_ID) = TRIM( DEST.FORWARDER_ID));
! echo completed loading into scct_db.rockport_ob_shipment_item ;
---jobname:scct-rockport-hive-stg1-scct_db.rockport_ocean_shipment -------------------------------------------------------------------------- ---target: stage1 table: scct_db.rockport_ocean_shipment  ---source: scct_work_db table : rockport_ocean_shipment_temp ---load type: upsert ---back posting: no ---author: ryadav ---last modified date: 05/08/2019 --------------------------------------------------------------------------  ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading rockport_ocean_shipment stage 1 base data -------------------------  set tez.job.name=stage1:rockport_ocean_shipment :load target table rockport_ocean_shipment ;
! echo started loading into scct_db.rockport_ocean_shipment ;
INSERT OVERWRITE INTO SCCT_DB.ROCKPORT_OCEAN_SHIPMENT
(SELECT COALESCE(SRC.SHIPPING_METHOD, DEST.SHIPPING_METHOD, 'n/a') AS SHIPPING_METHOD, COALESCE(SRC.FCR_NBR, DEST.FCR_NBR, 'n/a') AS FCR_NBR, COALESCE(SRC.CARRIER_CD, DEST.CARRIER_CD, 'n/a') AS CARRIER_CD, COALESCE(SRC.OBOL_NBR, DEST.OBOL_NBR, 'n/a') AS OBOL_NBR, COALESCE(SRC.CONTAINER_NBR, DEST.CONTAINER_NBR, 'n/a') AS CONTAINER_NBR, COALESCE(SRC.SEAL_NBR, DEST.SEAL_NBR, 'n/a') AS SEAL_NBR, COALESCE(SRC.EQUIP_SIZE, DEST.EQUIP_SIZE, 'n/a') AS EQUIP_SIZE, COALESCE(SRC.CARGO_RECEIPT_DTM, DEST.CARGO_RECEIPT_DTM, '9999-12-31 00:00:00') AS CARGO_RECEIPT_DTM, COALESCE(SRC.SCHEDULED_DEPARTURE_DTM, DEST.SCHEDULED_DEPARTURE_DTM, '9999-12-31 00:00:00') AS SCHEDULED_DEPARTURE_DTM, COALESCE(SRC.ACTUAL_DEPARTURE_DTM, DEST.ACTUAL_DEPARTURE_DTM, '9999-12-31 00:00:00') AS ACTUAL_DEPARTURE_DTM, COALESCE(SRC.ETA_DISCHARGE_PORT_DTM, DEST.ETA_DISCHARGE_PORT_DTM, '9999-12-31 00:00:00') AS ETA_DISCHARGE_PORT_DTM, COALESCE(SRC.DISCHARGE_POINT_ARRIVAL_DTM, DEST.DISCHARGE_POINT_ARRIVAL_DTM, '9999-12-31 00:00:00') AS DISCHARGE_POINT_ARRIVAL_DTM, COALESCE(SRC.VESSEL_UNLOADED_DTM, DEST.VESSEL_UNLOADED_DTM, '9999-12-31 00:00:00') AS VESSEL_UNLOADED_DTM, COALESCE(SRC.RAIL_DEPARTURE_DTM, DEST.RAIL_DEPARTURE_DTM, '9999-12-31 00:00:00') AS RAIL_DEPARTURE_DTM, COALESCE(SRC.FINAL_DEST_ARRIVAL_DTM, DEST.FINAL_DEST_ARRIVAL_DTM, '9999-12-31 00:00:00') AS FINAL_DEST_ARRIVAL_DTM, COALESCE(SRC.PORT_OF_LADING_ID, DEST.PORT_OF_LADING_ID, 'n/a') AS PORT_OF_LADING_ID, COALESCE(SRC.PORT_OF_LADING_NAME, DEST.PORT_OF_LADING_NAME, 'n/a') AS PORT_OF_LADING_NAME, COALESCE(SRC.DISCHARGE_POINT_ID, DEST.DISCHARGE_POINT_ID, 'n/a') AS DISCHARGE_POINT_ID, COALESCE(SRC.PORT_OF_DISCHARGE_NAME, DEST.PORT_OF_DISCHARGE_NAME, 'n/a') AS PORT_OF_DISCHARGE_NAME, COALESCE(SRC.DESTINATION_ID, DEST.DESTINATION_ID, 'n/a') AS DESTINATION_ID, COALESCE(SRC.DESTINATION_NAME, DEST.DESTINATION_NAME, 'n/a') AS DESTINATION_NAME, COALESCE(SRC.VESSEL_NAME, DEST.VESSEL_NAME, 'n/a') AS VESSEL_NAME, COALESCE(SRC.VOYAGE_NBR, DEST.VOYAGE_NBR, 'n/a') AS VOYAGE_NBR, COALESCE(SRC.FORWARDER_ID, DEST.FORWARDER_ID, 'n/a') AS FORWARDER_ID, COALESCE(SRC.PO_NBR, DEST.PO_NBR, 'n/a') AS PO_NBR, COALESCE(SRC.CONTRACT_NBR, DEST.CONTRACT_NBR, 'n/a') AS CONTRACT_NBR, COALESCE(SRC.ORDER_NBR, DEST.ORDER_NBR, 'n/a') AS ORDER_NBR, COALESCE(SRC.ITM_NBR, DEST.ITM_NBR, 'n/a') AS ITM_NBR, COALESCE(SRC.LOAD_SEQ, DEST.LOAD_SEQ, 'n/a') AS LOAD_SEQ, COALESCE(SRC.PACKAGE_QTY, DEST.PACKAGE_QTY, 0.00) AS PACKAGE_QTY, COALESCE(SRC.PACKAGE_QTY_UOM, DEST.PACKAGE_QTY_UOM, 'n/a') AS PACKAGE_QTY_UOM, COALESCE(SRC.VOLUME, DEST.VOLUME, 0.00) AS VOLUME, COALESCE(SRC.VOLUME_UOM, DEST.VOLUME_UOM, 'n/a') AS VOLUME_UOM, COALESCE(SRC.WGT, DEST.WGT, 0.00) AS WGT, COALESCE(SRC.WGT_UOM, DEST.WGT_UOM, 'n/a') AS WGT_UOM, COALESCE(SRC.CONTAINER_WGT, DEST.CONTAINER_WGT, 0.00) AS CONTAINER_WGT, COALESCE(SRC.CONTAINER_WGT_UOM, DEST.CONTAINER_WGT_UOM, 'n/a') AS CONTAINER_WGT_UOM, COALESCE(SRC.LOAD_TYPE_ID, DEST.LOAD_TYPE_ID, 'n/a') AS LOAD_TYPE_ID, COALESCE(SRC.DELIVERY_LOCATION, DEST.DELIVERY_LOCATION, 'n/a') AS DELIVERY_LOCATION, COALESCE(SRC.HEADER_LAST_UPD_DTM, DEST.HEADER_LAST_UPD_DTM, '9999-12-31 00:00:00') AS HEADER_LAST_UPD_DTM, COALESCE(SRC.DETAIL_LAST_UPD_DTM, DEST.DETAIL_LAST_UPD_DTM, '9999-12-31 00:00:00') AS DETAIL_LAST_UPD_DTM
FROM SCCT_WORK_DB.ROCKPORT_OCEAN_SHIPMENT_TEMP AS SRC
FULL JOIN SCCT_DB.ROCKPORT_OCEAN_SHIPMENT AS DEST ON TRIM( SRC.FCR_NBR) = TRIM( DEST.FCR_NBR) AND TRIM( SRC.OBOL_NBR) = TRIM( DEST.OBOL_NBR) AND TRIM( SRC.CONTAINER_NBR) = TRIM( DEST.CONTAINER_NBR) AND TRIM( SRC.CONTRACT_NBR) = TRIM( DEST.CONTRACT_NBR) AND TRIM( SRC.ORDER_NBR) = TRIM( DEST.ORDER_NBR) AND TRIM( SRC.ITM_NBR) = TRIM( DEST.ITM_NBR) AND TRIM( SRC.DESTINATION_NAME) = TRIM( DEST.DESTINATION_NAME) AND TRIM( SRC.PORT_OF_DISCHARGE_NAME) = TRIM( DEST.PORT_OF_DISCHARGE_NAME) AND TRIM( SRC.LOAD_SEQ) = TRIM( DEST.LOAD_SEQ) AND TRIM( SRC.FORWARDER_ID) = TRIM( DEST.FORWARDER_ID));
! echo completed loading into scct_db.rockport_ob_shipment_item ;
---jobname:scct-rockport-hive-stg1-scct_db.rockport_ocean_shipment -------------------------------------------------------------------------- ---target: stage1 table: scct_db.rockport_ocean_shipment  ---source: scct_work_db table : rockport_ocean_shipment_temp ---load type: upsert ---back posting: no ---author: ryadav ---last modified date: 05/08/2019 --------------------------------------------------------------------------  ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading rockport_ocean_shipment stage 1 base data -------------------------  set tez.job.name=stage1:rockport_ocean_shipment :load target table rockport_ocean_shipment ;
! echo started loading into scct_db.rockport_ocean_shipment ;
INSERT OVERWRITE INTO SCCT_DB.ROCKPORT_OCEAN_SHIPMENT
(SELECT COALESCE(SRC.SHIPPING_METHOD, DEST.SHIPPING_METHOD, 'n/a') AS SHIPPING_METHOD, COALESCE(SRC.FCR_NBR, DEST.FCR_NBR, 'n/a') AS FCR_NBR, COALESCE(SRC.CARRIER_CD, DEST.CARRIER_CD, 'n/a') AS CARRIER_CD, COALESCE(SRC.OBOL_NBR, DEST.OBOL_NBR, 'n/a') AS OBOL_NBR, COALESCE(SRC.CONTAINER_NBR, DEST.CONTAINER_NBR, 'n/a') AS CONTAINER_NBR, COALESCE(SRC.SEAL_NBR, DEST.SEAL_NBR, 'n/a') AS SEAL_NBR, COALESCE(SRC.EQUIP_SIZE, DEST.EQUIP_SIZE, 'n/a') AS EQUIP_SIZE, COALESCE(SRC.CARGO_RECEIPT_DTM, DEST.CARGO_RECEIPT_DTM, '9999-12-31 00:00:00') AS CARGO_RECEIPT_DTM, COALESCE(SRC.SCHEDULED_DEPARTURE_DTM, DEST.SCHEDULED_DEPARTURE_DTM, '9999-12-31 00:00:00') AS SCHEDULED_DEPARTURE_DTM, COALESCE(SRC.ACTUAL_DEPARTURE_DTM, DEST.ACTUAL_DEPARTURE_DTM, '9999-12-31 00:00:00') AS ACTUAL_DEPARTURE_DTM, COALESCE(SRC.ETA_DISCHARGE_PORT_DTM, DEST.ETA_DISCHARGE_PORT_DTM, '9999-12-31 00:00:00') AS ETA_DISCHARGE_PORT_DTM, COALESCE(SRC.DISCHARGE_POINT_ARRIVAL_DTM, DEST.DISCHARGE_POINT_ARRIVAL_DTM, '9999-12-31 00:00:00') AS DISCHARGE_POINT_ARRIVAL_DTM, COALESCE(SRC.VESSEL_UNLOADED_DTM, DEST.VESSEL_UNLOADED_DTM, '9999-12-31 00:00:00') AS VESSEL_UNLOADED_DTM, COALESCE(SRC.RAIL_DEPARTURE_DTM, DEST.RAIL_DEPARTURE_DTM, '9999-12-31 00:00:00') AS RAIL_DEPARTURE_DTM, COALESCE(SRC.FINAL_DEST_ARRIVAL_DTM, DEST.FINAL_DEST_ARRIVAL_DTM, '9999-12-31 00:00:00') AS FINAL_DEST_ARRIVAL_DTM, COALESCE(SRC.PORT_OF_LADING_ID, DEST.PORT_OF_LADING_ID, 'n/a') AS PORT_OF_LADING_ID, COALESCE(SRC.PORT_OF_LADING_NAME, DEST.PORT_OF_LADING_NAME, 'n/a') AS PORT_OF_LADING_NAME, COALESCE(SRC.DISCHARGE_POINT_ID, DEST.DISCHARGE_POINT_ID, 'n/a') AS DISCHARGE_POINT_ID, COALESCE(SRC.PORT_OF_DISCHARGE_NAME, DEST.PORT_OF_DISCHARGE_NAME, 'n/a') AS PORT_OF_DISCHARGE_NAME, COALESCE(SRC.DESTINATION_ID, DEST.DESTINATION_ID, 'n/a') AS DESTINATION_ID, COALESCE(SRC.DESTINATION_NAME, DEST.DESTINATION_NAME, 'n/a') AS DESTINATION_NAME, COALESCE(SRC.VESSEL_NAME, DEST.VESSEL_NAME, 'n/a') AS VESSEL_NAME, COALESCE(SRC.VOYAGE_NBR, DEST.VOYAGE_NBR, 'n/a') AS VOYAGE_NBR, COALESCE(SRC.FORWARDER_ID, DEST.FORWARDER_ID, 'n/a') AS FORWARDER_ID, COALESCE(SRC.PO_NBR, DEST.PO_NBR, 'n/a') AS PO_NBR, COALESCE(SRC.CONTRACT_NBR, DEST.CONTRACT_NBR, 'n/a') AS CONTRACT_NBR, COALESCE(SRC.ORDER_NBR, DEST.ORDER_NBR, 'n/a') AS ORDER_NBR, COALESCE(SRC.ITM_NBR, DEST.ITM_NBR, 'n/a') AS ITM_NBR, COALESCE(SRC.LOAD_SEQ, DEST.LOAD_SEQ, 'n/a') AS LOAD_SEQ, COALESCE(SRC.PACKAGE_QTY, DEST.PACKAGE_QTY, 0.00) AS PACKAGE_QTY, COALESCE(SRC.PACKAGE_QTY_UOM, DEST.PACKAGE_QTY_UOM, 'n/a') AS PACKAGE_QTY_UOM, COALESCE(SRC.VOLUME, DEST.VOLUME, 0.00) AS VOLUME, COALESCE(SRC.VOLUME_UOM, DEST.VOLUME_UOM, 'n/a') AS VOLUME_UOM, COALESCE(SRC.WGT, DEST.WGT, 0.00) AS WGT, COALESCE(SRC.WGT_UOM, DEST.WGT_UOM, 'n/a') AS WGT_UOM, COALESCE(SRC.CONTAINER_WGT, DEST.CONTAINER_WGT, 0.00) AS CONTAINER_WGT, COALESCE(SRC.CONTAINER_WGT_UOM, DEST.CONTAINER_WGT_UOM, 'n/a') AS CONTAINER_WGT_UOM, COALESCE(SRC.LOAD_TYPE_ID, DEST.LOAD_TYPE_ID, 'n/a') AS LOAD_TYPE_ID, COALESCE(SRC.DELIVERY_LOCATION, DEST.DELIVERY_LOCATION, 'n/a') AS DELIVERY_LOCATION, COALESCE(SRC.HEADER_LAST_UPD_DTM, DEST.HEADER_LAST_UPD_DTM, '9999-12-31 00:00:00') AS HEADER_LAST_UPD_DTM, COALESCE(SRC.DETAIL_LAST_UPD_DTM, DEST.DETAIL_LAST_UPD_DTM, '9999-12-31 00:00:00') AS DETAIL_LAST_UPD_DTM
FROM SCCT_WORK_DB.ROCKPORT_OCEAN_SHIPMENT_TEMP AS SRC
FULL JOIN SCCT_DB.ROCKPORT_OCEAN_SHIPMENT AS DEST ON TRIM( SRC.FCR_NBR) = TRIM( DEST.FCR_NBR) AND TRIM( SRC.OBOL_NBR) = TRIM( DEST.OBOL_NBR) AND TRIM( SRC.CONTAINER_NBR) = TRIM( DEST.CONTAINER_NBR) AND TRIM( SRC.CONTRACT_NBR) = TRIM( DEST.CONTRACT_NBR) AND TRIM( SRC.ORDER_NBR) = TRIM( DEST.ORDER_NBR) AND TRIM( SRC.ITM_NBR) = TRIM( DEST.ITM_NBR) AND TRIM( SRC.DESTINATION_NAME) = TRIM( DEST.DESTINATION_NAME) AND TRIM( SRC.PORT_OF_DISCHARGE_NAME) = TRIM( DEST.PORT_OF_DISCHARGE_NAME) AND TRIM( SRC.LOAD_SEQ) = TRIM( DEST.LOAD_SEQ) AND TRIM( SRC.FORWARDER_ID) = TRIM( DEST.FORWARDER_ID));
! echo completed loading into scct_db.rockport_ob_shipment_item ;
---jobname:scct-rockport-hive-stg1-scct_db.rockport_ocean_shipment -------------------------------------------------------------------------- ---target: stage1 table: scct_db.rockport_ocean_shipment  ---source: scct_work_db table : rockport_ocean_shipment_temp ---load type: upsert ---back posting: no ---author: ryadav ---last modified date: 05/08/2019 --------------------------------------------------------------------------  ------------------------- ---job related hive settings to merge files for target table -------------------------  set hive.merge.orcfile.stripe.level=false;
------------------------- ---loading rockport_ocean_shipment stage 1 base data -------------------------  set tez.job.name=stage1:rockport_ocean_shipment :load target table rockport_ocean_shipment ;
! echo started loading into scct_db.rockport_ocean_shipment ;
INSERT OVERWRITE INTO SCCT_DB.ROCKPORT_OCEAN_SHIPMENT
(SELECT COALESCE(SRC.SHIPPING_METHOD, DEST.SHIPPING_METHOD, 'n/a') AS SHIPPING_METHOD, COALESCE(SRC.FCR_NBR, DEST.FCR_NBR, 'n/a') AS FCR_NBR, COALESCE(SRC.CARRIER_CD, DEST.CARRIER_CD, 'n/a') AS CARRIER_CD, COALESCE(SRC.OBOL_NBR, DEST.OBOL_NBR, 'n/a') AS OBOL_NBR, COALESCE(SRC.CONTAINER_NBR, DEST.CONTAINER_NBR, 'n/a') AS CONTAINER_NBR, COALESCE(SRC.SEAL_NBR, DEST.SEAL_NBR, 'n/a') AS SEAL_NBR, COALESCE(SRC.EQUIP_SIZE, DEST.EQUIP_SIZE, 'n/a') AS EQUIP_SIZE, COALESCE(SRC.CARGO_RECEIPT_DTM, DEST.CARGO_RECEIPT_DTM, '9999-12-31 00:00:00') AS CARGO_RECEIPT_DTM, COALESCE(SRC.SCHEDULED_DEPARTURE_DTM, DEST.SCHEDULED_DEPARTURE_DTM, '9999-12-31 00:00:00') AS SCHEDULED_DEPARTURE_DTM, COALESCE(SRC.ACTUAL_DEPARTURE_DTM, DEST.ACTUAL_DEPARTURE_DTM, '9999-12-31 00:00:00') AS ACTUAL_DEPARTURE_DTM, COALESCE(SRC.ETA_DISCHARGE_PORT_DTM, DEST.ETA_DISCHARGE_PORT_DTM, '9999-12-31 00:00:00') AS ETA_DISCHARGE_PORT_DTM, COALESCE(SRC.DISCHARGE_POINT_ARRIVAL_DTM, DEST.DISCHARGE_POINT_ARRIVAL_DTM, '9999-12-31 00:00:00') AS DISCHARGE_POINT_ARRIVAL_DTM, COALESCE(SRC.VESSEL_UNLOADED_DTM, DEST.VESSEL_UNLOADED_DTM, '9999-12-31 00:00:00') AS VESSEL_UNLOADED_DTM, COALESCE(SRC.RAIL_DEPARTURE_DTM, DEST.RAIL_DEPARTURE_DTM, '9999-12-31 00:00:00') AS RAIL_DEPARTURE_DTM, COALESCE(SRC.FINAL_DEST_ARRIVAL_DTM, DEST.FINAL_DEST_ARRIVAL_DTM, '9999-12-31 00:00:00') AS FINAL_DEST_ARRIVAL_DTM, COALESCE(SRC.PORT_OF_LADING_ID, DEST.PORT_OF_LADING_ID, 'n/a') AS PORT_OF_LADING_ID, COALESCE(SRC.PORT_OF_LADING_NAME, DEST.PORT_OF_LADING_NAME, 'n/a') AS PORT_OF_LADING_NAME, COALESCE(SRC.DISCHARGE_POINT_ID, DEST.DISCHARGE_POINT_ID, 'n/a') AS DISCHARGE_POINT_ID, COALESCE(SRC.PORT_OF_DISCHARGE_NAME, DEST.PORT_OF_DISCHARGE_NAME, 'n/a') AS PORT_OF_DISCHARGE_NAME, COALESCE(SRC.DESTINATION_ID, DEST.DESTINATION_ID, 'n/a') AS DESTINATION_ID, COALESCE(SRC.DESTINATION_NAME, DEST.DESTINATION_NAME, 'n/a') AS DESTINATION_NAME, COALESCE(SRC.VESSEL_NAME, DEST.VESSEL_NAME, 'n/a') AS VESSEL_NAME, COALESCE(SRC.VOYAGE_NBR, DEST.VOYAGE_NBR, 'n/a') AS VOYAGE_NBR, COALESCE(SRC.FORWARDER_ID, DEST.FORWARDER_ID, 'n/a') AS FORWARDER_ID, COALESCE(SRC.PO_NBR, DEST.PO_NBR, 'n/a') AS PO_NBR, COALESCE(SRC.CONTRACT_NBR, DEST.CONTRACT_NBR, 'n/a') AS CONTRACT_NBR, COALESCE(SRC.ORDER_NBR, DEST.ORDER_NBR, 'n/a') AS ORDER_NBR, COALESCE(SRC.ITM_NBR, DEST.ITM_NBR, 'n/a') AS ITM_NBR, COALESCE(SRC.LOAD_SEQ, DEST.LOAD_SEQ, 'n/a') AS LOAD_SEQ, COALESCE(SRC.PACKAGE_QTY, DEST.PACKAGE_QTY, 0.00) AS PACKAGE_QTY, COALESCE(SRC.PACKAGE_QTY_UOM, DEST.PACKAGE_QTY_UOM, 'n/a') AS PACKAGE_QTY_UOM, COALESCE(SRC.VOLUME, DEST.VOLUME, 0.00) AS VOLUME, COALESCE(SRC.VOLUME_UOM, DEST.VOLUME_UOM, 'n/a') AS VOLUME_UOM, COALESCE(SRC.WGT, DEST.WGT, 0.00) AS WGT, COALESCE(SRC.WGT_UOM, DEST.WGT_UOM, 'n/a') AS WGT_UOM, COALESCE(SRC.CONTAINER_WGT, DEST.CONTAINER_WGT, 0.00) AS CONTAINER_WGT, COALESCE(SRC.CONTAINER_WGT_UOM, DEST.CONTAINER_WGT_UOM, 'n/a') AS CONTAINER_WGT_UOM, COALESCE(SRC.LOAD_TYPE_ID, DEST.LOAD_TYPE_ID, 'n/a') AS LOAD_TYPE_ID, COALESCE(SRC.DELIVERY_LOCATION, DEST.DELIVERY_LOCATION, 'n/a') AS DELIVERY_LOCATION, COALESCE(SRC.HEADER_LAST_UPD_DTM, DEST.HEADER_LAST_UPD_DTM, '9999-12-31 00:00:00') AS HEADER_LAST_UPD_DTM, COALESCE(SRC.DETAIL_LAST_UPD_DTM, DEST.DETAIL_LAST_UPD_DTM, '9999-12-31 00:00:00') AS DETAIL_LAST_UPD_DTM
FROM SCCT_WORK_DB.ROCKPORT_OCEAN_SHIPMENT_TEMP AS SRC
FULL JOIN SCCT_DB.ROCKPORT_OCEAN_SHIPMENT AS DEST ON TRIM( SRC.FCR_NBR) = TRIM( DEST.FCR_NBR) AND TRIM( SRC.OBOL_NBR) = TRIM( DEST.OBOL_NBR) AND TRIM( SRC.CONTAINER_NBR) = TRIM( DEST.CONTAINER_NBR) AND TRIM( SRC.CONTRACT_NBR) = TRIM( DEST.CONTRACT_NBR) AND TRIM( SRC.ORDER_NBR) = TRIM( DEST.ORDER_NBR) AND TRIM( SRC.ITM_NBR) = TRIM( DEST.ITM_NBR) AND TRIM( SRC.DESTINATION_NAME) = TRIM( DEST.DESTINATION_NAME) AND TRIM( SRC.PORT_OF_DISCHARGE_NAME) = TRIM( DEST.PORT_OF_DISCHARGE_NAME) AND TRIM( SRC.LOAD_SEQ) = TRIM( DEST.LOAD_SEQ) AND TRIM( SRC.FORWARDER_ID) = TRIM( DEST.FORWARDER_ID));
