---jobname:scct-rockport-hive-stg1-scct_db.rockport_ocean_shipment
--------------------------------------------------------------------------
---Target: STAGE1 table: SCCT_DB.ROCKPORT_OCEAN_SHIPMENT 
---Source: SCCT_WORK_DB table : ROCKPORT_OCEAN_SHIPMENT_TEMP
---Load Type: Upsert
---Back posting: No
---Author: ryadav
---Last modified date: 05/08/2019
--------------------------------------------------------------------------

-------------------------
---Job related Hive settings to merge files for Target table
-------------------------

set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

-------------------------
---Loading ROCKPORT_OCEAN_SHIPMENT stage 1 base data
-------------------------

SET tez.job.name=STAGE1:ROCKPORT_OCEAN_SHIPMENT :load target table ROCKPORT_OCEAN_SHIPMENT ;
! echo Started Loading into SCCT_DB.ROCKPORT_OCEAN_SHIPMENT ;

INSERT OVERWRITE TABLE SCCT_DB.ROCKPORT_OCEAN_SHIPMENT
SELECT
COALESCE(SRC.shipping_method,DEST.shipping_method, 'N/A') AS shipping_method
,COALESCE(SRC.fcr_nbr,DEST.fcr_nbr, 'N/A') AS fcr_nbr
,COALESCE(SRC.carrier_cd,DEST.carrier_cd, 'N/A') AS carrier_cd
,COALESCE(SRC.obol_nbr,DEST.obol_nbr, 'N/A') AS obol_nbr
,COALESCE(SRC.container_nbr,DEST.container_nbr, 'N/A') AS container_nbr
,COALESCE(SRC.seal_nbr,DEST.seal_nbr, 'N/A') AS seal_nbr
,COALESCE(SRC.equip_size,DEST.equip_size, 'N/A') AS equip_size
,COALESCE(SRC.cargo_receipt_dtm,DEST.cargo_receipt_dtm, '9999-12-31 00:00:00') AS cargo_receipt_dtm
,COALESCE(SRC.scheduled_departure_dtm,DEST.scheduled_departure_dtm, '9999-12-31 00:00:00') AS scheduled_departure_dtm
,COALESCE(SRC.actual_departure_dtm,DEST.actual_departure_dtm, '9999-12-31 00:00:00') AS actual_departure_dtm
,COALESCE(SRC.eta_discharge_port_dtm,DEST.eta_discharge_port_dtm, '9999-12-31 00:00:00') AS eta_discharge_port_dtm
,COALESCE(SRC.discharge_point_arrival_dtm,DEST.discharge_point_arrival_dtm, '9999-12-31 00:00:00') AS discharge_point_arrival_dtm
,COALESCE(SRC.vessel_unloaded_dtm,DEST.vessel_unloaded_dtm, '9999-12-31 00:00:00') AS vessel_unloaded_dtm
,COALESCE(SRC.rail_departure_dtm,DEST.rail_departure_dtm, '9999-12-31 00:00:00') AS rail_departure_dtm
,COALESCE(SRC.final_dest_arrival_dtm,DEST.final_dest_arrival_dtm, '9999-12-31 00:00:00') AS final_dest_arrival_dtm
,COALESCE(SRC.port_of_lading_id,DEST.port_of_lading_id, 'N/A') AS port_of_lading_id
,COALESCE(SRC.port_of_lading_name,DEST.port_of_lading_name, 'N/A') AS port_of_lading_name
,COALESCE(SRC.discharge_point_id,DEST.discharge_point_id, 'N/A') AS discharge_point_id
,COALESCE(SRC.port_of_discharge_name,DEST.port_of_discharge_name, 'N/A') AS port_of_discharge_name
,COALESCE(SRC.destination_id,DEST.destination_id, 'N/A') AS destination_id
,COALESCE(SRC.destination_name,DEST.destination_name, 'N/A') AS destination_name
,COALESCE(SRC.vessel_name,DEST.vessel_name, 'N/A') AS vessel_name
,COALESCE(SRC.voyage_nbr,DEST.voyage_nbr, 'N/A') AS voyage_nbr
,COALESCE(SRC.forwarder_id,DEST.forwarder_id, 'N/A') AS forwarder_id
,COALESCE(SRC.po_nbr,DEST.po_nbr, 'N/A') AS po_nbr
,COALESCE(SRC.contract_nbr,DEST.contract_nbr, 'N/A') AS contract_nbr
,COALESCE(SRC.order_nbr,DEST.order_nbr, 'N/A') AS order_nbr
,COALESCE(SRC.itm_nbr,DEST.itm_nbr, 'N/A') AS itm_nbr
,COALESCE(SRC.load_seq,DEST.load_seq, 'N/A') AS load_seq
,COALESCE(SRC.package_qty,DEST.package_qty, 0.00) AS package_qty
,COALESCE(SRC.package_qty_uom,DEST.package_qty_uom, 'N/A') AS package_qty_uom
,COALESCE(SRC.volume,DEST.volume, 0.00) AS volume
,COALESCE(SRC.volume_uom,DEST.volume_uom, 'N/A') AS volume_uom
,COALESCE(SRC.wgt,DEST.wgt, 0.00) AS wgt
,COALESCE(SRC.wgt_uom,DEST.wgt_uom, 'N/A') AS wgt_uom
,COALESCE(SRC.container_wgt,DEST.container_wgt, 0.00) AS container_wgt
,COALESCE(SRC.container_wgt_uom,DEST.container_wgt_uom, 'N/A') AS container_wgt_uom
,COALESCE(SRC.load_type_id,DEST.load_type_id, 'N/A') AS load_type_id
,COALESCE(SRC.delivery_location,DEST.delivery_location, 'N/A') AS delivery_location
,COALESCE(SRC.header_last_upd_dtm,DEST.header_last_upd_dtm, '9999-12-31 00:00:00') AS header_last_upd_dtm
,COALESCE(SRC.detail_last_upd_dtm,DEST.detail_last_upd_dtm, '9999-12-31 00:00:00') AS detail_last_upd_dtm
FROM SCCT_WORK_DB.ROCKPORT_OCEAN_SHIPMENT_TEMP SRC
FULL OUTER JOIN SCCT_DB.ROCKPORT_OCEAN_SHIPMENT DEST
ON TRIM(SRC.FCR_NBR)=TRIM(DEST.FCR_NBR)
AND TRIM(SRC.OBOL_NBR)=TRIM(DEST.OBOL_NBR)
AND TRIM(SRC.CONTAINER_NBR)=TRIM(DEST.CONTAINER_NBR)
AND TRIM(SRC.CONTRACT_NBR)=TRIM(DEST.CONTRACT_NBR)
AND TRIM(SRC.ORDER_NBR)=TRIM(DEST.ORDER_NBR)
AND TRIM(SRC.itm_nbr)=TRIM(DEST.itm_nbr)
AND TRIM(SRC.DESTINATION_NAME)=TRIM(DEST.DESTINATION_NAME)
AND TRIM(SRC.PORT_OF_DISCHARGE_NAME)=TRIM(DEST.PORT_OF_DISCHARGE_NAME)
AND TRIM(SRC.LOAD_SEQ)=TRIM(DEST.LOAD_SEQ)
AND TRIM(SRC.FORWARDER_ID)=TRIM(DEST.FORWARDER_ID)
;


! echo Completed Loading into SCCT_DB.ROCKPORT_OCEAN_SHIPMENT ;

 -------------------------
 ---end script
 -------------------------
