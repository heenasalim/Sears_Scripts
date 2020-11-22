/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_kmart_vendor_package_location_warehouse_level.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Wed Apr 23 02:52:57 EDT 2014
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>
#
#
#
# DEPENDENCIES: <<TODO>>
#
#
# REV LIST:
#        DATE         BY            MODIFICATION
#	 1/16/2017      SRUJAN DUSSA    IPS-779 - Added rim_last_record_creation_dt to the vendor pack location tables.  This is a sears only column that will be defaulted to '1970-01-01'
#
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

SET default_parallel $NUM_PARALLEL;
REGISTER  $UDF_JAR;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

--------------------------------------------------------------------------

gold__item_aprk_current_data = 
      LOAD '$GOLD__ITEM_APRK_CURRENT_LOCATION'
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($GOLD__ITEM_APRK_CURRENT_SCHEMA);

work__store_level_vend_pack_loc_final_data = 
      LOAD '$WORK__IDRP_STORE_LEVEL_VEND_PACK_LOC_FINAL_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_STORE_LEVEL_VEND_PACK_LOC_FINAL_SCHEMA);

work__store_level_vend_pack_loc_final_data_fltr = 
      FILTER work__store_level_vend_pack_loc_final_data 
      BY (flow_type_cd=='JIT' OR servicing_dc_nbr>'0');


work__idrp_vp_dc_start = 
      FOREACH work__store_level_vend_pack_loc_final_data_fltr
      GENERATE
              shc_item_id AS shc_item_id,
              'K' AS source_owner_cd,
              ksn_id, 
              item_purchase_status_cd AS item_purchase_status_cd,
              vendor_package_id AS vendor_package_id, 
              vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              flow_type_cd AS vendor_package_flow_type_cd,
              vendor_carton_qty AS vendor_carton_qty,
              vendor_stock_nbr AS vendor_stock_nbr,
              ksn_package_id AS ksn_package_id,
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              import_ind AS import_ind,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr, 
              scan_based_trading_ind AS scan_based_trading_ind,
              cross_merchandising_cd AS cross_merchandising_cd,
              retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              vendor_package_owner_cd AS vendor_package_owner_cd,
              can_carry_model_id AS can_carry_model_id,
              '' AS days_to_check_begin_day_qty,
              '' AS days_to_check_end_day_qty,
              dotcom_allocation_ind AS dotcom_orderable_cd,
              retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              allocation_replenishment_cd AS allocation_replenishment_cd,
              --dc_configuration_cd AS dc_configuration_cd,
              shc_item_type_cd AS shc_item_type_cd,
              idrp_order_method_cd AS idrp_order_method_cd,
              --Spira# 4377
              source_package_qty AS store_source_package_qty,
			  order_duns_nbr AS order_duns_nbr; -- << Spira -4436

work__idrp_vp_dc_start = DISTINCT work__idrp_vp_dc_start;


--------------------------------------------------------------------------------------------------------------- 

---------------------------------------------------------------------------------------------------------------
smith__idrp_eligible_loc_data = 
      LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);

smith__idrp_eligible_loc_data = 
      FOREACH smith__idrp_eligible_loc_data
      GENERATE 
               loc AS loc,
               srs_loc AS srs_loc,
               loc_lvl_cd AS loc_lvl_cd,
               fmt_typ_cd AS fmt_typ_cd,
               loc_fmt_typ_cd AS loc_fmt_typ_cd,
               loc_owner_cd AS loc_owner_cd;


smith__idrp_vend_pack_dc_combined_data = 
      LOAD '$SMITH__IDRP_VEND_PACK_DC_COMBINED_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($SMITH__IDRP_VEND_PACK_DC_COMBINED_SCHEMA);

smith__idrp_vend_pack_dc_combined_data = 
      FOREACH smith__idrp_vend_pack_dc_combined_data
      GENERATE 
              vendor_package_id AS vendor_package_id,
              location_nbr AS location_nbr,
              inbound_carton_per_layer_qty AS inbound_carton_per_layer_qty,
              inbound_layer_per_pallet_qty AS inbound_layer_per_pallet_qty,
              inbound_order_uom_cd AS inbound_order_uom_cd,
              ship_aprk_id AS ship_aprk_id,
              ksn_pack_purchase_status_cd AS ksn_pack_purchase_status_cd,
              ksn_package_id AS ksn_package_id,
              dc_stock_ind AS stock_ind,
              substition_eligibile_ind AS substitution_eligible_ind,
              outbound_package_qty AS outbound_package_qty,
	          ship_duns_nbr AS ship_duns_nbr,
              effective_ts AS effective_ts,
              expiration_ts AS expiration_ts,
              vendor_managed_inventory_cd  AS vendor_managed_inventory_cd,
              dc_handling_cd AS dc_handling_cd;


smith__idrp_inbound_vendor_package_dc_driver_data = 
      LOAD '$SMITH__IDRP_INBOUND_VENDOR_PACKAGE_DC_DRIVER_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($SMITH__IDRP_INBOUND_VENDOR_PACKAGE_DC_DRIVER_SCHEMA); 


smith__idrp_inbound_vendor_package_dc_driver_data = 
      FOREACH smith__idrp_inbound_vendor_package_dc_driver_data 
      GENERATE 
              item_id AS inbnd_item_id,
              vendor_package_id AS inbnd_vend_pack_id,
              dc_location_nbr AS inbnd_dc_locn_nbr,
              replenishment_planning_ind AS replenishment_planning_ind;             

             

work__idrp_kmart_vendor_package_location_store_level_data = 
      LOAD '$WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_STORE_LEVEL_LOCATION_PART1' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_STORE_LEVEL_SCHEMA);


work__idrp_kmart_vendor_package_location_store_level_fltr = 
      FILTER work__idrp_kmart_vendor_package_location_store_level_data 
      BY active_ind=='Y';


smith__idrp_dc_location_current_data = 
      LOAD '$SMITH_IDRP_DC_LOCATION_CURRENT_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($SMITH_IDRP_DC_LOCATION_CURRENT_SCHEMA);

smith__idrp_ie_batchdate_data = 
      LOAD '$SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_SCHEMA);


smith__join_smith = 
      CROSS smith__idrp_ie_batchdate_data,
            smith__idrp_vend_pack_dc_combined_data;


smith__idrp_vend_pack_dc_combined_data_fltr_time = 
      FILTER smith__join_smith 
      BY (TRIM(smith__idrp_ie_batchdate_data::processing_ts) >= TRIM(smith__idrp_vend_pack_dc_combined_data::effective_ts)
      AND
          TRIM(smith__idrp_ie_batchdate_data::processing_ts) <= TRIM(smith__idrp_vend_pack_dc_combined_data::expiration_ts)); 
              

smith__idrp_vend_pack_dc_combined_data_fltr_time_fltr = 
      FILTER smith__idrp_vend_pack_dc_combined_data_fltr_time BY smith__idrp_vend_pack_dc_combined_data::location_nbr!='8277';


smith__idrp_vend_pack_dc_combined_fltr = 
      FOREACH smith__idrp_vend_pack_dc_combined_data_fltr_time_fltr 
      GENERATE 
              vendor_package_id AS vendor_package_id,
              location_nbr AS location_nbr,
              inbound_carton_per_layer_qty AS inbound_carton_per_layer_qty,
              inbound_layer_per_pallet_qty AS inbound_layer_per_pallet_qty,
              inbound_order_uom_cd AS inbound_order_uom_cd,
              ship_aprk_id AS ship_aprk_id,
              ksn_pack_purchase_status_cd AS ksn_pack_purchase_status_cd,
              stock_ind AS stock_ind,
              substitution_eligible_ind AS substitution_eligible_ind,
              outbound_package_qty AS outbound_package_qty,
	          ship_duns_nbr AS ship_duns_nbr,
	          vendor_managed_inventory_cd  AS vendor_managed_inventory_cd,
              dc_handling_cd AS dc_handling_cd;

---------------------------------------------------------------------------------------------------------------

work__join_smith_vend_pack = 
      JOIN smith__idrp_vend_pack_dc_combined_fltr BY (int)vendor_package_id,
           work__idrp_vp_dc_start BY (int)vendor_package_id;

              
work__idrp_vp_dc_config_applied_flow_thru = 
      FOREACH work__join_smith_vend_pack
      GENERATE
              work__idrp_vp_dc_start::shc_item_id AS shc_item_id,
              work__idrp_vp_dc_start::source_owner_cd AS source_owner_cd,
              work__idrp_vp_dc_start::ksn_id AS ksn_id,
              work__idrp_vp_dc_start::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_vp_dc_start::vendor_package_id AS vendor_package_id,
              work__idrp_vp_dc_start::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_vp_dc_start::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_vp_dc_start::vendor_package_flow_type_cd AS vendor_package_flow_type_cd,
              work__idrp_vp_dc_start::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_vp_dc_start::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_vp_dc_start::ksn_package_id AS ksn_package_id,
              work__idrp_vp_dc_start::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_vp_dc_start::import_ind AS import_ind,
              work__idrp_vp_dc_start::sears_division_nbr AS sears_division_nbr,
              work__idrp_vp_dc_start::sears_item_nbr AS sears_item_nbr,
              work__idrp_vp_dc_start::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_vp_dc_start::scan_based_trading_ind AS scan_based_trading_ind,
              work__idrp_vp_dc_start::cross_merchandising_cd AS cross_merchandising_cd,
              work__idrp_vp_dc_start::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              work__idrp_vp_dc_start::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_vp_dc_start::can_carry_model_id AS can_carry_model_id,
              work__idrp_vp_dc_start::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_vp_dc_start::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__idrp_vp_dc_start::dotcom_orderable_cd AS dotcom_orderable_cd,
              work__idrp_vp_dc_start::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              work__idrp_vp_dc_start::idrp_order_method_cd AS idrp_order_method_cd,
              work__idrp_vp_dc_start::allocation_replenishment_cd AS allocation_replenishment_cd,
              --work__idrp_vp_dc_start::dc_configuration_cd AS dc_configuration_cd,
              work__idrp_vp_dc_start::shc_item_type_cd AS shc_item_type_cd,
	          smith__idrp_vend_pack_dc_combined_fltr::ship_duns_nbr AS ship_duns_nbr,
              smith__idrp_vend_pack_dc_combined_fltr::location_nbr AS location_id,
              smith__idrp_vend_pack_dc_combined_fltr::ksn_pack_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              smith__idrp_vend_pack_dc_combined_fltr::stock_ind AS stock_ind,
              smith__idrp_vend_pack_dc_combined_fltr::substitution_eligible_ind AS substitution_eligible_ind,
              smith__idrp_vend_pack_dc_combined_fltr::outbound_package_qty AS outbound_package_qty,
              smith__idrp_vend_pack_dc_combined_fltr::ship_aprk_id AS ship_aprk_id,
              smith__idrp_vend_pack_dc_combined_fltr::inbound_order_uom_cd AS inbound_order_uom_cd,
              smith__idrp_vend_pack_dc_combined_fltr::inbound_carton_per_layer_qty AS carton_per_layer_qty,
              smith__idrp_vend_pack_dc_combined_fltr::inbound_layer_per_pallet_qty AS layer_per_pallet_qty,
			  --Spira 4377
              work__idrp_vp_dc_start::store_source_package_qty AS store_source_package_qty,
              work__idrp_vp_dc_start::order_duns_nbr AS order_duns_nbr, -- << added field as part of Spira - 4436
			  smith__idrp_vend_pack_dc_combined_fltr::vendor_managed_inventory_cd  AS vendor_managed_inventory_cd,
              smith__idrp_vend_pack_dc_combined_fltr::dc_handling_cd AS dc_handling_cd,
              (work__idrp_vp_dc_start::import_ind =='1' ? 'N':((smith__idrp_vend_pack_dc_combined_fltr::vendor_managed_inventory_cd is not null or IsNull(TRIM(smith__idrp_vend_pack_dc_combined_fltr::vendor_managed_inventory_cd),'') !='' ) AND(smith__idrp_vend_pack_dc_combined_fltr::vendor_managed_inventory_cd =='5' OR
                                                                 smith__idrp_vend_pack_dc_combined_fltr::vendor_managed_inventory_cd =='6'   OR 
                                                                 smith__idrp_vend_pack_dc_combined_fltr::vendor_managed_inventory_cd =='7'   OR 
                                                                 smith__idrp_vend_pack_dc_combined_fltr::vendor_managed_inventory_cd =='8'   OR 
                                                                 smith__idrp_vend_pack_dc_combined_fltr::vendor_managed_inventory_cd =='9') ? 'N' :
                                                               (smith__idrp_vend_pack_dc_combined_fltr::stock_ind=='N' AND ((smith__idrp_vend_pack_dc_combined_fltr::dc_handling_cd is not null or IsNull(smith__idrp_vend_pack_dc_combined_fltr::dc_handling_cd,'') !='') and smith__idrp_vend_pack_dc_combined_fltr::dc_handling_cd == 'CASE') ?'Y':'N'))) as dc_flowthru_ind;

join_work__join_gold_gen_enable_jif_dc_ind = 
   JOIN work__idrp_vp_dc_config_applied_flow_thru  BY TRIM(location_id) LEFT OUTER,
          smith__idrp_dc_location_current_data BY TRIM(dc_location_nbr);

work__idrp_vp_dc_config_applied = 
      FOREACH join_work__join_gold_gen_enable_jif_dc_ind 
      GENERATE
              work__idrp_vp_dc_config_applied_flow_thru::shc_item_id AS shc_item_id,
              work__idrp_vp_dc_config_applied_flow_thru::source_owner_cd AS source_owner_cd,
              work__idrp_vp_dc_config_applied_flow_thru::ksn_id AS ksn_id,
              work__idrp_vp_dc_config_applied_flow_thru::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_vp_dc_config_applied_flow_thru::vendor_package_id AS vendor_package_id,
              work__idrp_vp_dc_config_applied_flow_thru::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_vp_dc_config_applied_flow_thru::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_vp_dc_config_applied_flow_thru::vendor_package_flow_type_cd AS vendor_package_flow_type_cd,
              work__idrp_vp_dc_config_applied_flow_thru::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_vp_dc_config_applied_flow_thru::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_vp_dc_config_applied_flow_thru::ksn_package_id AS ksn_package_id,
              work__idrp_vp_dc_config_applied_flow_thru::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_vp_dc_config_applied_flow_thru::import_ind AS import_ind,
              work__idrp_vp_dc_config_applied_flow_thru::sears_division_nbr AS sears_division_nbr,
              work__idrp_vp_dc_config_applied_flow_thru::sears_item_nbr AS sears_item_nbr,
              work__idrp_vp_dc_config_applied_flow_thru::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_vp_dc_config_applied_flow_thru::scan_based_trading_ind AS scan_based_trading_ind,
              work__idrp_vp_dc_config_applied_flow_thru::cross_merchandising_cd AS cross_merchandising_cd,
              work__idrp_vp_dc_config_applied_flow_thru::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              work__idrp_vp_dc_config_applied_flow_thru::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_vp_dc_config_applied_flow_thru::can_carry_model_id AS can_carry_model_id,
              work__idrp_vp_dc_config_applied_flow_thru::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_vp_dc_config_applied_flow_thru::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__idrp_vp_dc_config_applied_flow_thru::dotcom_orderable_cd AS dotcom_orderable_cd,
              work__idrp_vp_dc_config_applied_flow_thru::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              work__idrp_vp_dc_config_applied_flow_thru::shc_item_type_cd AS shc_item_type_cd,
              work__idrp_vp_dc_config_applied_flow_thru::location_id AS location_id,
              work__idrp_vp_dc_config_applied_flow_thru::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              work__idrp_vp_dc_config_applied_flow_thru::stock_ind AS stock_ind,
              work__idrp_vp_dc_config_applied_flow_thru::substitution_eligible_ind AS substitution_eligible_ind,
              work__idrp_vp_dc_config_applied_flow_thru::outbound_package_qty AS outbound_package_qty,
              work__idrp_vp_dc_config_applied_flow_thru::ship_aprk_id AS ship_aprk_id,
              work__idrp_vp_dc_config_applied_flow_thru::inbound_order_uom_cd AS inbound_order_uom_cd,
              work__idrp_vp_dc_config_applied_flow_thru::carton_per_layer_qty AS carton_per_layer_qty,
              work__idrp_vp_dc_config_applied_flow_thru::layer_per_pallet_qty AS layer_per_pallet_qty,
              work__idrp_vp_dc_config_applied_flow_thru::ship_duns_nbr AS ship_duns_nbr,
              work__idrp_vp_dc_config_applied_flow_thru::store_source_package_qty AS store_source_package_qty,
              work__idrp_vp_dc_config_applied_flow_thru::order_duns_nbr as order_duns_nbr,
              work__idrp_vp_dc_config_applied_flow_thru::vendor_managed_inventory_cd AS vendor_managed_inventory_cd,
              work__idrp_vp_dc_config_applied_flow_thru::dc_handling_cd as dc_handling_cd,
              work__idrp_vp_dc_config_applied_flow_thru::dc_flowthru_ind  AS dc_flowthru_ind,
             smith__idrp_dc_location_current_data::enable_jif_dc_ind AS enable_jif_dc_ind,
              (work__idrp_vp_dc_config_applied_flow_thru::vendor_package_flow_type_cd == 'JIT' ? 'JIT': ((work__idrp_vp_dc_config_applied_flow_thru::dc_flowthru_ind =='Y' AND smith__idrp_dc_location_current_data::enable_jif_dc_ind == 'N' )? 'FLT':
                                                                                        ((work__idrp_vp_dc_config_applied_flow_thru::dc_flowthru_ind =='Y' AND smith__idrp_dc_location_current_data::enable_jif_dc_ind == 'Y') ? 'JIF':
                                                                                        ((work__idrp_vp_dc_config_applied_flow_thru::dc_flowthru_ind =='N') ? 'STK' : '' )))) AS dc_configuration_cd,
             work__idrp_vp_dc_config_applied_flow_thru::allocation_replenishment_cd AS allocation_replenishment_cd;

work__join_gold_gen = 
      FOREACH work__idrp_vp_dc_config_applied
      GENERATE
              shc_item_id AS shc_item_id,
              source_owner_cd AS source_owner_cd,
              ksn_id AS ksn_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              vendor_package_id AS vendor_package_id,
              vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              vendor_package_flow_type_cd AS vendor_package_flow_type_cd,
              vendor_carton_qty AS vendor_carton_qty,
              vendor_stock_nbr AS vendor_stock_nbr,
              ksn_package_id AS ksn_package_id,
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              import_ind AS import_ind,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              scan_based_trading_ind AS scan_based_trading_ind,
              cross_merchandising_cd AS cross_merchandising_cd,
              retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              vendor_package_owner_cd AS vendor_package_owner_cd,
              can_carry_model_id AS can_carry_model_id,
              days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              days_to_check_end_day_qty AS days_to_check_end_day_qty,
              dotcom_orderable_cd AS dotcom_orderable_cd,
              retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              allocation_replenishment_cd AS allocation_replenishment_cd,
              dc_configuration_cd AS dc_configuration_cd,
              shc_item_type_cd AS shc_item_type_cd,
              ship_duns_nbr AS ship_duns_nbr,
              location_id AS location_id,
              ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              stock_ind AS stock_ind,
              substitution_eligible_ind AS substitution_eligible_ind,
              outbound_package_qty AS outbound_package_qty,
              ship_aprk_id AS ship_aprk_id,
              inbound_order_uom_cd AS inbound_order_uom_cd,
              carton_per_layer_qty AS carton_per_layer_qty,
              layer_per_pallet_qty AS layer_per_pallet_qty,
              ((import_ind=='1' AND location_id!='8277') ? '8277' : CONCAT((chararray)(int)TRIM(ship_duns_nbr),'_S')) AS source_location_id,
              CONCAT(ship_duns_nbr,'_S') AS purchase_order_vendor_location_id,
              store_source_package_qty AS store_source_package_qty,
              order_duns_nbr AS order_duns_nbr, -- << added field as part of Spira - 4436
			  dc_flowthru_ind AS dc_flowthru_ind,
			  vendor_managed_inventory_cd AS vendor_managed_inventory_cd,
			  dc_handling_cd as dc_handling_cd;
		  
-------------------------------------------------------------------------------------------------------------------
load_work__idrp_dummy_vend_whse_ref = LOAD '$WORK__IDRP_DUMMY_VEND_WHSE_REF_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($WORK__IDRP_DUMMY_VEND_WHSE_REF_SCHEMA);

work__idrp_dummy_vend_whse_ref = 
		FOREACH load_work__idrp_dummy_vend_whse_ref 
		GENERATE 
			vendor_nbr, 
			warehouse_nbr;

SPLIT work__join_gold_gen 
INTO work__join_gold_gen_cross_mdse_attr_cd_fltr 
     IF cross_merchandising_cd=='SK1400', 
     work__join_gold_gen_cross_mdse_attr_cd_nt_fltr 
     IF (cross_merchandising_cd!='SK1400' OR cross_merchandising_cd is null);

work__gold_gen_join_above_step = 
JOIN work__join_gold_gen_cross_mdse_attr_cd_fltr BY (int)TRIM(order_duns_nbr) LEFT OUTER, work__idrp_dummy_vend_whse_ref BY (int)TRIM(vendor_nbr);

work__gold_gen_join_above_step_gen = 
      FOREACH work__gold_gen_join_above_step
      GENERATE
              shc_item_id AS shc_item_id,
              source_owner_cd AS source_owner_cd,
              ksn_id AS ksn_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              vendor_package_id AS vendor_package_id,
              vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              vendor_package_flow_type_cd AS vendor_package_flow_type_cd,
              vendor_carton_qty AS vendor_carton_qty,
              vendor_stock_nbr AS vendor_stock_nbr,
              ksn_package_id AS ksn_package_id,
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              import_ind AS import_ind,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              scan_based_trading_ind AS scan_based_trading_ind,
              cross_merchandising_cd AS cross_merchandising_cd,
              retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              vendor_package_owner_cd AS vendor_package_owner_cd,
              can_carry_model_id AS can_carry_model_id,
              days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              days_to_check_end_day_qty AS days_to_check_end_day_qty,
              dotcom_orderable_cd AS dotcom_orderable_cd,
              retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              allocation_replenishment_cd AS allocation_replenishment_cd,
              dc_configuration_cd AS dc_configuration_cd,
              shc_item_type_cd AS shc_item_type_cd,
              work__join_gold_gen_cross_mdse_attr_cd_fltr::ship_duns_nbr AS ship_duns_nbr,
              location_id AS location_id,
              ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              stock_ind AS stock_ind,
              substitution_eligible_ind AS substitution_eligible_ind,
              outbound_package_qty AS outbound_package_qty,
              ship_aprk_id AS ship_aprk_id,
              inbound_order_uom_cd AS inbound_order_uom_cd,
              carton_per_layer_qty AS carton_per_layer_qty,
              layer_per_pallet_qty AS layer_per_pallet_qty,
              (IsNull(work__idrp_dummy_vend_whse_ref::vendor_nbr,'') != '' ? (chararray)(int)TRIM(work__idrp_dummy_vend_whse_ref::warehouse_nbr) :CONCAT((chararray)TRIM(work__join_gold_gen_cross_mdse_attr_cd_fltr::ship_duns_nbr),'_S')) AS source_location_id,
	          (IsNull(work__idrp_dummy_vend_whse_ref::vendor_nbr,'') != '' ? '' : CONCAT((chararray)TRIM(work__join_gold_gen_cross_mdse_attr_cd_fltr::ship_duns_nbr),'_S')) AS  purchase_order_vendor_location_id,
		      work__join_gold_gen_cross_mdse_attr_cd_fltr::store_source_package_qty AS store_source_package_qty,
              order_duns_nbr AS order_duns_nbr,
              dc_flowthru_ind AS dc_flowthru_ind,
			  vendor_managed_inventory_cd AS vendor_managed_inventory_cd,
			  dc_handling_cd as dc_handling_cd;

work__idrp_vp_dc_srcloc_applied = 
      UNION work__join_gold_gen_cross_mdse_attr_cd_nt_fltr,
            work__gold_gen_join_above_step_gen;
 
 
work__idrp_vp_dc_srcloc_applied = 
      FOREACH work__idrp_vp_dc_srcloc_applied
      GENERATE
              shc_item_id AS shc_item_id,
              source_owner_cd AS source_owner_cd,
              ksn_id AS ksn_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              vendor_package_id AS vendor_package_id,
              vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              vendor_package_flow_type_cd AS vendor_package_flow_type_cd,
              vendor_carton_qty AS vendor_carton_qty,
              vendor_stock_nbr AS vendor_stock_nbr,
              ksn_package_id AS ksn_package_id,
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              import_ind AS import_ind,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              scan_based_trading_ind AS scan_based_trading_ind,
              cross_merchandising_cd AS cross_merchandising_cd,
              retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              vendor_package_owner_cd AS vendor_package_owner_cd,
              can_carry_model_id AS can_carry_model_id,
              days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              days_to_check_end_day_qty AS days_to_check_end_day_qty,
              dotcom_orderable_cd AS dotcom_orderable_cd,
              retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              allocation_replenishment_cd AS allocation_replenishment_cd,
              dc_configuration_cd AS dc_configuration_cd,
              shc_item_type_cd AS shc_item_type_cd,
              location_id AS location_id,
              ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              stock_ind AS stock_ind,
              substitution_eligible_ind AS substitution_eligible_ind,
              outbound_package_qty AS outbound_package_qty,
              ship_aprk_id AS ship_aprk_id,
              inbound_order_uom_cd AS inbound_order_uom_cd,
              carton_per_layer_qty AS carton_per_layer_qty,
              layer_per_pallet_qty AS layer_per_pallet_qty,
              source_location_id AS source_location_id,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              store_source_package_qty AS store_source_package_qty;


join_work_store_srcloc = 
   JOIN work__idrp_kmart_vendor_package_location_store_level_data BY ((int)TRIM(vendor_package_id),TRIM(source_location_id)) LEFT OUTER,
          work__idrp_vp_dc_srcloc_applied BY ((int)TRIM(vendor_package_id),TRIM(location_id));

 
work__idrp_kmart_vendor_package_location_store_level = 
      FOREACH join_work_store_srcloc
      GENERATE
	      '$CURRENT_TIMESTAMP' AS load_ts,
              work__idrp_kmart_vendor_package_location_store_level_data::vendor_package_id AS vendor_package_id,
              work__idrp_kmart_vendor_package_location_store_level_data::location_id AS location_id,
              location_format_type_cd AS location_format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              work__idrp_kmart_vendor_package_location_store_level_data::source_owner_cd AS source_owner_cd,
              active_ind AS active_ind,
              active_ind_change_dt AS active_ind_change_dt,
              work__idrp_kmart_vendor_package_location_store_level_data::allocation_replenishment_cd AS allocation_replenishment_cd,
              ((int)work__idrp_kmart_vendor_package_location_store_level_data::vendor_package_id==
              (int)(work__idrp_vp_dc_srcloc_applied::vendor_package_id is null ? '0':work__idrp_vp_dc_srcloc_applied::vendor_package_id)
               ? IsNull(work__idrp_vp_dc_srcloc_applied::purchase_order_vendor_location_id,'') :
                IsNull(work__idrp_kmart_vendor_package_location_store_level_data::purchase_order_vendor_location_id,'')) AS purchase_order_vendor_location_id,
              replenishment_planning_ind AS replenishment_planning_ind,
              work__idrp_kmart_vendor_package_location_store_level_data::scan_based_trading_ind AS scan_based_trading_ind,
              work__idrp_kmart_vendor_package_location_store_level_data::source_location_id AS source_location_id,
              source_location_level_cd AS source_location_level_cd,
              source_package_qty AS source_package_qty,
              work__idrp_kmart_vendor_package_location_store_level_data::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_kmart_vendor_package_location_store_level_data::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              flow_type_cd AS flow_type_cd,
              work__idrp_kmart_vendor_package_location_store_level_data::import_ind AS import_ind,
              work__idrp_kmart_vendor_package_location_store_level_data::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              work__idrp_kmart_vendor_package_location_store_level_data::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_kmart_vendor_package_location_store_level_data::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_kmart_vendor_package_location_store_level_data::shc_item_id AS shc_item_id,
              work__idrp_kmart_vendor_package_location_store_level_data::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_kmart_vendor_package_location_store_level_data::can_carry_model_id AS can_carry_model_id,
              work__idrp_kmart_vendor_package_location_store_level_data::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_kmart_vendor_package_location_store_level_data::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              reorder_method_cd AS reorder_method_cd,
              work__idrp_kmart_vendor_package_location_store_level_data::ksn_id AS ksn_id,
              work__idrp_kmart_vendor_package_location_store_level_data::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_kmart_vendor_package_location_store_level_data::cross_merchandising_cd AS cross_merchandising_cd,
              work__idrp_kmart_vendor_package_location_store_level_data::dotcom_orderable_cd AS dotcom_orderable_cd,
              kmart_markdown_ind AS kmart_markdown_ind,
              work__idrp_kmart_vendor_package_location_store_level_data::ksn_package_id AS ksn_package_id,
              work__idrp_kmart_vendor_package_location_store_level_data::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              --stock_ind AS stock_ind,
              work__idrp_kmart_vendor_package_location_store_level_data::dc_configuration_cd AS dc_configuration_cd,
              work__idrp_kmart_vendor_package_location_store_level_data::substitution_eligible_ind AS substitution_eligible_ind,
              work__idrp_kmart_vendor_package_location_store_level_data::sears_division_nbr AS sears_division_nbr,
              work__idrp_kmart_vendor_package_location_store_level_data::sears_item_nbr AS sears_item_nbr,
              work__idrp_kmart_vendor_package_location_store_level_data::sears_sku_nbr AS sears_sku_nbr,
              sears_location_id AS sears_location_id,
              sears_source_location_id AS sears_source_location_id,
              rim_status_cd AS rim_status_cd,
              stock_type_cd AS stock_type_cd,
              non_stock_source_cd AS non_stock_source_cd,
              dos_item_active_ind AS dos_item_active_ind,
              dos_item_reserve_cd AS dos_item_reserve_cd,
              create_dt AS create_dt,
              last_update_dt AS last_update_dt,
              work__idrp_kmart_vendor_package_location_store_level_data::shc_item_type_cd AS shc_item_type_cd,
              format_type_cd AS format_type_cd,
              work__idrp_kmart_vendor_package_location_store_level_data::outbound_package_qty AS outbound_package_qty,
              --ship_aprk_id AS ship_aprk_id,
              work__idrp_kmart_vendor_package_location_store_level_data::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              work__idrp_kmart_vendor_package_location_store_level_data::vendor_carton_qty AS vendor_carton_qty,
              enable_jif_dc_ind AS enable_jif_dc_ind,
	          '1970-01-01' AS rim_last_record_creation_dt,
             '$batchid' AS idrp_batch_id;


smith__idrp_eligible_loc_data_K = filter smith__idrp_eligible_loc_data  by loc_owner_cd =='K';

work__idrp_join_smith_loc = 
      JOIN work__idrp_vp_dc_srcloc_applied BY (int)location_id,
           smith__idrp_eligible_loc_data_K BY (int)loc;


work__idrp_vp_dc_searsloc_applied_temp = 
      FOREACH work__idrp_join_smith_loc
      GENERATE
              work__idrp_vp_dc_srcloc_applied::shc_item_id AS shc_item_id,
              work__idrp_vp_dc_srcloc_applied::source_owner_cd AS source_owner_cd,
              work__idrp_vp_dc_srcloc_applied::ksn_id AS ksn_id,
              work__idrp_vp_dc_srcloc_applied::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_vp_dc_srcloc_applied::vendor_package_id AS vendor_package_id,
              work__idrp_vp_dc_srcloc_applied::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_vp_dc_srcloc_applied::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_vp_dc_srcloc_applied::vendor_package_flow_type_cd AS vendor_package_flow_type_cd,
              work__idrp_vp_dc_srcloc_applied::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_vp_dc_srcloc_applied::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_vp_dc_srcloc_applied::ksn_package_id AS ksn_package_id,
              work__idrp_vp_dc_srcloc_applied::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_vp_dc_srcloc_applied::import_ind AS import_ind,
              work__idrp_vp_dc_srcloc_applied::sears_division_nbr AS sears_division_nbr,
              work__idrp_vp_dc_srcloc_applied::sears_item_nbr AS sears_item_nbr,
              work__idrp_vp_dc_srcloc_applied::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_vp_dc_srcloc_applied::scan_based_trading_ind AS scan_based_trading_ind,
              work__idrp_vp_dc_srcloc_applied::cross_merchandising_cd AS cross_merchandising_cd,
              work__idrp_vp_dc_srcloc_applied::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              work__idrp_vp_dc_srcloc_applied::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_vp_dc_srcloc_applied::can_carry_model_id AS can_carry_model_id,
              work__idrp_vp_dc_srcloc_applied::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_vp_dc_srcloc_applied::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__idrp_vp_dc_srcloc_applied::dotcom_orderable_cd AS dotcom_orderable_cd,
              work__idrp_vp_dc_srcloc_applied::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              work__idrp_vp_dc_srcloc_applied::allocation_replenishment_cd AS allocation_replenishment_cd,
              work__idrp_vp_dc_srcloc_applied::dc_configuration_cd AS dc_configuration_cd,
              work__idrp_vp_dc_srcloc_applied::shc_item_type_cd AS shc_item_type_cd,
              work__idrp_vp_dc_srcloc_applied::location_id AS location_id,
              work__idrp_vp_dc_srcloc_applied::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              work__idrp_vp_dc_srcloc_applied::stock_ind AS stock_ind,
              work__idrp_vp_dc_srcloc_applied::substitution_eligible_ind AS substitution_eligible_ind,
              work__idrp_vp_dc_srcloc_applied::outbound_package_qty AS outbound_package_qty,
              work__idrp_vp_dc_srcloc_applied::ship_aprk_id AS ship_aprk_id,
              work__idrp_vp_dc_srcloc_applied::inbound_order_uom_cd AS inbound_order_uom_cd,
              work__idrp_vp_dc_srcloc_applied::carton_per_layer_qty AS carton_per_layer_qty,
              work__idrp_vp_dc_srcloc_applied::layer_per_pallet_qty AS layer_per_pallet_qty,
              work__idrp_vp_dc_srcloc_applied::source_location_id AS source_location_id,
              work__idrp_vp_dc_srcloc_applied::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              work__idrp_vp_dc_srcloc_applied::store_source_package_qty AS store_source_package_qty,
              smith__idrp_eligible_loc_data_K::srs_loc AS sears_location_id,
              smith__idrp_eligible_loc_data_K::fmt_typ_cd AS format_type_cd,
              smith__idrp_eligible_loc_data_K::loc_fmt_typ_cd AS location_format_type_cd,
              smith__idrp_eligible_loc_data_K::loc_lvl_cd AS location_level_cd,
              smith__idrp_eligible_loc_data_K::loc_owner_cd AS location_owner_cd;

outer_join_work_smith_loc = 
      JOIN work__idrp_vp_dc_searsloc_applied_temp BY TRIM(source_location_id),
           smith__idrp_eligible_loc_data BY TRIM(loc);


work__idrp_vp_dc_searsloc_applied = 
      FOREACH outer_join_work_smith_loc
      GENERATE
              shc_item_id,
              source_owner_cd,
              ksn_id,
              item_purchase_status_cd,
              vendor_package_id,
              vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt,
              vendor_package_flow_type_cd,
              vendor_carton_qty,
              vendor_stock_nbr,
              ksn_package_id,
              ksn_purchase_status_cd,
              import_ind,
              sears_division_nbr,
              sears_item_nbr,
              sears_sku_nbr,
              scan_based_trading_ind,
              cross_merchandising_cd,
              retail_carton_vendor_package_id,
              vendor_package_owner_cd,
              can_carry_model_id,
              days_to_check_begin_day_qty,
              days_to_check_end_day_qty,
              dotcom_orderable_cd,
              retail_carton_internal_package_qty,
              allocation_replenishment_cd,
              dc_configuration_cd,
              shc_item_type_cd,
              location_id,
              ksn_dc_package_purchase_status_cd,
              stock_ind,
              substitution_eligible_ind,
              outbound_package_qty,
              ship_aprk_id,
              inbound_order_uom_cd,
              carton_per_layer_qty,
              layer_per_pallet_qty,
              source_location_id,
              purchase_order_vendor_location_id,
              sears_location_id,
              format_type_cd,
              location_format_type_cd,
              location_level_cd,
              location_owner_cd,
              smith__idrp_eligible_loc_data::srs_loc AS sears_source_location_id,
              smith__idrp_eligible_loc_data::loc_lvl_cd AS source_location_level_cd,
              store_source_package_qty AS store_source_package_qty,
              --Spira 4377,4607
             (inbound_order_uom_cd is not null ? (inbound_order_uom_cd=='LAYR' ? (chararray)((int)vendor_carton_qty * (int)carton_per_layer_qty) :
             										(inbound_order_uom_cd=='PALL' ? (chararray)((int)vendor_carton_qty * (int)carton_per_layer_qty * (int)layer_per_pallet_qty)
             										 : vendor_carton_qty)) : vendor_carton_qty) AS source_package_qty;	
             				
             				
work__idrp_vp_dc_srcpack_applied = 
      FOREACH work__idrp_vp_dc_searsloc_applied
      GENERATE
              shc_item_id,
              source_owner_cd,
              ksn_id,
              item_purchase_status_cd,
              vendor_package_id,
              vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt,
              vendor_package_flow_type_cd,
              vendor_carton_qty,
              vendor_stock_nbr,
              ksn_package_id,
              ksn_purchase_status_cd,
              import_ind,
              sears_division_nbr,
              sears_item_nbr,
              sears_sku_nbr,
              scan_based_trading_ind,
              cross_merchandising_cd,
              retail_carton_vendor_package_id,
              vendor_package_owner_cd,
              can_carry_model_id,
              days_to_check_begin_day_qty,
              days_to_check_end_day_qty,
              dotcom_orderable_cd,
              retail_carton_internal_package_qty,
              allocation_replenishment_cd,
              dc_configuration_cd,
              shc_item_type_cd,
              location_id,
              ksn_dc_package_purchase_status_cd,
              stock_ind,
              substitution_eligible_ind,
              outbound_package_qty,
              ship_aprk_id,
              inbound_order_uom_cd,
              carton_per_layer_qty,
              layer_per_pallet_qty,
              source_location_id,
              purchase_order_vendor_location_id,
              sears_location_id,
              format_type_cd,
              location_format_type_cd,
              location_level_cd,
              location_owner_cd,
              sears_source_location_id,
              source_location_level_cd,
              store_source_package_qty,
              --Spira 4377
              ((source_package_qty is null or IsNull(source_package_qty,'') == '' or source_package_qty  == '0') ? store_source_package_qty : source_package_qty) AS source_package_qty;

join_work_above_smith__dc_locn = 
     JOIN work__idrp_vp_dc_srcpack_applied BY location_id,
          smith__idrp_dc_location_current_data BY dc_location_nbr;


work__idrp_vp_dc_enable_jif_applied = 
      FOREACH join_work_above_smith__dc_locn
      GENERATE
              shc_item_id,
              source_owner_cd,
              ksn_id,
              item_purchase_status_cd,
              vendor_package_id,
              vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt,
              vendor_package_flow_type_cd,
              vendor_carton_qty,
              vendor_stock_nbr,
              ksn_package_id,
              ksn_purchase_status_cd,
              work__idrp_vp_dc_srcpack_applied::work__idrp_vp_dc_searsloc_applied_temp::import_ind AS import_ind,
              sears_division_nbr,
              sears_item_nbr,
              sears_sku_nbr,
              scan_based_trading_ind,
              cross_merchandising_cd,
              retail_carton_vendor_package_id,
              vendor_package_owner_cd,
              can_carry_model_id,
              days_to_check_begin_day_qty,
              days_to_check_end_day_qty,
              dotcom_orderable_cd,
              retail_carton_internal_package_qty,
              allocation_replenishment_cd,
              dc_configuration_cd,
              shc_item_type_cd,
              location_id,
              ksn_dc_package_purchase_status_cd,
              work__idrp_vp_dc_srcpack_applied::work__idrp_vp_dc_searsloc_applied_temp::stock_ind AS stock_ind,
              substitution_eligible_ind,
              outbound_package_qty,
              ship_aprk_id,
              inbound_order_uom_cd,
              carton_per_layer_qty,
              layer_per_pallet_qty,
              source_location_id,
              purchase_order_vendor_location_id,
              sears_location_id,
              format_type_cd,
              location_format_type_cd,
              location_level_cd,
              location_owner_cd,
              sears_source_location_id,
              source_location_level_cd,
              source_package_qty,
              smith__idrp_dc_location_current_data::enable_jif_dc_ind AS enable_jif_dc_ind;


---------------------------------------work__vpdc_eligible_store_count table creation process---------------------------------------------------

grouped_data = 
     GROUP work__idrp_kmart_vendor_package_location_store_level_fltr  
     BY (vendor_package_id,source_location_id);

grouped_data_gen = 
     FOREACH grouped_data 
     GENERATE 
             group.vendor_package_id AS vendor_package_id,
             group.source_location_id AS source_location_id,
             COUNT(work__idrp_kmart_vendor_package_location_store_level_fltr.active_ind) AS eligible_store_record_count;


grouped_data_gen_fltr = 
     FILTER grouped_data_gen 
     BY (int)eligible_store_record_count > 0;


------------------------------------------------------------------------------------------------------------------------------------------------

work__join_work = 
      JOIN work__idrp_vp_dc_enable_jif_applied BY (vendor_package_id,location_id) LEFT OUTER,
           grouped_data_gen_fltr BY (vendor_package_id,source_location_id);

work__dc_level_vend_pack_loc_post_active = 
      FOREACH work__join_work
      GENERATE
              shc_item_id,
              source_owner_cd,
              ksn_id,
              item_purchase_status_cd,
              work__idrp_vp_dc_enable_jif_applied::work__idrp_vp_dc_srcpack_applied::work__idrp_vp_dc_searsloc_applied_temp::vendor_package_id AS vendor_package_id,
              vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt,
              vendor_package_flow_type_cd,
              vendor_carton_qty,
              vendor_stock_nbr,
              ksn_package_id,
              ksn_purchase_status_cd,
              import_ind,
              sears_division_nbr,
              sears_item_nbr,
              sears_sku_nbr,
              scan_based_trading_ind,
              cross_merchandising_cd,
              retail_carton_vendor_package_id,
              vendor_package_owner_cd,
              can_carry_model_id,
              days_to_check_begin_day_qty,
              days_to_check_end_day_qty,
              dotcom_orderable_cd,
              retail_carton_internal_package_qty,
              allocation_replenishment_cd,
              dc_configuration_cd,
              shc_item_type_cd,
              location_id,
              ksn_dc_package_purchase_status_cd,
              stock_ind,
              substitution_eligible_ind,
              outbound_package_qty,
              ship_aprk_id,
              inbound_order_uom_cd,
              carton_per_layer_qty,
              layer_per_pallet_qty,
              work__idrp_vp_dc_enable_jif_applied::work__idrp_vp_dc_srcpack_applied::work__idrp_vp_dc_searsloc_applied_temp::source_location_id AS source_location_id,
              purchase_order_vendor_location_id,
              sears_location_id,
              format_type_cd,
              location_format_type_cd,
              location_level_cd,
              location_owner_cd,
              sears_source_location_id,
              source_location_level_cd,
              source_package_qty,
              enable_jif_dc_ind AS enable_jif_dc_ind,
              (grouped_data_gen_fltr::vendor_package_id IS NOT NULL or IsNull(grouped_data_gen_fltr::vendor_package_id,'') != '' ? 'Y' : 'N') AS active_ind;


smith__idrp_inbound_vendor_package_dc_driver_data = filter smith__idrp_inbound_vendor_package_dc_driver_data by replenishment_planning_ind =='Y';  

outer_work_post_active_smith_dc_driver = 
      JOIN work__dc_level_vend_pack_loc_post_active BY (shc_item_id,vendor_package_id,location_id) LEFT OUTER,
           smith__idrp_inbound_vendor_package_dc_driver_data BY (inbnd_item_id,inbnd_vend_pack_id,inbnd_dc_locn_nbr);


work__dc_level_vend_pack_replen_plan = 
      FOREACH outer_work_post_active_smith_dc_driver
      GENERATE
              shc_item_id,
              source_owner_cd,
              ksn_id,
              item_purchase_status_cd,
              vendor_package_id,
              vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt,
              vendor_package_flow_type_cd,
              vendor_carton_qty,
              vendor_stock_nbr,
              ksn_package_id,
              ksn_purchase_status_cd,
              import_ind,
              sears_division_nbr,
              sears_item_nbr,
              sears_sku_nbr,
              scan_based_trading_ind,
              cross_merchandising_cd,
              retail_carton_vendor_package_id,
              vendor_package_owner_cd,
              can_carry_model_id,
              days_to_check_begin_day_qty,
              days_to_check_end_day_qty,
              dotcom_orderable_cd,
              retail_carton_internal_package_qty,
              allocation_replenishment_cd,
              dc_configuration_cd,
              shc_item_type_cd,
              location_id,
              ksn_dc_package_purchase_status_cd,
              stock_ind,
              substitution_eligible_ind,
              outbound_package_qty,
              ship_aprk_id,
              inbound_order_uom_cd,
              carton_per_layer_qty,
              layer_per_pallet_qty,
              source_location_id,
              purchase_order_vendor_location_id,
              sears_location_id,
              format_type_cd,
              location_format_type_cd,
              location_level_cd,
              location_owner_cd,
              sears_source_location_id,
              source_location_level_cd,
              source_package_qty,
              enable_jif_dc_ind AS enable_jif_dc_ind,
              active_ind AS active_ind,
              ((shc_item_type_cd == 'EXAS') OR (smith__idrp_inbound_vendor_package_dc_driver_data::inbnd_item_id IS NULL OR smith__idrp_inbound_vendor_package_dc_driver_data::inbnd_item_id=='') ? 'N' : 'Y') AS replenishment_planning_ind; 


work__idrp_kmart_vp_loc_dc_level_1 = 
      FOREACH work__dc_level_vend_pack_replen_plan 
      GENERATE
              '$CURRENT_TIMESTAMP' AS load_ts, 
              vendor_package_id AS vendor_package_id,
              location_id AS location_id,
              location_format_type_cd AS location_format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              'K' AS source_owner_cd,
              active_ind AS active_ind,
              '$CURRENT_DATE' AS active_ind_change_dt ,
              allocation_replenishment_cd AS allocation_replenishment_cd,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              replenishment_planning_ind AS replenishment_planning_ind,
              scan_based_trading_ind AS scan_based_trading_ind,
              source_location_id AS source_location_id,
              source_location_level_cd AS source_location_level_cd,
              source_package_qty AS source_package_qty,
              vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              vendor_package_flow_type_cd AS flow_type_cd,
              import_ind AS import_ind,
              retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              vendor_package_owner_cd AS vendor_package_owner_cd,
              vendor_stock_nbr AS vendor_stock_nbr,
              shc_item_id AS shc_item_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              can_carry_model_id AS can_carry_model_id,
              '' AS days_to_check_begin_day_qty,
              '' AS days_to_check_end_day_qty,
              '' AS reorder_method_cd,
              ksn_id AS ksn_id,
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              cross_merchandising_cd AS cross_merchandising_cd,
              '' AS dotcom_orderable_cd,
              '' AS kmart_markdown_ind,
              ksn_package_id AS ksn_package_id,
              ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              dc_configuration_cd AS dc_configuration_cd,
              substitution_eligible_ind AS substitution_eligible_ind,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              sears_location_id AS sears_location_id,
              sears_source_location_id AS sears_source_location_id,
              '' AS rim_status_cd,
              '' AS stock_type_cd,
              '' AS non_stock_source_cd,
              '' AS dos_item_active_ind,
              '' AS dos_item_reserve_cd,
              '$CURRENT_DATE' AS create_dt ,
              '$CURRENT_DATE' AS last_update_dt ,
              shc_item_type_cd AS shc_item_type_cd,
              format_type_cd AS format_type_cd,
              outbound_package_qty AS outbound_package_qty,
              retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              vendor_carton_qty AS vendor_carton_qty,
              enable_jif_dc_ind AS enable_jif_dc_ind,
	          '1970-01-01' AS rim_last_record_creation_dt,
              '$batchid' AS idrp_batch_id;


SPLIT work__idrp_kmart_vp_loc_dc_level_1 INTO
work__idrp_kmart_vp_loc_dc_level IF
      source_location_id=='8277',
work__idrp_kmart_vp_loc_dc_level_not_8277 IF
      source_location_id!='8277';

smith__idrp_vend_pack_dc_combined_data_8277 = 
      FILTER smith__idrp_vend_pack_dc_combined_data BY location_nbr =='8277';
      
work__idrp_kmart_vp_loc_dc_level_join = JOIN work__idrp_kmart_vp_loc_dc_level BY ((int)vendor_package_id)  left outer, smith__idrp_vend_pack_dc_combined_data_8277 by ((int)vendor_package_id);

work__idrp_kmart_vp_loc_dc_level_fltr = FILTER work__idrp_kmart_vp_loc_dc_level_join by (IsNull(smith__idrp_vend_pack_dc_combined_data_8277::vendor_package_id,'') =='' OR  smith__idrp_vend_pack_dc_combined_data_8277::vendor_package_id is null);

work__idrp_kmart_vp_loc_dc_level_gen = 	FOREACH work__idrp_kmart_vp_loc_dc_level_fltr GENERATE 
												work__idrp_kmart_vp_loc_dc_level::vendor_package_id as vendor_package_id;

work__idrp_kmart_vp_loc_dc_level_dist = DISTINCT work__idrp_kmart_vp_loc_dc_level_gen;

work__idrp_import_direct_vpdc_join = 	JOIN work__idrp_kmart_vp_loc_dc_level_dist by ((int)vendor_package_id), smith__idrp_vend_pack_dc_combined_data by ((int)vendor_package_id);

work__idrp_import_direct_vpdc_gen = foreach work__idrp_import_direct_vpdc_join generate 
										work__idrp_kmart_vp_loc_dc_level_dist::vendor_package_id as vendor_package_id,
										smith__idrp_vend_pack_dc_combined_data::ksn_package_id as ksn_package_id,
										smith__idrp_vend_pack_dc_combined_data::location_nbr as location_nbr,
										smith__idrp_vend_pack_dc_combined_data::outbound_package_qty as outbound_package_qty,
										smith__idrp_vend_pack_dc_combined_data::ksn_pack_purchase_status_cd as ksn_pack_purchase_status_cd,
										smith__idrp_vend_pack_dc_combined_data::substitution_eligible_ind as substitution_eligible_ind,
										smith__idrp_vend_pack_dc_combined_data::ship_duns_nbr as ship_duns_nbr;
										
work__idrp_import_direct_vpdc_grp = 	GROUP work__idrp_import_direct_vpdc_gen BY
        									(vendor_package_id);

work__idrp_import_direct_vpdc_sort =  	FOREACH work__idrp_import_direct_vpdc_grp
											{
							        	        sorted = ORDER work__idrp_import_direct_vpdc_gen BY vendor_package_id, ksn_pack_purchase_status_cd, location_nbr;
							            	    x = LIMIT sorted 1;
												GENERATE FLATTEN (x);
							       		 	};
							       		 	
						       		 	
work__idrp_import_direct_vpdc_info =	FOREACH work__idrp_import_direct_vpdc_sort 
										GENERATE  						       		 	
												vendor_package_id AS vendor_package_id,
												ksn_package_id AS ksn_package_id,
												location_nbr AS location_nbr,
												outbound_package_qty AS outbound_package_qty,
												ksn_pack_purchase_status_cd AS ksn_pack_purchase_status_cd,
												substitution_eligible_ind AS substitution_eligible_ind,
												ship_duns_nbr AS ship_duns_nbr;        		 	
														       		 	
work__idrp_kmart_vpdc_ie_loc_join =   JOIN work__idrp_kmart_vp_loc_dc_level  BY source_location_id, smith__idrp_eligible_loc_data by loc;

work__idrp_kmart_vpdc_ie_loc_direct_vpdc_info_join = JOIN work__idrp_kmart_vpdc_ie_loc_join BY  vendor_package_id, work__idrp_import_direct_vpdc_info BY vendor_package_id;

work__idrp_import_direct_decon   =  foreach work__idrp_kmart_vpdc_ie_loc_direct_vpdc_info_join generate 
											'$CURRENT_TIMESTAMP' AS load_ts,
											work__idrp_kmart_vp_loc_dc_level::vendor_package_id AS vendor_package_id,
											work__idrp_kmart_vp_loc_dc_level::source_location_id AS location_id,
											work__idrp_kmart_vp_loc_dc_level::location_format_type_cd AS location_format_type_cd,
											work__idrp_kmart_vp_loc_dc_level::location_level_cd AS location_level_cd,
											work__idrp_kmart_vp_loc_dc_level::location_owner_cd AS location_owner_cd,
											'K' AS source_owner_cd,
											'N' AS active_ind,
											'$CURRENT_DATE' AS active_ind_change_dt,
											work__idrp_kmart_vp_loc_dc_level::allocation_replenishment_cd AS allocation_replenishment_cd,
											(CONCAT(work__idrp_import_direct_vpdc_info::ship_duns_nbr,'_S')) AS purchase_order_vendor_location_id,
											'N' AS replenishment_planning_ind,
											work__idrp_kmart_vp_loc_dc_level::scan_based_trading_ind AS scan_based_trading_ind,
											(CONCAT(work__idrp_import_direct_vpdc_info::ship_duns_nbr,'_S')) AS source_location_id,
											(work__idrp_kmart_vp_loc_dc_level::vendor_carton_qty == '0' ? '1' : work__idrp_kmart_vp_loc_dc_level::vendor_carton_qty ) AS source_package_qty,
											work__idrp_kmart_vp_loc_dc_level::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
											work__idrp_kmart_vp_loc_dc_level::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
											work__idrp_kmart_vp_loc_dc_level::flow_type_cd AS flow_type_cd,
											work__idrp_kmart_vp_loc_dc_level::import_ind AS import_ind,
											work__idrp_kmart_vp_loc_dc_level::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
											work__idrp_kmart_vp_loc_dc_level::vendor_package_owner_cd AS vendor_package_owner_cd,
											work__idrp_kmart_vp_loc_dc_level::vendor_stock_nbr AS vendor_stock_nbr,
											work__idrp_kmart_vp_loc_dc_level::shc_item_id AS shc_item_id,
											work__idrp_kmart_vp_loc_dc_level::item_purchase_status_cd AS item_purchase_status_cd,
											work__idrp_kmart_vp_loc_dc_level::can_carry_model_id AS can_carry_model_id,
											work__idrp_kmart_vp_loc_dc_level::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
											work__idrp_kmart_vp_loc_dc_level::days_to_check_end_day_qty AS days_to_check_end_day_qty,
											work__idrp_kmart_vp_loc_dc_level::reorder_method_cd AS reorder_method_cd,
											work__idrp_kmart_vp_loc_dc_level::ksn_id AS ksn_id,
											work__idrp_kmart_vp_loc_dc_level::ksn_purchase_status_cd AS ksn_purchase_status_cd,
											work__idrp_kmart_vp_loc_dc_level::cross_merchandising_cd AS cross_merchandising_cd,
											work__idrp_kmart_vp_loc_dc_level::dotcom_orderable_cd AS dotcom_orderable_cd,
											work__idrp_kmart_vp_loc_dc_level::kmart_markdown_ind AS kmart_markdown_ind,
											work__idrp_import_direct_vpdc_info::ksn_package_id AS ksn_package_id,
											work__idrp_import_direct_vpdc_info::ksn_pack_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
											'STK' AS dc_configuration_cd,
											work__idrp_import_direct_vpdc_info::substitution_eligible_ind AS substitution_eligible_ind,
											work__idrp_kmart_vp_loc_dc_level::sears_division_nbr AS sears_division_nbr,
											work__idrp_kmart_vp_loc_dc_level::sears_item_nbr AS sears_item_nbr,
											work__idrp_kmart_vp_loc_dc_level::sears_sku_nbr AS sears_sku_nbr,
											work__idrp_kmart_vp_loc_dc_level::sears_source_location_id AS sears_location_id,
											work__idrp_kmart_vp_loc_dc_level::rim_status_cd AS rim_status_cd,
											work__idrp_kmart_vp_loc_dc_level::stock_type_cd AS stock_type_cd,
											work__idrp_kmart_vp_loc_dc_level::non_stock_source_cd AS non_stock_source_cd,
											work__idrp_kmart_vp_loc_dc_level::dos_item_active_ind AS dos_item_active_ind,
											work__idrp_kmart_vp_loc_dc_level::dos_item_reserve_cd AS dos_item_reserve_cd,
											'$CURRENT_DATE' AS create_dt,
											'$CURRENT_DATE' AS last_update_dt,
											work__idrp_kmart_vp_loc_dc_level::shc_item_type_cd AS shc_item_type_cd,
											smith__idrp_eligible_loc_data::fmt_typ_cd AS format_type_cd,
											work__idrp_import_direct_vpdc_info::outbound_package_qty AS outbound_package_qty,
											work__idrp_kmart_vp_loc_dc_level::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
											work__idrp_kmart_vp_loc_dc_level::vendor_carton_qty AS vendor_carton_qty,
											'N' AS enable_jif_dc_ind,
											'$batchid' AS idrp_batch_id;					
  

work__join_smith_idrp_loc = 
      JOIN work__idrp_kmart_vp_loc_dc_level BY source_location_id,
           smith__idrp_eligible_loc_data BY loc;

work__join_smith_idrp_loc_join = 
      FOREACH work__join_smith_idrp_loc 
      GENERATE
              '$CURRENT_TIMESTAMP' AS load_ts, 
              vendor_package_id AS vendor_package_id,
              source_location_id AS location_id,
              location_format_type_cd AS location_format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              'K' AS source_owner_cd,
              'N' AS active_ind,
              '$CURRENT_DATE' AS active_ind_change_dt ,
               allocation_replenishment_cd AS allocation_replenishment_cd,
              '' AS purchase_order_vendor_location_id,
              'N' AS replenishment_planning_ind,
              scan_based_trading_ind AS scan_based_trading_ind,
              '' AS source_location_id,
              '' AS source_location_level_cd,
              '' AS source_package_qty,
              vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              flow_type_cd AS flow_type_cd,
              import_ind AS import_ind,
              retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              vendor_package_owner_cd AS vendor_package_owner_cd,
              vendor_stock_nbr AS vendor_stock_nbr,
              shc_item_id AS shc_item_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              can_carry_model_id AS can_carry_model_id,
              '' AS days_to_check_begin_day_qty,
              '' AS days_to_check_end_day_qty,
              '' AS reorder_method_cd,
              ksn_id AS ksn_id,
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              cross_merchandising_cd AS cross_merchandising_cd,
              '' AS dotcom_orderable_cd,
              '' AS kmart_markdown_ind,
              ksn_package_id AS ksn_package_id,
              '' AS ksn_dc_package_purchase_status_cd,
              'STK' AS dc_configuration_cd,
              '' AS substitution_eligible_ind,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              sears_location_id AS sears_location_id,
              sears_source_location_id AS sears_source_location_id,
              '' AS rim_status_cd,
              '' AS stock_type_cd,
              '' AS non_stock_source_cd,
              '' AS dos_item_active_ind,
              '' AS dos_item_reserve_cd,
              '$CURRENT_DATE' AS create_dt ,
              '$CURRENT_DATE' AS last_update_dt ,
              shc_item_type_cd AS shc_item_type_cd,
              format_type_cd AS format_type_cd,
              '' AS outbound_package_qty,
              retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              vendor_carton_qty AS vendor_carton_qty,
              'N' AS enable_jif_dc_ind,
              '$batchid' AS idrp_batch_id;

smith__idrp_vend_pack_dc_combined_data_8277_filter = 
      FOREACH smith__idrp_vend_pack_dc_combined_data_8277 
      GENERATE 
              vendor_package_id AS vendor_package_id,
              location_nbr AS location_nbr,
              inbound_carton_per_layer_qty AS inbound_carton_per_layer_qty,
              inbound_layer_per_pallet_qty AS inbound_layer_per_pallet_qty,
              inbound_order_uom_cd AS inbound_order_uom_cd,
              ship_aprk_id AS ship_aprk_id,
              ksn_pack_purchase_status_cd AS ksn_pack_purchase_status_cd,
              stock_ind AS stock_ind,
              substitution_eligible_ind AS substitution_eligible_ind,
              outbound_package_qty AS outbound_package_qty,
	          ship_duns_nbr AS ship_duns_nbr,
	          vendor_managed_inventory_cd  AS vendor_managed_inventory_cd,
	          effective_ts AS effective_ts,
              expiration_ts AS expiration_ts,
              dc_handling_cd AS dc_handling_cd,
              ksn_package_id AS ksn_package_id;
             
work__idrp_kmart_vp_loc_decon_level_raw = JOIN work__join_smith_idrp_loc_join BY((int)vendor_package_id,(int)location_id),
										  smith__idrp_vend_pack_dc_combined_data_8277_filter BY ((int)vendor_package_id,(int)location_nbr);
										  

work__idrp_kmart_vp_loc_decon_level_raw_generate = 
      FOREACH work__idrp_kmart_vp_loc_decon_level_raw 
      GENERATE 
	      '$CURRENT_TIMESTAMP' AS load_ts,
             work__join_smith_idrp_loc_join::vendor_package_id AS vendor_package_id,
              work__join_smith_idrp_loc_join::location_id AS location_id,
              work__join_smith_idrp_loc_join::location_format_type_cd AS location_format_type_cd,
              work__join_smith_idrp_loc_join::location_level_cd AS location_level_cd,
              work__join_smith_idrp_loc_join::location_owner_cd AS location_owner_cd,
              'K' AS source_owner_cd,
              work__join_smith_idrp_loc_join::active_ind AS active_ind,
              '$CURRENT_DATE' AS active_ind_change_dt,
              work__join_smith_idrp_loc_join::allocation_replenishment_cd AS allocation_replenishment_cd,
              (smith__idrp_vend_pack_dc_combined_data_8277_filter::ship_duns_nbr IS NULL OR IsNull(TRIM(smith__idrp_vend_pack_dc_combined_data_8277_filter::ship_duns_nbr),'') =='' ? '' : CONCAT((chararray)TRIM(smith__idrp_vend_pack_dc_combined_data_8277_filter::ship_duns_nbr), '_S')) AS purchase_order_vendor_location_id,
              work__join_smith_idrp_loc_join::replenishment_planning_ind AS replenishment_planning_ind,
              work__join_smith_idrp_loc_join::scan_based_trading_ind AS scan_based_trading_ind, 
              (smith__idrp_vend_pack_dc_combined_data_8277_filter::ship_duns_nbr IS NULL ? '' : CONCAT((chararray)TRIM(smith__idrp_vend_pack_dc_combined_data_8277_filter::ship_duns_nbr), '_S')) AS source_location_id,
              (work__join_smith_idrp_loc_join::vendor_carton_qty == '0' ? '1' : work__join_smith_idrp_loc_join::vendor_carton_qty)  AS source_package_qty,
              work__join_smith_idrp_loc_join::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__join_smith_idrp_loc_join::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__join_smith_idrp_loc_join::flow_type_cd AS flow_type_cd,
              work__join_smith_idrp_loc_join::import_ind AS import_ind,
              work__join_smith_idrp_loc_join::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              work__join_smith_idrp_loc_join::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__join_smith_idrp_loc_join::vendor_stock_nbr AS vendor_stock_nbr,
              work__join_smith_idrp_loc_join::shc_item_id AS shc_item_id,
              work__join_smith_idrp_loc_join::item_purchase_status_cd AS item_purchase_status_cd,
              work__join_smith_idrp_loc_join::can_carry_model_id AS can_carry_model_id,
              work__join_smith_idrp_loc_join::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__join_smith_idrp_loc_join::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__join_smith_idrp_loc_join::reorder_method_cd AS reorder_method_cd,
              work__join_smith_idrp_loc_join::ksn_id AS ksn_id,
              work__join_smith_idrp_loc_join::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__join_smith_idrp_loc_join::cross_merchandising_cd AS cross_merchandising_cd,
              work__join_smith_idrp_loc_join::dotcom_orderable_cd AS dotcom_orderable_cd,
              work__join_smith_idrp_loc_join::kmart_markdown_ind AS kmart_markdown_ind,
              smith__idrp_vend_pack_dc_combined_data_8277_filter::ksn_package_id AS ksn_package_id,
              smith__idrp_vend_pack_dc_combined_data_8277_filter::ksn_pack_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              work__join_smith_idrp_loc_join::dc_configuration_cd AS dc_configuration_cd,
              smith__idrp_vend_pack_dc_combined_data_8277_filter::substitution_eligible_ind AS substitution_eligible_ind,
              work__join_smith_idrp_loc_join::sears_division_nbr AS sears_division_nbr,
              work__join_smith_idrp_loc_join::sears_item_nbr AS sears_item_nbr,
              work__join_smith_idrp_loc_join::sears_sku_nbr AS sears_sku_nbr,
              work__join_smith_idrp_loc_join::sears_source_location_id AS sears_source_location_id,
              work__join_smith_idrp_loc_join::rim_status_cd AS rim_status_cd,
              work__join_smith_idrp_loc_join::stock_type_cd AS stock_type_cd,
              work__join_smith_idrp_loc_join::non_stock_source_cd AS non_stock_source_cd,
              work__join_smith_idrp_loc_join::dos_item_active_ind AS dos_item_active_ind,
              work__join_smith_idrp_loc_join::dos_item_reserve_cd AS dos_item_reserve_cd,
              '$CURRENT_DATE' AS create_dt,
              '$CURRENT_DATE' AS last_update_dt,
              work__join_smith_idrp_loc_join::shc_item_type_cd AS shc_item_type_cd,
              work__join_smith_idrp_loc_join::format_type_cd AS format_type_cd,
              smith__idrp_vend_pack_dc_combined_data_8277_filter::outbound_package_qty AS outbound_package_qty,
              work__join_smith_idrp_loc_join::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              work__join_smith_idrp_loc_join::vendor_carton_qty AS vendor_carton_qty,
              work__join_smith_idrp_loc_join::enable_jif_dc_ind AS enable_jif_dc_ind,
	         '$batchid' AS idrp_batch_id;



work__idrp_kmart_vp_loc_dc_level_active_ind_filter = filter work__idrp_kmart_vp_loc_dc_level by active_ind == 'Y';

work__idrp_kmart_vp_loc_dc_level_filter_active_ind_gen = FOREACH work__idrp_kmart_vp_loc_dc_level_active_ind_filter GENERATE 
											  vendor_package_id AS vendor_package_id;

work__idrp_kmart_vp_loc_decon_level_act_ind_distinct = DISTINCT work__idrp_kmart_vp_loc_dc_level_filter_active_ind_gen; 
											  
work__idrp_kmart_vp_loc_dc_level_rep_plan_ind_filter = filter work__idrp_kmart_vp_loc_dc_level by replenishment_planning_ind == 'Y';

work__idrp_kmart_vp_loc_dc_level_filter_rep_plan_ind_gen = FOREACH work__idrp_kmart_vp_loc_dc_level_rep_plan_ind_filter GENERATE 
											  vendor_package_id AS vendor_package_id;
											  
work__idrp_kmart_vp_loc_dc_level_repln_plan_ind_filter = DISTINCT work__idrp_kmart_vp_loc_dc_level_filter_rep_plan_ind_gen;

work__idrp_kmart_vp_loc_decon_level_raw_join = JOIN work__idrp_kmart_vp_loc_decon_level_raw_generate BY ((int) vendor_package_id) LEFT OUTER, work__idrp_kmart_vp_loc_decon_level_act_ind_distinct BY ((int)vendor_package_id);

work__idrp_kmart_vp_loc_decon_level_raw_join_gen =  FOREACH work__idrp_kmart_vp_loc_decon_level_raw_join GENERATE 
	         '$CURRENT_TIMESTAMP' AS load_ts,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::vendor_package_id AS vendor_package_id,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::location_id AS location_id,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::location_format_type_cd AS location_format_type_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::location_level_cd AS location_level_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::location_owner_cd AS location_owner_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::source_owner_cd AS source_owner_cd,
              (work__idrp_kmart_vp_loc_decon_level_act_ind_distinct::vendor_package_id IS NOT NULL or IsNull(work__idrp_kmart_vp_loc_decon_level_act_ind_distinct::vendor_package_id,'') != '' ? 'Y' : work__idrp_kmart_vp_loc_decon_level_raw_generate::active_ind) AS active_ind,
              '$CURRENT_DATE' AS active_ind_change_dt,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::allocation_replenishment_cd AS allocation_replenishment_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::replenishment_planning_ind AS replenishment_planning_ind,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::scan_based_trading_ind AS scan_based_trading_ind,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::source_location_id AS source_location_id,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::source_package_qty AS source_package_qty,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::flow_type_cd AS flow_type_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::import_ind AS import_ind,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::shc_item_id AS shc_item_id,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::can_carry_model_id AS can_carry_model_id,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::reorder_method_cd AS reorder_method_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::ksn_id AS ksn_id,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::cross_merchandising_cd AS cross_merchandising_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::dotcom_orderable_cd AS dotcom_orderable_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::kmart_markdown_ind AS kmart_markdown_ind,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::ksn_package_id AS ksn_package_id,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::dc_configuration_cd AS dc_configuration_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::substitution_eligible_ind AS substitution_eligible_ind,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::sears_division_nbr AS sears_division_nbr,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::sears_item_nbr AS sears_item_nbr,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::sears_source_location_id AS sears_location_id,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::rim_status_cd AS rim_status_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::stock_type_cd AS stock_type_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::non_stock_source_cd AS non_stock_source_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::dos_item_active_ind AS dos_item_active_ind,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::dos_item_reserve_cd AS dos_item_reserve_cd,
              '$CURRENT_DATE' AS create_dt,
              '$CURRENT_DATE' AS last_update_dt,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::shc_item_type_cd AS shc_item_type_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::format_type_cd AS format_type_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::outbound_package_qty AS outbound_package_qty,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_kmart_vp_loc_decon_level_raw_generate::enable_jif_dc_ind AS enable_jif_dc_ind,
	         '$batchid' AS idrp_batch_id;

smith__idrp_shc_item_combined_data =  LOAD '$SMITH__IDRP_SHC_ITEM_COMBINED_LOCATION'  USING PigStorage('$FIELD_DELIMITER_CONTROL_A')  AS ($SMITH__IDRP_SHC_ITEM_COMBINED_SCHEMA);
         
work__idrp_kmart_vp_loc_decon_level_join_repl_plan_ind = join work__idrp_kmart_vp_loc_decon_level_raw_join_gen by (vendor_package_id) left outer,
	        									  work__idrp_kmart_vp_loc_dc_level_repln_plan_ind_filter by(vendor_package_id);


work__idrp_kmart_vp_loc_decon_level =  FOREACH work__idrp_kmart_vp_loc_decon_level_join_repl_plan_ind GENERATE 
	         '$CURRENT_TIMESTAMP' AS load_ts,
               work__idrp_kmart_vp_loc_decon_level_raw_join_gen::vendor_package_id AS vendor_package_id,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::location_id AS location_id,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::location_format_type_cd AS location_format_type_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::location_level_cd AS location_level_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::location_owner_cd AS location_owner_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::source_owner_cd AS source_owner_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::active_ind AS active_ind,
              '$CURRENT_DATE' AS active_ind_change_dt,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::allocation_replenishment_cd AS allocation_replenishment_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              (work__idrp_kmart_vp_loc_dc_level_repln_plan_ind_filter::vendor_package_id IS NOT NULL or IsNull(work__idrp_kmart_vp_loc_dc_level_repln_plan_ind_filter::vendor_package_id,'') != '' ? 'Y' : work__idrp_kmart_vp_loc_decon_level_raw_join_gen::replenishment_planning_ind) AS replenishment_planning_ind,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::scan_based_trading_ind AS scan_based_trading_ind,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::source_location_id AS source_location_id,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::source_package_qty AS source_package_qty,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::flow_type_cd AS flow_type_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::import_ind AS import_ind,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::shc_item_id AS shc_item_id,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::can_carry_model_id AS can_carry_model_id,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::reorder_method_cd AS reorder_method_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::ksn_id AS ksn_id,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::cross_merchandising_cd AS cross_merchandising_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::dotcom_orderable_cd AS dotcom_orderable_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::kmart_markdown_ind AS kmart_markdown_ind,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::ksn_package_id AS ksn_package_id,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::dc_configuration_cd AS dc_configuration_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::substitution_eligible_ind AS substitution_eligible_ind,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::sears_division_nbr AS sears_division_nbr,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::sears_item_nbr AS sears_item_nbr,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::sears_location_id AS sears_location_id,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::rim_status_cd AS rim_status_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::stock_type_cd AS stock_type_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::non_stock_source_cd AS non_stock_source_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::dos_item_active_ind AS dos_item_active_ind,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::dos_item_reserve_cd AS dos_item_reserve_cd,
              '$CURRENT_DATE' AS create_dt,
              '$CURRENT_DATE' AS last_update_dt,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::shc_item_type_cd AS shc_item_type_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::format_type_cd AS format_type_cd,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::outbound_package_qty AS outbound_package_qty,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_kmart_vp_loc_decon_level_raw_join_gen::enable_jif_dc_ind AS enable_jif_dc_ind,
	         '$batchid' AS idrp_batch_id;


work__idrp_kmart_vp_loc_decon_level_full = union work__idrp_import_direct_decon,work__idrp_kmart_vp_loc_decon_level ;


work_idrp_decon_level_join_smith_loc = 
      JOIN work__idrp_kmart_vp_loc_decon_level_full BY source_location_id,
           smith__idrp_eligible_loc_data BY loc;


work__idrp_kmart_vp_loc_decon_src_loc = 
      FOREACH work_idrp_decon_level_join_smith_loc 
      GENERATE 
              '$CURRENT_TIMESTAMP' AS load_ts,
              vendor_package_id,
              location_id,
              location_format_type_cd,
              location_level_cd,
              location_owner_cd,
              source_owner_cd,
              active_ind,
              active_ind_change_dt,
              allocation_replenishment_cd,
              purchase_order_vendor_location_id,
              replenishment_planning_ind,
              scan_based_trading_ind,
              source_location_id AS source_location_id,
              smith__idrp_eligible_loc_data::loc_lvl_cd AS source_location_level_cd,
              source_package_qty,
              vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt,
              flow_type_cd,
              import_ind,
              retail_carton_vendor_package_id,
              vendor_package_owner_cd,
              vendor_stock_nbr,
              shc_item_id,
              item_purchase_status_cd,
              can_carry_model_id,
              days_to_check_begin_day_qty,
              days_to_check_end_day_qty,
              reorder_method_cd,
              ksn_id,
              ksn_purchase_status_cd,
              cross_merchandising_cd,
              dotcom_orderable_cd,
              kmart_markdown_ind,
              ksn_package_id,
              ksn_dc_package_purchase_status_cd,
              dc_configuration_cd,
              substitution_eligible_ind,
              sears_division_nbr,
              sears_item_nbr,
              sears_sku_nbr,
              sears_location_id,
              smith__idrp_eligible_loc_data::srs_loc AS sears_source_location_id,
              rim_status_cd,
              stock_type_cd,
              non_stock_source_cd,
              dos_item_active_ind,
              dos_item_reserve_cd,
              create_dt,
              last_update_dt,
              shc_item_type_cd,
              format_type_cd,
              outbound_package_qty,
              retail_carton_internal_package_qty,
              vendor_carton_qty,
              enable_jif_dc_ind,
	          '1970-01-01' AS rim_last_record_creation_dt,
              '$batchid' AS idrp_batch_id;

work__idrp_kmart_vp_loc_decon_src_loc = distinct work__idrp_kmart_vp_loc_decon_src_loc;

work__idrp_kmart_vendor_package_location_warehouse_level = 
      UNION work__idrp_kmart_vp_loc_dc_level,
            work__idrp_kmart_vp_loc_decon_src_loc,
	        work__idrp_kmart_vp_loc_dc_level_not_8277;


work__idrp_kmart_vendor_package_location_warehouse_level_dist = DISTINCT work__idrp_kmart_vendor_package_location_warehouse_level;

work__idrp_kmart_vendor_package_location_store_level_dist = DISTINCT work__idrp_kmart_vendor_package_location_store_level;

work__idrp_kmart_vp_loc_dc_level_1_dist = DISTINCT work__idrp_kmart_vp_loc_dc_level_1;

STORE work__idrp_kmart_vp_loc_dc_level_1_dist INTO '$WORK__IDRP_KMART_VP_LOC_DC_LEVEL_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

STORE work__idrp_kmart_vendor_package_location_store_level_dist INTO '$WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_STORE_LEVEL_LOCATION_PART2' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

STORE work__idrp_kmart_vendor_package_location_warehouse_level_dist INTO '$WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_WAREHOUSE_LEVEL_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
