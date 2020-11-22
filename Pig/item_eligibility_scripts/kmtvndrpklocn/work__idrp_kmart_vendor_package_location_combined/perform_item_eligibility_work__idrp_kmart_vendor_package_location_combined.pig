/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_kmart_vendor_package_location_combined.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Wed Apr 23 02:53:48 EDT 2014
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
#		03/19/2015	nthadan			included load_ts column in work__idrp_kmart_vendor_package_location_combined to match with Hive Table definition
#		1/16/2017      SRUJAN DUSSA    IPS-779 - Added rim_last_record_creation_dt to the vendor pack location tables.  This is a sears only column that will be defaulted to '1970-01-01'
#
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

SET default_parallel $NUM_PARALLEL;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

work__idrp_kmart_vendor_package_location_store_level_data = 
      LOAD '$WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_STORE_LEVEL_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_STORE_LEVEL_SCHEMA);


work__idrp_kmart_vendor_package_location_warehouse_level_data = 
      LOAD '$WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_WAREHOUSE_LEVEL_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_WAREHOUSE_LEVEL_SCHEMA);


work__idrp_kmart_vendor_package_location_vendor_level_data = 
      LOAD '$WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_VENDOR_LEVEL_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_VENDOR_LEVEL_SCHEMA);


work__idrp_kmart_vendor_package_location_combined = 
      UNION work__idrp_kmart_vendor_package_location_store_level_data,
            work__idrp_kmart_vendor_package_location_warehouse_level_data,
            work__idrp_kmart_vendor_package_location_vendor_level_data;


netapp_vend_pack_loc = 
      FOREACH work__idrp_kmart_vendor_package_location_combined
      GENERATE
      		'$CURRENT_TIMESTAMP' AS load_ts,
              vendor_package_id AS vendor_package_id,
              location_id AS location_id,
              location_format_type_cd AS location_format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              source_owner_cd AS source_owner_cd,
              active_ind AS active_ind,
              active_ind_change_dt AS active_ind_change_dt,
              allocation_replenishment_cd AS allocation_replenishment_cd,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              replenishment_planning_ind AS replenishment_planning_ind,
              scan_based_trading_ind AS scan_based_trading_ind,
              source_location_id AS source_location_id,
              source_location_level_cd AS source_location_level_cd,
              source_package_qty AS source_package_qty,
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
              days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              days_to_check_end_day_qty AS days_to_check_end_day_qty,
              reorder_method_cd AS reorder_method_cd,
              ksn_id AS ksn_id,
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              cross_merchandising_cd AS cross_merchandising_cd,
              dotcom_orderable_cd AS dotcom_orderable_cd,
              kmart_markdown_ind AS kmart_markdown_ind,
              ksn_package_id AS ksn_package_id,
              ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              dc_configuration_cd AS dc_configuration_cd,
              substitution_eligible_ind AS substitution_eligible_ind,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              sears_location_id AS sears_location_id,
              sears_source_location_id AS sears_source_location_id,
              rim_status_cd AS rim_status_cd,
              stock_type_cd AS stock_type_cd,
              non_stock_source_cd AS non_stock_source_cd,
              dos_item_active_ind AS dos_item_active_ind,
              dos_item_reserve_cd AS dos_item_reserve_cd,
              create_dt AS create_dt,
              last_update_dt AS last_update_dt,
	          rim_last_record_creation_dt,
              '$batchid' AS batch_id;


netapp_vend_pack_loc = DISTINCT netapp_vend_pack_loc;


STORE netapp_vend_pack_loc 
INTO '$NETAPP_VEND_PACK_LOC_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


STORE work__idrp_kmart_vendor_package_location_combined
INTO '$WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_COMBINED_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
