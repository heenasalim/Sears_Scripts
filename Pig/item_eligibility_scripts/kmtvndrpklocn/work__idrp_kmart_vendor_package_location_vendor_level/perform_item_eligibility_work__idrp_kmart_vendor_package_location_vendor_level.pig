/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_kmart_vendor_package_location_vendor_level.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Wed Apr 23 02:53:25 EDT 2014
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
#		03/19/2015	nthadan			included load_ts & batchId columns in work__idrp_kmart_vendor_package_location_vendor_level to match with Hive Table definition
#									& similar changes for work__idrp_kmart_vendor_package_location_store_level
#		1/16/2017      SRUJAN DUSSA    IPS-779 - Added rim_last_record_creation_dt to the vendor pack location tables.  This is a sears only column that will be defaulted to '1970-01-01'
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

smith__idrp_eligible_loc_data = 
      LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);

work__idrp_kmart_vendor_package_location_store_level_data = 
      LOAD '$WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_STORE_LEVEL_LOCATION_PART2'
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_STORE_LEVEL_SCHEMA);

work__idrp_kmart_vendor_package_location_warehouse_level_data = 
      LOAD '$WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_WAREHOUSE_LEVEL_LOCATION'
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_WAREHOUSE_LEVEL_SCHEMA);


work__idrp_kmart_vendor_package_location_warehouse_level_data_fltr = 
      FILTER work__idrp_kmart_vendor_package_location_warehouse_level_data 
      BY source_location_id!='8277';

work__idrp_kmart_vp_loc_vend_decon_dc_1 = 
      FOREACH work__idrp_kmart_vendor_package_location_warehouse_level_data_fltr 
      GENERATE 
              vendor_package_id,
              source_location_id AS location_id,
              '' AS location_format_type_cd,
              'VENDOR' AS location_level_cd,
              'K' AS source_owner_cd,
              'Y' AS active_ind,
              '$CURRENT_DATE' AS active_ind_change_dt,
              allocation_replenishment_cd,
              '' AS purchase_order_vendor_location_id,
              (shc_item_type_cd == 'EXAS' ? 'N':'Y')  AS replenishment_planning_ind,
              '' AS scan_based_trading_ind,
              '' AS source_location_id,
              '' AS source_location_level_cd,
              '1' AS source_package_qty,
              vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt,
              flow_type_cd,
              import_ind,
              retail_carton_vendor_package_id,
              vendor_package_owner_cd,
              vendor_stock_nbr,
              shc_item_id,
              item_purchase_status_cd,
              '' AS can_carry_model_id,
              '' AS days_to_check_begin_day_qty,
              '' AS days_to_check_end_day_qty,
              '' AS reorder_method_cd,
              ksn_id,
              ksn_purchase_status_cd,
              '' AS cross_merchandising_cd,
              dotcom_orderable_cd,
              '' AS kmart_markdown_ind,
              ksn_package_id,
              '' AS ksn_dc_package_purchase_status_cd,
              '' AS dc_configuration_cd,
              '' AS substitution_eligible_ind,
              sears_division_nbr,
              sears_item_nbr,
              sears_sku_nbr ,
              '' AS sears_location_id,
              '' AS sears_source_location_id,
              '' AS rim_status_cd,
              '' AS stock_type_cd,
              '' AS non_stock_source_cd,
              '' AS dos_item_active_ind,
              '' AS dos_item_reserve_cd,
              '$CURRENT_DATE' AS create_dt,
              '$CURRENT_DATE' AS last_update_dt,
              shc_item_type_cd,
              '' AS format_type_cd,
              '' AS outbound_package_qty,
              '' AS retail_carton_internal_package_qty,
              '' AS vendor_carton_qty,
              '' AS enable_jif_dc_ind;
              
              
work__idrp_kmart_vendor_package_location_store_level_fltr = 
      FILTER work__idrp_kmart_vendor_package_location_store_level_data
      BY /*servicing_dc_nbr=='0' AND */ source_location_level_cd=='VENDOR' AND source_location_id!='447'; 

work__idrp_kmart_vp_loc_vend_store_1 = 
      FOREACH work__idrp_kmart_vendor_package_location_store_level_fltr
      GENERATE
              vendor_package_id,
              source_location_id AS location_id,
              '' AS location_format_type_cd,
              'VENDOR' AS location_level_cd,
              'K' AS source_owner_cd,
              'Y' AS active_ind,
              '$CURRENT_DATE' AS active_ind_change_dt,
              allocation_replenishment_cd,
              '' AS purchase_order_vendor_location_id,
              (shc_item_type_cd == 'EXAS' ? 'N':'Y') AS replenishment_planning_ind,
              '' AS scan_based_trading_ind,
              '' AS source_location_id,
              '' AS source_location_level_cd,
              '1' AS source_package_qty,
              vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt,
              flow_type_cd,
              import_ind,
              retail_carton_vendor_package_id,
              vendor_package_owner_cd,
              vendor_stock_nbr,
              shc_item_id,
              item_purchase_status_cd,
              '' AS can_carry_model_id,
              '' AS days_to_check_begin_day_qty,
              '' AS days_to_check_end_day_qty,
              '' AS reorder_method_cd,
              ksn_id,
              ksn_purchase_status_cd,
              '' AS cross_merchandising_cd,
              dotcom_orderable_cd,
              '' AS kmart_markdown_ind,
              ksn_package_id,
              '' AS ksn_dc_package_purchase_status_cd,
              '' AS dc_configuration_cd,
              '' AS substitution_eligible_ind,
              sears_division_nbr,
              sears_item_nbr,
              sears_sku_nbr ,
              '' AS sears_location_id,
              '' AS sears_source_location_id,
              '' AS rim_status_cd,
              '' AS stock_type_cd,
              '' AS non_stock_source_cd,
              '' AS dos_item_active_ind,
              '' AS dos_item_reserve_cd,
              '$CURRENT_DATE' AS create_dt ,
              '$CURRENT_DATE' AS last_update_dt ,
              shc_item_type_cd,
              '' AS format_type_cd,
              '' AS outbound_package_qty,
              '' AS retail_carton_internal_package_qty,
              '' AS vendor_carton_qty,
              '' AS enable_jif_dc_ind;

 
 work__idrp_kmart_vp_loc_vendor_union = 
      UNION work__idrp_kmart_vp_loc_vend_store_1,
            work__idrp_kmart_vp_loc_vend_decon_dc_1;


work__idrp_kmart_vp_loc_vendor = DISTINCT work__idrp_kmart_vp_loc_vendor_union;
 
 smith__idrp_eligible_loc_data_filter  = FILTER smith__idrp_eligible_loc_data
                                        BY loc_lvl_cd=='VENDOR';
 
join_smith_loc_vendor_loc = 
     JOIN work__idrp_kmart_vp_loc_vendor BY location_id,
          smith__idrp_eligible_loc_data_filter BY loc;

/* CR4383 ends here*/
             
work__idrp_kmart_vendor_package_location_vendor_level = 
      FOREACH join_smith_loc_vendor_loc
      GENERATE 
              '$CURRENT_TIMESTAMP' AS load_ts,
              vendor_package_id,
              location_id,
              location_format_type_cd,
              location_level_cd,
              smith__idrp_eligible_loc_data_filter::loc_owner_cd AS location_owner_cd,
              source_owner_cd,
              active_ind,
              active_ind_change_dt,
              allocation_replenishment_cd,
              purchase_order_vendor_location_id,
              replenishment_planning_ind,
              scan_based_trading_ind,
              source_location_id,
              source_location_level_cd,
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
              sears_source_location_id,
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


STORE work__idrp_kmart_vendor_package_location_store_level_data
INTO '$WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_STORE_LEVEL_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


STORE work__idrp_kmart_vendor_package_location_vendor_level 
INTO '$WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_VENDOR_LEVEL_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
