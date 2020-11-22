/*
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_mips1_smith__idrp_vend_pack_combined.pig
# AUTHOR NAME:         Kumari Shalinee
# CREATION DATE:       06-09-2013 09:12
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>
#
#
#
# DEPENDENCIES: None
# RESTARTABLE:  N/A
#
#
# REV LIST:
#        DATE         BY            	MODIFICATION
#		 13-06-2014	  Mayank Agarwal	Changes for IE master data
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

-- Register the jar containing all PIG UDFs
REGISTER $UDF_JAR;

SET default_parallel $NUM_PARALLEL;

/*
###############################################################################
#<<                           START CUSTOM HEADER CODE                      >>#
###############################################################################

###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

/******************************************************Load all the required tables and file**********************************************************/

gold__item_vendor_package_current_data = LOAD '$GOLD__ITEM_VENDOR_PACKAGE_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_VENDOR_PACKAGE_CURRENT_SCHEMA);

work__idrp_item_hierarchy_combined_all_current_data = LOAD '$WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_SCHEMA);

gold__item_aprk_current_data = LOAD '$GOLD__ITEM_APRK_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_APRK_CURRENT_SCHEMA);

/**************************************************************************************************************************************/

/******************************************************Applied Join Logic**********************************************************/

gold_item_vend_aprk_join = JOIN gold__item_vendor_package_current_data BY (aprk_id), gold__item_aprk_current_data BY (aprk_id);

gold_item_vend_aprk_field = FOREACH gold_item_vend_aprk_join GENERATE gold__item_vendor_package_current_data::vendor_package_id AS vendor_package_id,
                                                                    gold__item_vendor_package_current_data::aprk_id AS aprk_id,
                                                                    gold__item_vendor_package_current_data::carton_per_layer_qty AS carton_per_layer_qty,
                                                                    gold__item_vendor_package_current_data::cost_amt AS cost_amt,
                                                                    gold__item_vendor_package_current_data::cost_mode_cd AS cost_mode_cd,
                                                                    gold__item_vendor_package_current_data::cost_uom_cd AS cost_uom_cd,
                                                                    gold__item_vendor_package_current_data::cost_uom_desc AS cost_uom_desc,
                                                                    gold__item_vendor_package_current_data::eas_tag_ind AS eas_tag_ind,
                                                                    gold__item_vendor_package_current_data::effective_ts AS effective_ts,
                                                                    gold__item_vendor_package_current_data::expiration_ts AS expiration_ts,
                                                                    gold__item_vendor_package_current_data::flow_type_cd AS flow_type_cd,
                                                                    gold__item_vendor_package_current_data::flow_type_desc AS flow_type_desc,
                                                                    gold__item_vendor_package_current_data::gtin_usage_cd AS gtin_usage_cd,
                                                                    gold__item_vendor_package_current_data::gtin_usage_desc AS gtin_usage_desc,
                                                                    gold__item_vendor_package_current_data::import_cd AS import_cd,
                                                                    gold__item_vendor_package_current_data::import_desc AS import_desc,
                                                                    gold__item_vendor_package_current_data::ksn_id AS ksn_id,
                                                                    gold__item_vendor_package_current_data::ksn_package_id AS ksn_package_id,
                                                                    gold__item_vendor_package_current_data::land_cost_analysis_ind AS land_cost_analysis_ind,
                                                                    gold__item_vendor_package_current_data::last_change_user_id AS last_change_user_id,
                                                                    gold__item_vendor_package_current_data::layer_per_pallet_qty AS layer_per_pallet_qty,
                                                                    gold__item_vendor_package_current_data::order_ind AS order_ind,
                                                                    gold__item_vendor_package_current_data::order_multiply_qty AS order_multiply_qty,
                                                                    gold__item_vendor_package_current_data::order_uom_cd AS order_uom_cd,
                                                                    gold__item_vendor_package_current_data::order_uom_desc AS order_uom_desc,
                                                                    gold__item_vendor_package_current_data::owner_cd AS owner_cd,
                                                                    gold__item_vendor_package_current_data::package_id AS package_id,
                                                                    gold__item_vendor_package_current_data::projected_first_in_store_dt AS projected_first_in_store_dt,
                                                                    gold__item_vendor_package_current_data::projected_store_sale_qty AS projected_store_sale_qty,
                                                                    gold__item_vendor_package_current_data::projected_unit_sale_qty AS projected_unit_sale_qty,
                                                                    gold__item_vendor_package_current_data::purchase_status_cd AS purchase_status_cd,
                                                                    gold__item_vendor_package_current_data::purchase_status_desc AS purchase_status_desc,
                                                                    gold__item_vendor_package_current_data::purchase_status_dt AS purchase_status_dt,
                                                                    gold__item_vendor_package_current_data::sears_dc_store_order_multiply_qty AS sears_dc_store_order_multiply_qty,
                                                                    gold__item_vendor_package_current_data::service_area_restriction_model_id AS service_area_restriction_model_id,
                                                                    gold__item_vendor_package_current_data::ship_gtin_exempt_cd AS ship_gtin_exempt_cd,
                                                                    gold__item_vendor_package_current_data::ship_gtin_exempt_desc AS ship_gtin_exempt_desc,
                                                                    gold__item_vendor_package_current_data::supplier_arpk_id AS supplier_arpk_id,
                                                                    gold__item_vendor_package_current_data::vendor_carton_qty AS vendor_carton_qty,
                                                                    gold__item_vendor_package_current_data::vendor_cost_format_cd AS vendor_cost_format_cd,
                                                                    gold__item_vendor_package_current_data::vendor_package_alternate_id AS vendor_package_alternate_id,
                                                                    gold__item_vendor_package_current_data::vendor_stock_desc AS vendor_stock_desc,
                                                                    gold__item_vendor_package_current_data::vendor_stock_nbr AS vendor_stock_nbr,
                                                                    gold__item_aprk_current_data::activity_point_id AS activity_point_id,
                                                                    gold__item_aprk_current_data::activity_point_nm AS activity_point_nm,
                                                                    gold__item_aprk_current_data::address_role_type_cd AS address_role_type_cd,
                                                                    gold__item_aprk_current_data::aprk_type_cd AS aprk_type_cd,
                                                                    gold__item_aprk_current_data::duns_orgin_cd AS duns_orgin_cd,
                                                                    gold__item_aprk_current_data::duns_owner_cd AS duns_owner_cd,
                                                                    gold__item_aprk_current_data::effective_dt AS aprk_effective_dt,
                                                                    gold__item_aprk_current_data::hierarchy_instance_id AS hierarchy_instance_id,
                                                                    gold__item_aprk_current_data::import_ind AS order_duns_import_ind,
                                                                    gold__item_aprk_current_data::duns_nbr AS order_duns_nbr;

/******************************************************Applied Filter Logic**********************************************************/

gold_item_vend_aprk_filter = FILTER gold_item_vend_aprk_field BY aprk_type_cd == 'ORD';

/******************************************************Applied Join Logic**********************************************************/

smith_gold_item_vend_aprk_join = JOIN gold_item_vend_aprk_filter BY (ksn_id), work__idrp_item_hierarchy_combined_all_current_data BY (ksn_id);

smith_gold_item_vend_aprk_field = FOREACH smith_gold_item_vend_aprk_join GENERATE 
										'$CURRENT_TIMESTAMP' AS load_ts,
										gold_item_vend_aprk_filter::vendor_package_id AS vendor_package_id,
                                                                                gold_item_vend_aprk_filter::activity_point_id AS activity_point_id,
                                                                                gold_item_vend_aprk_filter::activity_point_nm AS activity_point_nm,
                                                                                gold_item_vend_aprk_filter::address_role_type_cd AS address_role_type_cd,
                                                                                gold_item_vend_aprk_filter::aprk_id AS aprk_id,
                                                                                gold_item_vend_aprk_filter::aprk_type_cd AS aprk_type_cd,
                                                                                gold_item_vend_aprk_filter::carton_per_layer_qty AS carton_per_layer_qty,
                                                                                gold_item_vend_aprk_filter::cost_amt AS cost_amt,
                                                                                gold_item_vend_aprk_filter::cost_mode_cd AS cost_mode_cd,
                                                                                gold_item_vend_aprk_filter::cost_uom_cd AS cost_uom_cd,
                                                                                gold_item_vend_aprk_filter::cost_uom_desc AS cost_uom_desc,
                                                                                gold_item_vend_aprk_filter::duns_orgin_cd AS duns_orgin_cd,
                                                                                gold_item_vend_aprk_filter::duns_owner_cd AS duns_owner_cd,
                                                                                gold_item_vend_aprk_filter::eas_tag_ind AS eas_tag_ind,
                                                                                gold_item_vend_aprk_filter::aprk_effective_dt AS aprk_effective_dt,
                                                                                gold_item_vend_aprk_filter::effective_ts AS effective_ts,
                                                                                gold_item_vend_aprk_filter::expiration_ts AS expiration_ts,
                                                                                gold_item_vend_aprk_filter::flow_type_cd AS flow_type_cd,
                                                                                gold_item_vend_aprk_filter::flow_type_desc AS flow_type_desc,
                                                                                gold_item_vend_aprk_filter::gtin_usage_cd AS gtin_usage_cd,
                                                                                gold_item_vend_aprk_filter::gtin_usage_desc AS gtin_usage_desc,
                                                                                gold_item_vend_aprk_filter::hierarchy_instance_id AS hierarchy_instance_id,
                                                                                gold_item_vend_aprk_filter::import_cd AS import_cd,
                                                                                gold_item_vend_aprk_filter::import_desc AS import_desc,
                                                                                gold_item_vend_aprk_filter::ksn_id AS ksn_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::ksn_id_effective_ts AS ksn_id_effective_ts,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::ksn_id_expiration_ts AS ksn_id_expiration_ts,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::ksn_last_change_user_id AS ksn_last_change_user_id,
                                                                                gold_item_vend_aprk_filter::ksn_package_id AS ksn_package_id,
                                                                                gold_item_vend_aprk_filter::land_cost_analysis_ind AS land_cost_analysis_ind,
                                                                                gold_item_vend_aprk_filter::last_change_user_id AS last_change_user_id,
                                                                                gold_item_vend_aprk_filter::layer_per_pallet_qty AS layer_per_pallet_qty,
                                                                                gold_item_vend_aprk_filter::order_duns_import_ind AS order_duns_import_ind,
                                                                                gold_item_vend_aprk_filter::order_duns_nbr AS order_duns_nbr,
                                                                                gold_item_vend_aprk_filter::order_ind AS order_ind,
                                                                                gold_item_vend_aprk_filter::order_multiply_qty AS order_multiply_qty,
                                                                                gold_item_vend_aprk_filter::order_uom_cd AS order_uom_cd,
                                                                                gold_item_vend_aprk_filter::order_uom_desc AS order_uom_desc,
                                                                                gold_item_vend_aprk_filter::owner_cd AS owner_cd,
                                                                                gold_item_vend_aprk_filter::package_id AS package_id,
                                                                                gold_item_vend_aprk_filter::projected_first_in_store_dt AS projected_first_in_store_dt,
                                                                                gold_item_vend_aprk_filter::projected_store_sale_qty AS projected_store_sale_qty,
                                                                                gold_item_vend_aprk_filter::projected_unit_sale_qty AS projected_unit_sale_qty,
                                                                                gold_item_vend_aprk_filter::purchase_status_cd AS purchase_status_cd,
                                                                                gold_item_vend_aprk_filter::purchase_status_desc AS purchase_status_desc,
                                                                                gold_item_vend_aprk_filter::purchase_status_dt AS purchase_status_dt,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_business_desc AS sears_business_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_business_nbr AS sears_business_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_category_desc AS sears_category_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_category_nbr AS sears_category_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_class_desc AS sears_class_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_class_nbr AS sears_class_nbr,
                                                                                gold_item_vend_aprk_filter::sears_dc_store_order_multiply_qty AS sears_dc_store_order_multiply_qty,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_division_desc AS sears_division_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_division_nbr AS sears_division_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_group_desc AS sears_group_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_group_nbr AS sears_group_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_hierarchy_exception_ind AS sears_hierarchy_exception_ind,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_item_color_cd AS sears_item_color_cd,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_item_color_desc AS sears_item_color_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_item_last_change_user_id AS sears_item_last_change_user_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_item_nbr AS sears_item_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_item_size_desc AS sears_item_size_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_item_size_ratio_cd AS sears_item_size_ratio_cd,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_item_size_sub_desc AS sears_item_size_sub_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_line_desc AS sears_line_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_line_nbr AS sears_line_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_sku_desc AS sears_sku_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_sku_nbr AS sears_sku_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_sub_line_desc AS sears_sub_line_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::sears_sub_line_nbr AS sears_sub_line_nbr,
                                                                                gold_item_vend_aprk_filter::service_area_restriction_model_id AS service_area_restriction_model_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_business_desc AS shc_business_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_business_id AS shc_business_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_business_level_id AS shc_business_level_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_business_nbr AS shc_business_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_business_unit_desc AS shc_business_unit_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_business_unit_id AS shc_business_unit_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_business_unit_level_id AS shc_business_unit_level_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_business_unit_nbr AS shc_business_unit_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_category_desc AS shc_category_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_category_group_desc AS shc_category_group_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_category_group_id AS shc_category_group_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_category_group_level_id AS shc_category_group_level_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_category_group_level_nbr AS shc_category_group_level_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_category_id AS shc_category_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_category_level_id AS shc_category_level_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_category_nbr AS shc_category_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_corporate_desc AS shc_corporate_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_corporate_id AS shc_corporate_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_corporate_level_id AS shc_corporate_level_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_corporate_nbr AS shc_corporate_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_department_desc AS shc_department_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_department_id AS shc_department_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_department_level_id AS shc_department_level_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_department_nbr AS shc_department_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_division_desc AS shc_division_desc,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_division_id AS shc_division_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_division_level_id AS shc_division_level_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_division_nbr AS shc_division_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_item_id AS shc_item_id,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::shc_item_type_cd AS shc_item_type_cd,
                                                                                gold_item_vend_aprk_filter::ship_gtin_exempt_cd AS ship_gtin_exempt_cd,
                                                                                gold_item_vend_aprk_filter::ship_gtin_exempt_desc AS ship_gtin_exempt_desc,
                                                                                gold_item_vend_aprk_filter::supplier_arpk_id AS supplier_arpk_id,
                                                                                gold_item_vend_aprk_filter::vendor_carton_qty AS vendor_carton_qty,
                                                                                gold_item_vend_aprk_filter::vendor_cost_format_cd AS vendor_cost_format_cd,
                                                                                gold_item_vend_aprk_filter::vendor_package_alternate_id AS vendor_package_alternate_id,
                                                                                gold_item_vend_aprk_filter::vendor_stock_desc AS vendor_stock_desc,
                                                                                gold_item_vend_aprk_filter::vendor_stock_nbr AS vendor_stock_nbr,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::ksn_purchase_status_cd AS ksn_purchase_status_cd,
                                                                                work__idrp_item_hierarchy_combined_all_current_data::dotcom_allocation_ind AS dotcom_allocation_ind,
																				'$batchid';

/**************************************************Storing the required file on HDFS***************************************************************/

STORE smith_gold_item_vend_aprk_field INTO '$SMITH__IDRP_VEND_PACK_COMBINED_WORK_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
