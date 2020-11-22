/*
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_mips1_smith__idrp_vend_pack_dc_combined.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       03-09-2013 04:35
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
/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

set default_parallel $NUM_PARALLEL;

gold__item_kmart_vendor_package_dc_location_data_load = LOAD '$GOLD__ITEM_KMART_VENDOR_PACKAGE_DC_LOCATION_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_KMART_VENDOR_PACKAGE_DC_LOCATION_SCHEMA);

gold__item_aprk_current_data = LOAD '$GOLD__ITEM_APRK_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS
                    ($GOLD__ITEM_APRK_CURRENT_SCHEMA);

gold__item_kmart_ksn_dc_package_data_load = LOAD '$GOLD__ITEM_KMART_KSN_DC_PACKAGE_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS
                    ($GOLD__ITEM_KMART_KSN_DC_PACKAGE_SCHEMA);

smith__idrp_vendor_package_combined_data = LOAD '$SMITH__IDRP_VEND_PACK_COMBINED_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS
                    ($SMITH__IDRP_VEND_PACK_COMBINED_SCHEMA);

gold__item_vendor_ship_point_dc_location_current_data = LOAD '$GOLD__ITEM_VENDOR_SHIP_POINT_DC_LOCATION_CURRENT_LOCATION' USING  PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_VENDOR_SHIP_POINT_DC_LOCATION_CURRENT_SCHEMA);

smith__idrp_item_eligibility_batchdate_data = LOAD '$SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS
                    ($SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_SCHEMA);

gold__item_aprk_current_data_fltr = FILTER gold__item_aprk_current_data BY
                    TRIM(aprk_type_cd) == 'SHIP';

/***************************************Joining smith_idrp_replenishment_day & gold__item_kmart_vendor_package_dc_location tables***********************/

smith_idrp_replenishment_day_join_gold__item_kmart_vendor_package_dc_location = CROSS smith__idrp_item_eligibility_batchdate_data,gold__item_kmart_vendor_package_dc_location_data_load;

cross_join_data = DISTINCT smith_idrp_replenishment_day_join_gold__item_kmart_vendor_package_dc_location;

/*************************************************Applying filter on timestamps and generating the required data****************************************/

gold__item_kmart_vendor_package_dc_location_data_fltr = FILTER cross_join_data BY
        (TRIM(smith__idrp_item_eligibility_batchdate_data::processing_ts) >= TRIM(gold__item_kmart_vendor_package_dc_location_data_load::effective_ts) AND
        TRIM(smith__idrp_item_eligibility_batchdate_data::processing_ts) <= TRIM(gold__item_kmart_vendor_package_dc_location_data_load::expiration_ts));
		
gold__item_kmart_vendor_package_dc_location_data = FOREACH gold__item_kmart_vendor_package_dc_location_data_fltr GENERATE
                                        gold__item_kmart_vendor_package_dc_location_data_load::load_ts AS load_ts,
                                       gold__item_kmart_vendor_package_dc_location_data_load::vendor_package_id AS vendor_package_id,
                                       gold__item_kmart_vendor_package_dc_location_data_load::location_nbr AS location_nbr,
                                       gold__item_kmart_vendor_package_dc_location_data_load::effective_ts AS effective_ts,
                                       gold__item_kmart_vendor_package_dc_location_data_load::expiration_ts AS expiration_ts,
                                       gold__item_kmart_vendor_package_dc_location_data_load::ship_aprk_id AS ship_aprk_id,
                                       gold__item_kmart_vendor_package_dc_location_data_load::purchase_status_cd AS purchase_status_cd,
                                       gold__item_kmart_vendor_package_dc_location_data_load::purchase_status_desc AS purchase_status_desc,
                                       gold__item_kmart_vendor_package_dc_location_data_load::inbound_unit_cost_amt AS inbound_unit_cost_amt,
                                       gold__item_kmart_vendor_package_dc_location_data_load::ksn_id AS ksn_id,
                                       gold__item_kmart_vendor_package_dc_location_data_load::first_receipt_dt AS first_receipt_dt,
                                       gold__item_kmart_vendor_package_dc_location_data_load::purchase_status_dt AS purchase_status_dt,
                                       gold__item_kmart_vendor_package_dc_location_data_load::unit_per_trailer_qty AS unit_per_trailer_qty,
                                       gold__item_kmart_vendor_package_dc_location_data_load::trailer_units_uom_cd AS trailer_units_uom_cd,
                                       gold__item_kmart_vendor_package_dc_location_data_load::trailer_units_uom_desc AS trailer_units_uom_desc,
                                       gold__item_kmart_vendor_package_dc_location_data_load::pallet_exempt_ind AS pallet_exempt_ind,
                                       gold__item_kmart_vendor_package_dc_location_data_load::pallet_per_trailer_qty AS pallet_per_trailer_qty,
                                       gold__item_kmart_vendor_package_dc_location_data_load::carton_per_layer_qty AS carton_per_layer_qty,
                                       gold__item_kmart_vendor_package_dc_location_data_load::layer_per_pallet_qty AS layer_per_pallet_qty,
                                       gold__item_kmart_vendor_package_dc_location_data_load::pallet_weight_qty AS pallet_weight_qty,
                                       gold__item_kmart_vendor_package_dc_location_data_load::ship_cube_qty AS ship_cube_qty,
                                       gold__item_kmart_vendor_package_dc_location_data_load::stack_pallet_cd AS stack_pallet_cd,
                                       gold__item_kmart_vendor_package_dc_location_data_load::stack_pallet_desc AS stack_pallet_desc,
                                       gold__item_kmart_vendor_package_dc_location_data_load::pallet_surface_balanced_ind AS pallet_surface_balanced_ind,
                                       gold__item_kmart_vendor_package_dc_location_data_load::inbound_order_uom_cd AS inbound_order_uom_cd,
                                       gold__item_kmart_vendor_package_dc_location_data_load::inbound_order_uom_desc AS inbound_order_uom_desc,
                                       gold__item_kmart_vendor_package_dc_location_data_load::trailer_capacity_id AS trailer_capacity_id,
                                       gold__item_kmart_vendor_package_dc_location_data_load::trailer_capacity_desc AS trailer_capacity_desc,
                                       gold__item_kmart_vendor_package_dc_location_data_load::trailer_capacity_cube_qty AS trailer_capacity_cube_qty,
                                       gold__item_kmart_vendor_package_dc_location_data_load::trailer_capacity_pallet_qty AS trailer_capacity_pallet_qty,
                                       gold__item_kmart_vendor_package_dc_location_data_load::trailer_capacity_weight_qty AS trailer_capacity_weight_qty,
                                       gold__item_kmart_vendor_package_dc_location_data_load::nested_product_ind AS nested_product_ind,
                                       gold__item_kmart_vendor_package_dc_location_data_load::standard_pallet_ind AS standard_pallet_ind,
                                       gold__item_kmart_vendor_package_dc_location_data_load::dc_gross_cost_amt AS dc_gross_cost_amt,
                   gold__item_kmart_vendor_package_dc_location_data_load::vendor_package_dc_alternate_id AS vendor_package_dc_alternate_id,
                                       gold__item_kmart_vendor_package_dc_location_data_load::last_change_user_id AS last_change_user_id;
                                       --gold__item_kmart_vendor_package_dc_location_data_load::record_status AS record_status;

/*******************************************Joining above filtered result with  gold__item_aprk_current***********************************************/

join_kmart_vend_pack_dc_loc_aprk_current = JOIN gold__item_kmart_vendor_package_dc_location_data BY ship_aprk_id,
                        gold__item_aprk_current_data_fltr BY aprk_id;
						

join_kmart_vend_pack_dc_loc_aprk_current_gen = FOREACH join_kmart_vend_pack_dc_loc_aprk_current GENERATE
                                        gold__item_kmart_vendor_package_dc_location_data::vendor_package_id AS vendor_package_id,
                                        gold__item_kmart_vendor_package_dc_location_data::location_nbr AS location_nbr,
                                        gold__item_aprk_current_data_fltr::activity_point_id AS activity_point_id,
                                        gold__item_aprk_current_data_fltr::activity_point_nm AS activity_point_nm,
                                        gold__item_aprk_current_data_fltr::address_role_type_cd AS address_role_type_cd,
                                        gold__item_aprk_current_data_fltr::aprk_type_cd AS ship_aprk_type_cd,
                                        gold__item_kmart_vendor_package_dc_location_data::dc_gross_cost_amt AS dc_gross_cost_amt,
                                        gold__item_aprk_current_data_fltr::duns_orgin_cd AS ship_duns_orgin_cd,
                                        gold__item_aprk_current_data_fltr::duns_owner_cd AS ship_duns_owner_cd,
                                        gold__item_aprk_current_data_fltr::effective_dt AS effective_dt,
                                        gold__item_kmart_vendor_package_dc_location_data::effective_ts AS effective_ts,
                                        gold__item_kmart_vendor_package_dc_location_data::expiration_ts AS expiration_ts,
                                        gold__item_kmart_vendor_package_dc_location_data::first_receipt_dt AS first_receipt_dt,
                                        gold__item_aprk_current_data_fltr::hierarchy_instance_id AS hierarchy_instance_id,
                                        gold__item_aprk_current_data_fltr::import_ind AS ship_duns_import_ind,
                                        gold__item_kmart_vendor_package_dc_location_data::carton_per_layer_qty AS inbound_dc_carton_per_layer_qty,
                                        gold__item_kmart_vendor_package_dc_location_data::layer_per_pallet_qty AS inbound_dc_layer_per_pallet_qty,
                                        gold__item_kmart_vendor_package_dc_location_data::inbound_order_uom_cd AS inbound_order_uom_cd,
                                        gold__item_kmart_vendor_package_dc_location_data::inbound_order_uom_desc AS inbound_order_uom_desc,
                                        gold__item_kmart_vendor_package_dc_location_data::inbound_unit_cost_amt AS inbound_unit_cost_amt,
                                        gold__item_kmart_vendor_package_dc_location_data::ksn_id AS ksn_id,
                                        gold__item_kmart_vendor_package_dc_location_data::last_change_user_id AS last_change_user_id,
                                        gold__item_kmart_vendor_package_dc_location_data::nested_product_ind AS nested_product_ind,
                                        gold__item_kmart_vendor_package_dc_location_data::pallet_exempt_ind AS pallet_exempt_ind,
                                        gold__item_kmart_vendor_package_dc_location_data::pallet_per_trailer_qty AS pallet_per_trailer_qty,
                                        gold__item_kmart_vendor_package_dc_location_data::pallet_surface_balanced_ind AS pallet_surface_balanced_ind,
                                        gold__item_kmart_vendor_package_dc_location_data::pallet_weight_qty AS pallet_weight_qty,
                                        gold__item_kmart_vendor_package_dc_location_data::purchase_status_cd AS purchase_status_cd,
                                        gold__item_kmart_vendor_package_dc_location_data::purchase_status_desc AS purchase_status_desc,
                                        gold__item_kmart_vendor_package_dc_location_data::purchase_status_dt AS purchase_status_dt,
                                        gold__item_kmart_vendor_package_dc_location_data::ship_aprk_id AS ship_aprk_id,
                                        gold__item_kmart_vendor_package_dc_location_data::ship_cube_qty AS ship_cube_qty,
                                        gold__item_aprk_current_data_fltr::duns_nbr AS ship_duns_nbr,
                                        gold__item_kmart_vendor_package_dc_location_data::stack_pallet_cd AS stack_pallet_cd,
                                        gold__item_kmart_vendor_package_dc_location_data::stack_pallet_desc AS stack_pallet_desc,
                                        gold__item_kmart_vendor_package_dc_location_data::standard_pallet_ind AS standard_pallet_ind,
                                        gold__item_kmart_vendor_package_dc_location_data::trailer_capacity_cube_qty AS trailer_capacity_cube_qty,
                                        gold__item_kmart_vendor_package_dc_location_data::trailer_capacity_desc AS trailer_capacity_desc,
                                        gold__item_kmart_vendor_package_dc_location_data::trailer_capacity_id AS trailer_capacity_id,
                                        gold__item_kmart_vendor_package_dc_location_data::trailer_capacity_pallet_qty AS trailer_capacity_pallet_qty,
                                        gold__item_kmart_vendor_package_dc_location_data::trailer_capacity_weight_qty AS trailer_capacity_weight_qty,
                                        gold__item_kmart_vendor_package_dc_location_data::trailer_units_uom_cd AS trailer_units_uom_cd,
                                        gold__item_kmart_vendor_package_dc_location_data::trailer_units_uom_desc AS trailer_units_uom_desc,
                    gold__item_kmart_vendor_package_dc_location_data::unit_per_trailer_qty AS unit_per_trailer_qty,
                                        gold__item_kmart_vendor_package_dc_location_data::vendor_package_dc_alternate_id AS vendor_package_dc_alternate_id;

/**********************************Joining the above result with smith__idrp_vend_pack_combined table to pull vendor pack and hierarchy data************/

join_smith_idrp_vend_pack_kmart_vend_pack_dc_loc_aprk_current = JOIN join_kmart_vend_pack_dc_loc_aprk_current_gen BY vendor_package_id,
                                                                     smith__idrp_vendor_package_combined_data BY vendor_package_id;
																 

vend_pack_kmart_vend_pack_dc_loc_aprk_gen = FOREACH join_smith_idrp_vend_pack_kmart_vend_pack_dc_loc_aprk_current GENERATE
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::vendor_package_id AS vendor_package_id,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::location_nbr AS location_nbr,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::activity_point_id AS activity_point_id,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::activity_point_nm AS activity_point_nm,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::address_role_type_cd AS address_role_type_cd,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::ship_aprk_type_cd AS ship_aprk_type_cd,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::dc_gross_cost_amt AS dc_gross_cost_amt,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::ship_duns_orgin_cd AS ship_duns_orgin_cd,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::ship_duns_owner_cd AS ship_duns_owner_cd,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::effective_dt AS effective_dt,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::effective_ts AS effective_ts,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::expiration_ts AS expiration_ts,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::first_receipt_dt AS first_receipt_dt,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::hierarchy_instance_id AS hierarchy_instance_id,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::ship_duns_import_ind AS ship_duns_import_ind,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::inbound_dc_carton_per_layer_qty AS inbound_dc_carton_per_layer_qty,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::inbound_dc_layer_per_pallet_qty AS inbound_dc_layer_per_pallet_qty,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::inbound_order_uom_cd AS inbound_order_uom_cd,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::inbound_order_uom_desc AS inbound_order_uom_desc,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::inbound_unit_cost_amt AS inbound_unit_cost_amt,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::ksn_id AS ksn_id,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::last_change_user_id AS last_change_user_id,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::nested_product_ind AS nested_product_ind,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::pallet_exempt_ind AS pallet_exempt_ind,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::pallet_per_trailer_qty AS pallet_per_trailer_qty,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::pallet_surface_balanced_ind AS pallet_surface_balanced_ind,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::pallet_weight_qty AS pallet_weight_qty,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::purchase_status_cd AS purchase_status_cd,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::purchase_status_desc AS purchase_status_desc,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::purchase_status_dt AS purchase_status_dt,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::ship_aprk_id AS ship_aprk_id,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::ship_cube_qty AS ship_cube_qty,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::ship_duns_nbr AS ship_duns_nbr,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::stack_pallet_cd AS stack_pallet_cd,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::stack_pallet_desc AS stack_pallet_desc,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::standard_pallet_ind AS standard_pallet_ind,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::trailer_capacity_cube_qty AS trailer_capacity_cube_qty,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::trailer_capacity_desc AS trailer_capacity_desc,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::trailer_capacity_id AS trailer_capacity_id,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::trailer_capacity_pallet_qty AS trailer_capacity_pallet_qty,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::trailer_capacity_weight_qty AS trailer_capacity_weight_qty,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::trailer_units_uom_cd AS trailer_units_uom_cd,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::trailer_units_uom_desc AS trailer_units_uom_desc,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::unit_per_trailer_qty AS unit_per_trailer_qty,
                                        join_kmart_vend_pack_dc_loc_aprk_current_gen::vendor_package_dc_alternate_id AS vendor_package_dc_alternate_id,
                    smith__idrp_vendor_package_combined_data::shc_business_desc AS shc_business_desc,
                                        smith__idrp_vendor_package_combined_data::shc_business_id AS shc_business_id,
                                        smith__idrp_vendor_package_combined_data::shc_business_level_id AS shc_business_level_id,
                                        smith__idrp_vendor_package_combined_data::shc_business_nbr AS shc_business_nbr,
                                        smith__idrp_vendor_package_combined_data::shc_business_unit_desc AS shc_business_unit_desc,
                                        smith__idrp_vendor_package_combined_data::shc_business_unit_id AS shc_business_unit_id,
                                        smith__idrp_vendor_package_combined_data::shc_business_unit_level_id AS shc_business_unit_level_id,
                                        smith__idrp_vendor_package_combined_data::shc_business_unit_nbr AS shc_business_unit_nbr,
                                        smith__idrp_vendor_package_combined_data::shc_category_desc AS shc_category_desc,
                                        smith__idrp_vendor_package_combined_data::shc_category_group_desc AS shc_category_group_desc,
                                        smith__idrp_vendor_package_combined_data::shc_category_group_id AS shc_category_group_id,
                                        smith__idrp_vendor_package_combined_data::shc_category_group_level_id AS shc_category_group_level_id,
                                        smith__idrp_vendor_package_combined_data::shc_category_group_level_nbr AS shc_category_group_nbr,
                                        smith__idrp_vendor_package_combined_data::shc_category_id AS shc_category_id,
                                        smith__idrp_vendor_package_combined_data::shc_category_level_id AS shc_category_level_id,
                                        smith__idrp_vendor_package_combined_data::shc_category_nbr AS shc_category_nbr,
                                        smith__idrp_vendor_package_combined_data::shc_corporate_desc AS shc_corporate_desc,
                                        smith__idrp_vendor_package_combined_data::shc_corporate_id AS shc_corporate_id,
                                        smith__idrp_vendor_package_combined_data::shc_corporate_level_id AS shc_corporate_level_id,
                                        smith__idrp_vendor_package_combined_data::shc_corporate_nbr AS shc_corporate_nbr,
                                        smith__idrp_vendor_package_combined_data::shc_department_desc AS shc_department_desc,
                                        smith__idrp_vendor_package_combined_data::shc_department_id AS shc_department_id,
                                        smith__idrp_vendor_package_combined_data::shc_department_level_id AS shc_department_level_id,
                                        smith__idrp_vendor_package_combined_data::shc_department_nbr AS shc_department_nbr,
                                        smith__idrp_vendor_package_combined_data::shc_division_desc AS shc_division_desc,
                                        smith__idrp_vendor_package_combined_data::shc_division_id AS shc_division_id,
                                        smith__idrp_vendor_package_combined_data::shc_division_level_id AS shc_division_level_id,
                                        smith__idrp_vendor_package_combined_data::shc_division_nbr AS shc_division_nbr,
                                        smith__idrp_vendor_package_combined_data::flow_type_cd AS flow_type_cd,
                                        smith__idrp_vendor_package_combined_data::flow_type_desc AS flow_type_desc,
                                        smith__idrp_vendor_package_combined_data::gtin_usage_cd AS gtin_usage_cd,
                                        smith__idrp_vendor_package_combined_data::gtin_usage_desc AS gtin_usage_desc,
                                        smith__idrp_vendor_package_combined_data::import_cd AS import_cd,
                                        smith__idrp_vendor_package_combined_data::import_desc AS import_desc,
                                        smith__idrp_vendor_package_combined_data::carton_per_layer_qty AS inbound_carton_per_layer_qty,
                                        smith__idrp_vendor_package_combined_data::layer_per_pallet_qty AS inbound_layer_per_pallet_qty,
                                        smith__idrp_vendor_package_combined_data::shc_item_id AS shc_item_id,
                                        smith__idrp_vendor_package_combined_data::shc_item_type_cd AS shc_item_type_cd,
                                        smith__idrp_vendor_package_combined_data::ksn_package_id AS ksn_package_id,
                                        smith__idrp_vendor_package_combined_data::order_duns_nbr AS order_duns_nbr,
                                        smith__idrp_vendor_package_combined_data::vendor_carton_qty AS vendor_carton_qty,
                                        smith__idrp_vendor_package_combined_data::purchase_status_cd AS vendor_purchase_status_cd,
                                        smith__idrp_vendor_package_combined_data::purchase_status_desc AS vendor_purchase_status_desc,
                                        smith__idrp_vendor_package_combined_data::purchase_status_dt AS vendor_purchase_status_dt;

/*************************************Joining smith_idrp_replenishment_day & gold__item_kmart_ksn_dc_package********************************************/

smith_idrp_replenishment_day_join_gold__item_kmart_ksn_dc_package = CROSS smith__idrp_item_eligibility_batchdate_data,gold__item_kmart_ksn_dc_package_data_load;

cross_join_data = DISTINCT smith_idrp_replenishment_day_join_gold__item_kmart_ksn_dc_package;

/**************************************Applying timestamp filters and generating required filterd data**************************************************/

gold__item_kmart_ksn_dc_package_data_fltr = FILTER cross_join_data BY
        (TRIM(smith__idrp_item_eligibility_batchdate_data::processing_ts) >= TRIM(gold__item_kmart_ksn_dc_package_data_load::effective_ts) AND
        TRIM(smith__idrp_item_eligibility_batchdate_data::processing_ts) <= TRIM(gold__item_kmart_ksn_dc_package_data_load::expiration_ts));
	

gold__item_kmart_ksn_dc_package_data = FOREACH gold__item_kmart_ksn_dc_package_data_fltr GENERATE
            gold__item_kmart_ksn_dc_package_data_load::load_ts AS load_ts,
                                         gold__item_kmart_ksn_dc_package_data_load::ksn_package_id AS ksn_package_id,
                                         gold__item_kmart_ksn_dc_package_data_load::location_nbr AS location_nbr,
                                         gold__item_kmart_ksn_dc_package_data_load::effective_ts AS effective_ts,
                                         gold__item_kmart_ksn_dc_package_data_load::expiration_ts AS expiration_ts,
                                         gold__item_kmart_ksn_dc_package_data_load::handling_cd AS handling_cd,
                                         gold__item_kmart_ksn_dc_package_data_load::handling_desc AS handling_desc,
                                         gold__item_kmart_ksn_dc_package_data_load::full_case_select_ind AS full_case_select_ind,
                                         gold__item_kmart_ksn_dc_package_data_load::stocked_ind AS stocked_ind,
                                         gold__item_kmart_ksn_dc_package_data_load::clean_out_dt AS clean_out_dt,
                                         gold__item_kmart_ksn_dc_package_data_load::ksn_id AS ksn_id,
                                         gold__item_kmart_ksn_dc_package_data_load::purchase_status_cd AS purchase_status_cd,
                                         gold__item_kmart_ksn_dc_package_data_load::purchase_status_desc AS purchase_status_desc,
                                         gold__item_kmart_ksn_dc_package_data_load::outbound_package_qty AS outbound_package_qty,
                                         gold__item_kmart_ksn_dc_package_data_load::vendor_carton_qty AS vendor_carton_qty,
                                         gold__item_kmart_ksn_dc_package_data_load::outbound_package_cube_qty AS outbound_package_cube_qty,
                                         gold__item_kmart_ksn_dc_package_data_load::outbound_package_weight_qty AS outbound_package_weight_qty,
                                         gold__item_kmart_ksn_dc_package_data_load::block_outbound_ind AS block_outbound_ind,
                                         gold__item_kmart_ksn_dc_package_data_load::substitution_ind AS substitution_ind,
                                         gold__item_kmart_ksn_dc_package_data_load::inventory_value_cost_amt AS inventory_value_cost_amt,
                                         gold__item_kmart_ksn_dc_package_data_load::dc_carton_per_layer_qty AS dc_carton_per_layer_qty,
                                         gold__item_kmart_ksn_dc_package_data_load::dc_layer_per_pallet_qty AS dc_layer_per_pallet_qty,
                                         gold__item_kmart_ksn_dc_package_data_load::total_each_qty AS total_each_qty,
                                         gold__item_kmart_ksn_dc_package_data_load::each_per_inner_qty AS each_per_inner_qty,
                                         gold__item_kmart_ksn_dc_package_data_load::purchase_status_dt AS purchase_status_dt,
                                         gold__item_kmart_ksn_dc_package_data_load::alternate_ksn_dc_package_id AS alternate_ksn_dc_package_id,
                                         gold__item_kmart_ksn_dc_package_data_load::last_change_user_id AS last_change_user_id,
                                         gold__item_kmart_ksn_dc_package_data_load::recommended_vendor_package_id AS recommended_vendor_package_id;
                                         --gold__item_kmart_ksn_dc_package_data_load::record_status AS record_status;
/********************Joining above filtered gold__item_kmart_ksn_dc_package data with join_kmart_vend_pack_dc_loc_aprk_current_gen data ***************/


join_kmart_vend_pack_dc_loc_aprk_current_ksn_dc_pkg = JOIN vend_pack_kmart_vend_pack_dc_loc_aprk_gen BY (ksn_package_id,location_nbr), gold__item_kmart_ksn_dc_package_data BY (ksn_package_id,location_nbr);

join_kmart_vend_pack_dc_loc_aprk_current_ksn_dc_pkg_gen = FOREACH join_kmart_vend_pack_dc_loc_aprk_current_ksn_dc_pkg GENERATE
                    vend_pack_kmart_vend_pack_dc_loc_aprk_gen::vendor_package_id AS vendor_package_id,
                                          gold__item_kmart_ksn_dc_package_data::location_nbr AS location_nbr,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::activity_point_id AS activity_point_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::activity_point_nm AS activity_point_nm,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::address_role_type_cd AS address_role_type_cd,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::ship_aprk_type_cd AS ship_aprk_type_cd,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::dc_gross_cost_amt AS dc_gross_cost_amt,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::ship_duns_orgin_cd AS ship_duns_orgin_cd,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::ship_duns_owner_cd AS ship_duns_owner_cd,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::effective_dt AS effective_dt,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::effective_ts AS effective_ts,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::expiration_ts AS expiration_ts,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::first_receipt_dt AS first_receipt_dt,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::hierarchy_instance_id AS hierarchy_instance_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::ship_duns_import_ind AS ship_duns_import_ind,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::inbound_dc_carton_per_layer_qty AS inbound_dc_carton_per_layer_qty,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::inbound_dc_layer_per_pallet_qty AS inbound_dc_layer_per_pallet_qty,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::inbound_order_uom_cd AS inbound_order_uom_cd,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::inbound_order_uom_desc AS inbound_order_uom_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::inbound_unit_cost_amt AS inbound_unit_cost_amt,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::ksn_id AS ksn_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::last_change_user_id AS last_change_user_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::nested_product_ind AS nested_product_ind,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::pallet_exempt_ind AS pallet_exempt_ind,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::pallet_per_trailer_qty AS pallet_per_trailer_qty,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::pallet_surface_balanced_ind AS pallet_surface_balanced_ind,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::pallet_weight_qty AS pallet_weight_qty,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::purchase_status_cd AS purchase_status_cd,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::purchase_status_desc AS purchase_status_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::purchase_status_dt AS purchase_status_dt,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::ship_aprk_id AS ship_aprk_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::ship_cube_qty AS ship_cube_qty,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::ship_duns_nbr AS ship_duns_nbr,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::stack_pallet_cd AS stack_pallet_cd,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::stack_pallet_desc AS stack_pallet_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::standard_pallet_ind AS standard_pallet_ind,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::trailer_capacity_cube_qty AS trailer_capacity_cube_qty,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::trailer_capacity_desc AS trailer_capacity_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::trailer_capacity_id AS trailer_capacity_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::trailer_capacity_pallet_qty AS trailer_capacity_pallet_qty,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::trailer_capacity_weight_qty AS trailer_capacity_weight_qty,
                      vend_pack_kmart_vend_pack_dc_loc_aprk_gen::trailer_units_uom_cd AS trailer_units_uom_cd,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::trailer_units_uom_desc AS trailer_units_uom_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::unit_per_trailer_qty AS unit_per_trailer_qty,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::vendor_package_dc_alternate_id AS vendor_package_dc_alternate_id,
                                          gold__item_kmart_ksn_dc_package_data::block_outbound_ind AS dc_blocked_outbound_ind,
                                          gold__item_kmart_ksn_dc_package_data::handling_cd AS dc_handling_cd,
                                          gold__item_kmart_ksn_dc_package_data::stocked_ind AS dc_stock_ind,
                                          gold__item_kmart_ksn_dc_package_data::full_case_select_ind AS full_case_select_ind,
                                          gold__item_kmart_ksn_dc_package_data::purchase_status_cd AS ksn_pack_purchase_status_cd,
                                          gold__item_kmart_ksn_dc_package_data::purchase_status_dt AS ksn_pack_purchase_status_dt,
                                          gold__item_kmart_ksn_dc_package_data::dc_carton_per_layer_qty AS outbound_carton_per_layer_qty,
                                          gold__item_kmart_ksn_dc_package_data::dc_layer_per_pallet_qty AS outbound_layer_per_pallet_qty,
                                          gold__item_kmart_ksn_dc_package_data::outbound_package_cube_qty AS outbound_pack_cube_qty,
										  gold__item_kmart_ksn_dc_package_data::outbound_package_qty AS outbound_package_qty,
                                          gold__item_kmart_ksn_dc_package_data::outbound_package_weight_qty AS outbound_pack_weight_qty,
                                          gold__item_kmart_ksn_dc_package_data::substitution_ind AS substition_eligibile_ind,
                      vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_business_desc AS shc_business_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_business_id AS shc_business_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_business_level_id AS shc_business_level_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_business_nbr AS shc_business_nbr,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_business_unit_desc AS shc_business_unit_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_business_unit_id AS shc_business_unit_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_business_unit_level_id AS shc_business_unit_level_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_business_unit_nbr AS shc_business_unit_nbr,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_category_desc AS shc_category_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_category_group_desc AS shc_category_group_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_category_group_id AS shc_category_group_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_category_group_level_id AS shc_category_group_level_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_category_group_nbr AS shc_category_group_nbr,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_category_id AS shc_category_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_category_level_id AS shc_category_level_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_category_nbr AS shc_category_nbr,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_corporate_desc AS shc_corporate_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_corporate_id AS shc_corporate_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_corporate_level_id AS shc_corporate_level_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_corporate_nbr AS shc_corporate_nbr,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_department_desc AS shc_department_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_department_id AS shc_department_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_department_level_id AS shc_department_level_id,
                                     vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_department_nbr AS shc_department_nbr,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_division_desc AS shc_division_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_division_id AS shc_division_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_division_level_id AS shc_division_level_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_division_nbr AS shc_division_nbr,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::flow_type_cd AS flow_type_cd,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::flow_type_desc AS flow_type_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::gtin_usage_cd AS gtin_usage_cd,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::gtin_usage_desc AS gtin_usage_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::import_cd AS import_cd,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::import_desc AS import_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::inbound_carton_per_layer_qty AS inbound_carton_per_layer_qty,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::inbound_layer_per_pallet_qty AS inbound_layer_per_pallet_qty,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_item_id AS shc_item_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::shc_item_type_cd AS shc_item_type_cd,
                                          gold__item_kmart_ksn_dc_package_data::ksn_package_id AS ksn_package_id,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::order_duns_nbr AS order_duns_nbr,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::vendor_carton_qty AS vendor_carton_qty,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::vendor_purchase_status_cd AS vendor_purchase_status_cd,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::vendor_purchase_status_desc AS vendor_purchase_status_desc,
                                          vend_pack_kmart_vend_pack_dc_loc_aprk_gen::vendor_purchase_status_dt AS vendor_purchase_status_dt;

/***********************************logic for extracting the maximum effective record corresponding to ship_duns_nbr,location_nbr columns**************/

gold__item_vendor_ship_point_dc_location_current_data_fetch_frst = GROUP gold__item_vendor_ship_point_dc_location_current_data BY
                                    (ship_duns_nbr,location_nbr);

gold__item_vendor_ship_point_dc_location_current_data_max_eff_date = FOREACH gold__item_vendor_ship_point_dc_location_current_data_fetch_frst{
                                                            data_order = ORDER gold__item_vendor_ship_point_dc_location_current_data BY effective_dt DESC;
                                                            data_limit = LIMIT data_order 1;
                                                            GENERATE FLATTEN(data_limit);
                                                                                };

/***********************************Joining the above result with join_gold_data_smith_vend_pack_combined_gen processed data**************************/


join_gold_smith_data_gold_vendor_shp_point_dc_loc = JOIN join_kmart_vend_pack_dc_loc_aprk_current_ksn_dc_pkg_gen
                                                       BY (ship_duns_nbr,location_nbr) LEFT OUTER,
                                gold__item_vendor_ship_point_dc_location_current_data_max_eff_date BY (ship_duns_nbr,location_nbr);

smith_idrp_vend_pack_dc_combined = FOREACH join_gold_smith_data_gold_vendor_shp_point_dc_loc GENERATE
                '$CURRENT_TIMESTAMP' AS load_ts,
                vendor_package_id,
                join_kmart_vend_pack_dc_loc_aprk_current_ksn_dc_pkg_gen::location_nbr AS location_nbr,
                activity_point_id,
                activity_point_nm,
                address_role_type_cd,
                ship_aprk_type_cd,
                shc_business_desc,
                shc_business_id,
                shc_business_level_id,
                shc_business_nbr,
                shc_business_unit_desc,
                shc_business_unit_id,
                shc_business_unit_level_id,
                shc_business_unit_nbr,
                shc_category_desc,
                shc_category_group_desc,
                shc_category_group_id,
                shc_category_group_level_id,
                shc_category_group_nbr,
                shc_category_id,
                shc_category_level_id,
                shc_category_nbr,
                shc_corporate_desc,
                shc_corporate_id,
                shc_corporate_level_id,
                shc_corporate_nbr,
                dc_blocked_outbound_ind,
                dc_gross_cost_amt,
                dc_handling_cd,
                dc_stock_ind,
                shc_department_desc,
                shc_department_id,
                shc_department_level_id,
                shc_department_nbr,
                shc_division_desc,
                shc_division_id,
                shc_division_level_id,
                shc_division_nbr,
                ship_duns_orgin_cd,
                ship_duns_owner_cd,
                join_kmart_vend_pack_dc_loc_aprk_current_ksn_dc_pkg_gen::effective_dt AS aprk_effective_dt,
        effective_ts,
                expiration_ts,
                first_receipt_dt,
                flow_type_cd,
                flow_type_desc,
                full_case_select_ind,
                gtin_usage_cd,
                gtin_usage_desc,
        gold__item_vendor_ship_point_dc_location_current_data_max_eff_date::data_limit::effective_dt AS ship_duns_location_effective_dt,
                hierarchy_instance_id,
                import_cd,
                import_desc,
                ship_duns_import_ind,
                inbound_carton_per_layer_qty,
                inbound_dc_carton_per_layer_qty,
                inbound_dc_layer_per_pallet_qty,
                inbound_layer_per_pallet_qty,
                inbound_order_uom_cd,
                inbound_order_uom_desc,
                inbound_unit_cost_amt,
                shc_item_id,
                shc_item_type_cd,
                ksn_id,
                ksn_pack_purchase_status_cd,
                ksn_pack_purchase_status_dt,
                ksn_package_id,
                last_change_user_id,
                gold__item_vendor_ship_point_dc_location_current_data_max_eff_date::data_limit::non_jit_lead_time_qty AS vendor_lead_time_qty,
                gold__item_vendor_ship_point_dc_location_current_data_max_eff_date::data_limit::minimum_order_qty AS minimum_order_qty,
                gold__item_vendor_ship_point_dc_location_current_data_max_eff_date::data_limit::minimum_order_uom_cd AS minimum_order_uom_cd,
                nested_product_ind,
                order_duns_nbr,
                outbound_carton_per_layer_qty,
                outbound_layer_per_pallet_qty,
                outbound_pack_cube_qty,
                outbound_pack_weight_qty,
                pallet_exempt_ind,
                pallet_per_trailer_qty,
                pallet_surface_balanced_ind,
                pallet_weight_qty,
                purchase_status_cd,
                purchase_status_desc,
                purchase_status_dt,
                ship_aprk_id,
                ship_cube_qty,
                gold__item_vendor_ship_point_dc_location_current_data_max_eff_date::data_limit::unauthorized_ind AS ship_duns_unauthorized_ind,
                join_kmart_vend_pack_dc_loc_aprk_current_ksn_dc_pkg_gen::ship_duns_nbr AS ship_duns_nbr,
                stack_pallet_cd,
                stack_pallet_desc,
                standard_pallet_ind,
                substition_eligibile_ind,
                trailer_capacity_cube_qty,
                trailer_capacity_desc,
                trailer_capacity_id,
                trailer_capacity_pallet_qty,
                trailer_capacity_weight_qty,
                trailer_units_uom_cd,
                trailer_units_uom_desc,
                unit_per_trailer_qty,
                vendor_carton_qty,
                gold__item_vendor_ship_point_dc_location_current_data_max_eff_date::data_limit::vendor_managed_inventory_cd AS vendor_managed_inventory_cd,
                vendor_package_dc_alternate_id,
                vendor_purchase_status_cd,
                vendor_purchase_status_desc,
                vendor_purchase_status_dt,
				outbound_package_qty,
				'$batchid';
/******************************************Storing the processed data for smith__idrp_vend_pack_dc_combined table****************************************/
smith_idrp_vend_pack_dc_combined_dist = DISTINCT smith_idrp_vend_pack_dc_combined;

STORE smith_idrp_vend_pack_dc_combined_dist INTO '$SMITH__IDRP_VEND_PACK_DC_COMBINED_WORK_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
