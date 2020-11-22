/*
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_mips1_smith__idrp_shc_item_combined.pig
# AUTHOR NAME:         Nachiket Paluskar
# CREATION DATE:       03-09-2013 03:04
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
#		 13-06-2014	  Mayank Agarwal	Changes for IE master data(CR 1977)
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
#<>                                  BODY                                   <>#
###############################################################################
*/
/*
###############################################################################
#<<                          START CUSTOM BODY CODE                         >>#
###############################################################################

/* Load data from smith__idrp_ie_item_combined_hierarchy_all_current table */

--CR4468
work__idrp_item_hierarchy_combined_all_current_data = LOAD '$WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS
                            ($WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_SCHEMA);
/* Load data from gold__item_package_current table */

gold__item_package_current_data = LOAD '$GOLD__ITEM_PACKAGE_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS
                            ($GOLD__ITEM_PACKAGE_CURRENT_SCHEMA);

/* joining KSN level smith__idrp_ie_item_combined_hierarchy_all_current and package level gold__item_package_current*/

smith__item_combined_hierarchy_gold__item_package_join = JOIN work__idrp_item_hierarchy_combined_all_current_data BY (long)referred_package_id,
                                  gold__item_package_current_data BY (long)package_id using 'skewed';

smith__item_combined_hierarchy_gold__item_package_join = DISTINCT smith__item_combined_hierarchy_gold__item_package_join;

smith_hrc_gold_pckg_ksn_join = FILTER smith__item_combined_hierarchy_gold__item_package_join BY
                            (long)work__idrp_item_hierarchy_combined_all_current_data::ksn_id == (long)gold__item_package_current_data::ksn_id;
                            
smith_hrc_gold_pckg_ksn_generate = FOREACH smith_hrc_gold_pckg_ksn_join generate                             
'$CURRENT_TIMESTAMP' as load_ts,
work__idrp_item_hierarchy_combined_all_current_data::ksn_id	as ksn_id,
work__idrp_item_hierarchy_combined_all_current_data::ksn_id_effective_ts	as	ksn_id_effective_ts,
work__idrp_item_hierarchy_combined_all_current_data::ksn_id_expiration_ts	as	ksn_id_expiration_ts,
work__idrp_item_hierarchy_combined_all_current_data::ksn_desc	as	ksn_desc,
work__idrp_item_hierarchy_combined_all_current_data::shc_item_id	as	shc_item_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_item_desc	as	shc_item_desc,
work__idrp_item_hierarchy_combined_all_current_data::shc_sub_category_nbr	as	shc_sub_category_nbr,
work__idrp_item_hierarchy_combined_all_current_data::shc_sub_category_desc	as	shc_sub_category_desc,
work__idrp_item_hierarchy_combined_all_current_data::shc_category_nbr	as	shc_category_nbr,
work__idrp_item_hierarchy_combined_all_current_data::shc_category_desc	as	shc_category_desc,
work__idrp_item_hierarchy_combined_all_current_data::shc_category_group_level_nbr	as	shc_category_group_level_nbr,
work__idrp_item_hierarchy_combined_all_current_data::shc_category_group_desc	as	shc_category_group_desc,
work__idrp_item_hierarchy_combined_all_current_data::shc_department_nbr	as	shc_department_nbr,
work__idrp_item_hierarchy_combined_all_current_data::shc_department_desc	as	shc_department_desc,
work__idrp_item_hierarchy_combined_all_current_data::shc_division_nbr	as	shc_division_nbr,
work__idrp_item_hierarchy_combined_all_current_data::shc_division_desc	as	shc_division_desc,
work__idrp_item_hierarchy_combined_all_current_data::shc_business_unit_nbr	as	shc_business_unit_nbr,
work__idrp_item_hierarchy_combined_all_current_data::shc_business_unit_desc	as	shc_business_unit_desc,
work__idrp_item_hierarchy_combined_all_current_data::shc_business_nbr	as	shc_business_nbr,
work__idrp_item_hierarchy_combined_all_current_data::shc_business_desc	as	shc_business_desc,
work__idrp_item_hierarchy_combined_all_current_data::shc_corporate_nbr	as	shc_corporate_nbr,
work__idrp_item_hierarchy_combined_all_current_data::shc_corporate_desc	as	shc_corporate_desc,
work__idrp_item_hierarchy_combined_all_current_data::sears_item_nbr	as	sears_item_nbr,
work__idrp_item_hierarchy_combined_all_current_data::sears_sku_nbr	as	sears_sku_nbr,
work__idrp_item_hierarchy_combined_all_current_data::sears_sku_desc	as	sears_sku_desc,
work__idrp_item_hierarchy_combined_all_current_data::sears_class_nbr	as	sears_class_nbr,
work__idrp_item_hierarchy_combined_all_current_data::sears_class_desc	as	sears_class_desc,
work__idrp_item_hierarchy_combined_all_current_data::sears_sub_line_nbr	as	sears_sub_line_nbr,
work__idrp_item_hierarchy_combined_all_current_data::sears_sub_line_desc	as	sears_sub_line_desc,
work__idrp_item_hierarchy_combined_all_current_data::sears_line_nbr	as	sears_line_nbr,
work__idrp_item_hierarchy_combined_all_current_data::sears_line_desc	as	sears_line_desc,
work__idrp_item_hierarchy_combined_all_current_data::sears_division_nbr	as	sears_division_nbr,
work__idrp_item_hierarchy_combined_all_current_data::sears_division_desc	as	sears_division_desc,
work__idrp_item_hierarchy_combined_all_current_data::sears_business_nbr	as	sears_business_nbr,
work__idrp_item_hierarchy_combined_all_current_data::sears_business_desc	as	sears_business_desc,
work__idrp_item_hierarchy_combined_all_current_data::sears_category_nbr	as	sears_category_nbr,
work__idrp_item_hierarchy_combined_all_current_data::sears_category_desc	as	sears_category_desc,
work__idrp_item_hierarchy_combined_all_current_data::sears_group_nbr	as	sears_group_nbr,
work__idrp_item_hierarchy_combined_all_current_data::sears_group_desc	as	sears_group_desc,
work__idrp_item_hierarchy_combined_all_current_data::sears_hierarchy_exception_ind	as	sears_hierarchy_exception_ind,
work__idrp_item_hierarchy_combined_all_current_data::ksn_alternate_id	as	ksn_alternate_id,
work__idrp_item_hierarchy_combined_all_current_data::ksn_register_desc	as	ksn_register_desc,
work__idrp_item_hierarchy_combined_all_current_data::shc_item_type_cd	as	shc_item_type_cd,
work__idrp_item_hierarchy_combined_all_current_data::shc_item_alternate_id	as	shc_item_alternate_id,
work__idrp_item_hierarchy_combined_all_current_data::lands_end_sku_nbr	as	lands_end_sku_nbr,
work__idrp_item_hierarchy_combined_all_current_data::shc_item_id_effective_ts	as	shc_item_id_effective_ts,
work__idrp_item_hierarchy_combined_all_current_data::shc_item_id_expiration_ts	as	shc_item_id_expiration_ts,
work__idrp_item_hierarchy_combined_all_current_data::shc_sub_category_level_id	as	shc_sub_category_level_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_sub_category_id	as	shc_sub_category_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_category_level_id	as	shc_category_level_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_category_id	as	shc_category_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_category_group_level_id	as	shc_category_group_level_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_category_group_id	as	shc_category_group_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_department_level_id	as	shc_department_level_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_department_id	as	shc_department_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_division_level_id	as	shc_division_level_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_division_id	as	shc_division_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_business_unit_level_id	as	shc_business_unit_level_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_business_unit_id	as	shc_business_unit_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_business_level_id	as	shc_business_level_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_business_id	as	shc_business_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_corporate_level_id	as	shc_corporate_level_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_corporate_id	as	shc_corporate_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_hierarchy_item_effective_dt	as	shc_hierarchy_item_effective_dt,
work__idrp_item_hierarchy_combined_all_current_data::shc_hierarchy_item_expiration_dt	as	shc_hierarchy_item_expiration_dt,
work__idrp_item_hierarchy_combined_all_current_data::shc_item_hier_last_update_ts	as	shc_item_hier_last_update_ts,
work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_division_id	as	shc_reporting_division_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_division_nbr	as	shc_reporting_division_nbr,
work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_department_id	as	shc_reporting_department_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_department_nbr	as	shc_reporting_department_nbr,
work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_category_id	as	shc_reporting_category_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_category_group_nbr	as	shc_reporting_category_group_nbr,
work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_category_nbr	as	shc_reporting_category_nbr,
work__idrp_item_hierarchy_combined_all_current_data::shc_item_userid_plus_tx	as	shc_item_userid_plus_tx,
work__idrp_item_hierarchy_combined_all_current_data::maximum_order_per_store_qty	as	maximum_order_per_store_qty,
work__idrp_item_hierarchy_combined_all_current_data::dc_bulk_ad_lead_time_nbr	as	dc_bulk_ad_lead_time_nbr,
work__idrp_item_hierarchy_combined_all_current_data::season_cd	as	season_cd,
work__idrp_item_hierarchy_combined_all_current_data::season_year_nbr	as	season_year_nbr,
work__idrp_item_hierarchy_combined_all_current_data::ksn_purchase_status_cd	as	ksn_purchase_status_cd,
work__idrp_item_hierarchy_combined_all_current_data::ksn_check_digit_nbr	as	ksn_check_digit_nbr,
work__idrp_item_hierarchy_combined_all_current_data::sub_season_id	as	sub_season_id,
work__idrp_item_hierarchy_combined_all_current_data::purchase_status_dt	as	purchase_status_dt,
work__idrp_item_hierarchy_combined_all_current_data::shelf_edge_marking_desc	as	shelf_edge_marking_desc,
work__idrp_item_hierarchy_combined_all_current_data::ksn_last_change_user_id	as	ksn_last_change_user_id,
work__idrp_item_hierarchy_combined_all_current_data::ksn_corp_owner_cd	as	ksn_corp_owner_cd,
work__idrp_item_hierarchy_combined_all_current_data::gift_registry_cd	as	gift_registry_cd,
work__idrp_item_hierarchy_combined_all_current_data::dotcom_eligibility_cd	as	dotcom_eligibility_cd,
work__idrp_item_hierarchy_combined_all_current_data::similar_ksn_id	as	similar_ksn_id,
work__idrp_item_hierarchy_combined_all_current_data::similar_ksn_pct	as	similar_ksn_pct,
work__idrp_item_hierarchy_combined_all_current_data::dotcom_allocation_ind	as	dotcom_allocation_ind,
work__idrp_item_hierarchy_combined_all_current_data::grocery_item_ind	as	grocery_item_ind,
work__idrp_item_hierarchy_combined_all_current_data::account_nbr	as	account_nbr,
work__idrp_item_hierarchy_combined_all_current_data::minimum_store_shelf_life_qty	as	minimum_store_shelf_life_qty,
work__idrp_item_hierarchy_combined_all_current_data::minimum_dc_shelf_life_qty	as	minimum_dc_shelf_life_qty,
work__idrp_item_hierarchy_combined_all_current_data::default_selling_amt	as	default_selling_amt,
work__idrp_item_hierarchy_combined_all_current_data::default_selling_amt_multiplier_qty	as	default_selling_amt_multiplier_qty,
work__idrp_item_hierarchy_combined_all_current_data::reorder_authentication_cd	as	reorder_authentication_cd,
work__idrp_item_hierarchy_combined_all_current_data::network_distribution_cd	as	network_distribution_cd,
work__idrp_item_hierarchy_combined_all_current_data::eas_tag_required_id	as	eas_tag_required_id,
work__idrp_item_hierarchy_combined_all_current_data::dc_security_cd	as	dc_security_cd,
work__idrp_item_hierarchy_combined_all_current_data::store_security_cd	as	store_security_cd,
work__idrp_item_hierarchy_combined_all_current_data::store_forecast_cd	as	store_forecast_cd,
work__idrp_item_hierarchy_combined_all_current_data::include_event_ind	as	include_event_ind,
work__idrp_item_hierarchy_combined_all_current_data::referred_package_id	as	referred_package_id,
work__idrp_item_hierarchy_combined_all_current_data::checkout_merchandise_ind	as	checkout_merchandise_ind,
work__idrp_item_hierarchy_combined_all_current_data::purchase_status_cd	as	purchase_status_cd,
work__idrp_item_hierarchy_combined_all_current_data::can_carry_model_id	as	can_carry_model_id,
work__idrp_item_hierarchy_combined_all_current_data::dc_forecast_cd	as	dc_forecast_cd,
work__idrp_item_hierarchy_combined_all_current_data::external_carton_type_cd	as	external_carton_type_cd,
work__idrp_item_hierarchy_combined_all_current_data::shc_item_check_digit_nbr	as	shc_item_check_digit_nbr,
work__idrp_item_hierarchy_combined_all_current_data::purchase_status_cd_dt	as	purchase_status_cd_dt,
work__idrp_item_hierarchy_combined_all_current_data::delivered_direct_ind	as	delivered_direct_ind,
work__idrp_item_hierarchy_combined_all_current_data::special_retail_order_system_ind	as	special_retail_order_system_ind,
work__idrp_item_hierarchy_combined_all_current_data::shc_item_last_change_user_id	as	shc_item_last_change_user_id,
work__idrp_item_hierarchy_combined_all_current_data::shc_item_corporate_owner_cd	as	shc_item_corporate_owner_cd,
work__idrp_item_hierarchy_combined_all_current_data::installation_ind	as	installation_ind,
work__idrp_item_hierarchy_combined_all_current_data::protection_agreement_ind	as	protection_agreement_ind,
work__idrp_item_hierarchy_combined_all_current_data::replacement_agreement_ind	as	replacement_agreement_ind,
work__idrp_item_hierarchy_combined_all_current_data::stain_agreement_ind	as	stain_agreement_ind,
work__idrp_item_hierarchy_combined_all_current_data::bottle_deposit_ind	as	bottle_deposit_ind,
work__idrp_item_hierarchy_combined_all_current_data::internet_tax_cd	as	internet_tax_cd,
work__idrp_item_hierarchy_combined_all_current_data::markdown_style_reference_cd	as	markdown_style_reference_cd,
work__idrp_item_hierarchy_combined_all_current_data::service_ind	as	service_ind,
work__idrp_item_hierarchy_combined_all_current_data::future_network_distribution_cd	as	future_network_distribution_cd,
work__idrp_item_hierarchy_combined_all_current_data::future_network_distribution_effective_dt	as	future_network_distribution_effective_dt,
work__idrp_item_hierarchy_combined_all_current_data::jit_network_distribution_cd	as	jit_network_distribution_cd,
work__idrp_item_hierarchy_combined_all_current_data::pdm_nbr	as	pdm_nbr,
work__idrp_item_hierarchy_combined_all_current_data::iplan_id	as	iplan_id,
work__idrp_item_hierarchy_combined_all_current_data::worksheet_url_nm	as	worksheet_url_nm,
work__idrp_item_hierarchy_combined_all_current_data::serial_nbr_required_ind	as	serial_nbr_required_ind,
work__idrp_item_hierarchy_combined_all_current_data::customer_direct_location_cd	as	customer_direct_location_cd,
work__idrp_item_hierarchy_combined_all_current_data::sears_item_size_ratio_cd	as	sears_item_size_ratio_cd,
work__idrp_item_hierarchy_combined_all_current_data::sears_item_size_desc	as	sears_item_size_desc,
work__idrp_item_hierarchy_combined_all_current_data::sears_item_size_sub_desc	as	sears_item_size_sub_desc,
work__idrp_item_hierarchy_combined_all_current_data::sears_item_color_cd	as	sears_item_color_cd,
work__idrp_item_hierarchy_combined_all_current_data::sears_item_color_desc	as	sears_item_color_desc,
work__idrp_item_hierarchy_combined_all_current_data::sears_item_last_change_user_id	as	sears_item_last_change_user_id,
work__idrp_item_hierarchy_combined_all_current_data::core_conversion_dt	as	core_conversion_dt,
work__idrp_item_hierarchy_combined_all_current_data::core_bridge_alternate_item_id	as	core_bridge_alternate_item_id,
work__idrp_item_hierarchy_combined_all_current_data::smart_plan_ind	as	smart_plan_ind,
work__idrp_item_hierarchy_combined_all_current_data::idrp_order_method_cd	as	idrp_order_method_cd,
work__idrp_item_hierarchy_combined_all_current_data::idrp_order_method_desc	as	idrp_order_method_desc,
work__idrp_item_hierarchy_combined_all_current_data::cma_color_style_id	as	cma_color_style_id,
work__idrp_item_hierarchy_combined_all_current_data::cma_color_style_desc	as	cma_color_style_desc,
work__idrp_item_hierarchy_combined_all_current_data::sears_network_distribution_cd	as	sears_network_distribution_cd,
work__idrp_item_hierarchy_combined_all_current_data::sears_emp_network_distribution_cd	as	sears_emp_network_distribution_cd,
work__idrp_item_hierarchy_combined_all_current_data::sears_future_network_distribution_cd	as	sears_future_network_distribution_cd,
work__idrp_item_hierarchy_combined_all_current_data::sears_future_network_distribution_effective_dt	as	sears_future_network_distribution_effective_dt,
work__idrp_item_hierarchy_combined_all_current_data::forecast_group_format_id	as	forecast_group_format_id,
work__idrp_item_hierarchy_combined_all_current_data::forecast_group_desc	as	forecast_group_desc,
'$batchid' AS batch_id;	

/* Load data from gold__item_core_bridge_item table */

work__item_core_bridge_item_data = LOAD '$WORK__IDRP_KSN_CORE_BRIDGE_ITEM_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS
                            ($WORK__IDRP_KSN_CORE_BRIDGE_ITEM_SCHEMA);

/* doing a left outer join and getting order_system_cd*/

smith__item_combined_hierarchy_gold__item_package_work_idrp_ksn_core_bridge_item_join = JOIN smith__item_combined_hierarchy_gold__item_package_join BY
																							work__idrp_item_hierarchy_combined_all_current_data::ksn_id LEFT OUTER, work__item_core_bridge_item_data BY ksn_id using 'skewed';

smith__item_combined_hierarchy_gold__item_package_work_idrp_ksn_core_bridge_item_join = DISTINCT smith__item_combined_hierarchy_gold__item_package_work_idrp_ksn_core_bridge_item_join;

/* doing a group on item_id and then finding max of order_system_cd , this will tell us any KSN has an order_system_cd of "SAMS"*/

smith_tim_cmbd_grpd = GROUP smith__item_combined_hierarchy_gold__item_package_work_idrp_ksn_core_bridge_item_join BY smith__item_combined_hierarchy_gold__item_package_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_id;

smith_tim_cmbd_max = FOREACH smith_tim_cmbd_grpd
                {
            smith_tim_cmbd_grpd_ord1 = ORDER smith__item_combined_hierarchy_gold__item_package_work_idrp_ksn_core_bridge_item_join BY
                            work__item_core_bridge_item_data::item_order_system_cd DESC;
            smith_tim_cmbd_grpd_ord = LIMIT smith_tim_cmbd_grpd_ord1 1;
            GENERATE FLATTEN(smith_tim_cmbd_grpd_ord);
                };

/* joining the results on item_id this will give us item level table with KSN data for referred KSN and sears_order_system_cd*/

smith__idrp_shc_item_combined = JOIN smith_hrc_gold_pckg_ksn_join BY (long)work__idrp_item_hierarchy_combined_all_current_data::shc_item_id LEFT OUTER,
                    smith_tim_cmbd_max BY (long)smith_tim_cmbd_grpd_ord::smith__item_combined_hierarchy_gold__item_package_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_id using 'skewed';

/* Generating the required columns for smith__idrp_shc_item_combined table*/

smith__idrp_shc_item_combined_final = FOREACH smith__idrp_shc_item_combined GENERATE
'$CURRENT_TIMESTAMP' AS load_ts,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_id AS shc_item_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::account_nbr AS account_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::bottle_deposit_ind AS bottle_deposit_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::can_carry_model_id AS can_carry_model_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::checkout_merchandise_ind AS checkout_merchandise_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::customer_direct_location_cd AS customer_direct_location_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::dc_forecast_cd AS dc_forecast_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::dc_security_cd AS dc_security_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::default_selling_amt AS default_selling_amt,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::default_selling_amt_multiplier_qty AS default_selling_amt_multiplier_qty,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::delivered_direct_ind AS delivered_direct_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::eas_tag_required_id AS eas_tag_required_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::external_carton_type_cd AS external_carton_type_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::future_network_distribution_cd AS future_network_distribution_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::future_network_distribution_effective_dt AS future_network_distribution_effective_dt,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::grocery_item_ind AS grocery_item_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_hierarchy_item_effective_dt AS shc_hierarchy_item_effective_dt,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_hierarchy_item_expiration_dt AS shc_hierarchy_item_expiration_dt,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::include_event_ind AS include_event_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::installation_ind AS installation_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::internet_tax_cd AS internet_tax_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::iplan_id AS iplan_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_alternate_id AS shc_item_alternate_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::jit_network_distribution_cd AS jit_network_distribution_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::markdown_style_reference_cd AS markdown_style_reference_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::minimum_dc_shelf_life_qty AS minimum_dc_shelf_life_qty,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::minimum_store_shelf_life_qty AS minimum_store_shelf_life_qty,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::network_distribution_cd AS network_distribution_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::pdm_nbr AS pdm_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::protection_agreement_ind AS protection_agreement_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::purchase_status_cd AS purchase_status_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::purchase_status_dt AS purchase_status_dt,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::gift_registry_cd AS referred_gift_registry_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::ksn_check_digit_nbr AS referred_ksn_check_digit_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::ksn_corp_owner_cd AS referred_ksn_corp_owner_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::ksn_desc AS referred_ksn_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::dotcom_allocation_ind AS referred_ksn_dotcom_allocation_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::dotcom_eligibility_cd AS referred_ksn_dotcom_eligibility_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::ksn_id AS referred_ksn_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::ksn_id_effective_ts AS reffered_ksn_id_effective_ts,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::ksn_id_expiration_ts AS referred_ksn_id_expiration_ts,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::lands_end_sku_nbr AS referred_ksn_lands_end_sku_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::ksn_last_change_user_id AS referred_ksn_last_change_user_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::ksn_purchase_status_cd AS referred_ksn_purchase_status_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::purchase_status_cd_dt AS referred_ksn_purchase_status_cd_dt,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::ksn_register_desc AS referred_ksn_register_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::season_cd AS referred_ksn_season_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::season_year_nbr AS referred_season_year_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shelf_edge_marking_desc AS referred_shelf_edge_marking_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sub_season_id AS referred_sub_season_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::referred_package_id AS referred_package_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::reorder_authentication_cd AS reorder_authentication_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::replacement_agreement_ind AS replacement_agreement_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_business_desc AS sears_business_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_business_nbr AS sears_business_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_category_desc AS sears_category_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_category_nbr AS sears_category_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_class_desc AS sears_class_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_class_nbr AS sears_class_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_division_desc AS sears_division_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_division_nbr AS sears_division_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_group_desc AS sears_group_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_group_nbr AS sears_group_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_hierarchy_exception_ind AS sears_hierarchy_exception_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_item_color_cd AS sears_item_color_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_item_color_desc AS sears_item_color_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_item_last_change_user_id AS sears_item_last_change_user_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_item_nbr AS sears_item_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_item_size_desc AS sears_item_size_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_item_size_ratio_cd AS sears_item_size_ratio_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_item_size_sub_desc AS sears_item_size_sub_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_line_desc AS sears_line_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_line_nbr AS sears_line_nbr,
(smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_corporate_owner_cd == 'K' OR smith_tim_cmbd_max::smith_tim_cmbd_grpd_ord::work__item_core_bridge_item_data::item_order_system_cd IS NULL ? '' : smith_tim_cmbd_max::smith_tim_cmbd_grpd_ord::work__item_core_bridge_item_data::item_order_system_cd) AS item_order_system_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_sku_desc AS sears_sku_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_sku_nbr AS sears_sku_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_sub_line_desc AS sears_sub_line_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_sub_line_nbr AS sears_sub_line_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::serial_nbr_required_ind AS serial_nbr_required_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::service_ind AS service_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_business_desc AS shc_business_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_business_id AS shc_business_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_business_level_id AS shc_business_level_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_business_nbr AS shc_business_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_business_unit_desc AS shc_business_unit_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_business_unit_id AS shc_business_unit_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_business_unit_level_id AS shc_business_unit_level_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_business_unit_nbr AS shc_business_unit_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_category_desc AS shc_category_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_category_group_desc AS shc_category_group_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_category_group_id AS shc_category_group_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_category_group_level_id AS shc_category_group_level_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_category_group_level_nbr AS shc_category_group_level_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_category_id AS shc_category_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_category_level_id AS shc_category_level_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_category_nbr AS shc_category_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_corporate_desc AS shc_corporate_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_corporate_id AS shc_corporate_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_corporate_level_id AS shc_corporate_level_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_corporate_nbr AS shc_corporate_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_department_desc AS shc_department_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_department_id AS shc_department_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_department_level_id AS shc_department_level_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_department_nbr AS shc_department_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_division_desc AS shc_division_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_division_id AS shc_division_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_division_level_id AS shc_division_level_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_division_nbr AS shc_division_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_check_digit_nbr AS shc_item_check_digit_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd ,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_desc AS shc_item_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_hier_last_update_ts AS shc_item_hier_last_update_ts,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_id_effective_ts AS shc_item_id_effective_ts,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_id_expiration_ts AS shc_item_id_expiration_ts,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_last_change_user_id AS shc_item_last_change_user_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_type_cd AS shc_item_type_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_item_userid_plus_tx AS shc_item_userid_plus_tx,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_category_group_nbr AS shc_reporting_category_group_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_category_id AS shc_reporting_category_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_category_nbr AS shc_reporting_category_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_department_id AS shc_reporting_department_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_department_nbr AS shc_reporting_department_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_division_id AS shc_reporting_division_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_reporting_division_nbr AS shc_reporting_division_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_sub_category_desc AS shc_sub_category_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_sub_category_id AS shc_sub_category_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_sub_category_level_id AS shc_sub_category_level_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::shc_sub_category_nbr AS shc_sub_category_nbr,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::special_retail_order_system_ind AS special_retail_order_system_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::stain_agreement_ind AS stain_agreement_ind,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::store_forecast_cd AS store_forecast_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::store_security_cd AS store_security_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::worksheet_url_nm AS worksheet_url_nm,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::idrp_order_method_cd AS idrp_order_method_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::idrp_order_method_desc AS idrp_order_method_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::forecast_group_format_id AS forecast_group_format_id,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::forecast_group_desc AS forecast_group_desc,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_network_distribution_cd AS sears_network_distribution_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_future_network_distribution_cd AS sears_future_network_distribution_cd,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_future_network_distribution_effective_dt AS sears_future_network_distribution_effective_dt,
smith_hrc_gold_pckg_ksn_join::work__idrp_item_hierarchy_combined_all_current_data::sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd,
'$batchid';

smith__idrp_shc_item_combined_final = DISTINCT smith__idrp_shc_item_combined_final;
STORE smith__idrp_shc_item_combined_final INTO '$SMITH__IDRP_SHC_ITEM_COMBINED_WORK_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
