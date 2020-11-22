/*
###############################################################################
#<>                           START HEADER DOCUMENT                         <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_current_4.pig
# AUTHOR NAME:         Mudit Mangal
# CREATION DATE:       07-07-2014 06:20
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
#        DATE           BY               MODIFICATION
#		 30/10/2014     Meghana 		 Added Column special_retail_order_system_ind CR#3216
#		 30/03/2015     Meghana			 Added Sears and Kmart Online Fulfillment Columns CR#3703
#                16/07/2019     Heena Salim              Added Single Item Replenishment flag to IE_ITEM  IPS-4042
###############################################################################
#<<                 START COMMON HEADER CODE - DO NOT MANUALLY EDIT         >>#
###############################################################################
*/

-- Register the jar containing all PIG UDFs
REGISTER $UDF_JAR;
SET default_parallel $NUM_PARALLEL;
DEFINE TrimLeadingZeros com.searshc.supplychain.idrp.udf.TrimLeadingZeros();
DEFINE AddDays com.searshc.supplychain.idrp.udf.AddOrRemoveDaysToDate();


/******************************* LOAD FOR ALL TABLES AND FILES REQUIRED ***********************************/

WORK__IDRP_ELIGIBLE_ITEM_CURRENT_PART_4 = LOAD '$WORK__IDRP_ELIGIBLE_ITEM_CURRENT_PART_4' 
	USING PigStorage('$FIELD_DELIMITER_PIPE') 
		AS ($WORK__IDRP_ELIGIBLE_ITEM_CURRENT_PART_4_SCHEMA) ;

final_smith_item = 
	FOREACH WORK__IDRP_ELIGIBLE_ITEM_CURRENT_PART_4 
	GENERATE
		'$CURRENT_TIMESTAMP' AS load_ts,
		shc_item_id,
		shc_item_desc,
		shc_division_nbr,
		shc_division_desc,
		shc_department_nbr,
		shc_department_desc,
		shc_category_group_level_nbr,
		shc_category_group_desc,
		shc_category_nbr,
		shc_category_desc,
		shc_sub_category_nbr,
		shc_sub_category_desc,
		reference_ksn_id,
		sears_business_nbr,
		sears_business_desc,
		sears_division_nbr,
		sears_division_desc,
		sears_line_nbr,
		sears_line_desc,
		sears_sub_line_nbr,
		sears_sub_line_desc,
		sears_class_nbr,
		sears_class_desc,
		sears_item_nbr,
		sears_sku_nbr,
		sears_division_item_id,
		sears_division_item_sku_id,
		ima_sim_to_shc_item_id,
		ima_sim_to_shc_item_id_desc,
		ima_sim_to_factor_qty,
		uom_cd,
		package_cube_volume_inch_qty,
		package_weight_pounds_qty,
		purchase_order_vendor_location_id,
		purchase_order_vendor_location_desc,
		vendor_stock_nbr,
		special_order_candidate_ind,
		item_emp_ind,
		easy_order_ind,
		rapid_item_ind,
		constrained_item_ind,
		import_ind,
		centrally_stocked_ind,
		delivered_direct_ind,
		installation_ind ,
		deprecated_1,
		deprecated_2,
		outbound_830_ind,
		outbound_830_duration_nbr,
		rapid_freeze_duration_nbr,
		distribution_type_cd,
		sales_performance_segment_cd,
		format_exclude_cd,
		store_forecast_cd,
		idrp_order_method_cd,
		shc_item_type_cd,
		item_purchase_status_cd,
		network_distribution_cd,
		future_network_distribution_cd,
		future_network_distribution_effective_dt,
		jit_network_distribution_cd,
		deprecated_3,
		store_reorder_authorization_cd,
		cross_merchandise_attribute_cd,
		warehouse_sizing_attribute_cd,
		can_carry_model_id,
		grocery_crossover_ind,
		shc_item_corporate_owner_cd,
		iplan_id,
		item_program_cd,
		key_program_cd,
		national_unit_cost_amt,
		product_selling_price_amt,
		size_nbr,
		style_nbr,
		markdown_style_reference_cd,
		season_cd,
		season_year,
		sub_season_id,
		item_report_group_id ,
		item_report_sequence_nbr,
		item_forecast_group_id,
		item_forecast_group_desc,
		idrp_item_type_desc,
		brand_desc,
		color_desc,
		tire_size_desc,
		eligible_status_cd,
		last_status_change_dt,
		deprecated_4,
		deprecated_5,
		'$batchid' AS idrp_batch_id,
		order_system_cd,
		dotcom_assorted_ind,
		dotcom_orderable_cd,
		roadrunner_eligible_ind,
		us_dot_ship_type_cd,
		dotcom_package_weight_pounds_qty,
		dotcom_package_depth_inch_qty,
		dotcom_package_height_inch_qty,
		dotcom_package_width_inch_qty,
		dotcom_mailable_ind,
		temporary_online_fulfillment_type_cd,
		default_online_fulfillment_type_cd,
		default_online_fulfillment_type_cd_ts,
		demand_online_fulfillment_cd,
		temporary_ups_billable_weight_qty,
		ups_billable_weight_qty,
		ups_billable_weight_ts,
		demand_ups_billable_weight_qty,
		web_exclusive_ind,
		idrp_order_method_desc,
		sim_to_sears_division_nbr,
		sim_to_sears_division_desc,
		sim_to_sears_item_nbr,
		sim_to_sears_sku_nbr,
		sim_to_sears_division_item_sku_id,
		sim_to_sears_division_item_id,
		sears_network_distribution_cd,
		sears_future_network_effective_dt,
		sears_emp_network_distribution_cd,
		sears_future_network_distribution_cd,
		sears_multiple_division_item_sku_ind,
		most_prevalent_ksn_id,
		most_prevalent_vendor_package_id,
		sears_price_type_desc,
		import_rebuy_ind,
		special_retail_order_system_ind,
		sears_temporary_online_fulfillment_type_cd,
		sears_default_online_fulfillment_type_cd,
		sears_default_online_fulfillment_type_cd_ts,
		sears_demand_online_fulfillment_cd,
		kmart_temporary_online_fulfillment_type_cd,
		kmart_default_online_fulfillment_type_cd,
		kmart_default_online_fulfillment_type_cd_ts,
		kmart_demand_online_fulfillment_cd,
		single_item_replen_ind;    -- IPS 4042

-----------------------------------------------------------------------------------------------------------------

final_smith_item = DISTINCT final_smith_item ;

-----------------------------------------------------------------------------------------------------------------
STORE final_smith_item 
	INTO '$SMITH__IDRP_ELIGIBLE_ITEM_CURRENT_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A');  
					 
/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
