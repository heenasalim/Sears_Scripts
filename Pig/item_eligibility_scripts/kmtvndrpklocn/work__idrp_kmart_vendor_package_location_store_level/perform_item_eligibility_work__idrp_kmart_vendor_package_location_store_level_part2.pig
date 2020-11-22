/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_kmart_vendor_package_location_store_level_part2.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Wed Apr 23 02:52:37 EDT 2014
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
#		03/19/2015	nthadan			included load_ts & batchId columns in work__idrp_kmart_vendor_package_location_store_level to match with Hive Table definition
#        23/04/2015 Sushauvik Deb	CR#4262  Added logic for setting the active_ind and the replenishment_planning_ind on exploding assortment records.
#		 10/13/2015 Kmehta2			CR5223   Setting Repln Planning Ind to Y for those items which are Active except EXAS  
#		10/19/2015  Kmehta2			CR5347 Defaulting the Repln Planning Indication to 'N' for EXAS items
#
#
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

SET default_parallel $NUM_PARALLEL;
REGISTER  $UDF_JAR;
DEFINE TO_JULIAN_DATE $TO_JULIAN_DATE;
DEFINE GetDotComOrderableIndicator com.searshc.supplychain.idrp.udf.GetDotComOrderableIndicator();

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

------------------------------------------------------------------------------------------------------------------------------------------------

gold__item_exploding_assortment_data = 
      LOAD '$GOLD__ITEM_EXPLODING_ASSORTMENT_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($GOLD__ITEM_EXPLODING_ASSORTMENT_SCHEMA);

smith__idrp_ie_batchdate_data = 
       LOAD '$SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_LOCATION' 
       USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
       AS ($SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_SCHEMA);

smith__idrp_vend_pack_combined_data =
      LOAD '$SMITH__IDRP_VEND_PACK_COMBINED_LOCATION'
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($SMITH__IDRP_VEND_PACK_COMBINED_SCHEMA);

gen_smith__idrp_vend_pack_combined_data =
      FOREACH smith__idrp_vend_pack_combined_data
      GENERATE
	vendor_package_id,
	dotcom_allocation_ind;

------------------------------------------------------------------------------------------------------------------------------------------------

smith__idrp_vendor_package_store_driver_data = 
      LOAD '$SMITH__IDRP_VENDOR_PACKAGE_STORE_DRIVER_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($SMITH__IDRP_VENDOR_PACKAGE_STORE_DRIVER_SCHEMA);


work__store_level_vend_pack_expl_assrt_eligible_status = 
      LOAD '$WORK__IDRP_STORE_LEVEL_VEND_PACK_LOC_FINAL_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_STORE_LEVEL_VEND_PACK_LOC_FINAL_SCHEMA);


-----------------------------------------gold__item_exploding_assortment Filter applied---------------------------------------------------------
gold__item_exploding_assortment_join_smith__idrp_replenishment_day = 
      CROSS gold__item_exploding_assortment_data,
            smith__idrp_ie_batchdate_data;

gold__item_exploding_assortment_data_fltrd = 
      FILTER gold__item_exploding_assortment_join_smith__idrp_replenishment_day
      BY (TRIM(smith__idrp_ie_batchdate_data::processing_ts) >= TRIM(gold__item_exploding_assortment_data::effective_ts)
      AND
          TRIM(smith__idrp_ie_batchdate_data::processing_ts) <= TRIM(gold__item_exploding_assortment_data::expiration_ts));

gold__item_exploding_assortment_data_fltrd_join_vend_pack_combined =
      JOIN gold__item_exploding_assortment_data_fltrd BY internal_vendor_package_id,
	gen_smith__idrp_vend_pack_combined_data BY vendor_package_id;

gold__item_exploding_assortment_data_fltrd = 
      FOREACH gold__item_exploding_assortment_data_fltrd_join_vend_pack_combined
      GENERATE 
              gold__item_exploding_assortment_data_fltrd::gold__item_exploding_assortment_data::external_vendor_package_id AS external_vendor_package_id,
              gold__item_exploding_assortment_data_fltrd::gold__item_exploding_assortment_data::internal_vendor_package_id AS internal_vendor_package_id,
	      gen_smith__idrp_vend_pack_combined_data::dotcom_allocation_ind AS dotcom_allocation_ind;
------------------------------------------------------------------------------------------------------------------------------------------------

work__idrp_join_smith__idrp_vendor_package = 
      JOIN work__store_level_vend_pack_expl_assrt_eligible_status BY (vendor_package_id,store_location_nbr) LEFT OUTER,
           smith__idrp_vendor_package_store_driver_data BY (vendor_package_id,store_location_nbr);


work__idrp_post_replen_plan_process = 
      FOREACH work__idrp_join_smith__idrp_vendor_package 
      GENERATE 
              work__store_level_vend_pack_expl_assrt_eligible_status::shc_item_id AS shc_item_id,
              work__store_level_vend_pack_expl_assrt_eligible_status::sears_division_nbr AS sears_division_nbr,
              work__store_level_vend_pack_expl_assrt_eligible_status::sears_item_nbr AS sears_item_nbr,
              work__store_level_vend_pack_expl_assrt_eligible_status::sears_sku_nbr AS sears_sku_nbr,
              work__store_level_vend_pack_expl_assrt_eligible_status::shc_item_type_cd AS shc_item_type_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::network_distribution_cd AS network_distribution_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::can_carry_model_id AS can_carry_model_id,
              work__store_level_vend_pack_expl_assrt_eligible_status::item_purchase_status_cd AS item_purchase_status_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::sears_order_system_cd AS sears_order_system_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::idrp_order_method_cd AS idrp_order_method_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::idrp_order_method_desc AS idrp_order_method_desc,
              work__store_level_vend_pack_expl_assrt_eligible_status::ksn_id AS ksn_id,
              work__store_level_vend_pack_expl_assrt_eligible_status::vendor_package_id AS vendor_package_id,
              work__store_level_vend_pack_expl_assrt_eligible_status::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__store_level_vend_pack_expl_assrt_eligible_status::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::ksn_package_id AS ksn_package_id,
              work__store_level_vend_pack_expl_assrt_eligible_status::service_area_restriction_model_id AS service_area_restriction_model_id,
              work__store_level_vend_pack_expl_assrt_eligible_status::flow_type_cd AS flow_type_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::aprk_id AS aprk_id,
              work__store_level_vend_pack_expl_assrt_eligible_status::import_ind AS import_ind,
              work__store_level_vend_pack_expl_assrt_eligible_status::order_duns_nbr AS order_duns_nbr,
              work__store_level_vend_pack_expl_assrt_eligible_status::vendor_carton_qty AS vendor_carton_qty,
              work__store_level_vend_pack_expl_assrt_eligible_status::vendor_stock_nbr AS vendor_stock_nbr,
              work__store_level_vend_pack_expl_assrt_eligible_status::carton_per_layer_qty AS carton_per_layer_qty,
              work__store_level_vend_pack_expl_assrt_eligible_status::layer_per_pallet_qty AS layer_per_pallet_qty,
              work__store_level_vend_pack_expl_assrt_eligible_status::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              --work__store_level_vend_pack_expl_assrt_eligible_status::dotcom_allocation_ind AS dotcom_allocation_ind,
              work__store_level_vend_pack_expl_assrt_eligible_status::store_location_nbr AS store_location_nbr,
              work__store_level_vend_pack_expl_assrt_eligible_status::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__store_level_vend_pack_expl_assrt_eligible_status::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__store_level_vend_pack_expl_assrt_eligible_status::days_to_check_begin_dt AS days_to_check_begin_dt,
              work__store_level_vend_pack_expl_assrt_eligible_status::days_to_check_end_dt AS days_to_check_end_dt,
              work__store_level_vend_pack_expl_assrt_eligible_status::location_format_type_cd AS location_format_type_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::format_type_cd AS format_type_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::location_level_cd AS location_level_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::location_owner_cd AS location_owner_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::scan_based_trading_ind AS scan_based_trading_ind,
              work__store_level_vend_pack_expl_assrt_eligible_status::cross_merchandising_cd AS cross_merchandising_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::dotcom_allocation_ind AS dotcom_order_ind,
              work__store_level_vend_pack_expl_assrt_eligible_status::servicing_dc_nbr AS servicing_dc_nbr,
              work__store_level_vend_pack_expl_assrt_eligible_status::source_location_nbr AS source_location_nbr,
              work__store_level_vend_pack_expl_assrt_eligible_status::dc_effective_dt AS dc_effective_dt,
              work__store_level_vend_pack_expl_assrt_eligible_status::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              work__store_level_vend_pack_expl_assrt_eligible_status::source_location_level_cd AS source_location_level_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              work__store_level_vend_pack_expl_assrt_eligible_status::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              work__store_level_vend_pack_expl_assrt_eligible_status::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::ksn_dc_package_purchase_status_dt AS ksn_dc_package_purchase_status_dt,
              work__store_level_vend_pack_expl_assrt_eligible_status::stock_ind AS stock_ind,
              work__store_level_vend_pack_expl_assrt_eligible_status::substitution_eligible_ind AS substitution_eligible_ind,
              work__store_level_vend_pack_expl_assrt_eligible_status::outbound_package_qty AS outbound_package_qty,
              work__store_level_vend_pack_expl_assrt_eligible_status::enable_jif_dc_ind AS enable_jif_dc_ind,
              work__store_level_vend_pack_expl_assrt_eligible_status::source_package_qty AS source_package_qty,
              work__store_level_vend_pack_expl_assrt_eligible_status::sears_location_nbr AS sears_location_nbr,
              work__store_level_vend_pack_expl_assrt_eligible_status::sears_source_location_nbr AS sears_source_location_nbr,
              work__store_level_vend_pack_expl_assrt_eligible_status::dc_configuration_cd AS dc_configuration_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::kmart_markdown_ind AS kmart_markdown_ind,
              work__store_level_vend_pack_expl_assrt_eligible_status::allocation_replenishment_cd AS allocation_replenishment_cd,
              work__store_level_vend_pack_expl_assrt_eligible_status::active_ind AS active_ind,
              (((IsNull(smith__idrp_vendor_package_store_driver_data::vendor_package_id,'') == '' ) OR work__store_level_vend_pack_expl_assrt_eligible_status::shc_item_type_cd == 'EXAS') ? 'N' : 'Y') AS replenishment_planning_ind,
              ((smith__idrp_vendor_package_store_driver_data::vendor_package_id IS NULL OR smith__idrp_vendor_package_store_driver_data::vendor_package_id=='') ? '' : smith__idrp_vendor_package_store_driver_data::reorder_method_cd) AS reorder_method_cd;


/****** CR5223 starts here ****/

work__idrp_post_replen_plan_process_filter 	= FILTER work__idrp_post_replen_plan_process BY (active_ind =='Y') AND shc_item_type_cd != 'EXAS';

work__idrp_post_replen_plan_process_grp 	= GROUP work__idrp_post_replen_plan_process_filter by (shc_item_id, store_location_nbr );

work__idrp_post_replen_plan_process_grp_limit 	= FOREACH work__idrp_post_replen_plan_process_grp
														{
															sorted = ORDER work__idrp_post_replen_plan_process_filter by shc_item_id, store_location_nbr;
															lim = LIMIT sorted 1;
															GENERATE FLATTEN(lim);
														};

work__idrp_post_replen_plan_process_dist_limit 	=  FOREACH work__idrp_post_replen_plan_process_grp_limit GENERATE 
											              lim::shc_item_id AS shc_item_id,
											              lim::store_location_nbr AS store_location_nbr,
											              lim::vendor_package_id  AS vendor_package_id,
											              lim::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt;


work__idrp_post_replen_plan_process_filter_two 	= FILTER work__idrp_post_replen_plan_process BY (active_ind =='Y' AND replenishment_planning_ind == 'Y' AND shc_item_type_cd != 'EXAS');

work__idrp_post_replen_plan_process_grp_two 	= GROUP work__idrp_post_replen_plan_process_filter_two by (shc_item_id, store_location_nbr,
																									vendor_package_id, vendor_package_purchase_status_dt);

work__idrp_post_replen_plan_process_grp_limit_two = FOREACH work__idrp_post_replen_plan_process_grp_two
														{
															sorted2 = ORDER work__idrp_post_replen_plan_process_filter_two by shc_item_id, store_location_nbr,vendor_package_purchase_status_dt desc ,vendor_package_id;
															lim2 = LIMIT sorted2 1;
															GENERATE FLATTEN(lim2);
														};

work__idrp_post_replen_plan_process_dist_limit_two = FOREACH work__idrp_post_replen_plan_process_grp_limit_two GENERATE 
											              lim2::shc_item_id AS shc_item_id,
											              lim2::store_location_nbr AS store_location_nbr,
											              lim2::vendor_package_id  AS vendor_package_id,
											              lim2::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt;

work__idrp_post_replen_plan_process_limited_join = JOIN work__idrp_post_replen_plan_process_dist_limit by (shc_item_id, store_location_nbr) left outer,
														work__idrp_post_replen_plan_process_dist_limit_two by (shc_item_id, store_location_nbr) ;

work__idrp_post_replen_plan_process_limited_filter = filter work__idrp_post_replen_plan_process_limited_join by (IsNull(work__idrp_post_replen_plan_process_dist_limit_two::vendor_package_id,'') == '');

work__idrp_post_replen_plan_process_limited_gen = FOREACH work__idrp_post_replen_plan_process_limited_filter GENERATE 
														  work__idrp_post_replen_plan_process_dist_limit::shc_item_id AS shc_item_id,
											              work__idrp_post_replen_plan_process_dist_limit::store_location_nbr AS store_location_nbr,
											              work__idrp_post_replen_plan_process_dist_limit::vendor_package_id AS vendor_package_id,
											              work__idrp_post_replen_plan_process_dist_limit::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt;
											             																	
work__idrp_post_replen_plan_process_limited_grp = group work__idrp_post_replen_plan_process_limited_gen by (shc_item_id, store_location_nbr);
																									
work__idrp_post_replen_plan_process_grp_limited = foreach work__idrp_post_replen_plan_process_limited_grp
														{
															sorted3 = order work__idrp_post_replen_plan_process_limited_gen by shc_item_id, store_location_nbr,vendor_package_purchase_status_dt desc,vendor_package_id;
															lim3 = LIMIT sorted3 1;
															generate FLATTEN(lim3);
														};														 

work__idrp_post_replen_plan_process_limited_ggen = FOREACH work__idrp_post_replen_plan_process_grp_limited GENERATE 
														  lim3::shc_item_id AS shc_item_id,
											              lim3::store_location_nbr AS store_location_nbr,
											              lim3::vendor_package_id AS vendor_package_id,
											              lim3::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt;

										              
work__idrp_post_replen_plan_process_complete_join = JOIN work__idrp_post_replen_plan_process by (vendor_package_id, store_location_nbr) left outer, 
												work__idrp_post_replen_plan_process_limited_ggen by (vendor_package_id, store_location_nbr);
												
work__idrp_post_replen_plan_process_complete = 
      FOREACH work__idrp_post_replen_plan_process_complete_join 
      GENERATE 
              '$CURRENT_TIMESTAMP'	AS 	load_ts,
				work__idrp_post_replen_plan_process::vendor_package_id	AS 	vendor_package_id,
				work__idrp_post_replen_plan_process::store_location_nbr	AS 	store_location_id,
				work__idrp_post_replen_plan_process::location_format_type_cd	AS 	location_format_type_cd,
				work__idrp_post_replen_plan_process::location_level_cd	AS 	location_level_cd,
				work__idrp_post_replen_plan_process::location_owner_cd	AS 	location_owner_cd,
				'K'	AS 	source_owner_cd,
				work__idrp_post_replen_plan_process::active_ind	AS 	active_ind,
				'$CURRENT_DATE'	AS 	active_ind_change_dt,
				work__idrp_post_replen_plan_process::allocation_replenishment_cd AS  allocation_replenishment_cd,
				work__idrp_post_replen_plan_process::purchase_order_vendor_location_id AS  purchase_order_vendor_location_id,
				(IsNull(work__idrp_post_replen_plan_process_limited_ggen::vendor_package_id,'') != '' ? 'Y' : work__idrp_post_replen_plan_process::replenishment_planning_ind) AS  replenishment_planning_ind,
				work__idrp_post_replen_plan_process::scan_based_trading_ind	AS 	scan_based_trading_ind,
				work__idrp_post_replen_plan_process::source_location_nbr	AS 	source_location_id,
				work__idrp_post_replen_plan_process::source_location_level_cd	AS 	source_location_level_cd,
				work__idrp_post_replen_plan_process::source_package_qty	AS 	source_package_qty,
				work__idrp_post_replen_plan_process::vendor_package_purchase_status_cd	AS 	vendor_package_purchase_status_cd,
				work__idrp_post_replen_plan_process::vendor_package_purchase_status_dt	AS 	vendor_package_purchase_status_dt,
				work__idrp_post_replen_plan_process::flow_type_cd	AS 	flow_type_cd,
				work__idrp_post_replen_plan_process::import_ind	AS 	import_ind,
				work__idrp_post_replen_plan_process::retail_carton_vendor_package_id	AS 	retail_carton_vendor_package_id,
				work__idrp_post_replen_plan_process::vendor_package_owner_cd	AS 	vendor_package_owner_cd,
				work__idrp_post_replen_plan_process::vendor_stock_nbr	AS 	vendor_stock_nbr,
				work__idrp_post_replen_plan_process::shc_item_id	AS 	shc_item_id,
				work__idrp_post_replen_plan_process::item_purchase_status_cd	AS 	item_purchase_status_cd,
				work__idrp_post_replen_plan_process::can_carry_model_id	AS 	can_carry_model_id,
				work__idrp_post_replen_plan_process::days_to_check_begin_day_qty	AS 	days_to_check_begin_day_qty,
				work__idrp_post_replen_plan_process::days_to_check_end_day_qty	AS 	days_to_check_end_day_qty,
				work__idrp_post_replen_plan_process::reorder_method_cd	AS 	reorder_method_cd,
				work__idrp_post_replen_plan_process::ksn_id	AS 	ksn_id,
				work__idrp_post_replen_plan_process::ksn_purchase_status_cd	AS 	ksn_purchase_status_cd,
				work__idrp_post_replen_plan_process::cross_merchandising_cd	AS 	cross_merchandising_cd,
				work__idrp_post_replen_plan_process::dotcom_order_ind	AS 	dotcom_orderable_cd,
				work__idrp_post_replen_plan_process::kmart_markdown_ind	AS 	kmart_markdown_ind,
				work__idrp_post_replen_plan_process::ksn_package_id	AS 	ksn_package_id,
				work__idrp_post_replen_plan_process::ksn_dc_package_purchase_status_cd	AS 	ksn_dc_package_purchase_status_cd,
				work__idrp_post_replen_plan_process::dc_configuration_cd	AS 	dc_configuration_cd,
				work__idrp_post_replen_plan_process::substitution_eligible_ind	AS 	substitution_eligible_ind,
				work__idrp_post_replen_plan_process::sears_division_nbr	AS 	sears_division_nbr,
				work__idrp_post_replen_plan_process::sears_item_nbr	AS 	sears_item_nbr,
				work__idrp_post_replen_plan_process::sears_sku_nbr	AS 	sears_sku_nbr,
				work__idrp_post_replen_plan_process::sears_location_nbr	AS 	sears_location_id,
				work__idrp_post_replen_plan_process::sears_source_location_nbr	AS 	sears_source_location_id,
				''	AS 	 rim_status_cd,
				''	AS 	stock_type_cd,
				''	AS 	non_stock_source_cd,
				''	AS 	dos_item_active_ind,
				''	AS 	dos_item_reserve_cd,
				'$CURRENT_DATE'	AS 	create_dt,
				'$CURRENT_DATE'	AS 	last_update_dt,
				work__idrp_post_replen_plan_process::shc_item_type_cd	AS 	shc_item_type_cd,
				work__idrp_post_replen_plan_process::format_type_cd	AS 	format_type_cd,
				work__idrp_post_replen_plan_process::outbound_package_qty	AS 	outbound_package_qty,
				work__idrp_post_replen_plan_process::retail_carton_internal_package_qty	AS 	retail_carton_internal_package_qty,
				work__idrp_post_replen_plan_process::vendor_carton_qty	AS 	vendor_carton_qty,
				work__idrp_post_replen_plan_process::enable_jif_dc_ind	AS 	enable_jif_dc_ind,
				'$batchid'	AS 	idrp_batch_id;
	
	work__idrp_post_replen_plan_process_complete_dist = distinct work__idrp_post_replen_plan_process_complete;			
				
STORE work__idrp_post_replen_plan_process_complete_dist INTO '$WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_STORE_LEVEL_LOCATION_PART1' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
