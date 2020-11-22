/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_smith__idrp_eligible_sears_vendor_package_location_current.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Thu Jul 31 02:41:03 EDT 2014
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
#        DATE         BY            		MODIFICATION
#21/10/2014			Siddhivinayak Karpe 	trim column purchase_order_vendor_location_id at line number 83, 262 ,332
#		22-Oct-2014 Priyanka Gurjar	against the CR 3205 we have eliminated easy order filter
#		28-10-2014	Priyanka Gurjar			Change the filter for store 9305 shop_your_way_attribte_cd = â€œOS0001â€� against CR 3239 (refer line: 248)
#       10-11-2014  Siddhivinayak Karpe     CR 3311 Added additional filter criteria for the non-active store records(purchase_order_vendor_location_id != '' OR cross_merchandising_cd == RIMFLOW)
#       02-12-2014  Meghana Dhage           CR 3415 (Line no: 63) (Added lookup smith__idrp_eligible_loc to provide a consistent format to sears_location_id and sears_source_location_id)
#		10/12/2014	Siddhivinayak Karpe		CR#3415 Added lookup smith__idrp_eligible_loc to provide a consistent format to sears_location_id and sears_source_location_id for NetApp Output
#		15/12/2014	Siddhivinayak Karpe		CR#3415 Code for Distinct Records from work__idrp_loc_xref
#		11/05/2015  Sushauvik Deb           CR#4407 Update to online authorization based on warehouse sizing attributes to Sears Vendor Pack Loc
#		01/19/2017  Srujan Dussa			IPS-779 � Adding rim_last_record_create_dt from gold__inventory_rim_daily_current to be included in the Extract File to Shared Items.
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

SET default_parallel $NUM_PARALLEL;
REGISTER $UDF_JAR;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/


work__idrp_sears_vendor_package_location_data = 
      LOAD '$WORK__IDRP_SEARS_VENDOR_PACKAGE_LOCATION_LOCATION'
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_SEARS_VENDOR_PACKAGE_LOCATION_SCHEMA);


work__idrp_sears_vendor_package_exploding_assortment_location_data = 
      LOAD '$WORK__IDRP_SEARS_VENDOR_PACKAGE_EXPLODING_ASSORTMENT_LOCATION_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_SEARS_VENDOR_PACKAGE_EXPLODING_ASSORTMENT_LOCATION_SCHEMA);


work__idrp_sears_vendor_package_vendor_location_data = 
      LOAD '$WORK__IDRP_SEARS_VENDOR_PACKAGE_VENDOR_LOCATION_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_SEARS_VENDOR_PACKAGE_VENDOR_LOCATION_SCHEMA);


smith__idrp_ksn_attribute_current_data = 
      LOAD '$SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_SCHEMA);

/********************** CR 3415 (Step 0) **************************************/
		  
smith__idrp_eligible_loc_data = 
      LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);

  
filter_smith__idrp_eligible_loc_data = 
	  FILTER smith__idrp_eligible_loc_data
	  BY ((TRIM(loc_lvl_cd) == 'VENDOR' AND TRIM(duns_type_cd) == 'ORD' AND IsNull(TRIM(srs_vndr_nbr),'') != '') OR
	      (TRIM(loc_lvl_cd) != 'VENDOR' AND IsNull(TRIM(srs_loc),'') != ''));
		  
work__idrp_loc_xref_temp = 
	  FOREACH filter_smith__idrp_eligible_loc_data
	  GENERATE
			((TRIM(loc_lvl_cd) == 'VENDOR' AND TRIM(duns_type_cd) == 'ORD')
			 ? srs_vndr_nbr
			 : srs_loc) AS sears_location_id_xref,
			TRIM(loc_lvl_cd) AS location_level_cd_xref;

work__idrp_loc_xref = distinct work__idrp_loc_xref_temp;

			
join_work__idrp_sears_vendor_package_location_loc = 
	  JOIN work__idrp_sears_vendor_package_location_data BY (location_level_cd, TrimLeadingZeros(sears_location_id)),
		   work__idrp_loc_xref BY (location_level_cd_xref, TrimLeadingZeros(sears_location_id_xref)) using 'replicated';
		   		   
join_left_outer_work__idrp_sears_vendor_package_location_loc = 
	  JOIN join_work__idrp_sears_vendor_package_location_loc BY (source_location_level_cd, TrimLeadingZeros(sears_source_location_id)) LEFT OUTER,
	       work__idrp_loc_xref BY (location_level_cd_xref, TrimLeadingZeros(sears_location_id_xref)) using 'replicated';
		   
gen_work__idrp_sears_vendor_package_location_data = 
FOREACH join_left_outer_work__idrp_sears_vendor_package_location_loc
		GENERATE
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::vendor_package_id AS vendor_pack_id,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::location_id as location_id,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::location_format_type_cd as location_format_type_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::location_level_cd as location_level_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::location_owner_cd as location_owner_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::source_owner_cd as source_owner_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::active_ind as active_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::active_ind_change_dt as active_ind_change_dt,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::allocation_replenishment_cd as allocation_replenishment_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::purchase_order_vendor_location_id as purchase_order_vendor_location_id,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::replenishment_planning_ind as replenishment_planning_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::scan_based_trading_ind as scan_based_trading_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::source_location_id  as source_location_id,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::source_location_level_cd as source_location_level_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::source_package_qty as source_package_qty,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::vendor_package_purchase_status_cd as vendor_package_purchase_status_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::vendor_package_purchase_status_dt as vendor_package_purchase_status_dt,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::flow_type_cd as flow_type_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::import_ind as import_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::retail_carton_vendorpackage_id as retail_carton_vendorpackage_id,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::vendor_package_owner_cd as vendor_package_owner_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::vendor_stock_nbr as vendor_stock_nbr,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::shc_item_id as shc_item_id,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::item_purchase_status_cd as item_purchase_status_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::can_carry_model_id as can_carry_model_id,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::days_to_check_begin_day_dt as days_to_check_begin_day_dt,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::days_to_check_end_day_dt as days_to_check_end_day_dt,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::reorder_method_cd as reorder_method_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::ksn_id as ksn_id,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::ksn_purchase_status_cd as ksn_purchase_status_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::cross_merchandising_cd as cross_merchandising_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::dotcom_orderable_cd  as  dotcom_orderable_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::kmart_markdown_ind as kmart_markdown_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::ksn_package_id  as  ksn_package_id,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::ksn_dc_package_purchase_status_cd as ksn_dc_package_purchase_status_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::dc_configuration_cd  as  dc_configuration_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::substitution_eligible_ind  as  substitution_eligible_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::sears_division_nbr  as  sears_division_nbr,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::sears_item_nbr  as  sears_item_nbr,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::sears_sku_nbr  as  sears_sku_nbr,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_loc_xref::sears_location_id_xref AS sears_location_id,
			(work__idrp_loc_xref::sears_location_id_xref IS NOT NULL
			? work__idrp_loc_xref::sears_location_id_xref
			: join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::sears_source_location_id) AS sears_source_location_id,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::rim_status_cd  as  rim_status_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::stock_type_cd  as  stock_type_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::non_stock_source_cd  as  non_stock_source_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::dos_item_active_ind  as  dos_item_active_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::item_reserve_cd  as  item_reserve_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::create_dt  as  create_dt,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::last_update_dt  as  last_update_dt,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::vendor_package_carton_qty  as  vendor_package_carton_qty,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::special_retail_order_system_ind  as  special_retail_order_system_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::shc_item_corporate_owner_cd  as  shc_item_corporate_owner_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::distribution_type_cd  as  distribution_type_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::only_rsu_distribution_channel_ind  as  only_rsu_distribution_channel_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::special_order_candidate_ind  as  special_order_candidate_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::item_emp_ind  as  item_emp_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::easy_order_ind  as  easy_order_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::warehouse_sizing_attribute_cd  as  warehouse_sizing_attribute_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::rapid_item_ind  as  rapid_item_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::constrained_item_ind  as  constrained_item_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::idrp_item_type_desc  as  idrp_item_type_desc,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::cross_merchandising_attribute_cd as cross_merchandising_attribute_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::sams_migration_ind  as  sams_migration_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::emp_to_jit_ind  as  emp_to_jit_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::rim_flow_ind  as  rim_flow_ind,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::source_system_cd  as  source_system_cd,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::original_source_nbr  as  original_source_nbr,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::item_next_period_on_hand_qty  as  item_next_period_on_hand_qty,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::item_on_order_qty  as  item_on_order_qty,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::item_reserve_qty  as  item_reserve_qty,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::item_back_order_qty  as  item_back_order_qty,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::item_next_period_future_order_qty  as  item_next_period_future_order_qty,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::item_next_period_in_transit_qty  as  item_next_period_in_transit_qty,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::item_last_receive_dt  as  item_last_receive_dt,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::item_last_ship_dt  as  item_last_ship_dt,
			join_work__idrp_sears_vendor_package_location_loc::work__idrp_sears_vendor_package_location_data::rim_last_record_creation_dt AS rim_last_record_creation_dt;

			
join_work__idrp_sears_vendor_package_exploding_assortment_location_loc = 
	  JOIN work__idrp_sears_vendor_package_exploding_assortment_location_data BY (location_level_cd, TrimLeadingZeros(sears_location_id)),
		   work__idrp_loc_xref BY (location_level_cd_xref, TrimLeadingZeros(sears_location_id_xref)) using 'replicated';
		   
join_left_outer_work__idrp_sears_vendor_package_exploding_assortment_location_loc = 
	  JOIN join_work__idrp_sears_vendor_package_exploding_assortment_location_loc BY (source_location_level_cd, TrimLeadingZeros(sears_source_location_id)) LEFT OUTER,
	       work__idrp_loc_xref BY (location_level_cd_xref, TrimLeadingZeros(sears_location_id_xref)) using 'replicated';		   

gen_work__idrp_sears_vendor_package_exploding_assortment_location_data = 
	  FOREACH join_left_outer_work__idrp_sears_vendor_package_exploding_assortment_location_loc
	  GENERATE
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::vendor_package_id  as  vendor_pack_id,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::location_id  as  location_id,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::location_format_type_cd  as  location_format_type_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::location_level_cd  as  location_level_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::location_owner_cd  as  location_owner_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::source_owner_cd  as  source_owner_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::active_ind  as  active_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::active_ind_change_dt  as  active_ind_change_dt,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::allocation_replenishment_cd  as  allocation_replenishment_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::purchase_order_vendor_location_id  as  purchase_order_vendor_location_id,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::replenishment_planning_ind  as  replenishment_planning_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::scan_based_trading_ind  as  scan_based_trading_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::source_location_id  as  source_location_id,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::source_location_level_cd  as  source_location_level_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::source_package_qty  as  source_package_qty,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::vendor_package_purchase_status_cd  as  vendor_package_purchase_status_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::vendor_package_purchase_status_dt  as  vendor_package_purchase_status_dt,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::flow_type_cd  as  flow_type_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::import_ind  as  import_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::retail_carton_vendor_package_id  as  retail_carton_vendor_package_id,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::vendor_package_owner_cd  as  vendor_package_owner_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::vendor_stock_nbr  as  vendor_stock_nbr,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::shc_item_id  as  shc_item_id,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::item_purchase_status_cd  as  item_purchase_status_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::can_carry_model_id  as  can_carry_model_id,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::days_to_check_begin_day_dt  as  days_to_check_begin_day_dt,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::days_to_check_end_day_dt  as  days_to_check_end_day_dt,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::reorder_method_cd  as  reorder_method_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::ksn_id  as  ksn_id,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::ksn_purchase_status_cd  as  ksn_purchase_status_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::cross_merchandising_cd  as  cross_merchandising_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::dotcom_orderable_cd  as  dotcom_orderable_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::kmart_markdown_ind  as  kmart_markdown_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::ksn_package_id  as  ksn_package_id,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::ksn_dc_package_purchase_status_cd  as  ksn_dc_package_purchase_status_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::dc_configuration_cd  as  dc_configuration_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::substitution_eligible_ind  as  substitution_eligible_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::sears_division_nbr  as  sears_division_nbr,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::sears_item_nbr  as  sears_item_nbr,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::sears_sku_nbr  as  sears_sku_nbr,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_loc_xref::sears_location_id_xref AS sears_location_id,
			(work__idrp_loc_xref::sears_location_id_xref IS NOT NULL
			? work__idrp_loc_xref::sears_location_id_xref
			: join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::sears_source_location_id) AS sears_source_location_id,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::rim_status_cd   as   rim_status_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::stock_type_cd   as   stock_type_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::non_stock_source_cd   as   non_stock_source_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::dos_item_active_ind   as   dos_item_active_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::item_reserve_cd   as   item_reserve_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::create_dt   as   create_dt,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::last_update_dt   as   last_update_dt,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::vendor_package_carton_qty   as   vendor_package_carton_qty,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::special_retail_order_system_ind   as   special_retail_order_system_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::shc_item_corporate_owner_cd   as   shc_item_corporate_owner_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::distribution_type_cd   as   distribution_type_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::only_rsu_distribution_channel_ind   as   only_rsu_distribution_channel_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::special_order_candidate_ind   as   special_order_candidate_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::item_emp_ind   as   item_emp_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::easy_order_ind   as   easy_order_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::warehouse_sizing_attribute_cd   as   warehouse_sizing_attribute_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::rapid_item_ind   as   rapid_item_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::constrained_item_ind   as   constrained_item_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::idrp_item_type_desc   as   idrp_item_type_desc,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::cross_merchandising_attribute_cd   as   cross_merchandising_attribute_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::sams_migration_ind   as   sams_migration_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::emp_to_jit_ind   as   emp_to_jit_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::rim_flow_ind   as   rim_flow_ind,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::source_system_cd   as   source_system_cd,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::original_source_nbr   as   original_source_nbr,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::item_next_period_on_hand_qty   as   item_next_period_on_hand_qty,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::item_on_order_qty   as   item_on_order_qty,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::item_reserve_qty   as   item_reserve_qty,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::item_back_order_qty   as   item_back_order_qty,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::item_next_period_future_order_qty   as   item_next_period_future_order_qty,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::item_next_period_in_transit_qty   as   item_next_period_in_transit_qty,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::item_last_receive_dt   as   item_last_receive_dt,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::item_last_ship_dt   as   item_last_ship_dt,
			join_work__idrp_sears_vendor_package_exploding_assortment_location_loc::work__idrp_sears_vendor_package_exploding_assortment_location_data::rim_last_record_creation_dt AS rim_last_record_creation_dt;
			
join_work__idrp_sears_vendor_package_vendor_location_loc = 
	  JOIN work__idrp_sears_vendor_package_vendor_location_data BY (location_level_cd, TrimLeadingZeros(sears_location_id)),
		   work__idrp_loc_xref BY (location_level_cd_xref, TrimLeadingZeros(sears_location_id_xref)) using 'replicated';
		   
join_left_outer_work__idrp_sears_vendor_package_vendor_location_loc = 
	  JOIN join_work__idrp_sears_vendor_package_vendor_location_loc BY (source_location_level_cd, TrimLeadingZeros(sears_source_location_id)) LEFT OUTER,
	       work__idrp_loc_xref BY (location_level_cd_xref, TrimLeadingZeros(sears_location_id_xref)) using 'replicated';		   

gen_work__idrp_sears_vendor_package_vendor_location_data = 
	FOREACH join_left_outer_work__idrp_sears_vendor_package_vendor_location_loc
		GENERATE
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::vendor_pack_id  as  vendor_pack_id,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::location_id  as  location_id,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::location_format_type_cd  as  location_format_type_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::location_level_cd  as  location_level_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::location_owner_cd  as  location_owner_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::source_owner_cd  as  source_owner_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::active_ind  as  active_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::active_ind_change_dt  as  active_ind_change_dt,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::allocation_replenishment_cd  as  allocation_replenishment_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::purchase_order_vendor_location_id  as  purchase_order_vendor_location_id,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::replenishment_planning_ind  as  replenishment_planning_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::scan_based_trading_ind  as  scan_based_trading_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::source_location_id  as  source_location_id,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::source_location_level_cd  as  source_location_level_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::source_package_qty  as  source_package_qty,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::vendor_package_purchase_status_cd  as  vendor_package_purchase_status_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::vendor_package_purchase_status_dt  as  vendor_package_purchase_status_dt,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::flow_type_cd  as  flow_type_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::import_ind  as  import_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::retail_carton_vendor_package_id  as  retail_carton_vendor_package_id,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::vendor_package_owner_cd  as  vendor_package_owner_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::vendor_stock_nbr  as  vendor_stock_nbr,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::shc_item_id  as  shc_item_id,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::item_purchase_status_cd  as  item_purchase_status_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::can_carry_model_id  as  can_carry_model_id,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::days_to_check_begin_dt  as  days_to_check_begin_dt,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::days_to_check_end_dt  as  days_to_check_end_dt,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::reorder_method_cd  as  reorder_method_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::ksn_id  as  ksn_id,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::ksn_purchase_status_cd  as  ksn_purchase_status_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::cross_merchandising_cd  as  cross_merchandising_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::dotcom_orderable_cd  as  dotcom_orderable_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::kmart_markdown_ind  as  kmart_markdown_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::ksn_package_id  as  ksn_package_id,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::ksn_dc_package_purchase_status_cd  as  ksn_dc_package_purchase_status_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::dc_configuration_cd  as  dc_configuration_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::substitution_eligible_ind  as  substitution_eligible_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::sears_division_nbr  as  sears_division_nbr,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::sears_item_nbr  as  sears_item_nbr,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::sears_sku_nbr  as  sears_sku_nbr,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_loc_xref::sears_location_id_xref as sears_location_id,
				(work__idrp_loc_xref::sears_location_id_xref IS NOT NULL
				? work__idrp_loc_xref::sears_location_id_xref
				: join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::sears_source_location_id) as sears_source_location_id,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::rim_status_cd  as  rim_status_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::stock_type_cd  as  stock_type_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::non_stock_source_cd  as  non_stock_source_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::dos_item_active_ind  as  dos_item_active_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::item_reserve_cd  as  item_reserve_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::create_dt  as  create_dt,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::last_update_dt  as  last_update_dt,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::vendor_package_carton_qty  as  vendor_package_carton_qty,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::special_retail_order_system_ind  as  special_retail_order_system_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::shc_item_corporate_owner_cd  as  shc_item_corporate_owner_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::distribution_type_cd  as  distribution_type_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::only_rsu_distribution_channel_ind  as  only_rsu_distribution_channel_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::special_order_candidate_ind  as  special_order_candidate_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::item_emp_ind  as  item_emp_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::easy_order_ind  as  easy_order_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::warehouse_sizing_attribute_cd  as  warehouse_sizing_attribute_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::rapid_item_ind  as  rapid_item_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::constrained_item_ind  as  constrained_item_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::idrp_item_type_desc  as  idrp_item_type_desc,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::cross_merchandising_attribute_cd  as  cross_merchandising_attribute_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::sams_migration_ind  as  sams_migration_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::emp_to_jit_ind  as  emp_to_jit_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::rim_flow_ind  as  rim_flow_ind,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::source_system_cd  as  source_system_cd,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::original_source_nbr  as  original_source_nbr,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::item_next_period_on_hand_qty  as  item_next_period_on_hand_qty,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::item_on_order_qty  as  item_on_order_qty,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::item_reserve_qty  as  item_reserve_qty,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::item_back_order_qty  as  item_back_order_qty,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::item_next_period_future_order_qty  as  item_next_period_future_order_qty,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::item_next_period_in_transit_qty  as  item_next_period_in_transit_qty,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::item_last_receive_dt  as  item_last_receive_dt,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::item_last_ship_dt  as  item_last_ship_dt,
				join_work__idrp_sears_vendor_package_vendor_location_loc::work__idrp_sears_vendor_package_vendor_location_data::rim_last_record_creation_dt AS rim_last_record_creation_dt;

----------------------------------- Logic union_work_idrp_gen -----------------------------------

union_work_idrp = 
      UNION gen_work__idrp_sears_vendor_package_location_data,
            gen_work__idrp_sears_vendor_package_exploding_assortment_location_data,
            gen_work__idrp_sears_vendor_package_vendor_location_data;


union_work_idrp = DISTINCT union_work_idrp;


union_work_idrp_gen = 
      FOREACH union_work_idrp
      GENERATE
              '$CURRENT_TIMESTAMP' AS load_ts,
              vendor_pack_id AS vendor_pack_id,
              location_id AS location_id,
              location_format_type_cd AS location_format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              source_owner_cd AS source_owner_cd,
              active_ind AS active_ind,
              active_ind_change_dt AS active_ind_change_dt,
              allocation_replenishment_cd AS allocation_replenishment_cd,
              TRIM(purchase_order_vendor_location_id) AS purchase_order_vendor_location_id,
              replenishment_planning_ind AS replenishment_planning_ind,
              scan_based_trading_ind AS scan_based_trading_ind,
              source_location_id AS source_location_id,
              source_location_level_cd AS source_location_level_cd,
              source_package_qty AS source_package_qty,
              vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              flow_type_cd AS flow_type_cd,
              import_ind AS import_ind,
              retail_carton_vendorpackage_id AS retail_carton_vendor_package_id,
              vendor_package_owner_cd AS vendor_package_owner_cd,
              vendor_stock_nbr AS vendor_stock_nbr,
              shc_item_id AS shc_item_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              can_carry_model_id AS can_carry_model_id,
              days_to_check_begin_day_dt AS days_to_check_begin_dt,
              days_to_check_end_day_dt AS days_to_check_end_dt,
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
              item_reserve_cd AS item_reserve_cd,
              create_dt AS create_dt,
              last_update_dt AS last_update_dt,
              vendor_package_carton_qty AS vendor_package_carton_qty,
              special_retail_order_system_ind AS special_retail_order_system_ind,
              shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
              distribution_type_cd AS distribution_type_cd,
              only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
              special_order_candidate_ind AS special_order_candidate_ind,
              item_emp_ind AS item_emp_ind,
              easy_order_ind AS easy_order_ind,
              warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
              rapid_item_ind AS rapid_item_ind,
              constrained_item_ind AS constrained_item_ind,
              idrp_item_type_desc AS idrp_item_type_desc,
              cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
              sams_migration_ind AS sams_migration_ind,
              emp_to_jit_ind AS emp_to_jit_ind,
              rim_flow_ind AS rim_flow_ind,
              source_system_cd AS source_system_cd,
              original_source_nbr AS original_source_nbr,
              item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
              item_on_order_qty AS item_on_order_qty,
              item_reserve_qty AS item_reserve_qty,
              item_back_order_qty AS item_back_order_qty,
              item_next_period_future_order_qty AS item_next_period_future_order_qty,
              item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
              item_last_receive_dt AS item_last_receive_dt,
              item_last_ship_dt AS item_last_ship_dt,
	      	  rim_last_record_creation_dt AS rim_last_record_creation_dt,
              '$batchid' AS idrp_batch_id;

----------------------------------- Logic netapp_ext_srs_vp_str -----------------------------------

work__idrp_sears_vendor_package_location_fltr = 
      FILTER gen_work__idrp_sears_vendor_package_location_data 
      BY TRIM(location_level_cd)=='STORE' AND TRIM(active_ind)=='N';



join_work_sears_vp_smith_ksn_attr = 
     JOIN work__idrp_sears_vendor_package_location_fltr BY ((int)sears_division_nbr,TrimLeadingZeros(sears_item_nbr),(int)sears_sku_nbr),
          smith__idrp_ksn_attribute_current_data BY ((int)sears_division_nbr,TrimLeadingZeros(sears_item_nbr),(int)sears_sku_nbr);


join_work_sears_vp_smith_ksn_attr_gen = 
     FOREACH join_work_sears_vp_smith_ksn_attr 
     GENERATE
             work__idrp_sears_vendor_package_location_fltr::vendor_pack_id AS vendor_package_id,
             work__idrp_sears_vendor_package_location_fltr::location_id AS location_id,
             work__idrp_sears_vendor_package_location_fltr::location_format_type_cd AS location_format_type_cd,
             work__idrp_sears_vendor_package_location_fltr::location_level_cd AS location_level_cd,
             work__idrp_sears_vendor_package_location_fltr::location_owner_cd AS location_owner_cd,
             work__idrp_sears_vendor_package_location_fltr::source_owner_cd AS source_owner_cd,
             work__idrp_sears_vendor_package_location_fltr::active_ind AS active_ind,
             work__idrp_sears_vendor_package_location_fltr::active_ind_change_dt AS active_ind_change_dt,
             work__idrp_sears_vendor_package_location_fltr::allocation_replenishment_cd AS allocation_replenishment_cd,
             work__idrp_sears_vendor_package_location_fltr::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
             work__idrp_sears_vendor_package_location_fltr::replenishment_planning_ind AS replenishment_planning_ind,
             work__idrp_sears_vendor_package_location_fltr::scan_based_trading_ind AS scan_based_trading_ind,
             work__idrp_sears_vendor_package_location_fltr::source_location_id AS source_location_id,
             work__idrp_sears_vendor_package_location_fltr::source_location_level_cd AS source_location_level_cd,
             work__idrp_sears_vendor_package_location_fltr::source_package_qty AS source_package_qty,
             work__idrp_sears_vendor_package_location_fltr::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
             work__idrp_sears_vendor_package_location_fltr::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
             work__idrp_sears_vendor_package_location_fltr::flow_type_cd AS flow_type_cd,
             work__idrp_sears_vendor_package_location_fltr::import_ind AS import_ind,
             work__idrp_sears_vendor_package_location_fltr::retail_carton_vendorpackage_id AS retail_carton_vendor_package_id,
             work__idrp_sears_vendor_package_location_fltr::vendor_package_owner_cd AS vendor_package_owner_cd,
             work__idrp_sears_vendor_package_location_fltr::vendor_stock_nbr AS vendor_stock_nbr,
             work__idrp_sears_vendor_package_location_fltr::shc_item_id AS shc_item_id,
             work__idrp_sears_vendor_package_location_fltr::item_purchase_status_cd AS item_purchase_status_cd,
             work__idrp_sears_vendor_package_location_fltr::can_carry_model_id AS can_carry_model_id,
             work__idrp_sears_vendor_package_location_fltr::days_to_check_begin_day_dt AS days_to_check_begin_day_dt,
             work__idrp_sears_vendor_package_location_fltr::days_to_check_end_day_dt AS days_to_check_end_day_dt,
             work__idrp_sears_vendor_package_location_fltr::reorder_method_cd AS reorder_method_cd,
             work__idrp_sears_vendor_package_location_fltr::ksn_id AS ksn_id,
             work__idrp_sears_vendor_package_location_fltr::ksn_purchase_status_cd AS ksn_purchase_status_cd,
             work__idrp_sears_vendor_package_location_fltr::cross_merchandising_cd AS cross_merchandising_cd,
             work__idrp_sears_vendor_package_location_fltr::dotcom_orderable_cd AS dotcom_orderable_cd,
             work__idrp_sears_vendor_package_location_fltr::kmart_markdown_ind AS kmart_markdown_ind,
             work__idrp_sears_vendor_package_location_fltr::ksn_package_id AS ksn_package_id,
             work__idrp_sears_vendor_package_location_fltr::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
             work__idrp_sears_vendor_package_location_fltr::dc_configuration_cd AS dc_configuration_cd,
             work__idrp_sears_vendor_package_location_fltr::substitution_eligible_ind AS substitution_eligible_ind,
             work__idrp_sears_vendor_package_location_fltr::sears_division_nbr AS sears_division_nbr,
             work__idrp_sears_vendor_package_location_fltr::sears_item_nbr AS sears_item_nbr,
             work__idrp_sears_vendor_package_location_fltr::sears_sku_nbr AS sears_sku_nbr,
             work__idrp_sears_vendor_package_location_fltr::sears_location_id AS sears_location_id,
             work__idrp_sears_vendor_package_location_fltr::sears_source_location_id AS sears_source_location_id,
             work__idrp_sears_vendor_package_location_fltr::rim_status_cd AS rim_status_cd,
             work__idrp_sears_vendor_package_location_fltr::stock_type_cd AS stock_type_cd,
             work__idrp_sears_vendor_package_location_fltr::non_stock_source_cd AS non_stock_source_cd,
             work__idrp_sears_vendor_package_location_fltr::dos_item_active_ind AS dos_item_active_ind,
             work__idrp_sears_vendor_package_location_fltr::item_reserve_cd AS item_reserve_cd,
             work__idrp_sears_vendor_package_location_fltr::create_dt AS create_dt,
             work__idrp_sears_vendor_package_location_fltr::last_update_dt AS last_update_dt,
             work__idrp_sears_vendor_package_location_fltr::vendor_package_carton_qty AS vendor_package_carton_qty,
             work__idrp_sears_vendor_package_location_fltr::special_retail_order_system_ind AS special_retail_order_system_ind,
             work__idrp_sears_vendor_package_location_fltr::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
             work__idrp_sears_vendor_package_location_fltr::distribution_type_cd AS distribution_type_cd,
             work__idrp_sears_vendor_package_location_fltr::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
             work__idrp_sears_vendor_package_location_fltr::special_order_candidate_ind AS special_order_candidate_ind,
             work__idrp_sears_vendor_package_location_fltr::item_emp_ind AS item_emp_ind,
             work__idrp_sears_vendor_package_location_fltr::easy_order_ind AS easy_order_ind,
             work__idrp_sears_vendor_package_location_fltr::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
             work__idrp_sears_vendor_package_location_fltr::rapid_item_ind AS rapid_item_ind,
             work__idrp_sears_vendor_package_location_fltr::constrained_item_ind AS constrained_item_ind,
             work__idrp_sears_vendor_package_location_fltr::idrp_item_type_desc AS idrp_item_type_desc,
             work__idrp_sears_vendor_package_location_fltr::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
             work__idrp_sears_vendor_package_location_fltr::sams_migration_ind AS sams_migration_ind,
             work__idrp_sears_vendor_package_location_fltr::emp_to_jit_ind AS emp_to_jit_ind,
             work__idrp_sears_vendor_package_location_fltr::rim_flow_ind AS rim_flow_ind,
             work__idrp_sears_vendor_package_location_fltr::source_system_cd AS source_system_cd,
             work__idrp_sears_vendor_package_location_fltr::original_source_nbr AS original_source_nbr,
             work__idrp_sears_vendor_package_location_fltr::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             work__idrp_sears_vendor_package_location_fltr::item_on_order_qty AS item_on_order_qty,
             work__idrp_sears_vendor_package_location_fltr::item_reserve_qty AS item_reserve_qty,
             work__idrp_sears_vendor_package_location_fltr::item_back_order_qty AS item_back_order_qty,
             work__idrp_sears_vendor_package_location_fltr::item_next_period_future_order_qty AS item_next_period_future_order_qty,
             work__idrp_sears_vendor_package_location_fltr::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             work__idrp_sears_vendor_package_location_fltr::item_last_receive_dt AS item_last_receive_dt,
             work__idrp_sears_vendor_package_location_fltr::item_last_ship_dt AS item_last_ship_dt,
             smith__idrp_ksn_attribute_current_data::idrp_batch_id AS idrp_batch_id,
             smith__idrp_ksn_attribute_current_data::shop_your_way_attribute_cd AS shop_your_way_attribute_cd,
	         work__idrp_sears_vendor_package_location_fltr::rim_last_record_creation_dt AS rim_last_record_creation_dt; 


work__idrp_sears_vendor_package_location_fltr_fltr = 
      FILTER join_work_sears_vp_smith_ksn_attr_gen  
      BY (TRIM(location_format_type_cd)!='SINT' OR (TRIM(location_format_type_cd)=='SINT' AND TRIM(location_id)!='9300' AND TRIM(location_id)!='9357' AND TRIM(location_id)!='9305')) OR (TRIM(location_format_type_cd)=='SINT' AND TRIM(location_id)=='9300' AND (TRIM(warehouse_sizing_attribute_cd)=='WG8804' OR TRIM(warehouse_sizing_attribute_cd)=='WG8807' OR (TRIM(warehouse_sizing_attribute_cd)=='WG8801' AND (TRIM(shop_your_way_attribute_cd)=='' or shop_your_way_attribute_cd is null or TRIM(shop_your_way_attribute_cd)!='OS0001')))) OR (TRIM(location_format_type_cd)=='SINT' AND TRIM(location_id)=='9357' AND (TRIM(warehouse_sizing_attribute_cd)=='WG8806' OR TRIM(warehouse_sizing_attribute_cd)=='WG8807' OR TRIM(warehouse_sizing_attribute_cd)=='WG8808' OR TRIM(warehouse_sizing_attribute_cd)=='WG8810')) OR (TRIM(location_format_type_cd)=='SINT' AND TRIM(location_id)=='9305' AND (TRIM(warehouse_sizing_attribute_cd)=='WG8801' AND TRIM(shop_your_way_attribute_cd)=='OS0001'));
	  
work__idrp_sears_vendor_package_location_fltr_fltr_non_active = 
	FILTER work__idrp_sears_vendor_package_location_fltr_fltr
	BY TRIM(purchase_order_vendor_location_id) != '' OR TRIM(cross_merchandising_cd) == 'RIMFLOW';


netapp_ext_srs_vp_str = 
      FOREACH work__idrp_sears_vendor_package_location_fltr_fltr_non_active
      GENERATE
              vendor_package_id AS vendor_package_id,
              location_id AS location_id,
              location_format_type_cd AS location_format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              source_owner_cd AS source_owner_cd,
              active_ind AS active_ind,
              active_ind_change_dt AS active_ind_change_dt,
              allocation_replenishment_cd AS allocation_replenishment_cd,
              TRIM(purchase_order_vendor_location_id) AS purchase_order_vendor_location_id,
              replenishment_planning_ind AS replenishment_planning_ind,
              scan_based_trading_ind AS scan_based_trading_ind,
              source_location_id AS source_location_id,
              source_location_level_cd AS source_location_level_cd,
              source_package_qty  AS source_package_qty ,
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
              '0' AS dtc_begin_day_qt,
              '365' AS dtc_end_day_qt,
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
              item_reserve_cd AS item_reserve_cd,
              create_dt AS create_dt,
              last_update_dt AS last_update_dt,
	          rim_last_record_creation_dt AS rim_last_record_creation_dt,
              '$batchid' AS idrp_batch_id;

----------------------------------- Logic netapp_ext_srs_vp_loc -----------------------------------------

work__idrp_sears_vendor_package_location_data_fltr_2 = 
      FILTER gen_work__idrp_sears_vendor_package_location_data 
      BY (TRIM(location_level_cd)!='STORE' OR (TRIM(location_level_cd)=='STORE' AND TRIM(active_ind)=='Y'));


union_work_fltrd = 
      UNION work__idrp_sears_vendor_package_location_data_fltr_2,
            gen_work__idrp_sears_vendor_package_exploding_assortment_location_data,
            gen_work__idrp_sears_vendor_package_vendor_location_data;

unq_union_work_fltrd = 
	DISTINCT union_work_fltrd;


netapp_ext_srs_vp_loc = 
      FOREACH unq_union_work_fltrd
      GENERATE
              vendor_pack_id AS vendor_package_id,
              location_id AS location_id,
              location_format_type_cd AS location_format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              source_owner_cd AS source_owner_cd,
              active_ind AS active_ind,
              active_ind_change_dt AS active_ind_change_dt,
              allocation_replenishment_cd AS allocation_replenishment_cd,
              TRIM(purchase_order_vendor_location_id) AS purchase_order_vendor_location_id,
              replenishment_planning_ind AS replenishment_planning_ind,
              scan_based_trading_ind AS scan_based_trading_ind,
              source_location_id AS source_location_id,
              source_location_level_cd AS source_location_level_cd,
              source_package_qty  AS source_package_qty,
              vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              flow_type_cd AS flow_type_cd,
              import_ind AS import_ind,
              retail_carton_vendorpackage_id AS retail_carton_vendor_package_id,
              vendor_package_owner_cd AS vendor_package_owner_cd,
              vendor_stock_nbr AS vendor_stock_nbr,
              shc_item_id AS shc_item_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              can_carry_model_id AS can_carry_model_id,
              '0' AS dtc_begin_day_qt,
              '365' AS dtc_end_day_qt,
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
              item_reserve_cd AS item_reserve_cd,
              create_dt AS create_dt,
              last_update_dt AS last_update_dt,
	          rim_last_record_creation_dt AS rim_last_record_creation_dt,
              '$batchid' AS idrp_batch_id;


STORE netapp_ext_srs_vp_str 
INTO '$NETAPP_EXT_SRS_VP_STR_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

STORE netapp_ext_srs_vp_loc 
INTO '$NETAPP_EXT_SRS_VP_LOC_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

STORE union_work_idrp_gen 
INTO '$SMITH__IDRP_ELIGIBLE_SEARS_VENDOR_PACKAGE_LOCATION_CURRENT_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
