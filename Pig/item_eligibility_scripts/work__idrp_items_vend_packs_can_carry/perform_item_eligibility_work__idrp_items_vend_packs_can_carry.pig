/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_items_vend_packs_can_carry.pig
# AUTHOR NAME:         Onkar Malewadikar
# CREATION DATE:       Mon May 26 05:51:48 EDT 2014
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
#   18-03-2016    Megha Bhalerao    CR#102558 moving the selection of sears item related fields from smith__idrp_shc_item_combined to smith__idrp_vend_pack_combined.
#   2020/04/07    Rajani Galpalli   IPS-4837: Added filter to remove process to create item / locations for items in category review	
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/


REGISTER $UDF_JAR;
DEFINE AddDays com.searshc.supplychain.idrp.udf.AddOrRemoveDaysToDate();
SET default_parallel $NUM_PARALLEL;
--set io.sort.mb 1024
--set mapred.child.java.opts -Xmx4096m
--set mapred.compress.map.output true
--set pig.cachedbag.memusage 0.15
--set io.sort.factor 100
--set opt.multiquery false
--SET mapred.min.split.size 5120000
--SET pig.maxCombinedSplitSize 4000000

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

smith__idrp_shc_item_combined_data = LOAD '$SMITH__IDRP_SHC_ITEM_COMBINED_LOCATION' USING PigStorage ('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_SHC_ITEM_COMBINED_SCHEMA);

smith__idrp_item_eligibility_batchdate_data = LOAD '$SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_SCHEMA);

smith__idrp_vend_pack_combined_data = LOAD '$SMITH__IDRP_VEND_PACK_COMBINED_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_VEND_PACK_COMBINED_SCHEMA);


/********* Get the list of items that will be processed from smith__idrp_shc_item_combined where idrp_order_method_cd = (.A. or .R.) and  shc_item_type_cd  in (.EXAS., .IIRC., .INVC., .TYP.) and purchase_status_cd <> .U. and (shc_item_corporate_owner_cd in (.K., .B.) or (shc_item_corporate_owner_cd = .S. and sears_order_system_cd = .SAMS.)). *******/
filter_smith__idrp_shc_item_combined_data = 
    FILTER smith__idrp_shc_item_combined_data 
	BY (idrp_order_method_cd IS NOT NULL AND shc_item_type_cd IS NOT NULL AND purchase_status_cd IS NOT NULL AND shc_item_corporate_owner_cd IS NOT NULL ) 
	    AND 
		(idrp_order_method_cd == 'A' OR idrp_order_method_cd == 'R') 
	    AND 
	   (shc_item_type_cd == 'EXAS' OR shc_item_type_cd == 'IIRC' OR shc_item_type_cd ==  'INVC' OR shc_item_type_cd == 'TYP') 
	    AND 
	   (purchase_status_cd != 'U') 
	    AND 
	   (shc_item_corporate_owner_cd == 'K' OR  shc_item_corporate_owner_cd == 'B' OR (shc_item_corporate_owner_cd == 'S' AND sears_order_system_cd == 'SAMS'))
	   AND ((int)can_carry_model_id != 76002);  --IPS-4837:added


/******** STORING DATA TO work__idrp_items ******/
--CR#102558 Removed sears item related fields
work__idrp_items = 	
    FOREACH filter_smith__idrp_shc_item_combined_data
    GENERATE 
        (shc_item_id is NULL ? '':shc_item_id) AS shc_item_id,
        shc_item_desc AS shc_item_desc,
        shc_division_nbr  AS shc_division_nbr ,
        shc_division_desc AS shc_division_desc,
        shc_department_nbr AS shc_department_nbr,
        shc_department_desc AS shc_department_desc,
        shc_category_group_level_nbr AS shc_category_group_level_nbr,
        shc_category_group_desc AS shc_category_group_desc,
        shc_category_nbr AS shc_category_nbr,
        shc_category_desc AS shc_category_desc,
        shc_sub_category_nbr AS shc_sub_category_nbr,
        shc_sub_category_desc AS shc_sub_category_desc,
        delivered_direct_ind AS delivered_direct_ind,
        installation_ind AS installation_ind,
        store_forecast_cd AS store_forecast_cd,
        shc_item_type_cd AS shc_item_type_cd,
        purchase_status_cd AS item_purchase_status_cd,
        network_distribution_cd AS network_distribution_cd,
        future_network_distribution_cd AS future_network_distribution_cd,
        future_network_distribution_effective_dt AS future_network_distribution_effective_dt,
        jit_network_distribution_cd AS jit_network_distribution_cd,
        reorder_authentication_cd AS reorder_authentication_cd,
        can_carry_model_id AS can_carry_model_id,
        grocery_item_ind AS grocery_item_ind,
        shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        iplan_id AS iplan_id,
        markdown_style_reference_cd AS markdown_style_reference_cd,
        sears_order_system_cd AS sears_order_system_cd,
        idrp_order_method_cd AS idrp_order_method_cd,
        idrp_order_method_desc AS idrp_order_method_desc,
        forecast_group_format_id AS forecast_group_format_id,
        forecast_group_desc AS forecast_group_desc,
        referred_ksn_dotcom_eligibility_cd AS dotcom_eligibility_cd;     

--STORE work__idrp_items INTO './d/work__idrp_items' USING PigStorage('\u0001');

/******************* Join smith__idrp_item_eligibility_batchdate with smith__idrp_vend_pack_combined with no criteria(CROSS JOIN).  Then filter the results of the join where the smith__idrp_item_eligibility_batchdate.processing_timestamp is between the smith__idrp_vend_pack_combined.effective_ts and smith__idrp_vend_pack_combined.expiration_ts.   ************************/
		
join_replenishment_day_and_vend_pack_combined =
	CROSS 	
		smith__idrp_item_eligibility_batchdate_data,	
		smith__idrp_vend_pack_combined_data;

filter_on_timestamp_join_replenishment_day_and_vend_pack_combined = 		
    FILTER join_replenishment_day_and_vend_pack_combined 
	BY (TRIM(smith__idrp_item_eligibility_batchdate_data::processing_ts) >= TRIM(smith__idrp_vend_pack_combined_data::effective_ts) AND
        TRIM(smith__idrp_item_eligibility_batchdate_data::processing_ts) <= TRIM(smith__idrp_vend_pack_combined_data::expiration_ts));


/******* STORING DATA TO work__idrp_filtered_vend_packs ******/

work__idrp_filtered_vend_packs = 
    FOREACH filter_on_timestamp_join_replenishment_day_and_vend_pack_combined
    GENERATE	
	vendor_package_id AS vendor_package_id,
--CR#102558
	    sears_business_nbr AS sears_business_nbr,
        sears_business_desc AS sears_business_desc,
        sears_division_nbr AS sears_division_nbr,
        sears_division_desc AS sears_division_desc,
        sears_line_nbr AS sears_line_nbr,
        sears_line_desc AS sears_line_desc,
        sears_sub_line_nbr AS sears_sub_line_nbr,
        sears_sub_line_desc AS sears_sub_line_desc,
        sears_class_nbr AS sears_class_nbr,
        sears_class_desc AS sears_class_desc,
        sears_item_nbr AS sears_item_nbr,
        sears_sku_nbr AS sears_sku_nbr,
--CR#102558
        aprk_id AS aprk_id,
        carton_per_layer_qty AS carton_per_layer_qty,
        flow_type_cd AS flow_type_cd,
        import_cd AS import_cd,
        ksn_id AS ksn_id,
        ksn_package_id AS ksn_package_id,
        layer_per_pallet_qty AS layer_per_pallet_qty,
        order_duns_nbr AS order_duns_nbr,
        owner_cd AS owner_cd,
        purchase_status_cd AS purchase_status_cd,
        purchase_status_dt AS purchase_status_dt,
        service_area_restriction_model_id AS service_area_restriction_model_id,
        vendor_carton_qty AS vendor_carton_qty,
        vendor_stock_nbr AS vendor_stock_nbr,
		(shc_item_id is NULL ? '': shc_item_id) AS shc_item_id,
		ksn_purchase_status_cd as ksn_purchase_status_cd,
		dotcom_allocation_ind as dotcom_allocation_ind;

/*************** join work__idrp_items to work__idrp_filtered_vend_packs on shc_item_id.  The result of this step is a list of item and vendor packs.  Note: all columns selected from work__idrp_filtered_vend_packs are direct moves except import_cd.  If the import_cd = .I., set the import_ind = .1., otherwise if the import_cd is <> .I. (or if NULL) set the import_ind = .0..   *******************/

join_work__idrp_items_to_work__idrp_filtered_vend_packs =
    JOIN work__idrp_items BY shc_item_id,
	 work__idrp_filtered_vend_packs BY shc_item_id PARALLEL $NUM_PARALLEL;



work__idrp_items_vend_packs_1 = 
    FOREACH join_work__idrp_items_to_work__idrp_filtered_vend_packs
	GENERATE 
		'$CURRENT_TIMESTAMP' AS load_ts	,
        work__idrp_items::shc_item_id AS shc_item_id,
        work__idrp_items::shc_item_desc AS shc_item_desc,
        work__idrp_items::shc_division_nbr AS shc_division_nbr,
        work__idrp_items::shc_division_desc AS shc_division_desc,
        work__idrp_items::shc_department_nbr AS shc_department_nbr,
        work__idrp_items::shc_department_desc AS shc_department_desc,
        work__idrp_items::shc_category_group_level_nbr AS shc_category_group_level_nbr,
        work__idrp_items::shc_category_group_desc AS shc_category_group_desc,
        work__idrp_items::shc_category_nbr AS shc_category_nbr,
        work__idrp_items::shc_category_desc AS shc_category_desc,
        work__idrp_items::shc_sub_category_nbr AS shc_sub_category_nbr,
        work__idrp_items::shc_sub_category_desc AS shc_sub_category_desc,
--CR#102558
        work__idrp_filtered_vend_packs::sears_business_nbr AS sears_business_nbr,
        work__idrp_filtered_vend_packs::sears_business_desc AS sears_business_desc,
        work__idrp_filtered_vend_packs::sears_division_nbr AS sears_division_nbr,
        work__idrp_filtered_vend_packs::sears_division_desc AS sears_division_desc,
        work__idrp_filtered_vend_packs::sears_line_nbr AS sears_line_nbr,
        work__idrp_filtered_vend_packs::sears_line_desc AS sears_line_desc,
        work__idrp_filtered_vend_packs::sears_sub_line_nbr AS sears_sub_line_nbr,
        work__idrp_filtered_vend_packs::sears_sub_line_desc AS sears_sub_line_desc,
        work__idrp_filtered_vend_packs::sears_class_nbr AS sears_class_nbr,
        work__idrp_filtered_vend_packs::sears_class_desc AS sears_class_desc,
        work__idrp_filtered_vend_packs::sears_item_nbr AS sears_item_nbr,
        work__idrp_filtered_vend_packs::sears_sku_nbr AS sears_sku_nbr,
--CR#102558
        work__idrp_items::delivered_direct_ind  AS delivered_direct_ind,
        work__idrp_items::installation_ind  AS installation_ind,
        work__idrp_items::store_forecast_cd AS store_forecast_cd,
        work__idrp_items::shc_item_type_cd AS shc_item_type_cd,
        work__idrp_items::item_purchase_status_cd AS item_purchase_status_cd,
        work__idrp_items::network_distribution_cd  AS network_distribution_cd,
        work__idrp_items::future_network_distribution_cd  AS future_network_distribution_cd,
        work__idrp_items::future_network_distribution_effective_dt  AS future_network_distribution_effective_dt,
        work__idrp_items::jit_network_distribution_cd  AS jit_network_distribution_cd,
        work__idrp_items::reorder_authentication_cd AS reorder_authentication_cd,
        work__idrp_items::can_carry_model_id AS can_carry_model_id,
        work__idrp_items::grocery_item_ind AS grocery_item_ind,
        work__idrp_items::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        work__idrp_items::iplan_id  AS iplan_id,
        work__idrp_items::markdown_style_reference_cd  AS markdown_style_reference_cd,
        work__idrp_items::sears_order_system_cd AS sears_order_system_cd,
        work__idrp_items::idrp_order_method_cd AS idrp_order_method_cd,
        work__idrp_items::idrp_order_method_desc AS idrp_order_method_desc,
        work__idrp_items::forecast_group_format_id  AS forecast_group_format_id,
        work__idrp_items::forecast_group_desc AS forecast_group_desc,
        work__idrp_items::dotcom_eligibility_cd AS dotcom_eligibility_cd,
        work__idrp_filtered_vend_packs::ksn_id AS ksn_id,
        work__idrp_filtered_vend_packs::vendor_package_id AS vendor_package_id,
        work__idrp_filtered_vend_packs::purchase_status_cd AS vendor_package_purchase_status_cd,
        work__idrp_filtered_vend_packs::purchase_status_dt AS vendor_package_purchase_status_dt,
        work__idrp_filtered_vend_packs::owner_cd AS vendor_package_owner_cd,
        work__idrp_filtered_vend_packs::ksn_package_id AS ksn_package_id,
        (work__idrp_filtered_vend_packs::service_area_restriction_model_id IS NULL ? '0' : work__idrp_filtered_vend_packs::service_area_restriction_model_id) AS service_area_restriction_model_id,
        work__idrp_filtered_vend_packs::flow_type_cd AS flow_type_cd,
        work__idrp_filtered_vend_packs::aprk_id AS aprk_id,
        (work__idrp_filtered_vend_packs::import_cd IS NOT NULL AND work__idrp_filtered_vend_packs::import_cd == 'I' ?  '1' : '0') AS import_ind,
        work__idrp_filtered_vend_packs::order_duns_nbr AS order_duns_nbr,
        work__idrp_filtered_vend_packs::vendor_carton_qty AS vendor_carton_qty,
        work__idrp_filtered_vend_packs::vendor_stock_nbr AS vendor_stock_nbr,
        work__idrp_filtered_vend_packs::carton_per_layer_qty AS carton_per_layer_qty,
        work__idrp_filtered_vend_packs::layer_per_pallet_qty AS layer_per_pallet_qty,
		work__idrp_filtered_vend_packs::ksn_purchase_status_cd as ksn_purchase_status_cd,
		work__idrp_filtered_vend_packs::dotcom_allocation_ind as dotcom_allocation_ind,
		'$batchid' AS idrp_batch_id 	;


work__idrp_items_vend_packs = FILTER work__idrp_items_vend_packs_1 BY (sears_order_system_cd == 'SAMS' OR vendor_package_owner_cd != 'S');

/*********a.	Split work__idrp_items_vend_packs into two files.
i.	The first file will contain all item / vendor pack records for items that are in category review (can_carry_model_id = 76002).  This file will be used as input into the .Processing of Items in Category Review. section below.

Output: work__idrp_items_vend_packs_catg_rvw.
 
ii.	The second file will contain all item / vendor pack records for items that are not in category review (can_carry_model_id <> 76002).  This file will be used as input into the .Create Authorized Vendor Package / Store File (SGDTN Clone). section below.

Output: work__idrp_items_vend_packs_can_carry.
   *************/

work__idrp_items_vend_packs_catg_rvw =
    FILTER work__idrp_items_vend_packs
    BY (int)can_carry_model_id == 76002;

STORE work__idrp_items_vend_packs_catg_rvw 
INTO '$WORK__IDRP_ITEMS_VEND_PACKS_CATG_RVW_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

work__idrp_items_vend_packs_can_carry =
    FILTER work__idrp_items_vend_packs
    BY (int)can_carry_model_id != 76002 ;
	
STORE work__idrp_items_vend_packs_can_carry 
INTO '$WORK__IDRP_ITEMS_VEND_PACKS_CAN_CARRY_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/

