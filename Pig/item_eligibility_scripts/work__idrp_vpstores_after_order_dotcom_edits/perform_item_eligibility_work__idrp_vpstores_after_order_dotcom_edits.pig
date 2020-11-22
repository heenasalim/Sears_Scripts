/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_vpstores_after_order_dotcom_edits.pig
# AUTHOR NAME:         Onkar Malewadikar
# CREATION DATE:       Mon May 26 06:40:03 EDT 2014
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
#		 2015-02-19   Meghana		CR 3849
#		06/01/2015	nthadan		CR4542: changed query 
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

REGISTER $UDF_JAR;
DEFINE AddDays com.searshc.supplychain.idrp.udf.AddOrRemoveDaysToDate();
DEFINE GetCurrentDateWithoutTimestamp com.searshc.supplychain.idrp.udf.GetCurrentDateWithoutTimestamp();
SET default_parallel $NUM_PARALLEL;
set io.sort.mb 1024
set mapred.child.java.opts -Xmx4096m
set mapred.compress.map.output true
set pig.cachedbag.memusage 0.15
set io.sort.factor 100
set opt.multiquery false
SET mapred.min.split.size 5243000
SET pig.maxCombinedSplitSize 4000000


/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/


work__idrp_post_drop_ship_proc = LOAD '$WORK__IDRP_POST_DROP_SHIP_PROC_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($WORK__IDRP_POST_DROP_SHIP_PROC_SCHEMA);

/*
gold__geographic_location_master_data = LOAD '$GOLD__GEOGRAPHIC_LOCATION_MASTER_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__GEOGRAPHIC_LOCATION_MASTER_SCHEMA);
*/

smith__idrp_eligible_loc_data = LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);

/********************
We will perform the conversion for all records on work__idrp_post_drop_ship_proc using the following rules:
 
a.	If days_to_check_begin_days_cnt = 0, set days_to_check_begin_dt = '1970-01-01'

b.	If days_to_check_begin_days_cnt > 0, set days_to_check_begin_dt = AddDays('$CURRENT_DATE',(int)days_to_check_begin_days_cnt))

c.	If days_to_check_end_days_cnt = 365, set days_to_check_end_dt = '1970-01-01'

d.	If If days_to_check_end_days_cnt <> 365, set days_to_check_end_dt = AddDays('$CURRENT_DATE',(int)days_to_check_end_days_cnt))

Output: work__idrp_vp_dtc_date

********************/

work__idrp_vp_dtc_date = 
    FOREACH work__idrp_post_drop_ship_proc
    GENERATE
        shc_item_id  AS shc_item_id ,
        shc_item_desc AS shc_item_desc,
        shc_division_nbr AS shc_division_nbr,
        shc_division_desc AS shc_division_desc,
        shc_department_nbr AS shc_department_nbr,
        shc_department_desc AS shc_department_desc,
        shc_category_group_level_nbr AS shc_category_group_level_nbr,
        shc_category_group_desc AS shc_category_group_desc,
        shc_category_nbr AS shc_category_nbr,
        shc_category_desc AS shc_category_desc,
        shc_sub_category_nbr AS shc_sub_category_nbr,
        shc_sub_category_desc AS shc_sub_category_desc,
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
        delivered_direct_ind  AS delivered_direct_ind ,
        installation_ind  AS installation_ind ,
        store_forecast_cd AS store_forecast_cd,
        shc_item_type_cd AS shc_item_type_cd,
        item_purchase_status_cd AS item_purchase_status_cd,
        network_distribution_cd  AS network_distribution_cd ,
        future_network_distribution_cd  AS future_network_distribution_cd ,
        future_network_distribution_effective_dt  AS future_network_distribution_effective_dt ,
        jit_network_distribution_cd  AS jit_network_distribution_cd ,
        reorder_authentication_cd  AS reorder_authentication_cd ,
        can_carry_model_id  AS can_carry_model_id ,
        grocery_item_ind  AS grocery_item_ind ,
        shc_item_corporate_owner_cd  AS shc_item_corporate_owner_cd ,
        iplan_id  AS iplan_id ,
        markdown_style_reference_cd  AS markdown_style_reference_cd ,
        sears_order_system_cd AS sears_order_system_cd,
        idrp_order_method_cd AS idrp_order_method_cd,
        idrp_order_method_desc AS idrp_order_method_desc,
        forecast_group_format_id AS forecast_group_format_id,
        forecast_group_desc AS forecast_group_desc,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        ksn_id AS ksn_id,
        vendor_package_id AS vendor_package_id,
        vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
        vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
        vendor_package_owner_cd AS vendor_package_owner_cd,
        ksn_package_id AS ksn_package_id,
        service_area_restriction_model_id  AS service_area_restriction_model_id ,
        flow_type_cd AS flow_type_cd,
        aprk_id AS aprk_id,
        import_ind AS import_ind,
        order_duns_nbr AS order_duns_nbr,
        vendor_carton_qty AS vendor_carton_qty,
        vendor_stock_nbr AS vendor_stock_nbr,
        carton_per_layer_qty AS carton_per_layer_qty,
        layer_per_pallet_qty AS layer_per_pallet_qty,
		ksn_purchase_status_cd as ksn_purchase_status_cd,
		dotcom_allocation_ind as dotcom_allocation_ind,
        store_location_nbr AS store_location_nbr,
        days_to_check_begin_days_cnt AS days_to_check_begin_days_cnt,
        days_to_check_end_days_cnt AS days_to_check_end_days_cnt,
        (days_to_check_begin_days_cnt == '0' 
			? '1970-01-01' 
			: AddDays(GetCurrentDateWithoutTimestamp(),(int)days_to_check_begin_days_cnt)) AS days_to_check_begin_dt,
        (days_to_check_end_days_cnt == '365' 
			? '1970-01-01' 
			: AddDays(GetCurrentDateWithoutTimestamp(),(int)days_to_check_end_days_cnt)) AS days_to_check_end_dt;

/******************
a.	Join the work__idrp_vp_dtc_date to gold__geographic_location_master on store_location_nbr = location_nbr to get location_format_type_cd for the store location.
 
Output: work__idrp_vp_stores_with_fmttype
	
******************/

join_vp_dtc_date_geographic_location_master_ = 
    JOIN work__idrp_vp_dtc_date BY store_location_nbr,
         smith__idrp_eligible_loc_data BY loc USING 'replicated';
	
	 
work__idrp_vp_stores_with_fmttype = 		 
	FOREACH join_vp_dtc_date_geographic_location_master_
    GENERATE	
        shc_item_id  AS shc_item_id ,
        shc_item_desc AS shc_item_desc,
        shc_division_nbr AS shc_division_nbr,
        shc_division_desc AS shc_division_desc,
        shc_department_nbr AS shc_department_nbr,
        shc_department_desc AS shc_department_desc,
        shc_category_group_level_nbr AS shc_category_group_level_nbr,
        shc_category_group_desc AS shc_category_group_desc,
        shc_category_nbr AS shc_category_nbr,
        shc_category_desc AS shc_category_desc,
        shc_sub_category_nbr AS shc_sub_category_nbr,
        shc_sub_category_desc AS shc_sub_category_desc,
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
        delivered_direct_ind  AS delivered_direct_ind ,
        installation_ind  AS installation_ind ,
        store_forecast_cd AS store_forecast_cd,
        shc_item_type_cd AS shc_item_type_cd,
        item_purchase_status_cd AS item_purchase_status_cd,
        network_distribution_cd  AS network_distribution_cd ,
        future_network_distribution_cd  AS future_network_distribution_cd ,
        future_network_distribution_effective_dt  AS future_network_distribution_effective_dt ,
        jit_network_distribution_cd  AS jit_network_distribution_cd ,
        reorder_authentication_cd  AS reorder_authentication_cd ,
        can_carry_model_id  AS can_carry_model_id ,
        grocery_item_ind  AS grocery_item_ind ,
        shc_item_corporate_owner_cd  AS shc_item_corporate_owner_cd ,
        iplan_id  AS iplan_id ,
        markdown_style_reference_cd  AS markdown_style_reference_cd ,
        sears_order_system_cd AS sears_order_system_cd,
        idrp_order_method_cd AS idrp_order_method_cd,
        idrp_order_method_desc AS idrp_order_method_desc,
        forecast_group_format_id AS forecast_group_format_id,
        forecast_group_desc AS forecast_group_desc,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        ksn_id AS ksn_id,
        vendor_package_id AS vendor_package_id,
        vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
        vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
        vendor_package_owner_cd AS vendor_package_owner_cd,
        ksn_package_id AS ksn_package_id,
        service_area_restriction_model_id  AS service_area_restriction_model_id ,
        flow_type_cd AS flow_type_cd,
        aprk_id AS aprk_id,
        import_ind AS import_ind,
        order_duns_nbr AS order_duns_nbr,
        vendor_carton_qty AS vendor_carton_qty,
        vendor_stock_nbr AS vendor_stock_nbr,
        carton_per_layer_qty AS carton_per_layer_qty,
        layer_per_pallet_qty AS layer_per_pallet_qty,
		ksn_purchase_status_cd as ksn_purchase_status_cd,
		dotcom_allocation_ind as dotcom_allocation_ind,
        store_location_nbr AS store_location_nbr,
        days_to_check_begin_days_cnt AS days_to_check_begin_days_cnt,
        days_to_check_end_days_cnt AS days_to_check_end_days_cnt,
        days_to_check_begin_dt AS days_to_check_begin_dt,
        days_to_check_end_dt AS days_to_check_end_dt,
		loc_fmt_typ_cd as location_format_type_cd,
		fmt_typ_cd as format_type_cd,
		loc_lvl_cd as location_level_cd,
		loc_owner_cd as location_owner_cd,
		fmt_sub_typ_cd as format_sub_type_cd ;  /*added changes for CR4542*/
		
/**********************
The following edits will be performed against work__idrp_vp_stores_with_fmttype.  For this step, add an indicator to each record called valid_location_ind and default the value to .Y..  We will use this field for the process below so we can identify records that fail the edits and eliminate them at the end of the process.  Once a field fails an edit, it does not have to be checked for any further edits in this step.

Output: work__idrp_vp_stores_into_ordedits

**********************/

work__idrp_vp_stores_into_ordedits = 
    FOREACH work__idrp_vp_stores_with_fmttype
    GENERATE
        shc_item_id  AS shc_item_id ,
        shc_item_desc AS shc_item_desc,
        shc_division_nbr AS shc_division_nbr,
        shc_division_desc AS shc_division_desc,
        shc_department_nbr AS shc_department_nbr,
        shc_department_desc AS shc_department_desc,
        shc_category_group_level_nbr AS shc_category_group_level_nbr,
        shc_category_group_desc AS shc_category_group_desc,
        shc_category_nbr AS shc_category_nbr,
        shc_category_desc AS shc_category_desc,
        shc_sub_category_nbr AS shc_sub_category_nbr,
        shc_sub_category_desc AS shc_sub_category_desc,
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
        delivered_direct_ind  AS delivered_direct_ind ,
        installation_ind  AS installation_ind ,
        store_forecast_cd AS store_forecast_cd,
        shc_item_type_cd AS shc_item_type_cd,
        item_purchase_status_cd AS item_purchase_status_cd,
        network_distribution_cd  AS network_distribution_cd ,
        future_network_distribution_cd  AS future_network_distribution_cd ,
        future_network_distribution_effective_dt  AS future_network_distribution_effective_dt ,
        jit_network_distribution_cd  AS jit_network_distribution_cd ,
        reorder_authentication_cd  AS reorder_authentication_cd ,
        can_carry_model_id  AS can_carry_model_id ,
        grocery_item_ind  AS grocery_item_ind ,
        shc_item_corporate_owner_cd  AS shc_item_corporate_owner_cd ,
        iplan_id  AS iplan_id ,
        markdown_style_reference_cd  AS markdown_style_reference_cd ,
        sears_order_system_cd AS sears_order_system_cd,
        idrp_order_method_cd AS idrp_order_method_cd,
        idrp_order_method_desc AS idrp_order_method_desc,
        forecast_group_format_id AS forecast_group_format_id,
        forecast_group_desc AS forecast_group_desc,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        ksn_id AS ksn_id,
        vendor_package_id AS vendor_package_id,
        vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
        vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
        vendor_package_owner_cd AS vendor_package_owner_cd,
        ksn_package_id AS ksn_package_id,
        service_area_restriction_model_id  AS service_area_restriction_model_id ,
        flow_type_cd AS flow_type_cd,
        aprk_id AS aprk_id,
        import_ind AS import_ind,
        order_duns_nbr AS order_duns_nbr,
        vendor_carton_qty AS vendor_carton_qty,
        vendor_stock_nbr AS vendor_stock_nbr,
        carton_per_layer_qty AS carton_per_layer_qty,
        layer_per_pallet_qty AS layer_per_pallet_qty,
		ksn_purchase_status_cd as ksn_purchase_status_cd,
		dotcom_allocation_ind as dotcom_allocation_ind,
        store_location_nbr AS store_location_nbr,
        days_to_check_begin_days_cnt AS days_to_check_begin_days_cnt,
        days_to_check_end_days_cnt AS days_to_check_end_days_cnt,
        days_to_check_begin_dt AS days_to_check_begin_dt,
        days_to_check_end_dt AS days_to_check_end_dt,
		location_format_type_cd as location_format_type_cd,
		format_type_cd as format_type_cd,
		location_level_cd as location_level_cd,
		location_owner_cd as location_owner_cd,
		format_sub_type_cd as format_sub_type_cd, /*added changes for CR4542*/
		'Y' AS valid_location_ind ;

/*******************
a.	Ordering Edits are not applied to items with a corporate owner code of Kmart (shc_corporate_owner_cd = .K.) and shc division numbers (shc_division_nbr) 4 and 33, therefore split work__idrp_vp_stores_into_ordedits into two files.  The first file will contain records that have shc_corporate_owner_cd = .K. and shc_division_nbr in (4, 33).  The records that don.t meet the criteria will be in the second file. 

Output: work__idrp_kmart_div4_div33_vpstores, work__idrp_vpstores_divs_for_ordedits.

*******************/

work__idrp_kmart_div4_div33_vpstores =
    FILTER work__idrp_vp_stores_into_ordedits
    BY (shc_item_corporate_owner_cd == 'K' AND ((int)shc_division_nbr == 4 OR (int)shc_division_nbr == 33));


work__idrp_vpstores_divs_for_ordedits = 
    FILTER work__idrp_vp_stores_into_ordedits
    BY (shc_item_corporate_owner_cd != 'K' OR ((int)shc_division_nbr != 4 AND (int)shc_division_nbr != 33));

/******************
Set the valid_location_ind to .N. where the shc_item_corporate_owner_cd = .K. and the fmt_type_ cd <> .002.. 


ii.	Items with a corporate owner code of Both have specific edits to be performed. If shc_item_corporate_owner_cd = .B., then

1.	For records with a vendor pack owner code of Kmart, Sears stores are invalid for any shc division number that is not equal to 4.  Set the valid_location_ind to .N. where smith__idrp_vend_pack_combined.owner_cd = .K. and shc_division_nbr <> 4 and location_format_type_cd = .001..
 
2.	For records with a vendor pack owner code of Sears, the product must be migrated product.  Set the valid_location_ind to .N. where smith__idrp_vend_pack_combined.owner_cd = .S. and sears_order_system_cd <> .SAMS..

Output: work__idrp_ord_edited_vpstores.

******************/

 work__idrp_ord_edited_vpstores = 
    FOREACH work__idrp_vpstores_divs_for_ordedits
	GENERATE
        shc_item_id  AS shc_item_id ,
        shc_item_desc AS shc_item_desc,
        shc_division_nbr AS shc_division_nbr,
        shc_division_desc AS shc_division_desc,
        shc_department_nbr AS shc_department_nbr,
        shc_department_desc AS shc_department_desc,
        shc_category_group_level_nbr AS shc_category_group_level_nbr,
        shc_category_group_desc AS shc_category_group_desc,
        shc_category_nbr AS shc_category_nbr,
        shc_category_desc AS shc_category_desc,
        shc_sub_category_nbr AS shc_sub_category_nbr,
        shc_sub_category_desc AS shc_sub_category_desc,
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
        delivered_direct_ind  AS delivered_direct_ind ,
        installation_ind  AS installation_ind ,
        store_forecast_cd AS store_forecast_cd,
        shc_item_type_cd AS shc_item_type_cd,
        item_purchase_status_cd AS item_purchase_status_cd,
        network_distribution_cd  AS network_distribution_cd, 
        future_network_distribution_cd  AS future_network_distribution_cd ,
        future_network_distribution_effective_dt  AS future_network_distribution_effective_dt ,
        jit_network_distribution_cd  AS jit_network_distribution_cd ,
        reorder_authentication_cd  AS reorder_authentication_cd ,
        can_carry_model_id  AS can_carry_model_id ,
        grocery_item_ind  AS grocery_item_ind ,
        shc_item_corporate_owner_cd  AS shc_item_corporate_owner_cd ,
        iplan_id  AS iplan_id ,
        markdown_style_reference_cd  AS markdown_style_reference_cd ,
        sears_order_system_cd AS sears_order_system_cd,
        idrp_order_method_cd AS idrp_order_method_cd,
        idrp_order_method_desc AS idrp_order_method_desc,
        forecast_group_format_id AS forecast_group_format_id,
        forecast_group_desc AS forecast_group_desc,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        ksn_id AS ksn_id,
        vendor_package_id AS vendor_package_id,
        vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
        vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
        vendor_package_owner_cd AS vendor_package_owner_cd,
        ksn_package_id AS ksn_package_id,
        service_area_restriction_model_id  AS service_area_restriction_model_id ,
        flow_type_cd AS flow_type_cd,
        aprk_id AS aprk_id,
        import_ind AS import_ind,
        order_duns_nbr AS order_duns_nbr,
        vendor_carton_qty AS vendor_carton_qty,
        vendor_stock_nbr AS vendor_stock_nbr,
        carton_per_layer_qty AS carton_per_layer_qty,
        layer_per_pallet_qty AS layer_per_pallet_qty,
		ksn_purchase_status_cd as ksn_purchase_status_cd,
		dotcom_allocation_ind as dotcom_allocation_ind,
        store_location_nbr AS store_location_nbr,
        days_to_check_begin_days_cnt AS days_to_check_begin_days_cnt,
        days_to_check_end_days_cnt AS days_to_check_end_days_cnt,
        days_to_check_begin_dt AS days_to_check_begin_dt,
        days_to_check_end_dt AS days_to_check_end_dt,
		location_format_type_cd as location_format_type_cd,
		format_type_cd as format_type_cd,
		location_level_cd as location_level_cd,
		location_owner_cd as location_owner_cd,
		format_sub_type_cd as format_sub_type_cd, /*added changes for CR4542*/
        (shc_item_corporate_owner_cd == 'K' AND location_owner_cd == 'S' 
			? 'N' 
			: (days_to_check_end_dt < '$CURRENT_DATE' AND  days_to_check_end_dt != '1970-01-01' 
				?'N'
				:(shc_item_corporate_owner_cd == 'B' AND vendor_package_owner_cd == 'K' AND (int)shc_division_nbr != 4  AND location_owner_cd == 'S' 
					?  'N' 
					: (shc_item_corporate_owner_cd == 'B' AND vendor_package_owner_cd == 'S' AND sears_order_system_cd != 'SAMS' 
						? 'N' 
						: valid_location_ind)))) AS valid_location_ind;
       

/**************************
c.	Union work__idrp_ord_edited_vpstores with work__idrp_kmart_div4_div33_vpstores that bypassed the ordering edits to make a combined file.

Output: work__idrp_vpstores_after_ordedits.

**************************/
      		
work__idrp_vpstores_after_ordedits = 
    UNION work__idrp_ord_edited_vpstores,
          work__idrp_kmart_div4_div33_vpstores;

work__idrp_vpstores_after_dotcom_edits = 
	FOREACH work__idrp_vpstores_after_ordedits
    GENERATE
        shc_item_id  AS shc_item_id ,
        shc_item_desc AS shc_item_desc,
        shc_division_nbr AS shc_division_nbr,
        shc_division_desc AS shc_division_desc,
        shc_department_nbr AS shc_department_nbr,
        shc_department_desc AS shc_department_desc,
        shc_category_group_level_nbr AS shc_category_group_level_nbr,
        shc_category_group_desc AS shc_category_group_desc,
        shc_category_nbr AS shc_category_nbr,
        shc_category_desc AS shc_category_desc,
        shc_sub_category_nbr AS shc_sub_category_nbr,
        shc_sub_category_desc AS shc_sub_category_desc,
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
        delivered_direct_ind  AS delivered_direct_ind ,
        installation_ind  AS installation_ind ,
        store_forecast_cd AS store_forecast_cd,
        shc_item_type_cd AS shc_item_type_cd,
        item_purchase_status_cd AS item_purchase_status_cd,
        network_distribution_cd  AS network_distribution_cd ,
        future_network_distribution_cd  AS future_network_distribution_cd ,
        future_network_distribution_effective_dt  AS future_network_distribution_effective_dt ,
        jit_network_distribution_cd  AS jit_network_distribution_cd ,
        reorder_authentication_cd  AS reorder_authentication_cd ,
        can_carry_model_id  AS can_carry_model_id ,
        grocery_item_ind  AS grocery_item_ind ,
        shc_item_corporate_owner_cd  AS shc_item_corporate_owner_cd ,
        iplan_id  AS iplan_id ,
        markdown_style_reference_cd  AS markdown_style_reference_cd ,
        sears_order_system_cd AS sears_order_system_cd,
        idrp_order_method_cd AS idrp_order_method_cd,
        idrp_order_method_desc AS idrp_order_method_desc,
        forecast_group_format_id AS forecast_group_format_id,
        forecast_group_desc AS forecast_group_desc,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        ksn_id AS ksn_id,
        vendor_package_id AS vendor_package_id,
        vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
        vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
        vendor_package_owner_cd AS vendor_package_owner_cd,
        ksn_package_id AS ksn_package_id,
        service_area_restriction_model_id  AS service_area_restriction_model_id ,
        flow_type_cd AS flow_type_cd,
        aprk_id AS aprk_id,
        import_ind AS import_ind,
        order_duns_nbr AS order_duns_nbr,
        vendor_carton_qty AS vendor_carton_qty,
        vendor_stock_nbr AS vendor_stock_nbr,
        carton_per_layer_qty AS carton_per_layer_qty,
        layer_per_pallet_qty AS layer_per_pallet_qty,
		ksn_purchase_status_cd as ksn_purchase_status_cd,
		dotcom_allocation_ind as dotcom_allocation_ind,
        store_location_nbr AS store_location_nbr,
        days_to_check_begin_days_cnt AS days_to_check_begin_days_cnt,
        days_to_check_end_days_cnt AS days_to_check_end_days_cnt,
        days_to_check_begin_dt AS days_to_check_begin_dt,
        days_to_check_end_dt AS days_to_check_end_dt,
        location_format_type_cd as location_format_type_cd,
		format_type_cd as format_type_cd,
		location_level_cd as location_level_cd,
		location_owner_cd as location_owner_cd,
		format_sub_type_cd as format_sub_type_cd,
        ((location_format_type_cd == 'KINT' AND 
		 ((shc_item_corporate_owner_cd  != 'K' AND shc_item_corporate_owner_cd != 'B') OR vendor_package_owner_cd != 'K')) 
			? 'N' 
			: ((location_format_type_cd == 'SINT' AND 
			  ((shc_item_corporate_owner_cd != 'S' AND shc_item_corporate_owner_cd != 'B') OR vendor_package_owner_cd != 'S')) 
				? 'N' /*added changes for CR4542*/
				: (( grocery_item_ind == 'Y' AND format_type_cd == '002' AND  format_sub_type_cd == 'C')? 'N' : 
				valid_location_ind))) AS valid_location_ind;

filter_work__idrp_vpstores_after_dotcom_edits = 
     FILTER work__idrp_vpstores_after_dotcom_edits
     BY valid_location_ind == 'Y';

work__idrp_vpstores_after_order_dotcom_edits = 
    FOREACH filter_work__idrp_vpstores_after_dotcom_edits
    GENERATE
		'$CURRENT_TIMESTAMP' AS	load_ts	,
        shc_item_id  AS shc_item_id ,
        shc_item_desc AS shc_item_desc,
        shc_division_nbr AS shc_division_nbr,
        shc_division_desc AS shc_division_desc,
        shc_department_nbr AS shc_department_nbr,
        shc_department_desc AS shc_department_desc,
        shc_category_group_level_nbr AS shc_category_group_level_nbr,
        shc_category_group_desc AS shc_category_group_desc,
        shc_category_nbr AS shc_category_nbr,
        shc_category_desc AS shc_category_desc,
        shc_sub_category_nbr AS shc_sub_category_nbr,
        shc_sub_category_desc AS shc_sub_category_desc,
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
        delivered_direct_ind  AS delivered_direct_ind ,
        installation_ind  AS installation_ind ,
        store_forecast_cd AS store_forecast_cd,
        shc_item_type_cd AS shc_item_type_cd,
        item_purchase_status_cd AS item_purchase_status_cd,
        network_distribution_cd  AS network_distribution_cd ,
        future_network_distribution_cd  AS future_network_distribution_cd ,
        future_network_distribution_effective_dt  AS future_network_distribution_effective_dt ,
        jit_network_distribution_cd  AS jit_network_distribution_cd ,
        reorder_authentication_cd  AS reorder_authentication_cd ,
        can_carry_model_id  AS can_carry_model_id ,
        grocery_item_ind  AS grocery_item_ind ,
        shc_item_corporate_owner_cd  AS shc_item_corporate_owner_cd ,
        iplan_id  AS iplan_id ,
        markdown_style_reference_cd  AS markdown_style_reference_cd ,
        sears_order_system_cd AS sears_order_system_cd,
        idrp_order_method_cd AS idrp_order_method_cd,
        idrp_order_method_desc AS idrp_order_method_desc,
        forecast_group_format_id AS forecast_group_format_id,
        forecast_group_desc AS forecast_group_desc,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        ksn_id AS ksn_id,
        vendor_package_id AS vendor_package_id,
        vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
        vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
        vendor_package_owner_cd AS vendor_package_owner_cd,
        ksn_package_id AS ksn_package_id,
        service_area_restriction_model_id  AS service_area_restriction_model_id ,
        flow_type_cd AS flow_type_cd,
        aprk_id AS aprk_id,
        import_ind AS import_ind,
        order_duns_nbr AS order_duns_nbr,
        vendor_carton_qty AS vendor_carton_qty,
        vendor_stock_nbr AS vendor_stock_nbr,
        carton_per_layer_qty AS carton_per_layer_qty,
        layer_per_pallet_qty AS layer_per_pallet_qty,
		ksn_purchase_status_cd as ksn_purchase_status_cd,
		dotcom_allocation_ind as dotcom_allocation_ind,
        store_location_nbr AS store_location_nbr,
        days_to_check_begin_days_cnt AS days_to_check_begin_days_cnt,
        days_to_check_end_days_cnt AS days_to_check_end_days_cnt,
        days_to_check_begin_dt AS days_to_check_begin_dt,
        days_to_check_end_dt AS days_to_check_end_dt,
		location_format_type_cd as location_format_type_cd,
		format_type_cd as format_type_cd,
		location_level_cd as location_level_cd,
		location_owner_cd as location_owner_cd,
		'$batchid'	AS	idrp_batch_id ;


STORE work__idrp_vpstores_after_order_dotcom_edits 
INTO '$WORK__IDRP_VPSTORES_AFTER_ORDER_DOTCOM_EDITS_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
