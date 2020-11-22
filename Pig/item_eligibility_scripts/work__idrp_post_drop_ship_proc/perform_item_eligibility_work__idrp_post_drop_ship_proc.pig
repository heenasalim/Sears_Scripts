/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_post_drop_ship_proc.pig
# AUTHOR NAME:         Onkar Malewadikar
# CREATION DATE:       Mon May 26 06:32:37 EDT 2014
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
#
#
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
--SET mapred.min.split.size 5243000
--SET pig.maxCombinedSplitSize 4000000


/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

work__idrp_vp_catgrvw_stores_after_legal_exmp = LOAD '$WORK__IDRP_VP_CATGRVW_STORES_AFTER_LEGAL_EXMP_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($WORK__IDRP_VP_CATGRVW_STORES_AFTER_LEGAL_EXMP_SCHEMA);


work__idrp_vp_cancarry_stores = LOAD '$WORK__IDRP_VP_EXAS_DOTCOM_CANCARRY_STORES_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($WORK__IDRP_VP_CANCARRY_STORES_SCHEMA);

smith__idrp_online_drop_ship_items_data  = LOAD '$SMITH__IDRP_ONLINE_DROP_SHIP_ITEMS_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ONLINE_DROP_SHIP_ITEMS_SCHEMA);
/****************
Union of work__idrp_vp_catgrvw_stores_after_legal_exmp (results file from Processing of Items in Category Review) with work__idrp_vp_cancarry_stores (Get Vendor Pack Store List For Items not in Category Review) to create a combined file of all vendor pack / store location records.

Output: work__idrp_combined_into_drop_ship_proc.

****************/

work__idrp_combined_into_drop_ship_proc = 
    UNION work__idrp_vp_catgrvw_stores_after_legal_exmp,
	      work__idrp_vp_cancarry_stores;
	


filter_work__idrp_combined_into_drop_ship_proc = 
    FILTER work__idrp_combined_into_drop_ship_proc
	BY vendor_package_purchase_status_cd == 'A';

/*****************
Select distinct
shc_item_id,shc_item_desc,shc_division_nbr,shc_division_desc,shc_department_nbr,shc_department_desc,shc_category_group_level_nbr,shc_category_group_desc,shc_category_nbr,shc_category_desc,shc_sub_category_nbr,shc_sub_category_desc,sears_business_nbr,sears_business_desc,sears_division_nbr,sears_division_desc,sears_line_nbr,sears_line_desc,sears_sub_line_nbr,sears_sub_line_desc,sears_class_nbr,sears_class_desc,sears_item_nbr,sears_sku_nbr,delivered_direct_ind,installation_id,store_forecast_cd,shc_item_type_cd,item_purchase_status_cd,network_distribution_cd,future_network_distribution_cd,future_network_distribution_effective_dt,jit_network_distribution_cd,reorder_authentication_cd,can_carry_model_id,grocery_item_ind,shc_item_corporate_owner_cd,iplan_id,markdown_style_reference_cd,sears_order_system_cd,idrp_order_method_cd,idrp_order_method_desc,forecast_group_format_id,forecast_group_desc,dotcom_eligibility_cd,ksn_id,vendor_package_id,vendor_package_purchase_status_cd,vendor_package_purchase_status_dt,vendor_package_owner_cd,ksn_package_id,service_area_restriction_model_id,flow_type_cd,aprk_id,import_ind,order_duns_nbr,vendor_carton_qty,vendor_stock_nbr,carton_per_layer_qty,layer_per_pallet_qty,ksn_purchase_status_cd ,dotcom_allocation_ind,
Case    When service_area_restriction_model_id  = 46162 Then 7800
        Else 9300 as store_location_nbr,
0 as days_to_check_begin_days_cnt,
365 as days_to_check_end_days_cnt
from work__idrp_combined_into_drop_ship_proc
  join smith__idrp_online_drop_ship_items on
work__idrp_combined_into_drop_ship_proc.shc_item_id = smith__idrp_online_drop_ship_items.item_id and service_area_restriction_model_id
where vendor_package_purchase_status_cd = .A.;

Output: work__idrp_drop_ship_vp_locs.

*****************/

join_combined_into_drop_ship_online_drop_ship = 
    JOIN filter_work__idrp_combined_into_drop_ship_proc BY (shc_item_id,service_area_restriction_model_id),
	     smith__idrp_online_drop_ship_items_data BY (item_id,service_area_restriction_model_id)  PARALLEL $NUM_PARALLEL;

work__idrp_drop_ship_vp_locs = 		 
    FOREACH join_combined_into_drop_ship_online_drop_ship
	GENERATE
        shc_item_id AS shc_item_id,
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
        delivered_direct_ind AS delivered_direct_ind,
        installation_ind AS installation_ind,
        store_forecast_cd AS store_forecast_cd,
        shc_item_type_cd AS shc_item_type_cd,
        item_purchase_status_cd AS item_purchase_status_cd,
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
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        ksn_id AS ksn_id,
        vendor_package_id AS vendor_package_id,
        vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
        vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
        vendor_package_owner_cd AS vendor_package_owner_cd,
        ksn_package_id AS ksn_package_id,
        filter_work__idrp_combined_into_drop_ship_proc::service_area_restriction_model_id AS service_area_restriction_model_id,
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
        (filter_work__idrp_combined_into_drop_ship_proc::service_area_restriction_model_id  == '46162' ? '7800' : '9300') as store_location_nbr,
        '0' as days_to_check_begin_days_cnt,
        '365' as days_to_check_end_days_cnt;

/*****************
	Perform a Union of work__idrp_combined_into_drop_ship_proc and work__idrp_drop_ship_vp_locs to create a combined file.  Note: the drop-ship vendor package / drop-ship location records should only be added to the existing data if there isn.t an existing record for that vendor package / store location.   

 Output: work__idrp_post_drop_ship_proc.

*****************/

join_work__idrp_post_drop_ship_proc = 
	JOIN work__idrp_combined_into_drop_ship_proc
		BY (vendor_package_id, store_location_nbr)
		   LEFT OUTER,
		 work__idrp_drop_ship_vp_locs
		BY (vendor_package_id, store_location_nbr);

filter_join_work__idrp_post_drop_ship_proc = 
	FILTER join_work__idrp_post_drop_ship_proc
	BY (IsNull(work__idrp_combined_into_drop_ship_proc::vendor_package_id,'') == '');
	
gen_filter_join_work__idrp_post_drop_ship_proc = 
	FOREACH filter_join_work__idrp_post_drop_ship_proc
	GENERATE
		work__idrp_drop_ship_vp_locs::shc_item_id AS shc_item_id,
		work__idrp_drop_ship_vp_locs::shc_item_desc AS shc_item_desc,
		work__idrp_drop_ship_vp_locs::shc_division_nbr AS shc_division_nbr,
		work__idrp_drop_ship_vp_locs::shc_division_desc AS shc_division_desc,
		work__idrp_drop_ship_vp_locs::shc_department_nbr AS shc_department_nbr,
		work__idrp_drop_ship_vp_locs::shc_department_desc AS shc_department_desc,
		work__idrp_drop_ship_vp_locs::shc_category_group_level_nbr AS shc_category_group_level_nbr,
		work__idrp_drop_ship_vp_locs::shc_category_group_desc AS shc_category_group_desc,
		work__idrp_drop_ship_vp_locs::shc_category_nbr AS shc_category_nbr,
		work__idrp_drop_ship_vp_locs::shc_category_desc AS shc_category_desc,
		work__idrp_drop_ship_vp_locs::shc_sub_category_nbr AS shc_sub_category_nbr,
		work__idrp_drop_ship_vp_locs::shc_sub_category_desc AS shc_sub_category_desc,
		work__idrp_drop_ship_vp_locs::sears_business_nbr AS sears_business_nbr,
		work__idrp_drop_ship_vp_locs::sears_business_desc AS sears_business_desc,
		work__idrp_drop_ship_vp_locs::sears_division_nbr AS sears_division_nbr,
		work__idrp_drop_ship_vp_locs::sears_division_desc AS sears_division_desc,
		work__idrp_drop_ship_vp_locs::sears_line_nbr AS sears_line_nbr,
		work__idrp_drop_ship_vp_locs::sears_line_desc AS sears_line_desc,
		work__idrp_drop_ship_vp_locs::sears_sub_line_nbr AS sears_sub_line_nbr,
		work__idrp_drop_ship_vp_locs::sears_sub_line_desc AS sears_sub_line_desc,
		work__idrp_drop_ship_vp_locs::sears_class_nbr AS sears_class_nbr,
		work__idrp_drop_ship_vp_locs::sears_class_desc AS sears_class_desc,
		work__idrp_drop_ship_vp_locs::sears_item_nbr AS sears_item_nbr,
		work__idrp_drop_ship_vp_locs::sears_sku_nbr AS sears_sku_nbr,
		work__idrp_drop_ship_vp_locs::delivered_direct_ind AS delivered_direct_ind,
		work__idrp_drop_ship_vp_locs::installation_ind AS installation_ind,
		work__idrp_drop_ship_vp_locs::store_forecast_cd AS store_forecast_cd,
		work__idrp_drop_ship_vp_locs::shc_item_type_cd AS shc_item_type_cd,
		work__idrp_drop_ship_vp_locs::item_purchase_status_cd AS item_purchase_status_cd,
		work__idrp_drop_ship_vp_locs::network_distribution_cd AS network_distribution_cd,
		work__idrp_drop_ship_vp_locs::future_network_distribution_cd AS future_network_distribution_cd,
		work__idrp_drop_ship_vp_locs::future_network_distribution_effective_dt AS future_network_distribution_effective_dt,
		work__idrp_drop_ship_vp_locs::jit_network_distribution_cd AS jit_network_distribution_cd,
		work__idrp_drop_ship_vp_locs::reorder_authentication_cd AS reorder_authentication_cd,
		work__idrp_drop_ship_vp_locs::can_carry_model_id AS can_carry_model_id,
		work__idrp_drop_ship_vp_locs::grocery_item_ind AS grocery_item_ind,
		work__idrp_drop_ship_vp_locs::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
		work__idrp_drop_ship_vp_locs::iplan_id AS iplan_id,
		work__idrp_drop_ship_vp_locs::markdown_style_reference_cd AS markdown_style_reference_cd,
		work__idrp_drop_ship_vp_locs::sears_order_system_cd AS sears_order_system_cd,
		work__idrp_drop_ship_vp_locs::idrp_order_method_cd AS idrp_order_method_cd,
		work__idrp_drop_ship_vp_locs::idrp_order_method_desc AS idrp_order_method_desc,
		work__idrp_drop_ship_vp_locs::forecast_group_format_id AS forecast_group_format_id,
		work__idrp_drop_ship_vp_locs::forecast_group_desc AS forecast_group_desc,
		work__idrp_drop_ship_vp_locs::dotcom_eligibility_cd AS dotcom_eligibility_cd,
		work__idrp_drop_ship_vp_locs::ksn_id AS ksn_id,
		work__idrp_drop_ship_vp_locs::vendor_package_id AS vendor_package_id,
		work__idrp_drop_ship_vp_locs::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
		work__idrp_drop_ship_vp_locs::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
		work__idrp_drop_ship_vp_locs::vendor_package_owner_cd AS vendor_package_owner_cd,
		work__idrp_drop_ship_vp_locs::ksn_package_id AS ksn_package_id,
		work__idrp_drop_ship_vp_locs::service_area_restriction_model_id AS service_area_restriction_model_id,
		work__idrp_drop_ship_vp_locs::flow_type_cd AS flow_type_cd,
		work__idrp_drop_ship_vp_locs::aprk_id AS aprk_id,
		work__idrp_drop_ship_vp_locs::import_ind AS import_ind,
		work__idrp_drop_ship_vp_locs::order_duns_nbr AS order_duns_nbr,
		work__idrp_drop_ship_vp_locs::vendor_carton_qty AS vendor_carton_qty,
		work__idrp_drop_ship_vp_locs::vendor_stock_nbr AS vendor_stock_nbr,
		work__idrp_drop_ship_vp_locs::carton_per_layer_qty AS carton_per_layer_qty,
		work__idrp_drop_ship_vp_locs::layer_per_pallet_qty AS layer_per_pallet_qty,
		work__idrp_drop_ship_vp_locs::ksn_purchase_status_cd AS ksn_purchase_status_cd,
		work__idrp_drop_ship_vp_locs::dotcom_allocation_ind AS dotcom_allocation_ind,
		work__idrp_drop_ship_vp_locs::store_location_nbr AS store_location_nbr,
		work__idrp_drop_ship_vp_locs::days_to_check_begin_days_cnt AS days_to_check_begin_days_cnt,
		work__idrp_drop_ship_vp_locs::days_to_check_end_days_cnt AS days_to_check_end_days_cnt;
	
work__idrp_post_drop_ship_proc = 
    UNION work__idrp_combined_into_drop_ship_proc,
          gen_filter_join_work__idrp_post_drop_ship_proc;
		  
STORE work__idrp_post_drop_ship_proc  INTO '$WORK__IDRP_POST_DROP_SHIP_PROC_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');




/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
