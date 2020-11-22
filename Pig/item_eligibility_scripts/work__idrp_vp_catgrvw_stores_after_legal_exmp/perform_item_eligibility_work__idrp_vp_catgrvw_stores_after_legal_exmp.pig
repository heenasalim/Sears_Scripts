/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_vp_catgrvw_stores_after_legal_exmp.pig
# AUTHOR NAME:         Onkar Malewadikar
# CREATION DATE:       Mon May 26 06:06:53 EDT 2014
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
#		06/01/2015	nthadan		CR4542: changed query 
#		19/10/2016	Pankaj		IPS-942 Performanace
#		17/04/2017  Rajiv		IPS-1982 CATG RVW temp close stores
# 		24/07/2017  Khim		IPS-2299

###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/


REGISTER $UDF_JAR;
DEFINE AddDays com.searshc.supplychain.idrp.udf.AddOrRemoveDaysToDate();
DEFINE TrimLeadingZeros com.searshc.supplychain.idrp.udf.TrimLeadingZeros();
SET default_parallel $NUM_PARALLEL;
--set io.sort.mb 1024
set mapred.child.java.opts -Xmx2048m
set mapred.compress.map.output true
--set pig.cachedbag.memusage 0.15
set io.sort.factor 100
set opt.multiquery false
SET pig.maxCombinedSplitSize 4000000
SET mapred.max.split.size 134217728;


/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/


/*gold__geographic_location_master_data = LOAD '$GOLD__GEOGRAPHIC_LOCATION_MASTER_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__GEOGRAPHIC_LOCATION_MASTER_SCHEMA);*/

/*added changes for CR4542*/
smith__idrp_eligible_loc_data = LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);

gold__geographic_model_store_data = LOAD '$GOLD__GEOGRAPHIC_MODEL_STORE_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__GEOGRAPHIC_MODEL_STORE_SCHEMA);

smith__space_planning_item_exmpt_explode_data = LOAD '$SMITH__SPACE_PLANNING_ITEM_EXMPT_EXPLODE_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__SPACE_PLANNING_ITEM_EXMPT_EXPLODE_SCHEMA);


work__idrp_items_vend_packs_catg_rvw = LOAD '$WORK__IDRP_ITEMS_VEND_PACKS_CATG_RVW_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($WORK__IDRP_ITEMS_VEND_PACKS_CATG_RVW_SCHEMA);


--------------------------------------------------------------------------------------------------------------------------------

work__idrp_items_vend_packs_catg_rvw = FOREACH work__idrp_items_vend_packs_catg_rvw GENERATE 

shc_item_id,shc_item_desc,shc_division_nbr,shc_division_desc,shc_department_nbr,shc_department_desc,shc_category_group_level_nbr,shc_category_group_desc,shc_category_nbr,shc_category_desc,shc_sub_category_nbr,shc_sub_category_desc,sears_business_nbr,sears_business_desc,sears_division_nbr,sears_division_desc,sears_line_nbr,sears_line_desc,sears_sub_line_nbr,sears_sub_line_desc,sears_class_nbr,sears_class_desc,sears_item_nbr,sears_sku_nbr,delivered_direct_ind,installation_ind,store_forecast_cd,shc_item_type_cd,item_purchase_status_cd,network_distribution_cd,future_network_distribution_cd,future_network_distribution_effective_dt,jit_network_distribution_cd,reorder_authentication_cd,can_carry_model_id,grocery_item_ind,shc_item_corporate_owner_cd,iplan_id,markdown_style_reference_cd,sears_order_system_cd,idrp_order_method_cd,idrp_order_method_desc,forecast_group_format_id,forecast_group_desc,dotcom_eligibility_cd,ksn_id,vendor_package_id,vendor_package_purchase_status_cd,vendor_package_purchase_status_dt,vendor_package_owner_cd,ksn_package_id,service_area_restriction_model_id,flow_type_cd,aprk_id,import_ind,order_duns_nbr,vendor_carton_qty,vendor_stock_nbr,carton_per_layer_qty,layer_per_pallet_qty,ksn_purchase_status_cd,dotcom_allocation_ind,idrp_batch_id;


gold__geographic_model_store_data = 
    FOREACH gold__geographic_model_store_data 
    GENERATE
        model_nbr AS model_nbr,
		location_nbr AS location_nbr;

gold__geographic_model_store_data = 
    DISTINCT gold__geographic_model_store_data;

-----------------------------------------------
/*********** Select case when location_format_type_cd = .001., then .S., else .K. as owner_cd, 
      location_nbr as store_location_nbr 
  from gold__geographic_location_master 
    where location_format_type_cd in ('001', '002') 
    and location_type_cd in (1, 5)
    and ((location_open_dt <> '0000-00-00' and location_open_dt <> '2049-12-31') 
    and (planned_closed_dt = '0000-00-00' or planned_closed_dt > current date) 
    and (location_close_dt = '0000-00-00' or location_close_dt > current date) and store_temporary_close_dt = '0000-00-00')

Output: work__idrp_open_futureopen_stores


Changed Query : CR 4542
Select case when format_type_cd = ‘001’, then ‘S’, else ‘K’ as owner_cd, 
      location_nbr as store_location_nbr 
  from smith__idrp_eligible_loc 
    where ((format_type_cd = '001' and location_format_type_cd in (‘FLS’, ‘SINT’))
                 or format_type_cd =  '002') 
    and location_level_cd = ‘STORE’
    and ((location_open_dt <> '1970-01-01-00:00:00' 
              and location_open_dt <> '2049-12-31-00:00:00') 
    and (location_close_dt = '1970-01-01-00:00:00' 
             or location_close_dt > current timestamp) 
    and location_temporary_close_dt = '1970-01-01-00:00:00')

 ****************/

/* ---- CR 4542 Changes Begin ----- */
filter_futureopen_smith__idrp_eligible_loc_data = 
    FILTER smith__idrp_eligible_loc_data
    BY 
    ((fmt_typ_cd == '001' AND (loc_fmt_typ_cd == 'FLS' or loc_fmt_typ_cd == 'SINT') ) OR fmt_typ_cd == '002') 
    AND (loc_lvl_cd == 'STORE' ) 
    AND (
    	(loc_opn_dt != '1970-01-01-00:00:00' AND loc_opn_dt != '2049-12-31-00:00:00') AND 
    	(loc_cls_dt == '1970-01-01-00:00:00' OR loc_cls_dt > '$CURRENT_DATE')    AND 
    	loc_temp_cls_dt == '1970-01-01-00:00:00');


work__idrp_open_futureopen_stores = 	
    FOREACH filter_futureopen_smith__idrp_eligible_loc_data
    GENERATE
        (fmt_typ_cd == '001' ? 'S' : 'K') AS owner_cd, 
        TrimLeadingZeros(loc) AS store_location_nbr;	


work__idrp_open_futureopen_stores = 
    DISTINCT work__idrp_open_futureopen_stores;

/************* Select case when location_format_type_cd = .001., then .S., else .K. as owner_cd, 
      location_nbr as store_location_nbr 
  from gold__geographic_location_master 
where location_format_type_cd in ('001', '002') 
    and location_type_cd = 1
    and (((store_temporary_close_dt <> '0000-00-00' and store_temporary_close_dt < current date)
         and (location_close_dt = '0000-00-00' and planned_closed_dt = '0000-00-00')
         and location_open_dt > current date)
         or
         (store_temporary_close_dt = '0000-00-00' and location_close_dt = '0000-00-00' 
             and planned_closed_dt > current date and location_open_dt <= current date));
    
    CR 4542: Changed above query to 
    
    Select case when format_type_cd = ‘001’, then ‘S’, else ‘K’ as owner_cd, 
      location_nbr as store_location_nbr 
  from smith__idrp_eligible_loc 
    where ((format_type_cd = '001' and location_format_type_cd in (‘FLS’, ‘SINT’))
                 or format_type_cd =  '002') 
    and location_level_cd = ‘STORE’
    and (location_temporary_close_dt <> '1970-01-01-00:00:00' and
             location_temporary_close_dt < current timestamp)
   and location_close_dt = '1970-01-01-00:00:00'
   and location_open_dt > current timestamp;
    
    
Output: work__idrp_tempclose_futrclose_stores

********************/
/* -- IPS-1982 - Correct IE Issue with stores with temporary close date for items in category review --  */
filter_future_close_smith__idrp_eligible_loc_data = 
    FILTER smith__idrp_eligible_loc_data
    BY ((fmt_typ_cd == '001' AND (loc_fmt_typ_cd == 'FLS' or loc_fmt_typ_cd == 'SINT') ) OR fmt_typ_cd == '002')  
   AND  (loc_lvl_cd == 'STORE' ) 
   AND 	(loc_temp_cls_dt != '1970-01-01-00:00:00' AND loc_temp_cls_dt < loc_temp_opn_dt AND loc_temp_opn_dt < '$CURRENT_DATE') AND
   			(loc_cls_dt == '1970-01-01-00:00:00' OR loc_cls_dt > '$CURRENT_DATE');
	
work__idrp_tempclose_futrclose_stores = 
    FOREACH filter_future_close_smith__idrp_eligible_loc_data
    GENERATE
	(fmt_typ_cd IS NULL ? 'K' :  (fmt_typ_cd == '001' ? 'S' : 'K')) AS owner_cd, 
    TrimLeadingZeros(loc) AS store_location_nbr;

work__idrp_tempclose_futrclose_stores = 
    DISTINCT work__idrp_tempclose_futrclose_stores;


/* ---- CR 4542 Changes End ----- */

/*********    c.	Union work__idrp_open_futureopen_stores and work__idrp_tempclose_futrclose_stores to create a file of the complete list of stores for the Sears and Kmart formats.
Output: work__idrp_format_stores
     ****************/
	 

union_futureopen_stores_futrclose_stores = 
    UNION work__idrp_open_futureopen_stores , 
	      work__idrp_tempclose_futrclose_stores ;

work__idrp_format_stores = 
    DISTINCT union_futureopen_stores_futrclose_stores;

/************ 2.	Join the work__idrp_items_vend_packs_catg_rvw to the work__idrp_format_stores on vendor_package_owner_cd = owner_cd.   
a.	Select store_location_nbr, 
0 AS days_to_check_begin_days_cnt, 
365 AS days_to_check_end_days_cnt,

Output: work__idrp_vp_catgrvw_stores
 ***********/

SET mapred.max.split.size 5242880;

join_items_vend_packs_catg_rvw_and_format_stores = 
    JOIN work__idrp_items_vend_packs_catg_rvw BY vendor_package_owner_cd , 
         work__idrp_format_stores BY owner_cd USING 'replicated';


work__idrp_vp_catgrvw_stores = 
    FOREACH join_items_vend_packs_catg_rvw_and_format_stores
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
        service_area_restriction_model_id AS service_area_restriction_model_id,
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
        '0' AS days_to_check_begin_days_cnt,
        '365' AS days_to_check_end_days_cnt;	    


/************  Split work__idrp_vp_catgrvw_stores, one file that contains records with service_area_restriction_model_id = 0 (work__idrp_vp_catgrvw_stores_nosarm); and the other file contains records with service_area_restriction_model_id > 0 (work__idrp_vp_catgrvw_stores_sarm). 
Output: work__idrp_vp_catgrvw_stores_nosarm, work__idrp_vp_catgrvw_stores_sarm
 ***************/

work__idrp_vp_catgrvw_stores_nosarm = 
    FILTER work__idrp_vp_catgrvw_stores
    BY (int)service_area_restriction_model_id == 0 ;



work__idrp_vp_catgrvw_stores_sarm = 
    FILTER work__idrp_vp_catgrvw_stores
    BY (int)service_area_restriction_model_id > 0;


/***********  a.	Join work__idrp_vp_catgrvw_stores_sarm to the gold__geographic_model_store on service_area_restriction_mdl = model_nbr.  
Output: work__idrp_vp_catgrvw_stores_aftersarm.
 ************/

SET mapred.max.split.size 134217728;

work__idrp_vp_catgrvw_stores_aftersarm = 
    JOIN work__idrp_vp_catgrvw_stores_sarm BY ((int)service_area_restriction_model_id, (int)store_location_nbr), 
         gold__geographic_model_store_data BY ((int)model_nbr, (int)location_nbr);


work__idrp_vp_catgrvw_stores_aftersarm = 
    FOREACH work__idrp_vp_catgrvw_stores_aftersarm
    GENERATE
        shc_item_id	AS	shc_item_id,
        shc_item_desc	AS	shc_item_desc,
        shc_division_nbr	AS	shc_division_nbr,
        shc_division_desc	AS	shc_division_desc,
        shc_department_nbr	AS	shc_department_nbr,
        shc_department_desc	AS	shc_department_desc,
        shc_category_group_level_nbr	AS	shc_category_group_level_nbr,
        shc_category_group_desc	AS	shc_category_group_desc,
        shc_category_nbr	AS	shc_category_nbr,
        shc_category_desc	AS	shc_category_desc,
        shc_sub_category_nbr	AS	shc_sub_category_nbr,
        shc_sub_category_desc	AS	shc_sub_category_desc,
        sears_business_nbr	AS	sears_business_nbr,
        sears_business_desc	AS	sears_business_desc,
        sears_division_nbr	AS	sears_division_nbr,
        sears_division_desc	AS	sears_division_desc,
        sears_line_nbr	AS	sears_line_nbr,
        sears_line_desc	AS	sears_line_desc,
        sears_sub_line_nbr	AS	sears_sub_line_nbr,
        sears_sub_line_desc	AS	sears_sub_line_desc,
        sears_class_nbr	AS	sears_class_nbr,
        sears_class_desc	AS	sears_class_desc,
        sears_item_nbr	AS	sears_item_nbr,
        sears_sku_nbr	AS	sears_sku_nbr,
        delivered_direct_ind	AS	delivered_direct_ind,
        installation_ind	AS	installation_ind,
        store_forecast_cd	AS	store_forecast_cd,
        shc_item_type_cd	AS	shc_item_type_cd,
        item_purchase_status_cd	AS	item_purchase_status_cd,
        network_distribution_cd	AS	network_distribution_cd,
        future_network_distribution_cd	AS	future_network_distribution_cd,
        future_network_distribution_effective_dt	AS	future_network_distribution_effective_dt,
        jit_network_distribution_cd	AS	jit_network_distribution_cd,
        reorder_authentication_cd	AS	reorder_authentication_cd,
        can_carry_model_id	AS	can_carry_model_id,
        grocery_item_ind	AS	grocery_item_ind,
        shc_item_corporate_owner_cd	AS	shc_item_corporate_owner_cd,
        iplan_id	AS	iplan_id,
        markdown_style_reference_cd	AS	markdown_style_reference_cd,
        sears_order_system_cd	AS	sears_order_system_cd,
        idrp_order_method_cd	AS	idrp_order_method_cd,
        idrp_order_method_desc	AS	idrp_order_method_desc,
        forecast_group_format_id	AS	forecast_group_format_id,
        forecast_group_desc	AS	forecast_group_desc,
        dotcom_eligibility_cd	AS	dotcom_eligibility_cd,
        ksn_id	AS	ksn_id,
        vendor_package_id	AS	vendor_package_id,
        vendor_package_purchase_status_cd	AS	vendor_package_purchase_status_cd,
        vendor_package_purchase_status_dt	AS	vendor_package_purchase_status_dt,
        vendor_package_owner_cd	AS	vendor_package_owner_cd,
        ksn_package_id	AS	ksn_package_id,
        service_area_restriction_model_id	AS	service_area_restriction_model_id,
        flow_type_cd	AS	flow_type_cd,
        aprk_id	AS	aprk_id,
        import_ind	AS	import_ind,
        order_duns_nbr	AS	order_duns_nbr,
        vendor_carton_qty	AS	vendor_carton_qty,
        vendor_stock_nbr	AS	vendor_stock_nbr,
        carton_per_layer_qty	AS	carton_per_layer_qty,
        layer_per_pallet_qty	AS	layer_per_pallet_qty,
	ksn_purchase_status_cd as ksn_purchase_status_cd,
	dotcom_allocation_ind as dotcom_allocation_ind,
        store_location_nbr	AS	store_location_nbr,
        days_to_check_begin_days_cnt	AS	days_to_check_begin_days_cnt,
        days_to_check_end_days_cnt	AS	days_to_check_end_days_cnt;
        

/************* 
b.	Union work__idrp_vp_catgrvw_stores_aftersarm with work__idrp_vp_catgrvw_stores_nosarm.  
Output: work__idrp_vp_catgrvw_stores_aftersarm_combined.
 ****************/

work__idrp_vp_catgrvw_stores_aftersarm_combined = 
    UNION work__idrp_vp_catgrvw_stores_aftersarm ,
	      work__idrp_vp_catgrvw_stores_nosarm;	 

/************** 
a.	Left outer join work__idrp_vp_catgrvw_stores_aftersarm_combined to smith__space_planning_item_exmpt_explode.  The resulting table should contain all fields from work__idrp_vp_catgrvw_stores_aftersarm_combined and the item_exemption_reason_cd from the legal exemption file.  If no match is found, then default the item_exemption_reason_cd to 0.
Output: work__idrp_vp_catgrvw_stores_legal_exmp

***************/

join_vp_catgrvw_stores_aftersarm_combined_item_exmpt_explode = 
    JOIN work__idrp_vp_catgrvw_stores_aftersarm_combined BY (shc_item_id,store_location_nbr) LEFT OUTER ,
	     smith__space_planning_item_exmpt_explode_data BY (item_id,locn_nbr);
		 
work__idrp_vp_catgrvw_stores_legal_exmp = 
    FOREACH join_vp_catgrvw_stores_aftersarm_combined_item_exmpt_explode
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
        service_area_restriction_model_id AS service_area_restriction_model_id,
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
        (exmpt_rsn_cd IS NULL ? '0' : exmpt_rsn_cd) AS item_exemption_reason_cd;


/************
b.	Select from work__idrp_vp_catgrvw_stores_legal_exmp any record where the item_exemption_reason_cd = 0.  This will remove any item store combination that has a legal exemption.  
Output: work__idrp_vp_catgrvw_stores_after_legal_exmp

*************/
		
work__idrp_vp_catgrvw_stores_legal_exmp_Itm_exm_cd_ZERO = 
    FILTER work__idrp_vp_catgrvw_stores_legal_exmp
    BY (int)item_exemption_reason_cd == 0;
   

smith__space_planning_item_exmpt_explode_data_item_id_zero = filter smith__space_planning_item_exmpt_explode_data by ((int)item_id ==0);

JOIN_work__idrp_vp_catgrvw_stores_legal_exmp_ZERO_item_exmpt_explode = 
    JOIN work__idrp_vp_catgrvw_stores_legal_exmp_Itm_exm_cd_ZERO BY (shc_division_nbr,shc_category_nbr,shc_sub_category_nbr,store_location_nbr) LEFT OUTER ,
	     smith__space_planning_item_exmpt_explode_data_item_id_zero BY (dvsn_nbr,catg_nbr,sub_catg_nbr,locn_nbr);


gen_work__idrp_vp_catgrvw_stores_legal_exmp_ZERO_item_exmpt_explode = 
    FOREACH JOIN_work__idrp_vp_catgrvw_stores_legal_exmp_ZERO_item_exmpt_explode
    GENERATE
        shc_item_id AS shc_item_id ,
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
        delivered_direct_ind AS delivered_direct_ind ,
        installation_ind AS installation_ind ,
        store_forecast_cd AS store_forecast_cd,
        shc_item_type_cd AS shc_item_type_cd,
        item_purchase_status_cd AS item_purchase_status_cd,
        network_distribution_cd AS network_distribution_cd ,
        future_network_distribution_cd AS future_network_distribution_cd ,
        future_network_distribution_effective_dt AS future_network_distribution_effective_dt ,
        jit_network_distribution_cd AS jit_network_distribution_cd ,
        reorder_authentication_cd AS reorder_authentication_cd ,
        can_carry_model_id AS can_carry_model_id ,
        grocery_item_ind AS grocery_item_ind ,
        shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd ,
        iplan_id AS iplan_id ,
        markdown_style_reference_cd AS markdown_style_reference_cd ,
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
        service_area_restriction_model_id AS service_area_restriction_model_id ,
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
        (smith__space_planning_item_exmpt_explode_data_item_id_zero::exmpt_rsn_cd IS NULL ? '0' : smith__space_planning_item_exmpt_explode_data_item_id_zero::exmpt_rsn_cd) AS item_exemption_reason_cd;;		


work__idrp_vp_catgrvw_stores_legal_exmp_rem_Itm_exm_cd_ZERO = 
    FILTER gen_work__idrp_vp_catgrvw_stores_legal_exmp_ZERO_item_exmpt_explode
    BY (int)item_exemption_reason_cd == 0;
   
/*
IPS-2299 - code changes for the exempt reason cd
*/
smith__space_planning_item_exmpt_explode_data_item_id_sub_cat_zero = filter smith__space_planning_item_exmpt_explode_data by ((int)item_id ==0 and (int)sub_catg_nbr ==0);

JOIN_work__idrp_vp_catgrvw_stores_legal_exmp_ZERO_item_exmpt_explode = 
    JOIN work__idrp_vp_catgrvw_stores_legal_exmp_rem_Itm_exm_cd_ZERO BY (shc_division_nbr,shc_category_nbr,store_location_nbr) LEFT OUTER ,
	     smith__space_planning_item_exmpt_explode_data_item_id_sub_cat_zero BY (dvsn_nbr,catg_nbr,locn_nbr);
	
gen_work__idrp_vp_catgrvw_stores_legal_exmp_ZERO_final = 
    FOREACH JOIN_work__idrp_vp_catgrvw_stores_legal_exmp_ZERO_item_exmpt_explode
    GENERATE
        shc_item_id AS shc_item_id ,
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
        delivered_direct_ind AS delivered_direct_ind ,
        installation_ind AS installation_ind ,
        store_forecast_cd AS store_forecast_cd,
        shc_item_type_cd AS shc_item_type_cd,
        item_purchase_status_cd AS item_purchase_status_cd,
        network_distribution_cd AS network_distribution_cd ,
        future_network_distribution_cd AS future_network_distribution_cd ,
        future_network_distribution_effective_dt AS future_network_distribution_effective_dt ,
        jit_network_distribution_cd AS jit_network_distribution_cd ,
        reorder_authentication_cd AS reorder_authentication_cd ,
        can_carry_model_id AS can_carry_model_id ,
        grocery_item_ind AS grocery_item_ind ,
        shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd ,
        iplan_id AS iplan_id ,
        markdown_style_reference_cd AS markdown_style_reference_cd ,
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
        service_area_restriction_model_id AS service_area_restriction_model_id ,
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
        (smith__space_planning_item_exmpt_explode_data_item_id_sub_cat_zero::exmpt_rsn_cd IS NULL ? '0' : smith__space_planning_item_exmpt_explode_data_item_id_sub_cat_zero::exmpt_rsn_cd) AS item_exemption_reason_cd;;		

work__idrp_vp_catgrvw_stores_legal_exmp_Zero_final = 
    FILTER gen_work__idrp_vp_catgrvw_stores_legal_exmp_ZERO_final
    BY (int)item_exemption_reason_cd == 0;

work__idrp_vp_catgrvw_stores_after_legal_exmp = 
    FOREACH work__idrp_vp_catgrvw_stores_legal_exmp_Zero_final
    GENERATE
        shc_item_id AS shc_item_id ,
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
        delivered_direct_ind AS delivered_direct_ind ,
        installation_ind AS installation_ind ,
        store_forecast_cd AS store_forecast_cd,
        shc_item_type_cd AS shc_item_type_cd,
        item_purchase_status_cd AS item_purchase_status_cd,
        network_distribution_cd AS network_distribution_cd ,
        future_network_distribution_cd AS future_network_distribution_cd ,
        future_network_distribution_effective_dt AS future_network_distribution_effective_dt ,
        jit_network_distribution_cd AS jit_network_distribution_cd ,
        reorder_authentication_cd AS reorder_authentication_cd ,
        can_carry_model_id AS can_carry_model_id ,
        grocery_item_ind AS grocery_item_ind ,
        shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd ,
        iplan_id AS iplan_id ,
        markdown_style_reference_cd AS markdown_style_reference_cd ,
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
        service_area_restriction_model_id AS service_area_restriction_model_id ,
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
        days_to_check_end_days_cnt AS days_to_check_end_days_cnt;
        
STORE work__idrp_vp_catgrvw_stores_after_legal_exmp 
INTO '$WORK__IDRP_VP_CATGRVW_STORES_AFTER_LEGAL_EXMP_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');






/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
