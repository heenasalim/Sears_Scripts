/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_vp_cancarry_stores.pig
# AUTHOR NAME:         Onkar Malewadikar
# CREATION DATE:       Mon May 26 06:23:52 EDT 2014
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
#	19.10.2016		Pankaj		IPS-942 Performance.
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

REGISTER $UDF_JAR;
DEFINE AddDays com.searshc.supplychain.idrp.udf.AddOrRemoveDaysToDate();
SET default_parallel 250;
--set io.sort.mb 1024
set mapred.child.java.opts -Xmx4096m
--set mapred.compress.map.output true
--set pig.cachedbag.memusage 0.15
--set io.sort.factor 100
--set opt.multiquery false

SET mapred.max.split.size 134217728;
SET pig.maxCombinedSplitSize 4000000


/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/


work__idrp_items_vend_packs_can_carry = LOAD '$WORK__IDRP_ITEMS_VEND_PACKS_CAN_CARRY_LOCATION' USING PigStorage ('$FIELD_DELIMITER_CONTROL_A') AS ($WORK__IDRP_ITEMS_VEND_PACKS_CAN_CARRY_SCHEMA);

smith__idrp_space_planning_authorized_vendor_package_stores_data = LOAD '$SMITH__IDRP_SPACE_PLANNING_AUTHORIZED_VENDOR_PACKAGE_STORES_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_SPACE_PLANNING_AUTHORIZED_VENDOR_PACKAGE_STORES_SCHEMA);

work__idrp_items_vend_packs_can_carry = FOREACH work__idrp_items_vend_packs_can_carry GENERATE
shc_item_id,shc_item_desc,shc_division_nbr,shc_division_desc,shc_department_nbr,shc_department_desc,shc_category_group_level_nbr,shc_category_group_desc,shc_category_nbr,shc_category_desc,shc_sub_category_nbr,shc_sub_category_desc,sears_business_nbr,sears_business_desc,sears_division_nbr,sears_division_desc,sears_line_nbr,sears_line_desc,sears_sub_line_nbr,sears_sub_line_desc,sears_class_nbr,sears_class_desc,sears_item_nbr,sears_sku_nbr,delivered_direct_ind,installation_ind,store_forecast_cd,shc_item_type_cd,item_purchase_status_cd,network_distribution_cd,future_network_distribution_cd,future_network_distribution_effective_dt,jit_network_distribution_cd,reorder_authentication_cd,can_carry_model_id,grocery_item_ind,shc_item_corporate_owner_cd,iplan_id,markdown_style_reference_cd,sears_order_system_cd,idrp_order_method_cd,idrp_order_method_desc,forecast_group_format_id,forecast_group_desc,dotcom_eligibility_cd,ksn_id,vendor_package_id,vendor_package_purchase_status_cd,vendor_package_purchase_status_dt,vendor_package_owner_cd,ksn_package_id,service_area_restriction_model_id,flow_type_cd,aprk_id,import_ind,order_duns_nbr,vendor_carton_qty,vendor_stock_nbr,carton_per_layer_qty,layer_per_pallet_qty,ksn_purchase_status_cd,dotcom_allocation_ind,idrp_batch_id ;


smith__idrp_space_planning_authorized_vendor_package_stores_data = FOREACH smith__idrp_space_planning_authorized_vendor_package_stores_data GENERATE
vend_pack_nbr,rev_store,rev_dtc_num,rev_dtce_num,rev_totplans,rev_cur_face,rev_cur_pres,rev_cur_fill,rev_cur_cap,rev_chkout,rev_dept,rev_catg,rev_rec_crdte,rev_rec_ludte,rev_dtc_vp_planbus_1,rev_dtc_vp_kcode_1,item_id,ksn_stat_1 ;

join_items_vend_packs_can_carry_and_authorized_vendor_package_stores = 
    JOIN work__idrp_items_vend_packs_can_carry BY vendor_package_id,
         smith__idrp_space_planning_authorized_vendor_package_stores_data BY vend_pack_nbr  USING 'skewed' parallel 300;

work__idrp_vp_cancarry_stores = 	
    FOREACH join_items_vend_packs_can_carry_and_authorized_vendor_package_stores
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
        rev_store AS store_location_nbr,
        rev_dtc_num AS days_to_check_begin_days_cnt,
        rev_dtce_num AS days_to_check_end_days_cnt;
		

work__idrp_vp_cancarry_stores_filter = FILTER work__idrp_vp_cancarry_stores BY ((can_carry_model_id is not null or can_carry_model_id !='') and shc_item_type_cd == 'EXAS'
												AND	(can_carry_model_id !='76002' OR  can_carry_model_id !='76500' OR can_carry_model_id !='76550') 
												AND (vendor_package_owner_cd == 'K' OR vendor_package_owner_cd =='S') )  ;

work__idrp_vp_cancarry_stores_grp = group work__idrp_vp_cancarry_stores_filter by (shc_item_id, vendor_package_id, vendor_package_owner_cd, can_carry_model_id, service_area_restriction_model_id );

work__idrp_vp_cancarry_stores_grp_limit = foreach work__idrp_vp_cancarry_stores_grp
														{
															sorted = order work__idrp_vp_cancarry_stores_filter by shc_item_id, vendor_package_id, vendor_package_owner_cd, can_carry_model_id, service_area_restriction_model_id;
															lim = LIMIT sorted 1;
															generate FLATTEN(lim);
														};

work__idrp_exas_vpacks_dotcom = FOREACH work__idrp_vp_cancarry_stores_grp_limit  GENERATE 
										lim::shc_item_id AS shc_item_id ,
										lim::shc_item_desc AS shc_item_desc,
										lim::shc_division_nbr AS shc_division_nbr,
										lim::shc_division_desc AS shc_division_desc,
										lim::shc_department_nbr AS shc_department_nbr,
										lim::shc_department_desc AS shc_department_desc,
										lim::shc_category_group_level_nbr AS shc_category_group_level_nbr,
										lim::shc_category_group_desc AS shc_category_group_desc,
										lim::shc_category_nbr AS shc_category_nbr,
										lim::shc_category_desc AS shc_category_desc,
										lim::shc_sub_category_nbr AS shc_sub_category_nbr,
										lim::shc_sub_category_desc AS shc_sub_category_desc,
										lim::sears_business_nbr AS sears_business_nbr,
										lim::sears_business_desc AS sears_business_desc,
										lim::sears_division_nbr AS sears_division_nbr,
										lim::sears_division_desc AS sears_division_desc,
										lim::sears_line_nbr AS sears_line_nbr,
										lim::sears_line_desc AS sears_line_desc,
										lim::sears_sub_line_nbr AS sears_sub_line_nbr,
										lim::sears_sub_line_desc AS sears_sub_line_desc,
										lim::sears_class_nbr AS sears_class_nbr,
										lim::sears_class_desc AS sears_class_desc,
										lim::sears_item_nbr AS sears_item_nbr,
										lim::sears_sku_nbr AS sears_sku_nbr,
										lim::delivered_direct_ind AS delivered_direct_ind ,
										lim::installation_ind AS installation_ind ,
										lim::store_forecast_cd AS store_forecast_cd,
										lim::shc_item_type_cd AS shc_item_type_cd,
										lim::item_purchase_status_cd AS item_purchase_status_cd,
										lim::network_distribution_cd AS network_distribution_cd ,
										lim::future_network_distribution_cd AS future_network_distribution_cd ,
										lim::future_network_distribution_effective_dt AS future_network_distribution_effective_dt ,
										lim::jit_network_distribution_cd AS jit_network_distribution_cd ,
										lim::reorder_authentication_cd AS reorder_authentication_cd ,
										lim::can_carry_model_id AS can_carry_model_id ,
										lim::grocery_item_ind AS grocery_item_ind ,
										lim::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd ,
										lim::iplan_id AS iplan_id ,
										lim::markdown_style_reference_cd AS markdown_style_reference_cd ,
										lim::sears_order_system_cd AS sears_order_system_cd,
										lim::idrp_order_method_cd AS idrp_order_method_cd,
										lim::idrp_order_method_desc AS idrp_order_method_desc,
										lim::forecast_group_format_id AS forecast_group_format_id,
										lim::forecast_group_desc AS forecast_group_desc,
										lim::dotcom_eligibility_cd AS dotcom_eligibility_cd,
										lim::ksn_id AS ksn_id,
										lim::vendor_package_id AS vendor_package_id,
										lim::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
										lim::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
										lim::vendor_package_owner_cd AS vendor_package_owner_cd,
										lim::ksn_package_id AS ksn_package_id,
										lim::service_area_restriction_model_id AS service_area_restriction_model_id ,
										lim::flow_type_cd AS flow_type_cd,
										lim::aprk_id AS aprk_id,
										lim::import_ind AS import_ind,
										lim::order_duns_nbr AS order_duns_nbr,
										lim::vendor_carton_qty AS vendor_carton_qty,
										lim::vendor_stock_nbr AS vendor_stock_nbr,
										lim::carton_per_layer_qty AS carton_per_layer_qty,
										lim::layer_per_pallet_qty AS layer_per_pallet_qty,
										lim::ksn_purchase_status_cd as ksn_purchase_status_cd,
										lim::dotcom_allocation_ind as dotcom_allocation_ind,
										lim::store_location_nbr AS store_location_nbr,
										lim::days_to_check_begin_days_cnt AS days_to_check_begin_days_cnt,
										lim::days_to_check_end_days_cnt AS days_to_check_end_days_cnt;

gold_geographic_model_str = LOAD '$GOLD__GEOGRAPHIC_MODEL_STORE_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__GEOGRAPHIC_MODEL_STORE_SCHEMA);

gold_geographic_model_str_data = FOREACH gold_geographic_model_str GENERATE model_nbr AS mdl_nbr,location_nbr AS locn_nbr;

gold_geographic_model_str_data_filter = FILTER gold_geographic_model_str_data BY (locn_nbr == '7840' OR locn_nbr == '9300');

work__idrp_exas_vpacks_dotcom_join_geo_model_store = JOIN work__idrp_exas_vpacks_dotcom by (can_carry_model_id),  gold_geographic_model_str_data_filter by (mdl_nbr) using 'replicated';

work__idrp_exas_vpacks_dotcom_join_geo_model_store_filter = filter work__idrp_exas_vpacks_dotcom_join_geo_model_store by 
																	((IsNull(work__idrp_exas_vpacks_dotcom::vendor_package_owner_cd,'') == 'K' and IsNull(gold_geographic_model_str_data_filter::locn_nbr,'') == '7840') 
																	OR (IsNull(work__idrp_exas_vpacks_dotcom::vendor_package_owner_cd,'') == 'S' and IsNull(gold_geographic_model_str_data_filter::locn_nbr,'') == '9300'));

work__idrp_exas_vpacks_dotcom_geo_model_store_gen = foreach work__idrp_exas_vpacks_dotcom_join_geo_model_store_filter generate 
										work__idrp_exas_vpacks_dotcom::shc_item_id AS shc_item_id,
										work__idrp_exas_vpacks_dotcom::shc_item_desc AS shc_item_desc,
										work__idrp_exas_vpacks_dotcom::shc_division_nbr AS shc_division_nbr,
										work__idrp_exas_vpacks_dotcom::shc_division_desc AS shc_division_desc,
										work__idrp_exas_vpacks_dotcom::shc_department_nbr AS shc_department_nbr,
										work__idrp_exas_vpacks_dotcom::shc_department_desc AS shc_department_desc,
										work__idrp_exas_vpacks_dotcom::shc_category_group_level_nbr AS shc_category_group_level_nbr,
										work__idrp_exas_vpacks_dotcom::shc_category_group_desc AS shc_category_group_desc,
										work__idrp_exas_vpacks_dotcom::shc_category_nbr AS shc_category_nbr,
										work__idrp_exas_vpacks_dotcom::shc_category_desc AS shc_category_desc,
										work__idrp_exas_vpacks_dotcom::shc_sub_category_nbr AS shc_sub_category_nbr,
										work__idrp_exas_vpacks_dotcom::shc_sub_category_desc AS shc_sub_category_desc,
										work__idrp_exas_vpacks_dotcom::sears_business_nbr AS sears_business_nbr,
										work__idrp_exas_vpacks_dotcom::sears_business_desc AS sears_business_desc,
										work__idrp_exas_vpacks_dotcom::sears_division_nbr AS sears_division_nbr,
										work__idrp_exas_vpacks_dotcom::sears_division_desc AS sears_division_desc,
										work__idrp_exas_vpacks_dotcom::sears_line_nbr AS sears_line_nbr,
										work__idrp_exas_vpacks_dotcom::sears_line_desc AS sears_line_desc,
										work__idrp_exas_vpacks_dotcom::sears_sub_line_nbr AS sears_sub_line_nbr,
										work__idrp_exas_vpacks_dotcom::sears_sub_line_desc AS sears_sub_line_desc,
										work__idrp_exas_vpacks_dotcom::sears_class_nbr AS sears_class_nbr,
										work__idrp_exas_vpacks_dotcom::sears_class_desc AS sears_class_desc,
										work__idrp_exas_vpacks_dotcom::sears_item_nbr AS sears_item_nbr,
										work__idrp_exas_vpacks_dotcom::sears_sku_nbr AS sears_sku_nbr,
										work__idrp_exas_vpacks_dotcom::delivered_direct_ind AS delivered_direct_ind,
										work__idrp_exas_vpacks_dotcom::installation_ind AS installation_ind,
										work__idrp_exas_vpacks_dotcom::store_forecast_cd AS store_forecast_cd,
										work__idrp_exas_vpacks_dotcom::shc_item_type_cd AS shc_item_type_cd,
										work__idrp_exas_vpacks_dotcom::item_purchase_status_cd AS item_purchase_status_cd,
										work__idrp_exas_vpacks_dotcom::network_distribution_cd AS network_distribution_cd,
										work__idrp_exas_vpacks_dotcom::future_network_distribution_cd AS future_network_distribution_cd,
										work__idrp_exas_vpacks_dotcom::future_network_distribution_effective_dt AS future_network_distribution_effective_dt,
										work__idrp_exas_vpacks_dotcom::jit_network_distribution_cd AS jit_network_distribution_cd,
										work__idrp_exas_vpacks_dotcom::reorder_authentication_cd AS reorder_authentication_cd,
										work__idrp_exas_vpacks_dotcom::can_carry_model_id AS can_carry_model_id,
										work__idrp_exas_vpacks_dotcom::grocery_item_ind AS grocery_item_ind,
										work__idrp_exas_vpacks_dotcom::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
										work__idrp_exas_vpacks_dotcom::iplan_id AS iplan_id,
										work__idrp_exas_vpacks_dotcom::markdown_style_reference_cd AS markdown_style_reference_cd,
										work__idrp_exas_vpacks_dotcom::sears_order_system_cd AS sears_order_system_cd,
										work__idrp_exas_vpacks_dotcom::idrp_order_method_cd AS idrp_order_method_cd,
										work__idrp_exas_vpacks_dotcom::idrp_order_method_desc AS idrp_order_method_desc,
										work__idrp_exas_vpacks_dotcom::forecast_group_format_id AS forecast_group_format_id,
										work__idrp_exas_vpacks_dotcom::forecast_group_desc AS forecast_group_desc,
										work__idrp_exas_vpacks_dotcom::dotcom_eligibility_cd AS dotcom_eligibility_cd,
										work__idrp_exas_vpacks_dotcom::ksn_id AS ksn_id,
										work__idrp_exas_vpacks_dotcom::vendor_package_id AS vendor_package_id,
										work__idrp_exas_vpacks_dotcom::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
										work__idrp_exas_vpacks_dotcom::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
										work__idrp_exas_vpacks_dotcom::vendor_package_owner_cd AS vendor_package_owner_cd,
										work__idrp_exas_vpacks_dotcom::ksn_package_id AS ksn_package_id,
										work__idrp_exas_vpacks_dotcom::service_area_restriction_model_id AS service_area_restriction_model_id,
										work__idrp_exas_vpacks_dotcom::flow_type_cd AS flow_type_cd,
										work__idrp_exas_vpacks_dotcom::aprk_id AS aprk_id,
										work__idrp_exas_vpacks_dotcom::import_ind AS import_ind,
										work__idrp_exas_vpacks_dotcom::order_duns_nbr AS order_duns_nbr,
										work__idrp_exas_vpacks_dotcom::vendor_carton_qty AS vendor_carton_qty,
										work__idrp_exas_vpacks_dotcom::vendor_stock_nbr AS vendor_stock_nbr,
										work__idrp_exas_vpacks_dotcom::carton_per_layer_qty AS carton_per_layer_qty,
										work__idrp_exas_vpacks_dotcom::layer_per_pallet_qty AS layer_per_pallet_qty,
										work__idrp_exas_vpacks_dotcom::ksn_purchase_status_cd as ksn_purchase_status_cd,
										work__idrp_exas_vpacks_dotcom::dotcom_allocation_ind as dotcom_allocation_ind,
										work__idrp_exas_vpacks_dotcom::store_location_nbr AS store_location_nbr,
										work__idrp_exas_vpacks_dotcom::days_to_check_begin_days_cnt AS days_to_check_begin_days_cnt,
										work__idrp_exas_vpacks_dotcom::days_to_check_end_days_cnt AS days_to_check_end_days_cnt,
										gold_geographic_model_str_data_filter::locn_nbr AS location_nbr;

SPLIT work__idrp_exas_vpacks_dotcom_geo_model_store_gen INTO 
										work__idrp_exas_vpacks_no_sarm IF IsNull(service_area_restriction_model_id,'') == '0',
										work__idrp_exas_vpacks_with_sarm IF IsNull(service_area_restriction_model_id,'') > '0';

work__idrp_exas_vpacks_with_sarm_join_geo_model = join work__idrp_exas_vpacks_with_sarm by (service_area_restriction_model_id,location_nbr),  gold_geographic_model_str_data_filter by (mdl_nbr,locn_nbr) ;

work__idrp_exas_vpacks_with_sarm_geo_model_gen = FOREACH work__idrp_exas_vpacks_with_sarm_join_geo_model GENERATE 
										work__idrp_exas_vpacks_with_sarm::shc_item_id AS shc_item_id,
										work__idrp_exas_vpacks_with_sarm::shc_item_desc AS shc_item_desc,
										work__idrp_exas_vpacks_with_sarm::shc_division_nbr AS shc_division_nbr,
										work__idrp_exas_vpacks_with_sarm::shc_division_desc AS shc_division_desc,
										work__idrp_exas_vpacks_with_sarm::shc_department_nbr AS shc_department_nbr,
										work__idrp_exas_vpacks_with_sarm::shc_department_desc AS shc_department_desc,
										work__idrp_exas_vpacks_with_sarm::shc_category_group_level_nbr AS shc_category_group_level_nbr,
										work__idrp_exas_vpacks_with_sarm::shc_category_group_desc AS shc_category_group_desc,
										work__idrp_exas_vpacks_with_sarm::shc_category_nbr AS shc_category_nbr,
										work__idrp_exas_vpacks_with_sarm::shc_category_desc AS shc_category_desc,
										work__idrp_exas_vpacks_with_sarm::shc_sub_category_nbr AS shc_sub_category_nbr,
										work__idrp_exas_vpacks_with_sarm::shc_sub_category_desc AS shc_sub_category_desc,
										work__idrp_exas_vpacks_with_sarm::sears_business_nbr AS sears_business_nbr,
										work__idrp_exas_vpacks_with_sarm::sears_business_desc AS sears_business_desc,
										work__idrp_exas_vpacks_with_sarm::sears_division_nbr AS sears_division_nbr,
										work__idrp_exas_vpacks_with_sarm::sears_division_desc AS sears_division_desc,
										work__idrp_exas_vpacks_with_sarm::sears_line_nbr AS sears_line_nbr,
										work__idrp_exas_vpacks_with_sarm::sears_line_desc AS sears_line_desc,
										work__idrp_exas_vpacks_with_sarm::sears_sub_line_nbr AS sears_sub_line_nbr,
										work__idrp_exas_vpacks_with_sarm::sears_sub_line_desc AS sears_sub_line_desc,
										work__idrp_exas_vpacks_with_sarm::sears_class_nbr AS sears_class_nbr,
										work__idrp_exas_vpacks_with_sarm::sears_class_desc AS sears_class_desc,
										work__idrp_exas_vpacks_with_sarm::sears_item_nbr AS sears_item_nbr,
										work__idrp_exas_vpacks_with_sarm::sears_sku_nbr AS sears_sku_nbr,
										work__idrp_exas_vpacks_with_sarm::delivered_direct_ind AS delivered_direct_ind,
										work__idrp_exas_vpacks_with_sarm::installation_ind AS installation_ind,
										work__idrp_exas_vpacks_with_sarm::store_forecast_cd AS store_forecast_cd,
										work__idrp_exas_vpacks_with_sarm::shc_item_type_cd AS shc_item_type_cd,
										work__idrp_exas_vpacks_with_sarm::item_purchase_status_cd AS item_purchase_status_cd,
										work__idrp_exas_vpacks_with_sarm::network_distribution_cd AS network_distribution_cd,
										work__idrp_exas_vpacks_with_sarm::future_network_distribution_cd AS future_network_distribution_cd,
										work__idrp_exas_vpacks_with_sarm::future_network_distribution_effective_dt AS future_network_distribution_effective_dt,
										work__idrp_exas_vpacks_with_sarm::jit_network_distribution_cd AS jit_network_distribution_cd,
										work__idrp_exas_vpacks_with_sarm::reorder_authentication_cd AS reorder_authentication_cd,
										work__idrp_exas_vpacks_with_sarm::can_carry_model_id AS can_carry_model_id,
										work__idrp_exas_vpacks_with_sarm::grocery_item_ind AS grocery_item_ind,
										work__idrp_exas_vpacks_with_sarm::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
										work__idrp_exas_vpacks_with_sarm::iplan_id AS iplan_id,
										work__idrp_exas_vpacks_with_sarm::markdown_style_reference_cd AS markdown_style_reference_cd,
										work__idrp_exas_vpacks_with_sarm::sears_order_system_cd AS sears_order_system_cd,
										work__idrp_exas_vpacks_with_sarm::idrp_order_method_cd AS idrp_order_method_cd,
										work__idrp_exas_vpacks_with_sarm::idrp_order_method_desc AS idrp_order_method_desc,
										work__idrp_exas_vpacks_with_sarm::forecast_group_format_id AS forecast_group_format_id,
										work__idrp_exas_vpacks_with_sarm::forecast_group_desc AS forecast_group_desc,
										work__idrp_exas_vpacks_with_sarm::dotcom_eligibility_cd AS dotcom_eligibility_cd,
										work__idrp_exas_vpacks_with_sarm::ksn_id AS ksn_id,
										work__idrp_exas_vpacks_with_sarm::vendor_package_id AS vendor_package_id,
										work__idrp_exas_vpacks_with_sarm::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
										work__idrp_exas_vpacks_with_sarm::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
										work__idrp_exas_vpacks_with_sarm::vendor_package_owner_cd AS vendor_package_owner_cd,
										work__idrp_exas_vpacks_with_sarm::ksn_package_id AS ksn_package_id,
										work__idrp_exas_vpacks_with_sarm::service_area_restriction_model_id AS service_area_restriction_model_id,
										work__idrp_exas_vpacks_with_sarm::flow_type_cd AS flow_type_cd,
										work__idrp_exas_vpacks_with_sarm::aprk_id AS aprk_id,
										work__idrp_exas_vpacks_with_sarm::import_ind AS import_ind,
										work__idrp_exas_vpacks_with_sarm::order_duns_nbr AS order_duns_nbr,
										work__idrp_exas_vpacks_with_sarm::vendor_carton_qty AS vendor_carton_qty,
										work__idrp_exas_vpacks_with_sarm::vendor_stock_nbr AS vendor_stock_nbr,
										work__idrp_exas_vpacks_with_sarm::carton_per_layer_qty AS carton_per_layer_qty,
										work__idrp_exas_vpacks_with_sarm::layer_per_pallet_qty AS layer_per_pallet_qty,
										work__idrp_exas_vpacks_with_sarm::ksn_purchase_status_cd as ksn_purchase_status_cd,
										work__idrp_exas_vpacks_with_sarm::dotcom_allocation_ind as dotcom_allocation_ind,
										work__idrp_exas_vpacks_with_sarm::store_location_nbr AS store_location_nbr,
										work__idrp_exas_vpacks_with_sarm::days_to_check_begin_days_cnt AS days_to_check_begin_days_cnt,
										work__idrp_exas_vpacks_with_sarm::days_to_check_end_days_cnt AS days_to_check_end_days_cnt,
										gold_geographic_model_str_data_filter::locn_nbr AS location_nbr;

work__idrp_exas_dotcom_vpacks_union =  union  work__idrp_exas_vpacks_with_sarm_geo_model_gen,work__idrp_exas_vpacks_no_sarm;

work__idrp_exas_dotcom_vpacks_gen =  foreach work__idrp_exas_dotcom_vpacks_union generate 
										vendor_package_id AS vendor_package_id,
										location_nbr AS store_location_nbr;
										
work__idrp_vp_exas_dotcom_stores_join = join  work__idrp_exas_vpacks_dotcom by ((int)vendor_package_id),  work__idrp_exas_dotcom_vpacks_gen by ((int)vendor_package_id);

work__idrp_vp_exas_dotcom_stores_dist = distinct work__idrp_vp_exas_dotcom_stores_join;

work__idrp_vp_exas_dotcom_stores =  foreach work__idrp_vp_exas_dotcom_stores_dist generate 
											work__idrp_exas_vpacks_dotcom::shc_item_id,
											work__idrp_exas_vpacks_dotcom::shc_item_desc,
											work__idrp_exas_vpacks_dotcom::shc_division_nbr,
											work__idrp_exas_vpacks_dotcom::shc_division_desc,
											work__idrp_exas_vpacks_dotcom::shc_department_nbr,
											work__idrp_exas_vpacks_dotcom::shc_department_desc,
											work__idrp_exas_vpacks_dotcom::shc_category_group_level_nbr,
											work__idrp_exas_vpacks_dotcom::shc_category_group_desc,
											work__idrp_exas_vpacks_dotcom::shc_category_nbr,
											work__idrp_exas_vpacks_dotcom::shc_category_desc,
											work__idrp_exas_vpacks_dotcom::shc_sub_category_nbr,
											work__idrp_exas_vpacks_dotcom::shc_sub_category_desc,
											work__idrp_exas_vpacks_dotcom::sears_business_nbr,
											work__idrp_exas_vpacks_dotcom::sears_business_desc,
											work__idrp_exas_vpacks_dotcom::sears_division_nbr,
											work__idrp_exas_vpacks_dotcom::sears_division_desc,
											work__idrp_exas_vpacks_dotcom::sears_line_nbr,
											work__idrp_exas_vpacks_dotcom::sears_line_desc,
											work__idrp_exas_vpacks_dotcom::sears_sub_line_nbr,
											work__idrp_exas_vpacks_dotcom::sears_sub_line_desc,
											work__idrp_exas_vpacks_dotcom::sears_class_nbr,
											work__idrp_exas_vpacks_dotcom::sears_class_desc,
											work__idrp_exas_vpacks_dotcom::sears_item_nbr,
											work__idrp_exas_vpacks_dotcom::sears_sku_nbr,
											work__idrp_exas_vpacks_dotcom::delivered_direct_ind,
											work__idrp_exas_vpacks_dotcom::installation_ind,
											work__idrp_exas_vpacks_dotcom::store_forecast_cd,
											work__idrp_exas_vpacks_dotcom::shc_item_type_cd,
											work__idrp_exas_vpacks_dotcom::item_purchase_status_cd,
											work__idrp_exas_vpacks_dotcom::network_distribution_cd,
											work__idrp_exas_vpacks_dotcom::future_network_distribution_cd,
											work__idrp_exas_vpacks_dotcom::future_network_distribution_effective_dt,
											work__idrp_exas_vpacks_dotcom::jit_network_distribution_cd,
											work__idrp_exas_vpacks_dotcom::reorder_authentication_cd,
											work__idrp_exas_vpacks_dotcom::can_carry_model_id,
											work__idrp_exas_vpacks_dotcom::grocery_item_ind,
											work__idrp_exas_vpacks_dotcom::shc_item_corporate_owner_cd,
											work__idrp_exas_vpacks_dotcom::iplan_id, 
											work__idrp_exas_vpacks_dotcom::markdown_style_reference_cd,
											work__idrp_exas_vpacks_dotcom::sears_order_system_cd,
											work__idrp_exas_vpacks_dotcom::idrp_order_method_cd,
											work__idrp_exas_vpacks_dotcom::idrp_order_method_desc,
											work__idrp_exas_vpacks_dotcom::forecast_group_format_id,
											work__idrp_exas_vpacks_dotcom::forecast_group_desc,
											work__idrp_exas_vpacks_dotcom::dotcom_eligibility_cd,
											work__idrp_exas_vpacks_dotcom::ksn_id,
											work__idrp_exas_vpacks_dotcom::vendor_package_id,
											work__idrp_exas_vpacks_dotcom::vendor_package_purchase_status_cd,
											work__idrp_exas_vpacks_dotcom::vendor_package_purchase_status_dt,
											work__idrp_exas_vpacks_dotcom::vendor_package_owner_cd,
											work__idrp_exas_vpacks_dotcom::ksn_package_id,
											work__idrp_exas_vpacks_dotcom::service_area_restriction_model_id,
											work__idrp_exas_vpacks_dotcom::flow_type_cd,
											work__idrp_exas_vpacks_dotcom::aprk_id,
											work__idrp_exas_vpacks_dotcom::import_ind,
											work__idrp_exas_vpacks_dotcom::order_duns_nbr,
											work__idrp_exas_vpacks_dotcom::vendor_carton_qty,
											work__idrp_exas_vpacks_dotcom::vendor_stock_nbr,
											work__idrp_exas_vpacks_dotcom::carton_per_layer_qty,
											work__idrp_exas_vpacks_dotcom::layer_per_pallet_qty,
											work__idrp_exas_vpacks_dotcom::ksn_purchase_status_cd,
											work__idrp_exas_vpacks_dotcom::dotcom_allocation_ind,
											work__idrp_exas_dotcom_vpacks_gen::store_location_nbr,
											'0' as days_to_check_begin_days_cnt,
											'365' as days_to_check_end_days_cnt;

work__idrp_vp_exas_dotcom_can_carry_stores  =  union work__idrp_vp_cancarry_stores, work__idrp_vp_exas_dotcom_stores;


STORE work__idrp_vp_exas_dotcom_can_carry_stores INTO '$WORK__IDRP_VP_EXAS_DOTCOM_CANCARRY_STORES_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');





/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
