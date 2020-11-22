/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_kmart_vendor_package_location_store_level_part1.pig
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
#		 2015-03-16   Meghana       Spira 3985
#		2015-08-14		Priyanka	SPIRA 3018. Implemented logic to set the markdown indicator for exploding assortments on line no: 1234 
#		2015-08-28		Priyanka	SPIRA	4373 Dotcomm Indicator 
#		2016-06-09		Pankaj		SPIRS IPS-348 Flow Through allocation failures due to no eligible stores when INFOREM shows store authorized.
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

SET default_parallel 300;
--SET io.sort.mb 1024;
--SET io.sort.factor 15;
--set mapred.child.java.opts -Xmx4096m;
--set mapred.reduce.child.java.opts -Xmx4096m;
REGISTER  $UDF_JAR;
DEFINE TO_JULIAN_DATE $TO_JULIAN_DATE;
DEFINE GetDotComOrderableIndicator com.searshc.supplychain.idrp.udf.GetDotComOrderableIndicator();

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

gold__item_scan_based_trading_vendor_package_current_data = 
      LOAD '$GOLD__ITEM_SCAN_BASED_TRADING_VENDOR_PACKAGE_CURRENT_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($GOLD__ITEM_SCAN_BASED_TRADING_VENDOR_PACKAGE_CURRENT_SCHEMA); 

work__idrp_vpstores_after_order_dotcom_edits_data = 
      LOAD '$WORK__IDRP_VPSTORES_AFTER_ORDER_DOTCOM_EDITS_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_VPSTORES_AFTER_ORDER_DOTCOM_EDITS_SCHEMA);

smith__idrp_ksn_attribute_current = LOAD '$SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_SCHEMA);

gold__geographic_network_store_dc_data = 
      LOAD '$GOLD__GEOGRAPHIC_NETWORK_STORE_DC_ALL_RECORDS_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($GOLD__GEOGRAPHIC_NETWORK_STORE_DC_SCHEMA);

smith__idrp_item_eligibility_batchdate  = 
       LOAD '$SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_LOCATION' 
       USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
       AS ($SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_SCHEMA);

gold__item_shc_hierarchy_current_data = 
       LOAD '$GOLD__ITEM_SHC_HIERARCHY_CURRENT_LOCATION' 
       USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
       AS ($GOLD__ITEM_SHC_HIERARCHY_CURRENT_SCHEMA);

gold__item_exploding_assortment_data = 
      LOAD '$GOLD__ITEM_EXPLODING_ASSORTMENT_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($GOLD__ITEM_EXPLODING_ASSORTMENT_SCHEMA);

------CR 5018----------------------------------------------------------------------------------
	  
	  /*
gold__item_exploding_assortment_active =  
	  LOAD '$GOLD__ITEM_EXPLODING_ASSORTMENT_LOCATION/record_status=active'  
	  USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($GOLD__ITEM_EXPLODING_ASSORTMENT_SCHEMA);
	  
gold__item_exploding_assortment_data_active = 
		foreach gold__item_exploding_assortment_active 
		generate 
		external_vendor_package_id as external_vendor_package_id,
		internal_vendor_package_id as internal_vendor_package_id;	  
*/

smith__idrp_collections_carton_pack_xref_current_data = 
      LOAD '$SMITH__IDRP_COLLECTIONS_CARTON_PACK_XREF_CURRENT_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($SMITH__IDRP_COLLECTIONS_CARTON_PACK_XREF_CURRENT_SCHEMA);

smith__idrp_vend_pack_dc_combined_data = 
      LOAD '$SMITH__IDRP_VEND_PACK_DC_COMBINED_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($SMITH__IDRP_VEND_PACK_DC_COMBINED_SCHEMA);

/************* CR 3736 - added join on smith__idrp_vend_pack_combined ***********/

smith__idrp_vend_pack_combined_data =
      LOAD '$SMITH__IDRP_VEND_PACK_COMBINED_LOCATION'
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($SMITH__IDRP_VEND_PACK_COMBINED_SCHEMA);

gen_smith__idrp_vend_pack_combined_data =
      FOREACH smith__idrp_vend_pack_combined_data
      GENERATE
	vendor_package_id,
	dotcom_allocation_ind;

/********************************************************************************/

smith__idrp_dc_location_current_data = 
      LOAD '$SMITH_IDRP_DC_LOCATION_CURRENT_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($SMITH_IDRP_DC_LOCATION_CURRENT_SCHEMA);

smith__idrp_eligible_loc_data = 
      LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);

smith__idrp_eligible_loc_data = 
      FOREACH smith__idrp_eligible_loc_data
      GENERATE
              loc AS loc,
              srs_loc AS srs_loc,
              loc_lvl_cd  AS loc_lvl_cd;  

smith__idrp_markdown_ksn_location_current_data = 
      LOAD '$SMITH__IDRP_MARKDOWN_KSN_LOCATION_CURRENT_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($SMITH__IDRP_MARKDOWN_KSN_LOCATION_CURRENT_SCHEMA);

smith__idrp_vendor_package_store_driver_data = 
      LOAD '$SMITH__IDRP_VENDOR_PACKAGE_STORE_DRIVER_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($SMITH__IDRP_VENDOR_PACKAGE_STORE_DRIVER_SCHEMA);


work__idrp_vpstores_after_order_dotcom_edits_join_gold__item_scan_based_trading_vendor_package_current = 
      JOIN work__idrp_vpstores_after_order_dotcom_edits_data BY vendor_package_id LEFT OUTER,
           gold__item_scan_based_trading_vendor_package_current_data BY vendor_package_id USING 'skewed';

work__idrp_vp_stores_sbt_ind = 
      FOREACH work__idrp_vpstores_after_order_dotcom_edits_join_gold__item_scan_based_trading_vendor_package_current 
      GENERATE 
              work__idrp_vpstores_after_order_dotcom_edits_data::shc_item_id AS shc_item_id,
              work__idrp_vpstores_after_order_dotcom_edits_data::sears_division_nbr AS sears_division_nbr,
              work__idrp_vpstores_after_order_dotcom_edits_data::sears_item_nbr AS sears_item_nbr,
              work__idrp_vpstores_after_order_dotcom_edits_data::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_vpstores_after_order_dotcom_edits_data::shc_item_type_cd AS shc_item_type_cd,
              work__idrp_vpstores_after_order_dotcom_edits_data::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_vpstores_after_order_dotcom_edits_data::network_distribution_cd AS network_distribution_cd,
              work__idrp_vpstores_after_order_dotcom_edits_data::can_carry_model_id AS can_carry_model_id,
              work__idrp_vpstores_after_order_dotcom_edits_data::sears_order_system_cd AS sears_order_system_cd,
              work__idrp_vpstores_after_order_dotcom_edits_data::idrp_order_method_cd AS idrp_order_method_cd,
              work__idrp_vpstores_after_order_dotcom_edits_data::idrp_order_method_desc AS idrp_order_method_desc,
              work__idrp_vpstores_after_order_dotcom_edits_data::ksn_id AS ksn_id,
              work__idrp_vpstores_after_order_dotcom_edits_data::vendor_package_id AS vendor_package_id,
              work__idrp_vpstores_after_order_dotcom_edits_data::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_vpstores_after_order_dotcom_edits_data::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_vpstores_after_order_dotcom_edits_data::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_vpstores_after_order_dotcom_edits_data::ksn_package_id AS ksn_package_id,
              work__idrp_vpstores_after_order_dotcom_edits_data::service_area_restriction_model_id AS service_area_restriction_model_id,
              work__idrp_vpstores_after_order_dotcom_edits_data::flow_type_cd AS flow_type_cd,
              work__idrp_vpstores_after_order_dotcom_edits_data::aprk_id AS aprk_id,
              work__idrp_vpstores_after_order_dotcom_edits_data::import_ind AS import_ind,
              work__idrp_vpstores_after_order_dotcom_edits_data::order_duns_nbr AS order_duns_nbr,
              work__idrp_vpstores_after_order_dotcom_edits_data::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_vpstores_after_order_dotcom_edits_data::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_vpstores_after_order_dotcom_edits_data::carton_per_layer_qty AS carton_per_layer_qty,
              work__idrp_vpstores_after_order_dotcom_edits_data::layer_per_pallet_qty AS layer_per_pallet_qty,
              work__idrp_vpstores_after_order_dotcom_edits_data::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_vpstores_after_order_dotcom_edits_data::dotcom_allocation_ind AS dotcom_allocation_ind,
              work__idrp_vpstores_after_order_dotcom_edits_data::store_location_nbr AS store_location_nbr,
              work__idrp_vpstores_after_order_dotcom_edits_data::days_to_check_begin_days_cnt AS days_to_check_begin_day_qty,
              work__idrp_vpstores_after_order_dotcom_edits_data::days_to_check_end_days_cnt AS days_to_check_end_day_qty,
              work__idrp_vpstores_after_order_dotcom_edits_data::days_to_check_begin_dt AS days_to_check_begin_dt,
              work__idrp_vpstores_after_order_dotcom_edits_data::days_to_check_end_dt AS days_to_check_end_dt,
              work__idrp_vpstores_after_order_dotcom_edits_data::location_format_type_cd AS location_format_type_cd,
              work__idrp_vpstores_after_order_dotcom_edits_data::format_type_cd AS format_type_cd,
              work__idrp_vpstores_after_order_dotcom_edits_data::location_level_cd AS location_level_cd,
              work__idrp_vpstores_after_order_dotcom_edits_data::location_owner_cd AS location_owner_cd,
              (gold__item_scan_based_trading_vendor_package_current_data::vendor_package_id IS NULL ? 'N' : 'Y') AS scan_based_trading_ind;


work__idrp_vp_stores_sbt_ind_join_gold__item_attribute_relate_current = 
      JOIN work__idrp_vp_stores_sbt_ind BY ksn_id LEFT OUTER,
           smith__idrp_ksn_attribute_current BY ksn_id; 


work__idrp_vp_stores_cross_mdse = 
      FOREACH work__idrp_vp_stores_sbt_ind_join_gold__item_attribute_relate_current
      GENERATE
              work__idrp_vp_stores_sbt_ind::shc_item_id AS shc_item_id,
              work__idrp_vp_stores_sbt_ind::sears_division_nbr AS sears_division_nbr,
              work__idrp_vp_stores_sbt_ind::sears_item_nbr AS sears_item_nbr,
              work__idrp_vp_stores_sbt_ind::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_vp_stores_sbt_ind::shc_item_type_cd AS shc_item_type_cd,
              work__idrp_vp_stores_sbt_ind::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_vp_stores_sbt_ind::network_distribution_cd AS network_distribution_cd,
              work__idrp_vp_stores_sbt_ind::can_carry_model_id AS can_carry_model_id,
              work__idrp_vp_stores_sbt_ind::sears_order_system_cd AS sears_order_system_cd,
              work__idrp_vp_stores_sbt_ind::idrp_order_method_cd AS idrp_order_method_cd,
              work__idrp_vp_stores_sbt_ind::idrp_order_method_desc AS idrp_order_method_desc,
              work__idrp_vp_stores_sbt_ind::ksn_id AS ksn_id,
              work__idrp_vp_stores_sbt_ind::vendor_package_id AS vendor_package_id,
              work__idrp_vp_stores_sbt_ind::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_vp_stores_sbt_ind::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_vp_stores_sbt_ind::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_vp_stores_sbt_ind::ksn_package_id AS ksn_package_id,
              work__idrp_vp_stores_sbt_ind::service_area_restriction_model_id AS service_area_restriction_model_id,
              work__idrp_vp_stores_sbt_ind::flow_type_cd AS flow_type_cd,
              work__idrp_vp_stores_sbt_ind::aprk_id AS aprk_id,
              work__idrp_vp_stores_sbt_ind::import_ind AS import_ind,
              work__idrp_vp_stores_sbt_ind::order_duns_nbr AS order_duns_nbr,
              work__idrp_vp_stores_sbt_ind::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_vp_stores_sbt_ind::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_vp_stores_sbt_ind::carton_per_layer_qty AS carton_per_layer_qty,
              work__idrp_vp_stores_sbt_ind::layer_per_pallet_qty AS layer_per_pallet_qty,
              work__idrp_vp_stores_sbt_ind::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_vp_stores_sbt_ind::dotcom_allocation_ind AS dotcom_allocation_ind,
              work__idrp_vp_stores_sbt_ind::store_location_nbr AS store_location_nbr,
              work__idrp_vp_stores_sbt_ind::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_vp_stores_sbt_ind::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__idrp_vp_stores_sbt_ind::days_to_check_begin_dt AS days_to_check_begin_dt,
              work__idrp_vp_stores_sbt_ind::days_to_check_end_dt AS days_to_check_end_dt,
              work__idrp_vp_stores_sbt_ind::location_format_type_cd AS location_format_type_cd,
              work__idrp_vp_stores_sbt_ind::format_type_cd AS format_type_cd,
              work__idrp_vp_stores_sbt_ind::location_level_cd AS location_level_cd,
              work__idrp_vp_stores_sbt_ind::location_owner_cd AS location_owner_cd,
              work__idrp_vp_stores_sbt_ind::scan_based_trading_ind AS scan_based_trading_ind,
              (smith__idrp_ksn_attribute_current::ksn_id IS NULL ? '' : smith__idrp_ksn_attribute_current::cross_merchandising_cd) AS cross_merchandising_cd; 
 
work__idrp_vp_stores_cross_mdse_post_SK3000 = 
      FILTER work__idrp_vp_stores_cross_mdse BY IsNull(cross_merchandising_cd,'') !='SK3000';

SPLIT work__idrp_vp_stores_cross_mdse_post_SK3000 
INTO work__idrp_vp_stores_flow_dc IF(TRIM(flow_type_cd)=='DC' OR TRIM(flow_type_cd)=='VCDC'),
     work__idrp_vp_stores_flow_direct IF(TRIM(flow_type_cd)!='DC' AND TRIM(flow_type_cd)!='VCDC');

	 /*

gold__geographic_network_store_dc_join_smith__idrp_replenishment_day = 
      CROSS gold__geographic_network_store_dc_data,
            smith__idrp_item_eligibility_batchdate;

gold__geographic_network_store_dc_data_fltrd = 
      FILTER gold__geographic_network_store_dc_join_smith__idrp_replenishment_day 
      BY (TRIM(smith__idrp_item_eligibility_batchdate::batch_dt) >= TRIM(gold__geographic_network_store_dc_data::effective_dt)
      AND
          TRIM(smith__idrp_item_eligibility_batchdate::batch_dt) <= TRIM(gold__geographic_network_store_dc_data::expiration_dt));
*/


gold__geographic_network_store_dc_data_fltrd = 
      FILTER gold__geographic_network_store_dc_data 
      BY (TRIM('$batch_dt') >= TRIM(effective_dt)
      AND
          TRIM('$batch_dt') <= TRIM(expiration_dt));

work__idrp_store_servicing_dc = 
      FOREACH gold__geographic_network_store_dc_data_fltrd
      GENERATE 
              store_location_nbr AS store_location_nbr,
              network_distribution_cd AS network_distribution_cd,
              servicing_dc_location_nbr AS servicing_dc_location_nbr,
              TO_JULIAN_DATE(expiration_dt) AS dc_effective_dt;

work__idrp_vp_stores_flow_dc_join_work__idrp_store_servicing_dc = 
      JOIN work__idrp_vp_stores_flow_dc BY ((int)store_location_nbr,network_distribution_cd) LEFT OUTER,
           work__idrp_store_servicing_dc BY ((int)store_location_nbr,network_distribution_cd);


work__idrp_vp_stores_serv_dc_fltr = FILTER work__idrp_vp_stores_flow_dc_join_work__idrp_store_servicing_dc BY 
									((work__idrp_vp_stores_flow_dc::flow_type_cd !='DC') OR (work__idrp_vp_stores_flow_dc::flow_type_cd =='DC' AND work__idrp_store_servicing_dc::servicing_dc_location_nbr != '0'));
 
work__idrp_vp_stores_serv_dc = 
      FOREACH work__idrp_vp_stores_serv_dc_fltr
      GENERATE
              work__idrp_vp_stores_flow_dc::shc_item_id AS shc_item_id,
              work__idrp_vp_stores_flow_dc::sears_division_nbr AS sears_division_nbr,
              work__idrp_vp_stores_flow_dc::sears_item_nbr AS sears_item_nbr,
              work__idrp_vp_stores_flow_dc::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_vp_stores_flow_dc::shc_item_type_cd AS shc_item_type_cd,
              work__idrp_vp_stores_flow_dc::network_distribution_cd AS network_distribution_cd,
              work__idrp_vp_stores_flow_dc::can_carry_model_id AS can_carry_model_id,
              work__idrp_vp_stores_flow_dc::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_vp_stores_flow_dc::sears_order_system_cd AS sears_order_system_cd,
              work__idrp_vp_stores_flow_dc::idrp_order_method_cd AS idrp_order_method_cd,
              work__idrp_vp_stores_flow_dc::idrp_order_method_desc AS idrp_order_method_desc,
              work__idrp_vp_stores_flow_dc::ksn_id AS ksn_id,
              work__idrp_vp_stores_flow_dc::vendor_package_id AS vendor_package_id,
              work__idrp_vp_stores_flow_dc::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_vp_stores_flow_dc::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_vp_stores_flow_dc::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_vp_stores_flow_dc::ksn_package_id AS ksn_package_id,
              work__idrp_vp_stores_flow_dc::service_area_restriction_model_id AS service_area_restriction_model_id,
              work__idrp_vp_stores_flow_dc::flow_type_cd AS flow_type_cd,
              work__idrp_vp_stores_flow_dc::aprk_id AS aprk_id,
              work__idrp_vp_stores_flow_dc::import_ind AS import_ind,
              work__idrp_vp_stores_flow_dc::order_duns_nbr AS order_duns_nbr,
              work__idrp_vp_stores_flow_dc::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_vp_stores_flow_dc::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_vp_stores_flow_dc::carton_per_layer_qty AS carton_per_layer_qty,
              work__idrp_vp_stores_flow_dc::layer_per_pallet_qty AS layer_per_pallet_qty,
              work__idrp_vp_stores_flow_dc::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_vp_stores_flow_dc::dotcom_allocation_ind AS dotcom_allocation_ind,
              work__idrp_vp_stores_flow_dc::store_location_nbr AS store_location_nbr,
              work__idrp_vp_stores_flow_dc::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_vp_stores_flow_dc::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__idrp_vp_stores_flow_dc::days_to_check_begin_dt AS days_to_check_begin_dt,
              work__idrp_vp_stores_flow_dc::days_to_check_end_dt AS days_to_check_end_dt,
              work__idrp_vp_stores_flow_dc::location_format_type_cd AS location_format_type_cd,
              work__idrp_vp_stores_flow_dc::format_type_cd AS format_type_cd,
              work__idrp_vp_stores_flow_dc::location_level_cd AS location_level_cd,
              work__idrp_vp_stores_flow_dc::location_owner_cd AS location_owner_cd,
              work__idrp_vp_stores_flow_dc::scan_based_trading_ind AS scan_based_trading_ind,
              work__idrp_vp_stores_flow_dc::cross_merchandising_cd AS cross_merchandising_cd,
              ((work__idrp_store_servicing_dc::servicing_dc_location_nbr IS NULL OR work__idrp_store_servicing_dc::network_distribution_cd IS NULL) ? '0' : work__idrp_store_servicing_dc::servicing_dc_location_nbr) AS servicing_dc_location_nbr,
              ((work__idrp_store_servicing_dc::servicing_dc_location_nbr IS NULL OR work__idrp_store_servicing_dc::network_distribution_cd IS NULL) ? '0' : work__idrp_store_servicing_dc::servicing_dc_location_nbr) AS source_location_nbr,
              ((work__idrp_store_servicing_dc::servicing_dc_location_nbr IS NULL OR work__idrp_store_servicing_dc::network_distribution_cd IS NULL) ? '1970-01-01' : work__idrp_store_servicing_dc::dc_effective_dt) AS dc_effective_dt,
              '' AS purchase_order_vendor_location_id;


work__idrp_vp_stores_direct_default_dc = 
      FOREACH work__idrp_vp_stores_flow_direct
      GENERATE
              shc_item_id AS shc_item_id,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              shc_item_type_cd AS shc_item_type_cd,
              network_distribution_cd AS network_distribution_cd,
              can_carry_model_id AS can_carry_model_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              sears_order_system_cd AS sears_order_system_cd,
              idrp_order_method_cd AS idrp_order_method_cd,
              idrp_order_method_desc AS idrp_order_method_desc,
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
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              dotcom_allocation_ind AS dotcom_allocation_ind,
              store_location_nbr AS store_location_nbr,
              days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              days_to_check_end_day_qty AS days_to_check_end_day_qty,
              days_to_check_begin_dt AS days_to_check_begin_dt,
              days_to_check_end_dt AS days_to_check_end_dt,
              location_format_type_cd AS location_format_type_cd,
              format_type_cd AS format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              scan_based_trading_ind AS scan_based_trading_ind,
              cross_merchandising_cd AS cross_merchandising_cd,
              '0' AS servicing_dc_location_nbr,
              '0' AS source_location_nbr,
              '1970-01-01' AS dc_effective_dt,
              '' AS purchase_order_vendor_location_id;



smith__idrp_vp_stores_with_dc_combined = 
       UNION work__idrp_vp_stores_serv_dc,
             work__idrp_vp_stores_direct_default_dc;


work__idrp_source_location_applied_work__idrp_source_supplier_applied_1 = 
      FOREACH smith__idrp_vp_stores_with_dc_combined
      GENERATE
              shc_item_id AS shc_item_id,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              shc_item_type_cd AS shc_item_type_cd,
              network_distribution_cd AS network_distribution_cd,
              can_carry_model_id AS can_carry_model_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              sears_order_system_cd AS sears_order_system_cd,
              idrp_order_method_cd AS idrp_order_method_cd,
              idrp_order_method_desc AS idrp_order_method_desc,
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
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              dotcom_allocation_ind AS dotcom_allocation_ind,
              store_location_nbr AS store_location_nbr,
              days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              days_to_check_end_day_qty AS days_to_check_end_day_qty,
              days_to_check_begin_dt AS days_to_check_begin_dt,
              days_to_check_end_dt AS days_to_check_end_dt,
              location_format_type_cd AS location_format_type_cd,
              format_type_cd AS format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              scan_based_trading_ind AS scan_based_trading_ind,
              cross_merchandising_cd AS cross_merchandising_cd,
              servicing_dc_location_nbr AS servicing_dc_nbr,
              (TRIM(source_location_nbr)=='0' ? CONCAT(order_duns_nbr,'_O') : source_location_nbr) AS source_location_nbr_temp,
              dc_effective_dt AS dc_effective_dt,
              (TRIM(source_location_nbr)=='0' ? CONCAT(order_duns_nbr,'_O') : purchase_order_vendor_location_id) AS purchase_order_vendor_location_id;


work__idrp_source_location_applied_work__idrp_source_supplier_applied = FOREACH
              work__idrp_source_location_applied_work__idrp_source_supplier_applied_1
              GENERATE
              shc_item_id,
              sears_division_nbr,
              sears_item_nbr,
              sears_sku_nbr,
              shc_item_type_cd,
              network_distribution_cd,
              can_carry_model_id,
              item_purchase_status_cd,
              sears_order_system_cd,
              idrp_order_method_cd,
              idrp_order_method_desc,
              ksn_id,
              vendor_package_id,
              vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt,
              vendor_package_owner_cd,
              ksn_package_id,
              service_area_restriction_model_id,
              flow_type_cd,
              aprk_id,
              import_ind,
              order_duns_nbr,
              vendor_carton_qty,
              vendor_stock_nbr,
              carton_per_layer_qty,
              layer_per_pallet_qty,
              ksn_purchase_status_cd,
              dotcom_allocation_ind,
              store_location_nbr,
              days_to_check_begin_day_qty,
              days_to_check_end_day_qty,
              days_to_check_begin_dt,
              days_to_check_end_dt,
              location_format_type_cd,
              format_type_cd,
              location_level_cd,
              location_owner_cd,
              scan_based_trading_ind,
              cross_merchandising_cd,
              servicing_dc_nbr,
              source_location_nbr_temp AS source_location_nbr,
              dc_effective_dt,
              purchase_order_vendor_location_id;


work__join_smith_loc = 
      JOIN work__idrp_source_location_applied_work__idrp_source_supplier_applied BY source_location_nbr, 
           smith__idrp_eligible_loc_data BY loc USING 'replicated';


work__idrp_source_level_applied = 
      FOREACH work__join_smith_loc 
      GENERATE 
              work__idrp_source_location_applied_work__idrp_source_supplier_applied::shc_item_id AS shc_item_id,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              shc_item_type_cd AS shc_item_type_cd,
              network_distribution_cd AS network_distribution_cd,
              can_carry_model_id AS can_carry_model_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              sears_order_system_cd AS sears_order_system_cd,
              idrp_order_method_cd AS idrp_order_method_cd,
              idrp_order_method_desc AS idrp_order_method_desc,
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
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              dotcom_allocation_ind AS dotcom_allocation_ind,
              store_location_nbr AS store_location_nbr,
              days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              days_to_check_end_day_qty AS days_to_check_end_day_qty,
              days_to_check_begin_dt AS days_to_check_begin_dt,
              days_to_check_end_dt AS days_to_check_end_dt,
              location_format_type_cd AS location_format_type_cd,
              format_type_cd AS format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              scan_based_trading_ind AS scan_based_trading_ind,
              cross_merchandising_cd AS cross_merchandising_cd,
              servicing_dc_nbr AS servicing_dc_nbr,
              source_location_nbr AS source_location_nbr,
              dc_effective_dt AS dc_effective_dt,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              loc_lvl_cd AS source_location_level_cd;


SPLIT work__idrp_source_level_applied 
INTO work__idrp_dotcom_order_exas IF TRIM(shc_item_type_cd)=='EXAS',
     work__idrp_dotcom_order_no_exas IF TRIM(shc_item_type_cd)!='EXAS';

-----------------------------------------gold__item_exploding_assortment Filter applied---------------------------------------------------------

/*
gold__item_exploding_assortment_join_smith__idrp_replenishment_day = 
      CROSS gold__item_exploding_assortment_data,
            smith__idrp_item_eligibility_batchdate;

gold__item_exploding_assortment_data_fltrd = 
      FILTER gold__item_exploding_assortment_join_smith__idrp_replenishment_day
      BY (TRIM(smith__idrp_item_eligibility_batchdate::processing_ts) >= TRIM(gold__item_exploding_assortment_data::effective_ts)
      AND
          TRIM(smith__idrp_item_eligibility_batchdate::processing_ts) <= TRIM(gold__item_exploding_assortment_data::expiration_ts));
*/

gold__item_exploding_assortment_data_fltrd =  
	FILTER gold__item_exploding_assortment_data 
		BY (REPLACE('$processing_ts','~', ' ') >= TRIM(effective_ts)
      AND         REPLACE('$processing_ts','~', ' ') <= TRIM(expiration_ts));
	  
  
gold__item_exploding_assortment_data_fltrd_join_vend_pack_combined =
      JOIN gold__item_exploding_assortment_data_fltrd BY internal_vendor_package_id,
	gen_smith__idrp_vend_pack_combined_data BY vendor_package_id;

----------------------------------------------------------CR 4373--------------------------------------------------------------------------------	
	
gold__item_exploding_assortment_data_fltrd = 
      FOREACH gold__item_exploding_assortment_data_fltrd_join_vend_pack_combined
      GENERATE 
              gold__item_exploding_assortment_data_fltrd::external_vendor_package_id AS external_vendor_package_id,
              gold__item_exploding_assortment_data_fltrd::internal_vendor_package_id AS internal_vendor_package_id,
			(dotcom_allocation_ind == 'B' ? 1 :0 ) as Bindicator,
			(dotcom_allocation_ind == 'K' ? 1 :0 ) as Kindicator,
			(dotcom_allocation_ind == 'S' ? 1 :0 ) as Sindicator,
			(dotcom_allocation_ind == 'N' ? 1 :0 ) as Nindicator;


grp_gold__item_exploding_assortment_data_fltrd = GROUP gold__item_exploding_assortment_data_fltrd by external_vendor_package_id;

gen_gold__item_exploding_assortment_data_fltrd = foreach grp_gold__item_exploding_assortment_data_fltrd generate group as external_vendor_package_id,
														SUM(gold__item_exploding_assortment_data_fltrd.Bindicator) as Bindicator,
														SUM(gold__item_exploding_assortment_data_fltrd.Kindicator) as Kindicator,
														SUM(gold__item_exploding_assortment_data_fltrd.Sindicator) as Sindicator,
														SUM(gold__item_exploding_assortment_data_fltrd.Nindicator) as Nindicator;
														
group_data_generate = foreach gen_gold__item_exploding_assortment_data_fltrd generate 
														external_vendor_package_id as external_vendor_package_id,
														( Bindicator>0 and Kindicator==0 and Nindicator==0 and Sindicator==0 ? 'B'
														: ( Kindicator>0 and Bindicator==0 and Sindicator==0  ? 'K'
															: (Sindicator>0 and Bindicator==0 and Kindicator==0 ? 'S'
																: (Nindicator>0 and Kindicator==0 and Bindicator==0 and Sindicator==0 ? 'N'
																	:'B'
														)))) as dotcom_order_ind;
														
join_group_data_work_exas = JOIN work__idrp_dotcom_order_exas BY vendor_package_id,
				group_data_generate BY external_vendor_package_id;														

join_group_data_work_exas_gen = FOREACH join_group_data_work_exas GENERATE
              work__idrp_dotcom_order_exas::shc_item_id AS shc_item_id,
              work__idrp_dotcom_order_exas::sears_division_nbr AS sears_division_nbr,
              work__idrp_dotcom_order_exas::sears_item_nbr AS sears_item_nbr,
              work__idrp_dotcom_order_exas::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_dotcom_order_exas::shc_item_type_cd AS shc_item_type_cd,
              work__idrp_dotcom_order_exas::network_distribution_cd AS network_distribution_cd,
              work__idrp_dotcom_order_exas::can_carry_model_id AS can_carry_model_id,
              work__idrp_dotcom_order_exas::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_dotcom_order_exas::sears_order_system_cd AS sears_order_system_cd,
              work__idrp_dotcom_order_exas::idrp_order_method_cd AS idrp_order_method_cd,
              work__idrp_dotcom_order_exas::idrp_order_method_desc AS idrp_order_method_desc,
              work__idrp_dotcom_order_exas::ksn_id AS ksn_id,
              work__idrp_dotcom_order_exas::vendor_package_id AS vendor_package_id,
              work__idrp_dotcom_order_exas::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_dotcom_order_exas::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_dotcom_order_exas::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_dotcom_order_exas::ksn_package_id AS ksn_package_id,
              work__idrp_dotcom_order_exas::service_area_restriction_model_id AS service_area_restriction_model_id,
              work__idrp_dotcom_order_exas::flow_type_cd AS flow_type_cd,
              work__idrp_dotcom_order_exas::aprk_id AS aprk_id,
              work__idrp_dotcom_order_exas::import_ind AS import_ind,
              work__idrp_dotcom_order_exas::order_duns_nbr AS order_duns_nbr,
              work__idrp_dotcom_order_exas::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_dotcom_order_exas::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_dotcom_order_exas::carton_per_layer_qty AS carton_per_layer_qty,
              work__idrp_dotcom_order_exas::layer_per_pallet_qty AS layer_per_pallet_qty,
              work__idrp_dotcom_order_exas::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              group_data_generate::dotcom_order_ind AS dotcom_order_ind,
              work__idrp_dotcom_order_exas::store_location_nbr AS store_location_nbr,
              work__idrp_dotcom_order_exas::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_dotcom_order_exas::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__idrp_dotcom_order_exas::days_to_check_begin_dt AS days_to_check_begin_dt,
              work__idrp_dotcom_order_exas::days_to_check_end_dt AS days_to_check_end_dt,
              work__idrp_dotcom_order_exas::location_format_type_cd AS location_format_type_cd,
              work__idrp_dotcom_order_exas::format_type_cd AS format_type_cd,
              work__idrp_dotcom_order_exas::location_level_cd AS location_level_cd,
              work__idrp_dotcom_order_exas::location_owner_cd AS location_owner_cd,
              work__idrp_dotcom_order_exas::scan_based_trading_ind AS scan_based_trading_ind,
              work__idrp_dotcom_order_exas::cross_merchandising_cd AS cross_merchandising_cd,
              work__idrp_dotcom_order_exas::servicing_dc_nbr AS servicing_dc_nbr,
              work__idrp_dotcom_order_exas::source_location_nbr AS source_location_nbr,
              work__idrp_dotcom_order_exas::dc_effective_dt AS dc_effective_dt,
              work__idrp_dotcom_order_exas::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              work__idrp_dotcom_order_exas::source_location_level_cd AS source_location_level_cd;

work__idrp_exas_dotcom_order_applied = 
      UNION join_group_data_work_exas_gen,
            work__idrp_dotcom_order_no_exas;


SPLIT work__idrp_exas_dotcom_order_applied 
INTO work__idrp_exas_dotcom_order_applied_1 IF TRIM(shc_item_type_cd)=='INVC',
     work__idrp_exas_dotcom_order_applied_2 IF TRIM(shc_item_type_cd)!='INVC';


work__idrp_exas_dotcom_order_applied_2_gen = 
      FOREACH work__idrp_exas_dotcom_order_applied_2 
      GENERATE 
              shc_item_id AS shc_item_id,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              shc_item_type_cd AS shc_item_type_cd,
              network_distribution_cd AS network_distribution_cd,
              can_carry_model_id AS can_carry_model_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              sears_order_system_cd AS sears_order_system_cd,
              idrp_order_method_cd AS idrp_order_method_cd,
              idrp_order_method_desc AS idrp_order_method_desc,
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
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              dotcom_order_ind AS dotcom_allocation_ind,
              store_location_nbr AS store_location_nbr,
              days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              days_to_check_end_day_qty AS days_to_check_end_day_qty,
              days_to_check_begin_dt AS days_to_check_begin_dt,
              days_to_check_end_dt AS days_to_check_end_dt,
              location_format_type_cd AS location_format_type_cd,
              format_type_cd AS format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              scan_based_trading_ind AS scan_based_trading_ind,
              cross_merchandising_cd AS cross_merchandising_cd,
              servicing_dc_nbr AS servicing_dc_nbr,
              source_location_nbr AS source_location_nbr,
              dc_effective_dt AS dc_effective_dt,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              source_location_level_cd AS source_location_level_cd,
              '' AS retail_carton_vendor_package_id,
              '0' AS retail_carton_internal_package_qty;


smith__idrp_collections_carton_pack_xref_current_data_join_work = 
       JOIN work__idrp_exas_dotcom_order_applied_1 BY vendor_package_id LEFT OUTER,
            smith__idrp_collections_carton_pack_xref_current_data BY external_vendor_package_id USING 'replicated';


work__idrp_intrnl_pack_applied = 
      FOREACH smith__idrp_collections_carton_pack_xref_current_data_join_work
      GENERATE
              work__idrp_exas_dotcom_order_applied_1::shc_item_id AS shc_item_id,
              work__idrp_exas_dotcom_order_applied_1::sears_division_nbr AS sears_division_nbr,
              work__idrp_exas_dotcom_order_applied_1::sears_item_nbr AS sears_item_nbr,
              work__idrp_exas_dotcom_order_applied_1::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_exas_dotcom_order_applied_1::shc_item_type_cd AS shc_item_type_cd,
              work__idrp_exas_dotcom_order_applied_1::network_distribution_cd AS network_distribution_cd,
              work__idrp_exas_dotcom_order_applied_1::can_carry_model_id AS can_carry_model_id,
              work__idrp_exas_dotcom_order_applied_1::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_exas_dotcom_order_applied_1::sears_order_system_cd AS sears_order_system_cd,
              work__idrp_exas_dotcom_order_applied_1::idrp_order_method_cd AS idrp_order_method_cd,
              work__idrp_exas_dotcom_order_applied_1::idrp_order_method_desc AS idrp_order_method_desc,
              work__idrp_exas_dotcom_order_applied_1::ksn_id AS ksn_id,
              work__idrp_exas_dotcom_order_applied_1::vendor_package_id AS vendor_package_id,
              work__idrp_exas_dotcom_order_applied_1::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_exas_dotcom_order_applied_1::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_exas_dotcom_order_applied_1::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_exas_dotcom_order_applied_1::ksn_package_id AS ksn_package_id,
              work__idrp_exas_dotcom_order_applied_1::service_area_restriction_model_id AS service_area_restriction_model_id,
              work__idrp_exas_dotcom_order_applied_1::flow_type_cd AS flow_type_cd,
              work__idrp_exas_dotcom_order_applied_1::aprk_id AS aprk_id,
              work__idrp_exas_dotcom_order_applied_1::import_ind AS import_ind,
              work__idrp_exas_dotcom_order_applied_1::order_duns_nbr AS order_duns_nbr,
              work__idrp_exas_dotcom_order_applied_1::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_exas_dotcom_order_applied_1::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_exas_dotcom_order_applied_1::carton_per_layer_qty AS carton_per_layer_qty,
              work__idrp_exas_dotcom_order_applied_1::layer_per_pallet_qty AS layer_per_pallet_qty,
              work__idrp_exas_dotcom_order_applied_1::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_exas_dotcom_order_applied_1::dotcom_order_ind AS dotcom_allocation_ind,
              work__idrp_exas_dotcom_order_applied_1::store_location_nbr AS store_location_nbr,
              work__idrp_exas_dotcom_order_applied_1::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_exas_dotcom_order_applied_1::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__idrp_exas_dotcom_order_applied_1::days_to_check_begin_dt AS days_to_check_begin_dt,
              work__idrp_exas_dotcom_order_applied_1::days_to_check_end_dt AS days_to_check_end_dt,
              work__idrp_exas_dotcom_order_applied_1::location_format_type_cd AS location_format_type_cd,
              work__idrp_exas_dotcom_order_applied_1::format_type_cd AS format_type_cd,
              work__idrp_exas_dotcom_order_applied_1::location_level_cd AS location_level_cd,
              work__idrp_exas_dotcom_order_applied_1::location_owner_cd AS location_owner_cd,
              work__idrp_exas_dotcom_order_applied_1::scan_based_trading_ind AS scan_based_trading_ind,
              work__idrp_exas_dotcom_order_applied_1::cross_merchandising_cd AS cross_merchandising_cd,
              work__idrp_exas_dotcom_order_applied_1::servicing_dc_nbr AS servicing_dc_nbr,
              work__idrp_exas_dotcom_order_applied_1::source_location_nbr AS source_location_nbr,
              work__idrp_exas_dotcom_order_applied_1::dc_effective_dt AS dc_effective_dt,
              work__idrp_exas_dotcom_order_applied_1::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              work__idrp_exas_dotcom_order_applied_1::source_location_level_cd AS source_location_level_cd,
              (smith__idrp_collections_carton_pack_xref_current_data::external_vendor_package_id IS NULL ? '' : smith__idrp_collections_carton_pack_xref_current_data::external_vendor_package_id) AS retail_carton_vendor_package_id,
              (smith__idrp_collections_carton_pack_xref_current_data::external_vendor_package_id IS NULL ? '0': (chararray)smith__idrp_collections_carton_pack_xref_current_data::internal_qty) AS retail_carton_internal_package_qty;


work__idrp_intrnl_pack_applied = 
      UNION work__idrp_exas_dotcom_order_applied_2_gen,
            work__idrp_intrnl_pack_applied;


SPLIT work__idrp_intrnl_pack_applied 
INTO work__idrp_dc_supplied IF (int)servicing_dc_nbr>0,
     work__idrp_vendor_supplied  IF (int)servicing_dc_nbr==0;


-----------------------------------------smith__idrp_vend_pack_dc_combined Filter applied----------------------------------------------------------

/*smith__idrp_vend_pack_dc_combined_join_smith__idrp_replenishment_day_data = 
       CROSS smith__idrp_vend_pack_dc_combined_data,
             smith__idrp_item_eligibility_batchdate;

smith__idrp_vend_pack_dc_combined_data_fltrd = 
       FILTER smith__idrp_vend_pack_dc_combined_join_smith__idrp_replenishment_day_data
       BY (TRIM(smith__idrp_item_eligibility_batchdate::processing_ts) >= TRIM(smith__idrp_vend_pack_dc_combined_data::effective_ts)
      AND
          TRIM(smith__idrp_item_eligibility_batchdate::processing_ts) <= TRIM(smith__idrp_vend_pack_dc_combined_data::expiration_ts));
*/


filter_smith__idrp_vend_pack_dc_combined_data_fltrd =  
	FILTER smith__idrp_vend_pack_dc_combined_data 
		BY (REPLACE('$processing_ts','~', ' ') >= TRIM(effective_ts)
      AND         REPLACE('$processing_ts','~', ' ') <= TRIM(expiration_ts));
	  
		  
smith__idrp_vend_pack_dc_combined_data_fltrd = 
      FOREACH filter_smith__idrp_vend_pack_dc_combined_data_fltrd
      GENERATE 
              vendor_package_id AS vendor_package_id,
              location_nbr AS location_nbr,
              ksn_pack_purchase_status_cd AS ksn_pack_purchase_status_cd,
              ksn_pack_purchase_status_dt AS ksn_pack_purchase_status_dt,
              dc_stock_ind AS stock_ind,
              substition_eligibile_ind AS substition_eligibile_ind,
              inbound_dc_carton_per_layer_qty AS inbound_dc_carton_per_layer_qty,
              inbound_dc_layer_per_pallet_qty AS inbound_dc_layer_per_pallet_qty,
              outbound_package_qty AS outbound_package_qty,
              vendor_managed_inventory_cd AS vendor_managed_inventory_cd,
              dc_handling_cd AS dc_handling_cd;

			  
---------------------------------------------------------------------------------------------------------------------------------------------------

smith__idrp_vend_pack_dc_combined_join_work__idrp_dc_supplied = 
      JOIN work__idrp_dc_supplied BY (vendor_package_id,(int)servicing_dc_nbr),
           smith__idrp_vend_pack_dc_combined_data_fltrd BY (vendor_package_id,(int)location_nbr);



work__idrp_dc_supplied_ksnpack_data_applied = 
      FOREACH smith__idrp_vend_pack_dc_combined_join_work__idrp_dc_supplied
      GENERATE
              work__idrp_dc_supplied::shc_item_id AS shc_item_id,
              work__idrp_dc_supplied::sears_division_nbr AS sears_division_nbr,
              work__idrp_dc_supplied::sears_item_nbr AS sears_item_nbr,
              work__idrp_dc_supplied::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_dc_supplied::shc_item_type_cd AS shc_item_type_cd,
              work__idrp_dc_supplied::network_distribution_cd AS network_distribution_cd,
              work__idrp_dc_supplied::can_carry_model_id AS can_carry_model_id,
              work__idrp_dc_supplied::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_dc_supplied::sears_order_system_cd AS sears_order_system_cd,
              work__idrp_dc_supplied::idrp_order_method_cd AS idrp_order_method_cd,
              work__idrp_dc_supplied::idrp_order_method_desc AS idrp_order_method_desc,
              work__idrp_dc_supplied::ksn_id AS ksn_id,
              work__idrp_dc_supplied::vendor_package_id AS vendor_package_id,
              work__idrp_dc_supplied::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_dc_supplied::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_dc_supplied::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_dc_supplied::ksn_package_id AS ksn_package_id,
              work__idrp_dc_supplied::service_area_restriction_model_id AS service_area_restriction_model_id,
              work__idrp_dc_supplied::flow_type_cd AS flow_type_cd,
              work__idrp_dc_supplied::aprk_id AS aprk_id,
              work__idrp_dc_supplied::import_ind AS import_ind,
              work__idrp_dc_supplied::order_duns_nbr AS order_duns_nbr,
              work__idrp_dc_supplied::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_dc_supplied::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_dc_supplied::carton_per_layer_qty AS carton_per_layer_qty,
              work__idrp_dc_supplied::layer_per_pallet_qty AS layer_per_pallet_qty,
              work__idrp_dc_supplied::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_dc_supplied::dotcom_allocation_ind AS dotcom_allocation_ind,
              work__idrp_dc_supplied::store_location_nbr AS store_location_nbr,
              work__idrp_dc_supplied::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_dc_supplied::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__idrp_dc_supplied::days_to_check_begin_dt AS days_to_check_begin_dt,
              work__idrp_dc_supplied::days_to_check_end_dt AS days_to_check_end_dt,
              work__idrp_dc_supplied::location_format_type_cd AS location_format_type_cd,
              work__idrp_dc_supplied::format_type_cd AS format_type_cd,
              work__idrp_dc_supplied::location_level_cd AS location_level_cd,
              work__idrp_dc_supplied::location_owner_cd AS location_owner_cd,
              work__idrp_dc_supplied::scan_based_trading_ind AS scan_based_trading_ind,
              work__idrp_dc_supplied::cross_merchandising_cd AS cross_merchandising_cd,
              work__idrp_dc_supplied::servicing_dc_nbr AS servicing_dc_nbr,
              work__idrp_dc_supplied::source_location_nbr AS source_location_nbr,
              work__idrp_dc_supplied::dc_effective_dt AS dc_effective_dt,
              work__idrp_dc_supplied::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              work__idrp_dc_supplied::source_location_level_cd AS source_location_level_cd,
              work__idrp_dc_supplied::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              work__idrp_dc_supplied::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              smith__idrp_vend_pack_dc_combined_data_fltrd::ksn_pack_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              smith__idrp_vend_pack_dc_combined_data_fltrd::ksn_pack_purchase_status_dt AS ksn_pack_purchase_status_dt,
              smith__idrp_vend_pack_dc_combined_data_fltrd::stock_ind AS stock_ind,
              smith__idrp_vend_pack_dc_combined_data_fltrd::substition_eligibile_ind AS substition_eligible_ind,
              smith__idrp_vend_pack_dc_combined_data_fltrd::outbound_package_qty AS outbound_package_qty,
               --Spira4421
              smith__idrp_vend_pack_dc_combined_data_fltrd::vendor_managed_inventory_cd AS vendor_managed_inventory_cd,
              smith__idrp_vend_pack_dc_combined_data_fltrd::dc_handling_cd AS dc_handling_cd;

         
smith__idrp_dc_location_current_join_work = 
      JOIN work__idrp_dc_supplied_ksnpack_data_applied BY servicing_dc_nbr,
           smith__idrp_dc_location_current_data BY dc_location_nbr USING 'replicated';
 

work__idrp_dc_supplied_enablejif_data_applied = 
      FOREACH smith__idrp_dc_location_current_join_work
      GENERATE
              work__idrp_dc_supplied_ksnpack_data_applied::shc_item_id AS shc_item_id,
              work__idrp_dc_supplied_ksnpack_data_applied::sears_division_nbr AS sears_division_nbr,
              work__idrp_dc_supplied_ksnpack_data_applied::sears_item_nbr AS sears_item_nbr,
              work__idrp_dc_supplied_ksnpack_data_applied::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_dc_supplied_ksnpack_data_applied::shc_item_type_cd AS shc_item_type_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::network_distribution_cd AS network_distribution_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::can_carry_model_id AS can_carry_model_id,
              work__idrp_dc_supplied_ksnpack_data_applied::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::sears_order_system_cd AS sears_order_system_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::idrp_order_method_cd AS idrp_order_method_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::idrp_order_method_desc AS idrp_order_method_desc,
              work__idrp_dc_supplied_ksnpack_data_applied::ksn_id AS ksn_id,
              work__idrp_dc_supplied_ksnpack_data_applied::vendor_package_id AS vendor_package_id,
              work__idrp_dc_supplied_ksnpack_data_applied::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_dc_supplied_ksnpack_data_applied::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::ksn_package_id AS ksn_package_id,
              work__idrp_dc_supplied_ksnpack_data_applied::service_area_restriction_model_id AS service_area_restriction_model_id,
              work__idrp_dc_supplied_ksnpack_data_applied::flow_type_cd AS flow_type_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::aprk_id AS aprk_id,
              work__idrp_dc_supplied_ksnpack_data_applied::import_ind AS import_ind,
              work__idrp_dc_supplied_ksnpack_data_applied::order_duns_nbr AS order_duns_nbr,
              work__idrp_dc_supplied_ksnpack_data_applied::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_dc_supplied_ksnpack_data_applied::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_dc_supplied_ksnpack_data_applied::carton_per_layer_qty AS carton_per_layer_qty,
              work__idrp_dc_supplied_ksnpack_data_applied::layer_per_pallet_qty AS layer_per_pallet_qty,
              work__idrp_dc_supplied_ksnpack_data_applied::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::dotcom_allocation_ind AS dotcom_allocation_ind,
              work__idrp_dc_supplied_ksnpack_data_applied::store_location_nbr AS store_location_nbr,
              work__idrp_dc_supplied_ksnpack_data_applied::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_dc_supplied_ksnpack_data_applied::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__idrp_dc_supplied_ksnpack_data_applied::days_to_check_begin_dt AS days_to_check_begin_dt,
              work__idrp_dc_supplied_ksnpack_data_applied::days_to_check_end_dt AS days_to_check_end_dt,
              work__idrp_dc_supplied_ksnpack_data_applied::location_format_type_cd AS location_format_type_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::format_type_cd AS format_type_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::location_level_cd AS location_level_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::location_owner_cd AS location_owner_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::scan_based_trading_ind AS scan_based_trading_ind,
              work__idrp_dc_supplied_ksnpack_data_applied::cross_merchandising_cd AS cross_merchandising_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::servicing_dc_nbr AS servicing_dc_nbr,
              work__idrp_dc_supplied_ksnpack_data_applied::source_location_nbr AS source_location_nbr,
              work__idrp_dc_supplied_ksnpack_data_applied::dc_effective_dt AS dc_effective_dt,
              work__idrp_dc_supplied_ksnpack_data_applied::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              work__idrp_dc_supplied_ksnpack_data_applied::source_location_level_cd AS source_location_level_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              work__idrp_dc_supplied_ksnpack_data_applied::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              work__idrp_dc_supplied_ksnpack_data_applied::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::ksn_pack_purchase_status_dt AS ksn_pack_purchase_status_dt,
              work__idrp_dc_supplied_ksnpack_data_applied::stock_ind AS stock_ind,
              work__idrp_dc_supplied_ksnpack_data_applied::substition_eligible_ind AS substition_eligible_ind,
              work__idrp_dc_supplied_ksnpack_data_applied::outbound_package_qty AS outbound_package_qty,
              smith__idrp_dc_location_current_data::enable_jif_dc_ind AS enable_jif_dc_ind,
              --4377 changes
              (((work__idrp_dc_supplied_ksnpack_data_applied::outbound_package_qty is null) OR (IsNull(work__idrp_dc_supplied_ksnpack_data_applied::outbound_package_qty,'') == '') OR (work__idrp_dc_supplied_ksnpack_data_applied::outbound_package_qty == '0') ) ? '1' : work__idrp_dc_supplied_ksnpack_data_applied::outbound_package_qty) AS source_package_qty,
              work__idrp_dc_supplied_ksnpack_data_applied::vendor_managed_inventory_cd AS vendor_managed_inventory_cd,
              work__idrp_dc_supplied_ksnpack_data_applied::dc_handling_cd AS dc_handling_cd;



work__idrp_vendor_supplied_srcpack_applied1 = 
      FOREACH work__idrp_vendor_supplied
      GENERATE              
              shc_item_id AS shc_item_id,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              shc_item_type_cd AS shc_item_type_cd,
              network_distribution_cd AS network_distribution_cd,
              can_carry_model_id AS can_carry_model_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              sears_order_system_cd AS sears_order_system_cd,
              idrp_order_method_cd AS idrp_order_method_cd,
              idrp_order_method_desc AS idrp_order_method_desc,
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
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              dotcom_allocation_ind AS dotcom_allocation_ind,
              store_location_nbr AS store_location_nbr,
              days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              days_to_check_end_day_qty AS days_to_check_end_day_qty,
              days_to_check_begin_dt AS days_to_check_begin_dt,
              days_to_check_end_dt AS days_to_check_end_dt,
              location_format_type_cd AS location_format_type_cd,
              format_type_cd AS format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              scan_based_trading_ind AS scan_based_trading_ind,
              cross_merchandising_cd AS cross_merchandising_cd,
              servicing_dc_nbr AS servicing_dc_nbr,
              source_location_nbr AS source_location_nbr,
              dc_effective_dt AS dc_effective_dt,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              source_location_level_cd AS source_location_level_cd,
              retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              '' AS ksn_dc_package_purchase_status_cd,
              '' AS ksn_dc_package_purchase_status_dt,
              '0' AS stock_ind,
              '' AS substitution_eligible_ind,
              '0' AS outbound_package_qty,
              '0' AS enable_jif_dc_ind,
              (TRIM(retail_carton_internal_package_qty)=='0' ? vendor_carton_qty   : (TRIM(retail_carton_internal_package_qty)>'0' ? (chararray)((int)vendor_carton_qty * (int)retail_carton_internal_package_qty) : '')) AS source_package_qty;


work__idrp_vendor_supplied_srcpack_applied = 
      FOREACH work__idrp_vendor_supplied_srcpack_applied1
      GENERATE              
              shc_item_id AS shc_item_id,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              shc_item_type_cd AS shc_item_type_cd,
              network_distribution_cd AS network_distribution_cd,
              can_carry_model_id AS can_carry_model_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              sears_order_system_cd AS sears_order_system_cd,
              idrp_order_method_cd AS idrp_order_method_cd,
              idrp_order_method_desc AS idrp_order_method_desc,
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
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              dotcom_allocation_ind AS dotcom_allocation_ind,
              store_location_nbr AS store_location_nbr,
              days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              days_to_check_end_day_qty AS days_to_check_end_day_qty,
              days_to_check_begin_dt AS days_to_check_begin_dt,
              days_to_check_end_dt AS days_to_check_end_dt,
              location_format_type_cd AS location_format_type_cd,
              format_type_cd AS format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              scan_based_trading_ind AS scan_based_trading_ind,
              cross_merchandising_cd AS cross_merchandising_cd,
              servicing_dc_nbr AS servicing_dc_nbr,
              source_location_nbr AS source_location_nbr,
              dc_effective_dt AS dc_effective_dt,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              source_location_level_cd AS source_location_level_cd,
              retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              '' AS ksn_dc_package_purchase_status_cd,
              '' AS ksn_dc_package_purchase_status_dt,
              '0' AS stock_ind,
              '' AS substitution_eligible_ind,
              '0' AS outbound_package_qty,
              '0' AS enable_jif_dc_ind,
              ((( source_package_qty is null) OR (IsNull(source_package_qty,'') == '') OR ((int)source_package_qty == (int)'0' ) ) ? '1' :source_package_qty) AS source_package_qty,
              '' AS vendor_managed_inventory_cd,
              '' AS dc_handling_cd;


work__idrp_post_ksnpack_enablejif_srcpack_process = 
      UNION work__idrp_vendor_supplied_srcpack_applied,
            work__idrp_dc_supplied_enablejif_data_applied;


 work__join_smith__idrp_eligible_loc_data = 
      JOIN work__idrp_post_ksnpack_enablejif_srcpack_process BY TRIM(store_location_nbr),
           smith__idrp_eligible_loc_data BY TRIM(loc) USING 'replicated';


work__join_smith__idrp_eligible_loc_data_join_smith = 
      JOIN work__join_smith__idrp_eligible_loc_data BY TRIM(work__idrp_post_ksnpack_enablejif_srcpack_process::source_location_nbr),
           smith__idrp_eligible_loc_data BY TRIM(loc) USING 'replicated';

		   
work__idrp_post_sears_location_process = 
      FOREACH work__join_smith__idrp_eligible_loc_data_join_smith 
      GENERATE
              work__idrp_post_ksnpack_enablejif_srcpack_process::shc_item_id AS shc_item_id,
              work__idrp_post_ksnpack_enablejif_srcpack_process::sears_division_nbr AS sears_division_nbr,
              work__idrp_post_ksnpack_enablejif_srcpack_process::sears_item_nbr AS sears_item_nbr,
              work__idrp_post_ksnpack_enablejif_srcpack_process::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_post_ksnpack_enablejif_srcpack_process::shc_item_type_cd AS shc_item_type_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::network_distribution_cd AS network_distribution_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::can_carry_model_id AS can_carry_model_id,
              work__idrp_post_ksnpack_enablejif_srcpack_process::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::sears_order_system_cd AS sears_order_system_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::idrp_order_method_cd AS idrp_order_method_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::idrp_order_method_desc AS idrp_order_method_desc,
              work__idrp_post_ksnpack_enablejif_srcpack_process::ksn_id AS ksn_id,
              work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_package_id AS vendor_package_id,
              work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::ksn_package_id AS ksn_package_id,
              work__idrp_post_ksnpack_enablejif_srcpack_process::service_area_restriction_model_id AS service_area_restriction_model_id,
              work__idrp_post_ksnpack_enablejif_srcpack_process::flow_type_cd AS flow_type_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::aprk_id AS aprk_id,
              work__idrp_post_ksnpack_enablejif_srcpack_process::import_ind AS import_ind,
              work__idrp_post_ksnpack_enablejif_srcpack_process::order_duns_nbr AS order_duns_nbr,
              work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_post_ksnpack_enablejif_srcpack_process::carton_per_layer_qty AS carton_per_layer_qty,
              work__idrp_post_ksnpack_enablejif_srcpack_process::layer_per_pallet_qty AS layer_per_pallet_qty,
              work__idrp_post_ksnpack_enablejif_srcpack_process::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::dotcom_allocation_ind AS dotcom_allocation_ind,
              work__idrp_post_ksnpack_enablejif_srcpack_process::store_location_nbr AS store_location_nbr,
              work__idrp_post_ksnpack_enablejif_srcpack_process::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_post_ksnpack_enablejif_srcpack_process::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__idrp_post_ksnpack_enablejif_srcpack_process::days_to_check_begin_dt AS days_to_check_begin_dt,
              work__idrp_post_ksnpack_enablejif_srcpack_process::days_to_check_end_dt AS days_to_check_end_dt,
              work__idrp_post_ksnpack_enablejif_srcpack_process::location_format_type_cd AS location_format_type_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::format_type_cd AS format_type_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::location_level_cd AS location_level_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::location_owner_cd AS location_owner_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::scan_based_trading_ind AS scan_based_trading_ind,
              work__idrp_post_ksnpack_enablejif_srcpack_process::cross_merchandising_cd AS cross_merchandising_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::servicing_dc_nbr AS servicing_dc_nbr,
              work__idrp_post_ksnpack_enablejif_srcpack_process::source_location_nbr AS source_location_nbr,
              work__idrp_post_ksnpack_enablejif_srcpack_process::dc_effective_dt AS dc_effective_dt,
              work__idrp_post_ksnpack_enablejif_srcpack_process::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              work__idrp_post_ksnpack_enablejif_srcpack_process::source_location_level_cd AS source_location_level_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              work__idrp_post_ksnpack_enablejif_srcpack_process::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              work__idrp_post_ksnpack_enablejif_srcpack_process::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::ksn_dc_package_purchase_status_dt AS ksn_dc_package_purchase_status_dt,
              work__idrp_post_ksnpack_enablejif_srcpack_process::stock_ind AS stock_ind,
              work__idrp_post_ksnpack_enablejif_srcpack_process::substitution_eligible_ind  AS substitution_eligible_ind,
              work__idrp_post_ksnpack_enablejif_srcpack_process::outbound_package_qty AS outbound_package_qty,
              work__idrp_post_ksnpack_enablejif_srcpack_process::enable_jif_dc_ind AS enable_jif_dc_ind,
              work__idrp_post_ksnpack_enablejif_srcpack_process::source_package_qty AS source_package_qty,
			  work__join_smith__idrp_eligible_loc_data::smith__idrp_eligible_loc_data::srs_loc AS sears_location_nbr,
              smith__idrp_eligible_loc_data::srs_loc AS sears_source_location_nbr,
              work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_managed_inventory_cd AS vendor_managed_inventory_cd,
              work__idrp_post_ksnpack_enablejif_srcpack_process::dc_handling_cd AS dc_handling_cd,
              (work__idrp_post_ksnpack_enablejif_srcpack_process::import_ind =='1' ? 'N':((IsNull(work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_managed_inventory_cd,'') !='' or work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_managed_inventory_cd is not null) AND  (work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_managed_inventory_cd =='5' OR
                                                                 work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_managed_inventory_cd =='6'   OR 
                                                                 work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_managed_inventory_cd =='7'   OR 
                                                                 work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_managed_inventory_cd =='8'   OR 
                                                                 work__idrp_post_ksnpack_enablejif_srcpack_process::vendor_managed_inventory_cd =='9') ? 'N' :
                                                               (work__idrp_post_ksnpack_enablejif_srcpack_process::stock_ind=='N' AND( (work__idrp_post_ksnpack_enablejif_srcpack_process::dc_handling_cd is not null or IsNull(work__idrp_post_ksnpack_enablejif_srcpack_process::dc_handling_cd,'') !='') and  work__idrp_post_ksnpack_enablejif_srcpack_process::dc_handling_cd == 'CASE')?'Y':'N'))) as dc_flowthru_ind;              
  
work__idrp_post_dc_config_process = 
      FOREACH work__idrp_post_sears_location_process 
      GENERATE
              shc_item_id AS shc_item_id,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              shc_item_type_cd AS shc_item_type_cd,
              network_distribution_cd AS network_distribution_cd,
              can_carry_model_id AS can_carry_model_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              sears_order_system_cd AS sears_order_system_cd,
              idrp_order_method_cd AS idrp_order_method_cd,
              idrp_order_method_desc AS idrp_order_method_desc,
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
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              dotcom_allocation_ind AS dotcom_allocation_ind,
              store_location_nbr AS store_location_nbr,
              days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              days_to_check_end_day_qty AS days_to_check_end_day_qty,
              days_to_check_begin_dt AS days_to_check_begin_dt,
              days_to_check_end_dt AS days_to_check_end_dt,
              location_format_type_cd AS location_format_type_cd,
              format_type_cd AS format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              scan_based_trading_ind AS scan_based_trading_ind,
              cross_merchandising_cd AS cross_merchandising_cd,
              servicing_dc_nbr AS servicing_dc_nbr,
              source_location_nbr AS source_location_nbr,
              dc_effective_dt AS dc_effective_dt,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              source_location_level_cd AS source_location_level_cd,
              retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              ksn_dc_package_purchase_status_dt AS ksn_dc_package_purchase_status_dt,
              stock_ind AS stock_ind,
              substitution_eligible_ind  AS substitution_eligible_ind,
              outbound_package_qty AS outbound_package_qty,
              enable_jif_dc_ind AS enable_jif_dc_ind,
              source_package_qty AS source_package_qty,
			  sears_location_nbr AS sears_location_nbr,
              sears_source_location_nbr AS sears_source_location_nbr,
              vendor_managed_inventory_cd AS vendor_managed_inventory_cd,
              dc_handling_cd AS dc_handling_cd,
              dc_flowthru_ind as dc_flowthru_ind,              
              (flow_type_cd IS NULL ? '' : (flow_type_cd=='JIT' ? 'JIT' : (flow_type_cd=='DSD' OR flow_type_cd=='DSDS' ? 'DSD' : ((flow_type_cd=='DC' AND dc_flowthru_ind=='Y' AND enable_jif_dc_ind=='N') ? 'FLT' : ((flow_type_cd=='DC' AND dc_flowthru_ind=='Y' AND enable_jif_dc_ind=='Y') ? 'JIF' : ((flow_type_cd=='DC' AND dc_flowthru_ind =='N') ? 'STK' : ((flow_type_cd=='VCDC' AND servicing_dc_nbr=='0') ? 'DSD' : ((flow_type_cd=='VCDC' AND servicing_dc_nbr>'0' AND dc_flowthru_ind =='Y' AND enable_jif_dc_ind=='N') ? 'FLT' : ((flow_type_cd=='VCDC' AND servicing_dc_nbr>'0' AND dc_flowthru_ind=='Y' AND enable_jif_dc_ind=='Y') ? 'JIF' : ((flow_type_cd=='VCDC' AND servicing_dc_nbr>'0' AND dc_flowthru_ind=='N') ? 'STK'  : '' )))))))))) AS dc_configuration_cd;

work__join_smith__idrp_markdown_data = 
      JOIN work__idrp_post_dc_config_process BY (ksn_id,store_location_nbr) LEFT OUTER,
           smith__idrp_markdown_ksn_location_current_data BY (ksn_id,store_location_nbr);


work__idrp_post_kmart_markdown_process = 
      FOREACH work__join_smith__idrp_markdown_data
      GENERATE
              work__idrp_post_dc_config_process::shc_item_id AS shc_item_id,
              work__idrp_post_dc_config_process::sears_division_nbr AS sears_division_nbr,
              work__idrp_post_dc_config_process::sears_item_nbr AS sears_item_nbr,
              work__idrp_post_dc_config_process::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_post_dc_config_process::shc_item_type_cd AS shc_item_type_cd,
              work__idrp_post_dc_config_process::network_distribution_cd AS network_distribution_cd,
              work__idrp_post_dc_config_process::can_carry_model_id AS can_carry_model_id,
              work__idrp_post_dc_config_process::item_purchase_status_cd AS item_purchase_status_cd,
              work__idrp_post_dc_config_process::sears_order_system_cd AS sears_order_system_cd,
              work__idrp_post_dc_config_process::idrp_order_method_cd AS idrp_order_method_cd,
              work__idrp_post_dc_config_process::idrp_order_method_desc AS idrp_order_method_desc,
              work__idrp_post_dc_config_process::ksn_id AS ksn_id,
              work__idrp_post_dc_config_process::vendor_package_id AS vendor_package_id,
              work__idrp_post_dc_config_process::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              work__idrp_post_dc_config_process::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              work__idrp_post_dc_config_process::vendor_package_owner_cd AS vendor_package_owner_cd,
              work__idrp_post_dc_config_process::ksn_package_id AS ksn_package_id,
              work__idrp_post_dc_config_process::service_area_restriction_model_id AS service_area_restriction_model_id,
              work__idrp_post_dc_config_process::flow_type_cd AS flow_type_cd,
              work__idrp_post_dc_config_process::aprk_id AS aprk_id,
              work__idrp_post_dc_config_process::import_ind AS import_ind,
              work__idrp_post_dc_config_process::order_duns_nbr AS order_duns_nbr,
              work__idrp_post_dc_config_process::vendor_carton_qty AS vendor_carton_qty,
              work__idrp_post_dc_config_process::vendor_stock_nbr AS vendor_stock_nbr,
              work__idrp_post_dc_config_process::carton_per_layer_qty AS carton_per_layer_qty,
              work__idrp_post_dc_config_process::layer_per_pallet_qty AS layer_per_pallet_qty,
              work__idrp_post_dc_config_process::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              work__idrp_post_dc_config_process::dotcom_allocation_ind AS dotcom_allocation_ind,
              work__idrp_post_dc_config_process::store_location_nbr AS store_location_nbr,
              work__idrp_post_dc_config_process::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              work__idrp_post_dc_config_process::days_to_check_end_day_qty AS days_to_check_end_day_qty,
              work__idrp_post_dc_config_process::days_to_check_begin_dt AS days_to_check_begin_dt,
              work__idrp_post_dc_config_process::days_to_check_end_dt AS days_to_check_end_dt,
              work__idrp_post_dc_config_process::location_format_type_cd AS location_format_type_cd,
              work__idrp_post_dc_config_process::format_type_cd AS format_type_cd,
              work__idrp_post_dc_config_process::location_level_cd AS location_level_cd,
              work__idrp_post_dc_config_process::location_owner_cd AS location_owner_cd,
              work__idrp_post_dc_config_process::scan_based_trading_ind AS scan_based_trading_ind,
              work__idrp_post_dc_config_process::cross_merchandising_cd AS cross_merchandising_cd,
              work__idrp_post_dc_config_process::servicing_dc_nbr AS servicing_dc_nbr,
              work__idrp_post_dc_config_process::source_location_nbr AS source_location_nbr,
              work__idrp_post_dc_config_process::dc_effective_dt AS dc_effective_dt,
              work__idrp_post_dc_config_process::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              work__idrp_post_dc_config_process::source_location_level_cd AS source_location_level_cd,
              work__idrp_post_dc_config_process::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              work__idrp_post_dc_config_process::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              work__idrp_post_dc_config_process::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              work__idrp_post_dc_config_process::ksn_dc_package_purchase_status_dt AS ksn_dc_package_purchase_status_dt,
              work__idrp_post_dc_config_process::stock_ind AS stock_ind,
              work__idrp_post_dc_config_process::substitution_eligible_ind  AS substitution_eligible_ind,
              work__idrp_post_dc_config_process::outbound_package_qty AS outbound_package_qty,
              work__idrp_post_dc_config_process::enable_jif_dc_ind AS enable_jif_dc_ind,
              work__idrp_post_dc_config_process::source_package_qty AS source_package_qty,
              work__idrp_post_dc_config_process::sears_location_nbr AS sears_location_nbr,
              work__idrp_post_dc_config_process::sears_source_location_nbr AS sears_source_location_nbr,
              work__idrp_post_dc_config_process::dc_configuration_cd AS dc_configuration_cd,
              ((smith__idrp_markdown_ksn_location_current_data::ksn_id IS NOT NULL AND smith__idrp_markdown_ksn_location_current_data::store_location_nbr IS NOT NULL) ? 'Y' : 'N') AS kmart_markdown_ind;


work__idrp_post_kmart_markdown_process_alloc_repln_fltr = FILTER 	work__idrp_post_kmart_markdown_process BY idrp_order_method_cd == 'R' AND servicing_dc_nbr > '0' 
															AND ksn_dc_package_purchase_status_cd !='U' AND substitution_eligible_ind == 'N'; 

work__idrp_post_kmart_markdown_process_alloc_repln_dist  = distinct work__idrp_post_kmart_markdown_process_alloc_repln_fltr;

work__idrp_post_kmart_markdown_process_alloc_repln_gen = 
      FOREACH work__idrp_post_kmart_markdown_process_alloc_repln_dist
      GENERATE
              shc_item_id AS shc_item_id,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              shc_item_type_cd AS shc_item_type_cd,
              network_distribution_cd AS network_distribution_cd,
              can_carry_model_id AS can_carry_model_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              sears_order_system_cd AS sears_order_system_cd,
              idrp_order_method_cd AS idrp_order_method_cd,
              idrp_order_method_desc AS idrp_order_method_desc,
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
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              dotcom_allocation_ind AS dotcom_allocation_ind,
              store_location_nbr AS store_location_nbr,
              days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
              days_to_check_end_day_qty AS days_to_check_end_day_qty,
              days_to_check_begin_dt AS days_to_check_begin_dt,
              days_to_check_end_dt AS days_to_check_end_dt,
              location_format_type_cd AS location_format_type_cd,
              format_type_cd AS format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              scan_based_trading_ind AS scan_based_trading_ind,
              cross_merchandising_cd AS cross_merchandising_cd,
              servicing_dc_nbr AS servicing_dc_nbr,
              source_location_nbr AS source_location_nbr,
              dc_effective_dt AS dc_effective_dt,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              source_location_level_cd AS source_location_level_cd,
              retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
              retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
              ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              ksn_dc_package_purchase_status_dt AS ksn_dc_package_purchase_status_dt,
              stock_ind AS stock_ind,
              substitution_eligible_ind  AS substitution_eligible_ind,
              outbound_package_qty AS outbound_package_qty,
              enable_jif_dc_ind AS enable_jif_dc_ind,
              source_package_qty AS source_package_qty,
              sears_location_nbr AS sears_location_nbr,
              sears_source_location_nbr AS sears_source_location_nbr,
              dc_configuration_cd AS dc_configuration_cd,
              kmart_markdown_ind as kmart_markdown_ind;



work__idrp_post_kmart_markdown_process_alloc_repln_join = JOIN work__idrp_post_kmart_markdown_process BY (vendor_package_id) LEFT OUTER
																,work__idrp_post_kmart_markdown_process_alloc_repln_gen BY (vendor_package_id);

work__idrp_post_kmart_markdown_process_alloc_repln = FOREACH work__idrp_post_kmart_markdown_process_alloc_repln_join GENERATE 
																work__idrp_post_kmart_markdown_process::shc_item_id AS shc_item_id,
																work__idrp_post_kmart_markdown_process::sears_division_nbr AS sears_division_nbr,
																work__idrp_post_kmart_markdown_process::sears_item_nbr AS sears_item_nbr,
																work__idrp_post_kmart_markdown_process::sears_sku_nbr AS sears_sku_nbr,
																work__idrp_post_kmart_markdown_process::shc_item_type_cd AS shc_item_type_cd,
																work__idrp_post_kmart_markdown_process::network_distribution_cd AS network_distribution_cd,
																work__idrp_post_kmart_markdown_process::can_carry_model_id AS can_carry_model_id,
																work__idrp_post_kmart_markdown_process::item_purchase_status_cd AS item_purchase_status_cd,
																work__idrp_post_kmart_markdown_process::sears_order_system_cd AS sears_order_system_cd,
																work__idrp_post_kmart_markdown_process::idrp_order_method_cd AS idrp_order_method_cd,
																work__idrp_post_kmart_markdown_process::idrp_order_method_desc AS idrp_order_method_desc,
																work__idrp_post_kmart_markdown_process::ksn_id AS ksn_id,
																work__idrp_post_kmart_markdown_process::vendor_package_id AS vendor_package_id,
																work__idrp_post_kmart_markdown_process::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
																work__idrp_post_kmart_markdown_process::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
																work__idrp_post_kmart_markdown_process::vendor_package_owner_cd AS vendor_package_owner_cd,
																work__idrp_post_kmart_markdown_process::ksn_package_id AS ksn_package_id,
																work__idrp_post_kmart_markdown_process::service_area_restriction_model_id AS service_area_restriction_model_id,
																work__idrp_post_kmart_markdown_process::flow_type_cd AS flow_type_cd,
																work__idrp_post_kmart_markdown_process::aprk_id AS aprk_id,
																work__idrp_post_kmart_markdown_process::import_ind AS import_ind,
																work__idrp_post_kmart_markdown_process::order_duns_nbr AS order_duns_nbr,
																work__idrp_post_kmart_markdown_process::vendor_carton_qty AS vendor_carton_qty,
																work__idrp_post_kmart_markdown_process::vendor_stock_nbr AS vendor_stock_nbr,
																work__idrp_post_kmart_markdown_process::carton_per_layer_qty AS carton_per_layer_qty,
																work__idrp_post_kmart_markdown_process::layer_per_pallet_qty AS layer_per_pallet_qty,
																work__idrp_post_kmart_markdown_process::ksn_purchase_status_cd AS ksn_purchase_status_cd,
																work__idrp_post_kmart_markdown_process::dotcom_allocation_ind AS dotcom_allocation_ind,
																work__idrp_post_kmart_markdown_process::store_location_nbr AS store_location_nbr,
																work__idrp_post_kmart_markdown_process::days_to_check_begin_day_qty AS days_to_check_begin_day_qty,
																work__idrp_post_kmart_markdown_process::days_to_check_end_day_qty AS days_to_check_end_day_qty,
																work__idrp_post_kmart_markdown_process::days_to_check_begin_dt AS days_to_check_begin_dt,
																work__idrp_post_kmart_markdown_process::days_to_check_end_dt AS days_to_check_end_dt,
																work__idrp_post_kmart_markdown_process::location_format_type_cd AS location_format_type_cd,
																work__idrp_post_kmart_markdown_process::format_type_cd AS format_type_cd,
																work__idrp_post_kmart_markdown_process::location_level_cd AS location_level_cd,
																work__idrp_post_kmart_markdown_process::location_owner_cd AS location_owner_cd,
																work__idrp_post_kmart_markdown_process::scan_based_trading_ind AS scan_based_trading_ind,
																work__idrp_post_kmart_markdown_process::cross_merchandising_cd AS cross_merchandising_cd,
																work__idrp_post_kmart_markdown_process::servicing_dc_nbr AS servicing_dc_nbr,
																work__idrp_post_kmart_markdown_process::source_location_nbr AS source_location_nbr,
																work__idrp_post_kmart_markdown_process::dc_effective_dt AS dc_effective_dt,
																work__idrp_post_kmart_markdown_process::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
																work__idrp_post_kmart_markdown_process::source_location_level_cd AS source_location_level_cd,
																work__idrp_post_kmart_markdown_process::retail_carton_vendor_package_id AS retail_carton_vendor_package_id,
																work__idrp_post_kmart_markdown_process::retail_carton_internal_package_qty AS retail_carton_internal_package_qty,
																work__idrp_post_kmart_markdown_process::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
																work__idrp_post_kmart_markdown_process::ksn_dc_package_purchase_status_dt AS ksn_dc_package_purchase_status_dt,
																work__idrp_post_kmart_markdown_process::stock_ind AS stock_ind,
																work__idrp_post_kmart_markdown_process::substitution_eligible_ind  AS substitution_eligible_ind,
																work__idrp_post_kmart_markdown_process::outbound_package_qty AS outbound_package_qty,
																work__idrp_post_kmart_markdown_process::enable_jif_dc_ind AS enable_jif_dc_ind,
																work__idrp_post_kmart_markdown_process::source_package_qty AS source_package_qty,
																work__idrp_post_kmart_markdown_process::sears_location_nbr AS sears_location_nbr,
																work__idrp_post_kmart_markdown_process::sears_source_location_nbr AS sears_source_location_nbr,
																work__idrp_post_kmart_markdown_process::dc_configuration_cd AS dc_configuration_cd,
																work__idrp_post_kmart_markdown_process::kmart_markdown_ind as kmart_markdown_ind,
																(IsNull(work__idrp_post_kmart_markdown_process_alloc_repln_gen::vendor_package_id,'')!='' ? 'A': work__idrp_post_kmart_markdown_process::idrp_order_method_cd)AS allocation_replenishment_cd;


STORE work__idrp_post_kmart_markdown_process_alloc_repln
INTO '$WORK__IDRP_POST_KMART_MARKDOWN_PROCESS_ALLOC_REPLN_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
