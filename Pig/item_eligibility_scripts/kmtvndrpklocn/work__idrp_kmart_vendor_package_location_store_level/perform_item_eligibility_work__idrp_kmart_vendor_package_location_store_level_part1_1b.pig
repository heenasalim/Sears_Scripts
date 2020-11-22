/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_kmart_vendor_package_location_store_level_part1_1b.pig
# AUTHOR NAME:         Pankaj
# CREATION DATE:       
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
#        DATE         BY            MODIFICATION
#		 2015-03-16   Meghana       Spira 3985
#		2015-08-14		Priyanka	SPIRA 3018. Implemented logic to set the markdown indicator for exploding assortments on line no: 1234 
#		2015-08-28		Priyanka	SPIRA	4373 Dotcomm Indicator 
#		2016-06-09		Pankaj		SPIRS IPS-348 Flow Through allocation failures due to no eligible stores when INFOREM shows store authorized.
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

SET default_parallel 500;
REGISTER  $UDF_JAR;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/


gold__item_exploding_assortment_active =  
	  LOAD '$GOLD__ITEM_EXPLODING_ASSORTMENT_LOCATION/record_status=active'  
	  USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($GOLD__ITEM_EXPLODING_ASSORTMENT_SCHEMA);
	  
gold__item_exploding_assortment_data_active = 
		foreach gold__item_exploding_assortment_active 
		generate 
		external_vendor_package_id as external_vendor_package_id,
		internal_vendor_package_id as internal_vendor_package_id;	  

load_work__idrp_post_kmart_markdown_process_alloc_repln  = 
      LOAD '$WORK__IDRP_POST_KMART_MARKDOWN_PROCESS_ALLOC_REPLN_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_POST_KMART_MARKDOWN_PROCESS_ALLOC_REPLN_SCHEMA);

work__idrp_post_kmart_markdown_process_alloc_repln = FOREACH load_work__idrp_post_kmart_markdown_process_alloc_repln GENERATE 
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
source_location_nbr,
dc_effective_dt,
purchase_order_vendor_location_id,
source_location_level_cd,
retail_carton_vendor_package_id,
retail_carton_internal_package_qty,
ksn_dc_package_purchase_status_cd,
ksn_dc_package_purchase_status_dt,
stock_ind,
substitution_eligible_ind,
outbound_package_qty,
enable_jif_dc_ind,
source_package_qty,
sears_location_nbr,
sears_source_location_nbr,
dc_configuration_cd,
kmart_markdown_ind,
allocation_replenishment_cd;


------CR 5018-----------------------------------------------------------------------------------------------------------------------------			  


fltr_gen_work__idrp_post_kmart_markdown_process = filter work__idrp_post_kmart_markdown_process_alloc_repln by kmart_markdown_ind == 'Y';
			  
gen_work__idrp_post_kmart_markdown_process =  foreach fltr_gen_work__idrp_post_kmart_markdown_process generate vendor_package_id,store_location_nbr;

join_work__idrp_post_kmart_markdown_exploding_assort = JOIN gold__item_exploding_assortment_data_active by internal_vendor_package_id,gen_work__idrp_post_kmart_markdown_process by vendor_package_id;

gen_work__idrp_post_kmart_markdown_exploding_assort = foreach join_work__idrp_post_kmart_markdown_exploding_assort generate 
																gold__item_exploding_assortment_data_active::external_vendor_package_id as external_vendor_package_id,
																gen_work__idrp_post_kmart_markdown_process::store_location_nbr as store_location_nbr;

dist_gen_work__idrp_post_kmart_markdown_exploding_assort = DISTINCT gen_work__idrp_post_kmart_markdown_exploding_assort;

Jn_work__idrp_post_kmart_markdown_exploding_assort = JOIN  work__idrp_post_kmart_markdown_process_alloc_repln by (vendor_package_id,store_location_nbr) LEFT OUTER,dist_gen_work__idrp_post_kmart_markdown_exploding_assort by (external_vendor_package_id,store_location_nbr);

work__store_level_vend_pack_default_eligible_status = 
      FOREACH Jn_work__idrp_post_kmart_markdown_exploding_assort 
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
              work__idrp_post_kmart_markdown_process_alloc_repln::store_location_nbr AS store_location_nbr,
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
              substitution_eligible_ind AS substitution_eligible_ind,
              outbound_package_qty AS outbound_package_qty,
              enable_jif_dc_ind AS enable_jif_dc_ind,
              (source_location_level_cd=='WAREHOUSE' ? source_package_qty : (TRIM(retail_carton_internal_package_qty)=='0' ? vendor_carton_qty : (TRIM(retail_carton_internal_package_qty)>'0' ? (chararray)((int)vendor_carton_qty * (int)retail_carton_internal_package_qty) : ''))) AS source_package_qty,
              sears_location_nbr AS sears_location_nbr,
              sears_source_location_nbr AS sears_source_location_nbr,
              dc_configuration_cd AS dc_configuration_cd,
              (IsNull(TRIM(dist_gen_work__idrp_post_kmart_markdown_exploding_assort::external_vendor_package_id),'') != '' ? 'Y' : kmart_markdown_ind) AS kmart_markdown_ind, --CR 5018
			  allocation_replenishment_cd AS allocation_replenishment_cd, --CR5028
--SPIRA IPS-348			  
			(((flow_type_cd=='DC' OR (flow_type_cd=='VCDC' AND (int)servicing_dc_nbr>0)) AND ksn_dc_package_purchase_status_cd != 'U') ? 'Y' : (vendor_package_purchase_status_cd=='U' ? 'N' : 'Y' )) AS active_ind;
		  --(vendor_package_purchase_status_cd=='U' ? 'N' : (((flow_type_cd=='DC' OR (flow_type_cd=='VCDC' AND (int)servicing_dc_nbr>0)) AND ksn_dc_package_purchase_status_cd=='U') ? 'N' : active_ind )) AS active_ind;

work__store_level_vend_pack_purch_stat_eligible_status = 
      FOREACH work__store_level_vend_pack_default_eligible_status
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
              ((( source_package_qty is null) OR (IsNull(source_package_qty,'') == '') OR ((int)source_package_qty == (int)'0' ) ) ? '1' :source_package_qty) AS source_package_qty,
              sears_location_nbr AS sears_location_nbr,
              sears_source_location_nbr AS sears_source_location_nbr,
              dc_configuration_cd AS dc_configuration_cd,
              kmart_markdown_ind AS kmart_markdown_ind,
              allocation_replenishment_cd AS allocation_replenishment_cd,
              ((flow_type_cd=='DC' AND servicing_dc_nbr=='0') ? 'N' : active_ind ) AS active_ind;
              
STORE work__store_level_vend_pack_purch_stat_eligible_status
INTO '$WORK__IDRP_STORE_LEVEL_VEND_PACK_LOC_FINAL_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');



/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
