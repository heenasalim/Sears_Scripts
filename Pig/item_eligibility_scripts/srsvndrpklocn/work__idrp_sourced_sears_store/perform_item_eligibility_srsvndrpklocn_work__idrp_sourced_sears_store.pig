/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_work__idrp_sourced_sears_store.pig
# AUTHOR NAME:         Neera Singh
# CREATION DATE:       Thu July 24 09:37:58 EST 2014
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
#        DATE         BY            			MODIFICATION
# 9/10/2014      Arjun Dabhade     		Changed binary condition for Spira#3091
# 14/10/2014	 Siddhivinayak Karpe	Value TF52-15 Changed to TF32-15 Spira#3171
# 21/10/2014	Priyanka Gurjar			Value Changed from i2k_source_vendor_nbr to i2k_source_vendor_location_id for purchase_order_vendor_location_id against SPIRA #3196
# 24/10/2014	Siddhivinayak Karpe		CR#3227 Condition (long)location_id!=9300 removed from line 298, 299, 305, 306, 349 and 350
# 11/05/2014    Meghana Dhage           CR#3294 Changed rim_source_nbr to source_location_id for non IMPORT items to populate source_location_id (line 370 - 375)
# 1/19/2017 	Srujan Dussa		IPS-779 . Adding rim_last_record_create_dt from gold__inventory_rim_daily_current to be included in the Extract File to Shared Items.
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

--register the jar containing all PIG UDFs
REGISTER $UDF_JAR;

--trim spaces around string
DEFINE TRIM_STRING $TRIM_STRING ;

--trim leading zeros
DEFINE TRIM_INTEGER $TRIM_INTEGER ;

--trim leading and trailing zeros
DEFINE TRIM_DECIMAL $TRIM_DECIMAL ;

DEFINE TrimLeadingZeros com.searshc.supplychain.idrp.udf.TrimLeadingZeros();

set default_parallel 97 ;


/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

/*********Loading work__idrp_candidate_sears_store*****************************************************************************/

work__idrp_candidate_sears_store =
        LOAD '$WORK__IDRP_CANDIDATE_SEARS_STORE_LOCATION'
        USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
        $WORK__IDRP_CANDIDATE_SEARS_STORE_SCHEMA
       );

/********Loading work__rrc_minimum_vendor***************************************************************************************/

work__rrc_minimum_vendor =
        LOAD '$WORK__RRC_MINIMUM_VENDOR_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
        $WORK__DD_IMPORT_MINIMUM_VENDOR_SCHEMA
       );

/******Loading work_ddc_minimum_vendor****************************************************************************************/

work__ddc_minimum_vendor = 
	LOAD '$WORK__DDC_MINIMUM_VENDOR_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
        $WORK__DD_IMPORT_MINIMUM_VENDOR_SCHEMA
       );
	   
/******Loading work__import_minimum_vendor ****************************************************************************************/

work__import_minimum_vendor  = 
	LOAD '$WORK__IMPORT_MINIMUM_VENDOR_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
        $WORK__DD_IMPORT_MINIMUM_VENDOR_SCHEMA
       );

/******Loading smith__idrp_i2k_sears_rebuy_vendor_package_current*************************************************************/

smith__idrp_i2k_sears_rebuy_vendor_package_current = 
	LOAD '$SMITH__IDRP_I2K_SEARS_REBUY_VENDOR_PACKAGE_CURRENT_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
        $SMITH__IDRP_I2K_SEARS_REBUY_VENDOR_PACKAGE_CURRENT_SCHEMA
       );

/**********Loading work__idrp_sears_location_xref***************************************************************************/

work__idrp_sears_location_xref = 
	LOAD '$WORK__IDRP_SEARS_LOCATION_XREF_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
        $WORK__IDRP_SEARS_LOCATION_XREF_SCHEMA
       );

/*******join work__rrc_minimum_vendor and work__idrp_candidate_sears_store ***************************************************/

join_candidate_sears_store_rrc_min_vendor = 
	JOIN
	work__idrp_candidate_sears_store 
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr))
	LEFT OUTER,
	work__rrc_minimum_vendor
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr));


gen_join_candidate_sears_store_rrc_min_vendor = 
	FOREACH
	join_candidate_sears_store_rrc_min_vendor
	GENERATE
	work__idrp_candidate_sears_store::sears_division_nbr AS sears_division_nbr,
	work__idrp_candidate_sears_store::sears_item_nbr AS sears_item_nbr,
	work__idrp_candidate_sears_store::sears_sku_nbr AS sears_sku_nbr,
	work__idrp_candidate_sears_store::sears_location_id AS sears_location_id,
	work__idrp_candidate_sears_store::location_id AS location_id,
	work__idrp_candidate_sears_store::location_level_cd AS location_level_cd,
	work__idrp_candidate_sears_store::location_format_type_cd AS location_format_type_cd,
	work__idrp_candidate_sears_store::location_owner_cd AS location_owner_cd,
	work__idrp_candidate_sears_store::rim_original_source_nbr AS rim_original_source_nbr,
	work__idrp_candidate_sears_store::rim_source_nbr AS rim_source_nbr,
	work__idrp_candidate_sears_store::source_location_id AS source_location_id,
	work__idrp_candidate_sears_store::source_location_level_cd AS source_location_level_cd,
	work__idrp_candidate_sears_store::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
	work__idrp_candidate_sears_store::rim_status_cd AS rim_status_cd,
	work__idrp_candidate_sears_store::active_ind AS active_ind,
	work__idrp_candidate_sears_store::source_package_qty AS source_package_qty,
	work__idrp_candidate_sears_store::shc_item_id AS shc_item_id,
	work__idrp_candidate_sears_store::ksn_id AS ksn_id,
	work__idrp_candidate_sears_store::special_retail_order_system_ind AS special_retail_order_system_ind,
	work__idrp_candidate_sears_store::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
	work__idrp_candidate_sears_store::dot_com_allocation_ind AS dot_com_allocation_ind,
	work__idrp_candidate_sears_store::distribution_type_cd AS distribution_type_cd,
	work__idrp_candidate_sears_store::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
	work__idrp_candidate_sears_store::special_order_candidate_ind AS special_order_candidate_ind,
	work__idrp_candidate_sears_store::item_emp_ind AS item_emp_ind,
	work__idrp_candidate_sears_store::easy_order_ind AS easy_order_ind,
	work__idrp_candidate_sears_store::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
	work__idrp_candidate_sears_store::rapid_item_ind AS rapid_item_ind,
	work__idrp_candidate_sears_store::constrained_item_ind AS constrained_item_ind,
	work__idrp_candidate_sears_store::sears_import_ind AS sears_import_ind,
	work__idrp_candidate_sears_store::idrp_item_type_desc AS idrp_item_type_desc,
	work__idrp_candidate_sears_store::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
	work__idrp_candidate_sears_store::sams_migration_ind AS sams_migration_ind,
	work__idrp_candidate_sears_store::emp_to_jit_ind AS emp_to_jit_ind,
	work__idrp_candidate_sears_store::rim_flow_ind AS rim_flow_ind,
	work__idrp_candidate_sears_store::cross_merchandising_cd AS cross_merchandising_cd,
	work__idrp_candidate_sears_store::cross_merchandising_store_type_cd AS cross_merchandising_store_type_cd,
	work__rrc_minimum_vendor::min_vendor_nbr AS min_rrc_vendor_nbr,
	work__rrc_minimum_vendor::min_vendor_package_qty AS min_rrc_vendor_package_qty,
	work__rrc_minimum_vendor::min_vendor_location_id AS min_rrc_vendor_location_id,
	work__idrp_candidate_sears_store::rim_last_record_creation_dt AS rim_last_record_creation_dt;


join_candidate_sears_store_ddc_min_vendor = 
	JOIN
	gen_join_candidate_sears_store_rrc_min_vendor
	BY((int)sears_division_nbr, (int)sears_item_nbr, (int)sears_sku_nbr)
	LEFT OUTER,
	work__ddc_minimum_vendor
	BY((int)sears_division_nbr, (int)sears_item_nbr, (int)sears_sku_nbr);

	
gen_join_candidate_sears_store_ddc_min_vendor = 
	FOREACH
	join_candidate_sears_store_ddc_min_vendor
	GENERATE
	gen_join_candidate_sears_store_rrc_min_vendor::sears_division_nbr AS sears_division_nbr,
 	gen_join_candidate_sears_store_rrc_min_vendor::sears_item_nbr AS sears_item_nbr,
        gen_join_candidate_sears_store_rrc_min_vendor::sears_sku_nbr AS sears_sku_nbr,
        gen_join_candidate_sears_store_rrc_min_vendor::sears_location_id AS sears_location_id,
        gen_join_candidate_sears_store_rrc_min_vendor::location_id AS location_id,
        gen_join_candidate_sears_store_rrc_min_vendor::location_level_cd AS location_level_cd,
        gen_join_candidate_sears_store_rrc_min_vendor::location_format_type_cd AS location_format_type_cd,
        gen_join_candidate_sears_store_rrc_min_vendor::location_owner_cd AS location_owner_cd,
        gen_join_candidate_sears_store_rrc_min_vendor::rim_original_source_nbr AS rim_original_source_nbr,
        gen_join_candidate_sears_store_rrc_min_vendor::rim_source_nbr AS rim_source_nbr,
        gen_join_candidate_sears_store_rrc_min_vendor::source_location_id AS source_location_id,
        gen_join_candidate_sears_store_rrc_min_vendor::source_location_level_cd AS source_location_level_cd,
        gen_join_candidate_sears_store_rrc_min_vendor::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
        gen_join_candidate_sears_store_rrc_min_vendor::rim_status_cd AS rim_status_cd,
        gen_join_candidate_sears_store_rrc_min_vendor::active_ind AS active_ind,
        gen_join_candidate_sears_store_rrc_min_vendor::source_package_qty AS source_package_qty,
        gen_join_candidate_sears_store_rrc_min_vendor::shc_item_id AS shc_item_id,
        gen_join_candidate_sears_store_rrc_min_vendor::ksn_id AS ksn_id,
        gen_join_candidate_sears_store_rrc_min_vendor::special_retail_order_system_ind AS special_retail_order_system_ind,
        gen_join_candidate_sears_store_rrc_min_vendor::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        gen_join_candidate_sears_store_rrc_min_vendor::dot_com_allocation_ind AS dot_com_allocation_ind,
        gen_join_candidate_sears_store_rrc_min_vendor::distribution_type_cd AS distribution_type_cd,
        gen_join_candidate_sears_store_rrc_min_vendor::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
        gen_join_candidate_sears_store_rrc_min_vendor::special_order_candidate_ind AS special_order_candidate_ind,
        gen_join_candidate_sears_store_rrc_min_vendor::item_emp_ind AS item_emp_ind,
        gen_join_candidate_sears_store_rrc_min_vendor::easy_order_ind AS easy_order_ind,
        gen_join_candidate_sears_store_rrc_min_vendor::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
        gen_join_candidate_sears_store_rrc_min_vendor::rapid_item_ind AS rapid_item_ind,
        gen_join_candidate_sears_store_rrc_min_vendor::constrained_item_ind AS constrained_item_ind,
        gen_join_candidate_sears_store_rrc_min_vendor::sears_import_ind AS sears_import_ind,
        gen_join_candidate_sears_store_rrc_min_vendor::idrp_item_type_desc AS idrp_item_type_desc,
        gen_join_candidate_sears_store_rrc_min_vendor::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
        gen_join_candidate_sears_store_rrc_min_vendor::sams_migration_ind AS sams_migration_ind,
        gen_join_candidate_sears_store_rrc_min_vendor::emp_to_jit_ind AS emp_to_jit_ind,
        gen_join_candidate_sears_store_rrc_min_vendor::rim_flow_ind AS rim_flow_ind,
        gen_join_candidate_sears_store_rrc_min_vendor::cross_merchandising_cd AS cross_merchandising_cd,
        gen_join_candidate_sears_store_rrc_min_vendor::cross_merchandising_store_type_cd AS cross_merchandising_store_type_cd,
        gen_join_candidate_sears_store_rrc_min_vendor::min_rrc_vendor_nbr AS min_rrc_vendor_nbr,
        gen_join_candidate_sears_store_rrc_min_vendor::min_rrc_vendor_package_qty AS min_rrc_vendor_package_qty,
        gen_join_candidate_sears_store_rrc_min_vendor::min_rrc_vendor_location_id AS min_rrc_vendor_location_id,
	work__ddc_minimum_vendor::min_vendor_nbr AS min_ddc_vendor_nbr,
	work__ddc_minimum_vendor::min_vendor_package_qty AS min_ddc_vendor_package_qty,
	work__ddc_minimum_vendor::min_vendor_location_id AS min_ddc_vendor_location_id,
   	gen_join_candidate_sears_store_rrc_min_vendor::rim_last_record_creation_dt AS rim_last_record_creation_dt;

join_candidate_sears_store_import_min_vendor = 
	JOIN
	gen_join_candidate_sears_store_ddc_min_vendor
	BY((int)sears_division_nbr, (int)sears_item_nbr, (int)sears_sku_nbr)
	LEFT OUTER,
	work__import_minimum_vendor
	BY((int)sears_division_nbr, (int)sears_item_nbr, (int)sears_sku_nbr);
	
gen_join_candidate_sears_store_import_min_vendor = 
	FOREACH
	join_candidate_sears_store_import_min_vendor
	GENERATE
	gen_join_candidate_sears_store_ddc_min_vendor::sears_division_nbr AS sears_division_nbr,
 	gen_join_candidate_sears_store_ddc_min_vendor::sears_item_nbr AS sears_item_nbr,
        gen_join_candidate_sears_store_ddc_min_vendor::sears_sku_nbr AS sears_sku_nbr,
        gen_join_candidate_sears_store_ddc_min_vendor::sears_location_id AS sears_location_id,
        gen_join_candidate_sears_store_ddc_min_vendor::location_id AS location_id,
        gen_join_candidate_sears_store_ddc_min_vendor::location_level_cd AS location_level_cd,
        gen_join_candidate_sears_store_ddc_min_vendor::location_format_type_cd AS location_format_type_cd,
        gen_join_candidate_sears_store_ddc_min_vendor::location_owner_cd AS location_owner_cd,
        gen_join_candidate_sears_store_ddc_min_vendor::rim_original_source_nbr AS rim_original_source_nbr,
        gen_join_candidate_sears_store_ddc_min_vendor::rim_source_nbr AS rim_source_nbr,
        gen_join_candidate_sears_store_ddc_min_vendor::source_location_id AS source_location_id,
        gen_join_candidate_sears_store_ddc_min_vendor::source_location_level_cd AS source_location_level_cd,
        gen_join_candidate_sears_store_ddc_min_vendor::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
        gen_join_candidate_sears_store_ddc_min_vendor::rim_status_cd AS rim_status_cd,
        gen_join_candidate_sears_store_ddc_min_vendor::active_ind AS active_ind,
        gen_join_candidate_sears_store_ddc_min_vendor::source_package_qty AS source_package_qty,
        gen_join_candidate_sears_store_ddc_min_vendor::shc_item_id AS shc_item_id,
        gen_join_candidate_sears_store_ddc_min_vendor::ksn_id AS ksn_id,
        gen_join_candidate_sears_store_ddc_min_vendor::special_retail_order_system_ind AS special_retail_order_system_ind,
        gen_join_candidate_sears_store_ddc_min_vendor::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        gen_join_candidate_sears_store_ddc_min_vendor::dot_com_allocation_ind AS dot_com_allocation_ind,
        gen_join_candidate_sears_store_ddc_min_vendor::distribution_type_cd AS distribution_type_cd,
        gen_join_candidate_sears_store_ddc_min_vendor::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
        gen_join_candidate_sears_store_ddc_min_vendor::special_order_candidate_ind AS special_order_candidate_ind,
        gen_join_candidate_sears_store_ddc_min_vendor::item_emp_ind AS item_emp_ind,
        gen_join_candidate_sears_store_ddc_min_vendor::easy_order_ind AS easy_order_ind,
        gen_join_candidate_sears_store_ddc_min_vendor::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
        gen_join_candidate_sears_store_ddc_min_vendor::rapid_item_ind AS rapid_item_ind,
        gen_join_candidate_sears_store_ddc_min_vendor::constrained_item_ind AS constrained_item_ind,
        gen_join_candidate_sears_store_ddc_min_vendor::sears_import_ind AS sears_import_ind,
        gen_join_candidate_sears_store_ddc_min_vendor::idrp_item_type_desc AS idrp_item_type_desc,
        gen_join_candidate_sears_store_ddc_min_vendor::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
        gen_join_candidate_sears_store_ddc_min_vendor::sams_migration_ind AS sams_migration_ind,
        gen_join_candidate_sears_store_ddc_min_vendor::emp_to_jit_ind AS emp_to_jit_ind,
        gen_join_candidate_sears_store_ddc_min_vendor::rim_flow_ind AS rim_flow_ind,
        gen_join_candidate_sears_store_ddc_min_vendor::cross_merchandising_cd AS cross_merchandising_cd,
        gen_join_candidate_sears_store_ddc_min_vendor::cross_merchandising_store_type_cd AS cross_merchandising_store_type_cd,
        gen_join_candidate_sears_store_ddc_min_vendor::min_rrc_vendor_nbr AS min_rrc_vendor_nbr,
        gen_join_candidate_sears_store_ddc_min_vendor::min_rrc_vendor_package_qty AS min_rrc_vendor_package_qty,
        gen_join_candidate_sears_store_ddc_min_vendor::min_rrc_vendor_location_id AS min_rrc_vendor_location_id,
	gen_join_candidate_sears_store_ddc_min_vendor::min_ddc_vendor_nbr AS min_ddc_vendor_nbr,
	gen_join_candidate_sears_store_ddc_min_vendor::min_ddc_vendor_package_qty AS min_ddc_vendor_package_qty,
	gen_join_candidate_sears_store_ddc_min_vendor::min_ddc_vendor_location_id AS min_ddc_vendor_location_id,	
	work__import_minimum_vendor::min_vendor_nbr as min_import_vendor_nbr,
	work__import_minimum_vendor::min_vendor_package_qty as min_import_vendor_package_qty,
	work__import_minimum_vendor::min_vendor_location_id as min_import_vendor_location_id,
        gen_join_candidate_sears_store_ddc_min_vendor::rim_last_record_creation_dt as rim_last_record_creation_dt;

join_candidate_sears_store_i2k_sears_rebuy_vendor_package_current = 
	JOIN
	gen_join_candidate_sears_store_import_min_vendor
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr))
	LEFT OUTER,
	smith__idrp_i2k_sears_rebuy_vendor_package_current
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr));


work__idrp_sourced_sears_store_step1 = 
	FOREACH
	join_candidate_sears_store_i2k_sears_rebuy_vendor_package_current
	GENERATE
	    gen_join_candidate_sears_store_import_min_vendor::sears_division_nbr AS sears_division_nbr,
        gen_join_candidate_sears_store_import_min_vendor::sears_item_nbr AS sears_item_nbr,
        gen_join_candidate_sears_store_import_min_vendor::sears_sku_nbr AS sears_sku_nbr,
        gen_join_candidate_sears_store_import_min_vendor::sears_location_id AS sears_location_id,
        gen_join_candidate_sears_store_import_min_vendor::location_id AS location_id,
        gen_join_candidate_sears_store_import_min_vendor::location_level_cd AS location_level_cd,
        gen_join_candidate_sears_store_import_min_vendor::location_format_type_cd AS location_format_type_cd,
        gen_join_candidate_sears_store_import_min_vendor::location_owner_cd AS location_owner_cd,
        gen_join_candidate_sears_store_import_min_vendor::rim_original_source_nbr AS rim_original_source_nbr,
        gen_join_candidate_sears_store_import_min_vendor::rim_source_nbr AS rim_source_nbr,
        gen_join_candidate_sears_store_import_min_vendor::source_location_id AS source_location_id,
        gen_join_candidate_sears_store_import_min_vendor::source_location_level_cd AS source_location_level_cd,
        gen_join_candidate_sears_store_import_min_vendor::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
        gen_join_candidate_sears_store_import_min_vendor::rim_status_cd AS rim_status_cd,
        gen_join_candidate_sears_store_import_min_vendor::active_ind AS active_ind,
        gen_join_candidate_sears_store_import_min_vendor::source_package_qty AS source_package_qty,
        gen_join_candidate_sears_store_import_min_vendor::shc_item_id AS shc_item_id,
        gen_join_candidate_sears_store_import_min_vendor::ksn_id AS ksn_id,
        gen_join_candidate_sears_store_import_min_vendor::special_retail_order_system_ind AS special_retail_order_system_ind,
        gen_join_candidate_sears_store_import_min_vendor::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        gen_join_candidate_sears_store_import_min_vendor::dot_com_allocation_ind AS dot_com_allocation_ind,
        gen_join_candidate_sears_store_import_min_vendor::distribution_type_cd AS distribution_type_cd,
        gen_join_candidate_sears_store_import_min_vendor::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
        gen_join_candidate_sears_store_import_min_vendor::special_order_candidate_ind AS special_order_candidate_ind,
        gen_join_candidate_sears_store_import_min_vendor::item_emp_ind AS item_emp_ind,
        gen_join_candidate_sears_store_import_min_vendor::easy_order_ind AS easy_order_ind,
        gen_join_candidate_sears_store_import_min_vendor::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
        gen_join_candidate_sears_store_import_min_vendor::rapid_item_ind AS rapid_item_ind,
        gen_join_candidate_sears_store_import_min_vendor::constrained_item_ind AS constrained_item_ind,
        gen_join_candidate_sears_store_import_min_vendor::sears_import_ind AS sears_import_ind,
        gen_join_candidate_sears_store_import_min_vendor::idrp_item_type_desc AS idrp_item_type_desc,
        gen_join_candidate_sears_store_import_min_vendor::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
        gen_join_candidate_sears_store_import_min_vendor::sams_migration_ind AS sams_migration_ind,
        gen_join_candidate_sears_store_import_min_vendor::emp_to_jit_ind AS emp_to_jit_ind,
        gen_join_candidate_sears_store_import_min_vendor::rim_flow_ind AS rim_flow_ind,
        gen_join_candidate_sears_store_import_min_vendor::cross_merchandising_cd AS cross_merchandising_cd,
        gen_join_candidate_sears_store_import_min_vendor::cross_merchandising_store_type_cd AS cross_merchandising_store_type_cd,
        gen_join_candidate_sears_store_import_min_vendor::min_rrc_vendor_nbr AS min_rrc_vendor_nbr,
        gen_join_candidate_sears_store_import_min_vendor::min_rrc_vendor_package_qty AS min_rrc_vendor_package_qty,
        gen_join_candidate_sears_store_import_min_vendor::min_rrc_vendor_location_id AS min_rrc_vendor_location_id,
        gen_join_candidate_sears_store_import_min_vendor::min_ddc_vendor_nbr AS min_ddc_vendor_nbr,
        gen_join_candidate_sears_store_import_min_vendor::min_ddc_vendor_package_qty AS min_ddc_vendor_package_qty,
        gen_join_candidate_sears_store_import_min_vendor::min_ddc_vendor_location_id AS min_ddc_vendor_location_id,
		gen_join_candidate_sears_store_import_min_vendor::min_import_vendor_nbr as min_import_vendor_nbr,
	    gen_join_candidate_sears_store_import_min_vendor::min_import_vendor_package_qty as min_import_vendor_package_qty,
	    gen_join_candidate_sears_store_import_min_vendor::min_import_vendor_location_id as min_import_vendor_location_id,
	    smith__idrp_i2k_sears_rebuy_vendor_package_current::sears_vendor_duns_nbr AS i2k_source_vendor_nbr,
	    smith__idrp_i2k_sears_rebuy_vendor_package_current::source_package_qty AS i2k_source_package_qty,
	    smith__idrp_i2k_sears_rebuy_vendor_package_current::location_id AS i2k_source_vendor_location_id,
	    smith__idrp_i2k_sears_rebuy_vendor_package_current::vendor_package_id AS i2k_source_vendor_package_id,
	    gen_join_candidate_sears_store_import_min_vendor::rim_last_record_creation_dt AS rim_last_record_creation_dt;

	
work__idrp_sourced_sears_store_step2 = 
	FOREACH
	work__idrp_sourced_sears_store_step1
	GENERATE
	sears_division_nbr,
	sears_item_nbr,
	sears_sku_nbr,
	sears_location_id,
	location_id,
	location_level_cd,
	location_format_type_cd,
	location_owner_cd,
	rim_original_source_nbr,
	rim_source_nbr,
(
(long)location_id==9300 AND TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW'
? min_rrc_vendor_location_id 
	:((long)location_id==9300 AND TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='DD'
		? min_ddc_vendor_location_id 
		: (TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'
			? ((TRIM(i2k_source_vendor_nbr)!= '' AND i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr != 0)
				?'TF32-15'
				:((TRIM(min_import_vendor_location_id) != '' AND min_import_vendor_location_id IS NOT NULL)
					?'TF32-15'
					:'')
				)
			:(TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)!='IMPORT'
				?source_location_id 
				:(TRIM(source_location_level_cd)!='VENDOR'
				 ?source_location_id
				 :source_location_id)
)))) AS source_location_id,
	source_location_level_cd,
(
(long)location_id==9300 AND TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW'
? min_rrc_vendor_location_id 
:((long)location_id==9300 AND TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='DD'
	? min_ddc_vendor_location_id 
	:(TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'
		?((i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) !='') 
			? i2k_source_vendor_location_id
			:((TRIM(min_import_vendor_location_id) != '' AND min_import_vendor_location_id IS NOT NULL)
				? min_import_vendor_location_id
				:'')
				)
		 : (TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)!='IMPORT' 
			? source_location_id 
			:(TRIM(source_location_level_cd)!='VENDOR'
				? ' ' 
				: purchase_order_vendor_location_id)
)))) AS purchase_order_vendor_location_id,
	rim_status_cd,
	active_ind,
(
(long)location_id==9300 AND TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW'
? min_rrc_vendor_package_qty 
:((long)location_id==9300 AND TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='DD'
	? min_ddc_vendor_package_qty  
	:(TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'
		?((i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) !='') 
			? i2k_source_package_qty
			: ((TRIM(min_import_vendor_location_id) != '' AND min_import_vendor_location_id IS NOT NULL)
				? min_import_vendor_package_qty 
				:'')
				)
		: (TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)!='IMPORT'
			? source_package_qty 
			:(TRIM(source_location_level_cd)!='VENDOR'
				? source_package_qty 
				: '')
)))) AS source_package_qty,
	shc_item_id,
	ksn_id,
	special_retail_order_system_ind,
	shc_item_corporate_owner_cd,
	dot_com_allocation_ind,
	distribution_type_cd,
	only_rsu_distribution_channel_ind,
	special_order_candidate_ind,
	item_emp_ind,
	easy_order_ind,
	warehouse_sizing_attribute_cd,
	rapid_item_ind,
	constrained_item_ind,
	sears_import_ind,
	idrp_item_type_desc,
	cross_merchandising_attribute_cd,
	sams_migration_ind,
	emp_to_jit_ind,
	rim_flow_ind,
	cross_merchandising_cd,
	cross_merchandising_store_type_cd,
	min_rrc_vendor_nbr,
	min_rrc_vendor_package_qty,
	min_rrc_vendor_location_id,
	min_ddc_vendor_nbr,
	min_ddc_vendor_package_qty,
	min_ddc_vendor_location_id,
	min_import_vendor_nbr,
	min_import_vendor_package_qty,
	min_import_vendor_location_id,
	i2k_source_vendor_nbr,
	i2k_source_package_qty,
	i2k_source_vendor_location_id,
	i2k_source_vendor_package_id,
(
(int)location_id==9300 AND TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW'
? min_rrc_vendor_nbr 
:((int)location_id==9300 AND TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='DD'
	? min_ddc_vendor_nbr 
	:(TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'
	   ?((i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) !='')
			?'TF32-15' 
			:((TRIM(min_import_vendor_location_id) != '' AND min_import_vendor_location_id IS NOT NULL)
				?'TF32-15'
				:'')
				)
	   :(TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)!='IMPORT' 
			? rim_source_nbr 
			:(TRIM(source_location_level_cd)!='VENDOR' 
				? rim_source_nbr 
				: '')		
)))) AS sears_source_location_nbr,
(
(long)location_id==9300 AND TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW'
?'MINRRC' 
:((long)location_id==9300 AND TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='DD'
	?'MINDDC' 
	:(TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'
		?((i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) !='') 
			? 'I2K'
			: ((TRIM(min_import_vendor_location_id) != '' AND min_import_vendor_location_id IS NOT NULL)
				?'IMP'
				:'')
				)
		:(TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)!='IMPORT'
			? 'RIM' 
			:(TRIM(source_location_level_cd)!='VENDOR'
			? 'RIM' :'' )
)))) AS source_system_cd,
(TRIM(source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'
?((i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) !='') 
	? i2k_source_vendor_package_id
	:'')
:'') AS vendor_package_id,
rim_last_record_creation_dt;


checkpoint = filter work__idrp_sourced_sears_store_step2 by(source_location_id is not null);

join_sourced_sears_store_step2_location_xref = 
        JOIN
        work__idrp_sourced_sears_store_step2
        BY(TrimLeadingZeros(source_location_id)),
        work__idrp_sears_location_xref
        BY(TrimLeadingZeros(location_id));


work__idrp_sourced_sears_store_step3 = 
	FOREACH
	join_sourced_sears_store_step2_location_xref
	GENERATE
	work__idrp_sourced_sears_store_step2::sears_division_nbr AS sears_division_nbr,
	work__idrp_sourced_sears_store_step2::sears_item_nbr AS sears_item_nbr,
        work__idrp_sourced_sears_store_step2::sears_sku_nbr AS sears_sku_nbr,
        work__idrp_sourced_sears_store_step2::sears_location_id AS sears_location_id,
        work__idrp_sourced_sears_store_step2::location_id AS location_id,
        work__idrp_sourced_sears_store_step2::location_level_cd AS location_level_cd,
        work__idrp_sourced_sears_store_step2::location_format_type_cd AS location_format_type_cd,
        work__idrp_sourced_sears_store_step2::location_owner_cd AS location_owner_cd,
        work__idrp_sourced_sears_store_step2::rim_original_source_nbr AS rim_original_source_nbr,
        work__idrp_sourced_sears_store_step2::rim_source_nbr AS rim_source_nbr,
        work__idrp_sourced_sears_store_step2::source_location_id AS source_location_id,
        work__idrp_sears_location_xref::location_level_cd AS source_location_level_cd,
	work__idrp_sourced_sears_store_step2::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
	work__idrp_sourced_sears_store_step2::rim_status_cd AS rim_status_cd,
        work__idrp_sourced_sears_store_step2::active_ind AS active_ind,
        work__idrp_sourced_sears_store_step2::source_package_qty AS source_package_qty,
        work__idrp_sourced_sears_store_step2::shc_item_id AS shc_item_id,
        work__idrp_sourced_sears_store_step2::ksn_id AS ksn_id,
        work__idrp_sourced_sears_store_step2::special_retail_order_system_ind AS special_retail_order_system_ind,
        work__idrp_sourced_sears_store_step2::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        work__idrp_sourced_sears_store_step2::dot_com_allocation_ind AS dot_com_allocation_ind,
        work__idrp_sourced_sears_store_step2::distribution_type_cd AS distribution_type_cd,
        work__idrp_sourced_sears_store_step2::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
        work__idrp_sourced_sears_store_step2::special_order_candidate_ind AS special_order_candidate_ind,
        work__idrp_sourced_sears_store_step2::item_emp_ind AS item_emp_ind,
        work__idrp_sourced_sears_store_step2::easy_order_ind AS easy_order_ind,
        work__idrp_sourced_sears_store_step2::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
        work__idrp_sourced_sears_store_step2::rapid_item_ind AS rapid_item_ind,
        work__idrp_sourced_sears_store_step2::constrained_item_ind AS constrained_item_ind,
        work__idrp_sourced_sears_store_step2::sears_import_ind AS sears_import_ind,
        work__idrp_sourced_sears_store_step2::idrp_item_type_desc AS idrp_item_type_desc,
        work__idrp_sourced_sears_store_step2::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
        work__idrp_sourced_sears_store_step2::sams_migration_ind AS sams_migration_ind,
        work__idrp_sourced_sears_store_step2::emp_to_jit_ind AS emp_to_jit_ind,
        work__idrp_sourced_sears_store_step2::rim_flow_ind AS rim_flow_ind,
        work__idrp_sourced_sears_store_step2::cross_merchandising_cd AS cross_merchandising_cd,
        work__idrp_sourced_sears_store_step2::cross_merchandising_store_type_cd AS cross_merchandising_store_type_cd,
        work__idrp_sourced_sears_store_step2::min_rrc_vendor_nbr AS min_rrc_vendor_nbr,
        work__idrp_sourced_sears_store_step2::min_rrc_vendor_package_qty AS min_rrc_vendor_package_qty,
        work__idrp_sourced_sears_store_step2::min_rrc_vendor_location_id AS min_rrc_vendor_location_id,
        work__idrp_sourced_sears_store_step2::min_ddc_vendor_nbr AS min_ddc_vendor_nbr,
        work__idrp_sourced_sears_store_step2::min_ddc_vendor_package_qty AS min_ddc_vendor_package_qty,
        work__idrp_sourced_sears_store_step2::min_ddc_vendor_location_id AS min_ddc_vendor_location_id,
		work__idrp_sourced_sears_store_step2::min_import_vendor_nbr as min_import_vendor_nbr,
	    work__idrp_sourced_sears_store_step2::min_import_vendor_package_qty as min_import_vendor_package_qty,
	    work__idrp_sourced_sears_store_step2::min_import_vendor_location_id as min_import_vendor_location_id,
        work__idrp_sourced_sears_store_step2::i2k_source_vendor_nbr AS i2k_source_vendor_nbr,
        work__idrp_sourced_sears_store_step2::i2k_source_package_qty AS i2k_source_package_qty,
        work__idrp_sourced_sears_store_step2::i2k_source_vendor_location_id AS i2k_source_vendor_location_id,
        work__idrp_sourced_sears_store_step2::i2k_source_vendor_package_id AS i2k_source_vendor_package_id,
	work__idrp_sourced_sears_store_step2::sears_source_location_nbr AS sears_source_location_nbr,
	work__idrp_sourced_sears_store_step2::source_system_cd AS source_system_cd,
	work__idrp_sourced_sears_store_step2::vendor_package_id AS vendor_package_id,
	(TRIM(work__idrp_sears_location_xref::location_level_cd)=='VENDOR'?source_package_qty:' ') AS vendor_package_carton_qty,
	work__idrp_sourced_sears_store_step2::rim_last_record_creation_dt AS rim_last_record_creation_dt;


work__idrp_sourced_sears_store = 
	FOREACH
	work__idrp_sourced_sears_store_step3
	GENERATE
	sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        sears_location_id,
        location_id,
        location_level_cd,
        location_format_type_cd,
        location_owner_cd,
        sears_source_location_nbr,
	source_location_id,
        source_location_level_cd,
        purchase_order_vendor_location_id,
        rim_status_cd,
        active_ind,
        source_package_qty,
        shc_item_id,
        ksn_id,
	vendor_package_id,
	vendor_package_carton_qty,
        special_retail_order_system_ind,
        shc_item_corporate_owner_cd,
        dot_com_allocation_ind,
        distribution_type_cd,
        only_rsu_distribution_channel_ind,
        special_order_candidate_ind,
        item_emp_ind,
        easy_order_ind,
        warehouse_sizing_attribute_cd,
        rapid_item_ind,
        constrained_item_ind,
        sears_import_ind,
        idrp_item_type_desc,
        cross_merchandising_attribute_cd,
        sams_migration_ind,
        emp_to_jit_ind,
        rim_flow_ind,
        cross_merchandising_cd,
        source_system_cd,
        rim_original_source_nbr,
        ' ' AS item_active_ind,
        ' ' AS stock_type_cd,
        ' ' AS item_reserve_cd,
        ' ' AS non_stock_source_cd,
        ' ' AS item_next_period_on_hand_qty,
        ' ' AS item_on_order_qty,
        ' ' AS item_reserve_qty,
        ' ' AS item_back_order_qty,
        ' ' AS item_next_period_future_order_qty,
        ' ' AS item_next_period_in_transit_qty,
        ' ' AS item_last_receive_dt,
        ' ' AS item_last_ship_dt,
	rim_last_record_creation_dt;

STORE work__idrp_sourced_sears_store
INTO '$WORK__IDRP_SOURCED_SEARS_STORE_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

	
/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
