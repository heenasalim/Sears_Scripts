/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_work__idrp_sourced_sears_warehouse.pig
# AUTHOR NAME:         Neera Singh
# CREATION DATE:       Fri Jul 11 09:37:58 EST 2014
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
# 10/14/2014	Arnab Dey			CR3166 and CR3187 implemented (refer to line 490)
# 28/10/2014    Priyanka Gurjar		CR 3208 (refer line 74, line 629 - 724)
# 14/11/2014    Meghana Dhage 		CR 3166 (changed line number 453,514,543,572,601)
# 25/11/2014    Meghana Dhage 		CR 3166A (changed line number 436,453)
# 28/11/2014    Meghana Dhage 		CR 3166A (changed line number 455,533,562,591,620)
# 12/01/2015    Sushauvik Deb 		CR#3517 modify minimum vendor and warehouse sourcing to use item type to determine whether to use DOS or SRIM source.
# 30-08-2016	Pankaj Gupta	 	IPS -700 Increased reducer to 99
# 01-19-2017    Srujan Dussa		IPS-779 . Adding rim_last_record_create_dt from gold__inventory_rim_daily_current to be included in the Extract File to Shared Items.
# 05/03/2019    Piyush Solanki		IPS-3972: stop gap Fix for Sears MDO Source Pack issue
# 
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

SET default_parallel 99;

--register the jar containing all PIG UDFs
REGISTER $UDF_JAR;

--trim spaces around string
DEFINE TRIM_STRING $TRIM_STRING ;

--trim leading zeros
DEFINE TRIM_INTEGER $TRIM_INTEGER ;

--trim leading and trailing zeros
DEFINE TRIM_DECIMAL $TRIM_DECIMAL ;

DEFINE TrimLeadingZeros com.searshc.supplychain.idrp.udf.TrimLeadingZeros();

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

/****************Loading Table smith__idrp_i2k_sears_rebuy_vendor_package_current*****************************************************/

smith__idrp_i2k_sears_rebuy_vendor_package_current = 
	LOAD
	'$SMITH__IDRP_I2K_SEARS_REBUY_VENDOR_PACKAGE_CURRENT_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
	AS($SMITH__IDRP_I2K_SEARS_REBUY_VENDOR_PACKAGE_CURRENT_SCHEMA);

/*************Loading table WORK__RRC_MINIMUM_VENDOR_LOCATION*************************************************************************/

work__rrc_minimum_vendor = 
	LOAD 
	'$WORK__RRC_MINIMUM_VENDOR_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS($WORK__DD_IMPORT_MINIMUM_VENDOR_SCHEMA);

/********Loading table WORK__DDC_MINIMUM_VENDOR_LOCATION*****************************************************************************/

work__ddc_minimum_vendor = 
	LOAD 
	'$WORK__DDC_MINIMUM_VENDOR_LOCATION'
	 USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS($WORK__DD_IMPORT_MINIMUM_VENDOR_SCHEMA);

/*******Loading Table WORK__TW_IMPORT_MINIMUM_VENDOR_LOCATION (change the name against CR #3208*****************************************************/

work__import_minimum_vendor = 
	LOAD 
	'$WORK__IMPORT_MINIMUM_VENDOR_LOCATION'
	USING  PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS($WORK__DD_IMPORT_MINIMUM_VENDOR_SCHEMA);

/*********LOADING TABLE WORK__IDRP_CANDIDATE_SEARS_WAREHOUSE***********************************************************************/

work__idrp_candidate_sears_warehouse = 
	LOAD
	'$WORK__IDRP_CANDIDATE_SEARS_WAREHOUSE_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS($WORK__IDRP_CANDIDATE_SEARS_WAREHOUSE_SCHEMA);

/*********LOADING TABLE work__idrp_sears_location_xref ****************************************************************************/

work__idrp_sears_location_xref  = 
	LOAD
	'$WORK__IDRP_SEARS_LOCATION_XREF_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS($WORK__IDRP_SEARS_LOCATION_XREF_SCHEMA);


gen_work__idrp_sears_location_xref = 
	FOREACH
	work__idrp_sears_location_xref
	GENERATE
	location_id,
	location_level_cd;

/***********JOIN WORK__IDRP_CANDIDATE_SEARS_WAREHOUSE & minimum_vendor TABLES**************************************************************/

join_candidate_sears_warehouse_rrc_minimum_vendor = 
	JOIN
	work__idrp_candidate_sears_warehouse
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr))
	LEFT OUTER,
	work__rrc_minimum_vendor
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr));


gen_candidate_sears_warehouse_rrc_vendor = 
	FOREACH
	join_candidate_sears_warehouse_rrc_minimum_vendor
	GENERATE
	work__idrp_candidate_sears_warehouse::sears_division_nbr AS sears_division_nbr,
	work__idrp_candidate_sears_warehouse::sears_item_nbr AS sears_item_nbr,
	work__idrp_candidate_sears_warehouse::sears_sku_nbr AS sears_sku_nbr,
	work__idrp_candidate_sears_warehouse::sears_location_id AS sears_location_id,
	work__idrp_candidate_sears_warehouse::location_id AS location_id,
	work__idrp_candidate_sears_warehouse::location_level_cd AS location_level_cd,
	work__idrp_candidate_sears_warehouse::location_format_type_cd AS location_format_type_cd,
	work__idrp_candidate_sears_warehouse::location_owner_cd AS location_owner_cd,
	work__idrp_candidate_sears_warehouse::dos_original_source_nbr AS dos_original_source_nbr,
	work__idrp_candidate_sears_warehouse::dos_source_nbr AS dos_source_nbr,
        work__idrp_candidate_sears_warehouse::dos_source_package_qty AS dos_source_package_qty,
	work__idrp_candidate_sears_warehouse::dos_source_location_id AS dos_source_location_id,
	work__idrp_candidate_sears_warehouse::dos_source_location_level_cd AS dos_source_location_level_cd,
	work__idrp_candidate_sears_warehouse::srim_source_nbr AS srim_source_nbr,
	work__idrp_candidate_sears_warehouse::srim_source_location_id AS srim_source_location_id,
	work__idrp_candidate_sears_warehouse::srim_source_location_level_cd AS srim_source_location_level_cd,
	work__idrp_candidate_sears_warehouse::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
	work__idrp_candidate_sears_warehouse::srim_status_cd AS srim_status_cd,
	work__idrp_candidate_sears_warehouse::active_ind AS active_ind,
	work__idrp_candidate_sears_warehouse::srim_source_package_qty AS srim_source_package_qty,
	work__idrp_candidate_sears_warehouse::item_active_ind AS item_active_ind,
	work__idrp_candidate_sears_warehouse::shc_item_id AS shc_item_id,
	work__idrp_candidate_sears_warehouse::ksn_id AS ksn_id,
	work__idrp_candidate_sears_warehouse::special_retail_order_system_ind AS special_retail_order_system_ind,
	work__idrp_candidate_sears_warehouse::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
	work__idrp_candidate_sears_warehouse::dot_com_allocation_ind AS dot_com_allocation_ind,
	work__idrp_candidate_sears_warehouse::distribution_type_cd AS distribution_type_cd,
	work__idrp_candidate_sears_warehouse::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
	work__idrp_candidate_sears_warehouse::special_order_candidate_ind AS special_order_candidate_ind,
	work__idrp_candidate_sears_warehouse::item_emp_ind AS item_emp_ind,
	work__idrp_candidate_sears_warehouse::easy_order_ind AS easy_order_ind,
	work__idrp_candidate_sears_warehouse::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
	work__idrp_candidate_sears_warehouse::rapid_item_ind AS rapid_item_ind,
	work__idrp_candidate_sears_warehouse::constrained_item_ind AS constrained_item_ind,
	work__idrp_candidate_sears_warehouse::sears_import_ind AS sears_import_ind,
	work__idrp_candidate_sears_warehouse::idrp_item_type_desc AS idrp_item_type_desc,
	work__idrp_candidate_sears_warehouse::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
	work__idrp_candidate_sears_warehouse::sams_migration_ind AS sams_migration_ind,
	work__idrp_candidate_sears_warehouse::emp_to_jit_ind AS emp_to_jit_ind,
	work__idrp_candidate_sears_warehouse::rim_flow_ind AS rim_flow_ind,
	work__idrp_candidate_sears_warehouse::cross_merchandising_cd AS cross_merchandising_cd,
	work__idrp_candidate_sears_warehouse::stock_type_cd AS stock_type_cd,
	work__idrp_candidate_sears_warehouse::item_reserve_cd AS item_reserve_cd,
	work__idrp_candidate_sears_warehouse::non_stock_source_cd AS non_stock_source_cd,
	work__idrp_candidate_sears_warehouse::product_condition_cd AS product_condition_cd,
	work__idrp_candidate_sears_warehouse::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
	work__idrp_candidate_sears_warehouse::item_on_order_qty AS item_on_order_qty,
	work__idrp_candidate_sears_warehouse::item_reserve_qty AS item_reserve_qty,
	work__idrp_candidate_sears_warehouse::item_back_order_qty AS item_back_order_qty,
	work__idrp_candidate_sears_warehouse::item_next_period_future_order_qty AS item_next_period_future_order_qty,
	work__idrp_candidate_sears_warehouse::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
	work__idrp_candidate_sears_warehouse::item_last_receive_dt AS item_last_receive_dt,
	work__idrp_candidate_sears_warehouse::item_last_ship_dt AS item_last_ship_dt,
	work__rrc_minimum_vendor::min_vendor_nbr AS min_rrc_vendor_nbr,
	work__rrc_minimum_vendor::min_vendor_package_qty AS min_rrc_vendor_package_qty,
	work__rrc_minimum_vendor::min_vendor_location_id AS min_rrc_vendor_location_id,
	work__idrp_candidate_sears_warehouse::rim_last_record_creation_dt AS rim_last_record_creation_dt;


join_gen_candidate_sears_warehouse_rrc_vendor_ddc_min = 
	JOIN
	gen_candidate_sears_warehouse_rrc_vendor
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr))
	LEFT OUTER,
	work__ddc_minimum_vendor
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr));

gen_candidate_sears_warehouse_rrc_vendor_ddc_min = 
	FOREACH
	join_gen_candidate_sears_warehouse_rrc_vendor_ddc_min
	GENERATE
	gen_candidate_sears_warehouse_rrc_vendor::sears_division_nbr AS sears_division_nbr,
	gen_candidate_sears_warehouse_rrc_vendor::sears_item_nbr AS sears_item_nbr,
	gen_candidate_sears_warehouse_rrc_vendor::sears_sku_nbr AS sears_sku_nbr,
	gen_candidate_sears_warehouse_rrc_vendor::sears_location_id AS sears_location_id,
	gen_candidate_sears_warehouse_rrc_vendor::location_id AS location_id,
	gen_candidate_sears_warehouse_rrc_vendor::location_level_cd AS location_level_cd,
	gen_candidate_sears_warehouse_rrc_vendor::location_format_type_cd AS location_format_type_cd,
	gen_candidate_sears_warehouse_rrc_vendor::location_owner_cd AS location_owner_cd,
	gen_candidate_sears_warehouse_rrc_vendor::dos_original_source_nbr AS dos_original_source_nbr,
	gen_candidate_sears_warehouse_rrc_vendor::dos_source_nbr AS dos_source_nbr,
        gen_candidate_sears_warehouse_rrc_vendor::dos_source_package_qty AS dos_source_package_qty,
	gen_candidate_sears_warehouse_rrc_vendor::dos_source_location_id AS dos_source_location_id,
	gen_candidate_sears_warehouse_rrc_vendor::dos_source_location_level_cd AS dos_source_location_level_cd,
	gen_candidate_sears_warehouse_rrc_vendor::srim_source_nbr AS srim_source_nbr,
	gen_candidate_sears_warehouse_rrc_vendor::srim_source_location_id AS srim_source_location_id,
	gen_candidate_sears_warehouse_rrc_vendor::srim_source_location_level_cd AS srim_source_location_level_cd,
	gen_candidate_sears_warehouse_rrc_vendor::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
	gen_candidate_sears_warehouse_rrc_vendor::srim_status_cd AS srim_status_cd,
	gen_candidate_sears_warehouse_rrc_vendor::active_ind AS active_ind,
	gen_candidate_sears_warehouse_rrc_vendor::srim_source_package_qty AS srim_source_package_qty,
	gen_candidate_sears_warehouse_rrc_vendor::item_active_ind AS item_active_ind,
	gen_candidate_sears_warehouse_rrc_vendor::shc_item_id AS shc_item_id,
	gen_candidate_sears_warehouse_rrc_vendor::ksn_id AS ksn_id,
	gen_candidate_sears_warehouse_rrc_vendor::special_retail_order_system_ind AS special_retail_order_system_ind,
	gen_candidate_sears_warehouse_rrc_vendor::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
	gen_candidate_sears_warehouse_rrc_vendor::dot_com_allocation_ind AS dot_com_allocation_ind,
	gen_candidate_sears_warehouse_rrc_vendor::distribution_type_cd AS distribution_type_cd,
	gen_candidate_sears_warehouse_rrc_vendor::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
	gen_candidate_sears_warehouse_rrc_vendor::special_order_candidate_ind AS special_order_candidate_ind,
	gen_candidate_sears_warehouse_rrc_vendor::item_emp_ind AS item_emp_ind,
	gen_candidate_sears_warehouse_rrc_vendor::easy_order_ind AS easy_order_ind,
	gen_candidate_sears_warehouse_rrc_vendor::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
	gen_candidate_sears_warehouse_rrc_vendor::rapid_item_ind AS rapid_item_ind,
	gen_candidate_sears_warehouse_rrc_vendor::constrained_item_ind AS constrained_item_ind,
	gen_candidate_sears_warehouse_rrc_vendor::sears_import_ind AS sears_import_ind,
	gen_candidate_sears_warehouse_rrc_vendor::idrp_item_type_desc AS idrp_item_type_desc,
	gen_candidate_sears_warehouse_rrc_vendor::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
	gen_candidate_sears_warehouse_rrc_vendor::sams_migration_ind AS sams_migration_ind,
	gen_candidate_sears_warehouse_rrc_vendor::emp_to_jit_ind AS emp_to_jit_ind,
	gen_candidate_sears_warehouse_rrc_vendor::rim_flow_ind AS rim_flow_ind,
	gen_candidate_sears_warehouse_rrc_vendor::cross_merchandising_cd AS cross_merchandising_cd,
	gen_candidate_sears_warehouse_rrc_vendor::stock_type_cd AS stock_type_cd,
	gen_candidate_sears_warehouse_rrc_vendor::item_reserve_cd AS item_reserve_cd,
	gen_candidate_sears_warehouse_rrc_vendor::non_stock_source_cd AS non_stock_source_cd,
	gen_candidate_sears_warehouse_rrc_vendor::product_condition_cd AS product_condition_cd,
	gen_candidate_sears_warehouse_rrc_vendor::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
	gen_candidate_sears_warehouse_rrc_vendor::item_on_order_qty AS item_on_order_qty,
	gen_candidate_sears_warehouse_rrc_vendor::item_reserve_qty AS item_reserve_qty,
	gen_candidate_sears_warehouse_rrc_vendor::item_back_order_qty AS item_back_order_qty,
	gen_candidate_sears_warehouse_rrc_vendor::item_next_period_future_order_qty AS item_next_period_future_order_qty,
	gen_candidate_sears_warehouse_rrc_vendor::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
	gen_candidate_sears_warehouse_rrc_vendor::item_last_receive_dt AS item_last_receive_dt,
	gen_candidate_sears_warehouse_rrc_vendor::item_last_ship_dt AS item_last_ship_dt,
	gen_candidate_sears_warehouse_rrc_vendor::min_rrc_vendor_nbr AS min_rrc_vendor_nbr,
	gen_candidate_sears_warehouse_rrc_vendor::min_rrc_vendor_package_qty AS min_rrc_vendor_package_qty,
	gen_candidate_sears_warehouse_rrc_vendor::min_rrc_vendor_location_id AS min_rrc_vendor_location_id,
	work__ddc_minimum_vendor::min_vendor_nbr AS min_ddc_vendor_nbr,
	work__ddc_minimum_vendor::min_vendor_package_qty AS min_ddc_vendor_package_qty,
	work__ddc_minimum_vendor::min_vendor_location_id AS min_ddc_vendor_location_id,
	gen_candidate_sears_warehouse_rrc_vendor::rim_last_record_creation_dt AS rim_last_record_creation_dt;


join_gen_candidate_sears_warehouse_rrc_vendor_ddc_min = 
	JOIN
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr))
	LEFT OUTER,
	work__import_minimum_vendor 
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr));


gen_candidate_sears_warehouse_rrc_vendor_ddc_min = 
	FOREACH
	join_gen_candidate_sears_warehouse_rrc_vendor_ddc_min
	GENERATE
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::sears_division_nbr AS sears_division_nbr,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::sears_item_nbr AS sears_item_nbr,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::sears_sku_nbr AS sears_sku_nbr,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::sears_location_id AS sears_location_id,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::location_id AS location_id,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::location_level_cd AS location_level_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::location_format_type_cd AS location_format_type_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::location_owner_cd AS location_owner_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::dos_original_source_nbr AS dos_original_source_nbr,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::dos_source_nbr AS dos_source_nbr,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::dos_source_package_qty AS dos_source_package_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::dos_source_location_id AS dos_source_location_id,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::dos_source_location_level_cd AS dos_source_location_level_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::srim_source_nbr AS srim_source_nbr,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::srim_source_location_id AS srim_source_location_id,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::srim_source_location_level_cd AS srim_source_location_level_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::srim_status_cd AS srim_status_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::active_ind AS active_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::srim_source_package_qty AS srim_source_package_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_active_ind AS item_active_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::shc_item_id AS shc_item_id,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::ksn_id AS ksn_id,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::special_retail_order_system_ind AS special_retail_order_system_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::dot_com_allocation_ind AS dot_com_allocation_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::distribution_type_cd AS distribution_type_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::special_order_candidate_ind AS special_order_candidate_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_emp_ind AS item_emp_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::easy_order_ind AS easy_order_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::rapid_item_ind AS rapid_item_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::constrained_item_ind AS constrained_item_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::sears_import_ind AS sears_import_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::idrp_item_type_desc AS idrp_item_type_desc,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::sams_migration_ind AS sams_migration_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::emp_to_jit_ind AS emp_to_jit_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::rim_flow_ind AS rim_flow_ind,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::cross_merchandising_cd AS cross_merchandising_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::stock_type_cd AS stock_type_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_reserve_cd AS item_reserve_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::non_stock_source_cd AS non_stock_source_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::product_condition_cd AS product_condition_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_on_order_qty AS item_on_order_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_reserve_qty AS item_reserve_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_back_order_qty AS item_back_order_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_next_period_future_order_qty AS item_next_period_future_order_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_last_receive_dt AS item_last_receive_dt,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_last_ship_dt AS item_last_ship_dt,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_rrc_vendor_nbr AS min_rrc_vendor_nbr,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_rrc_vendor_package_qty AS min_rrc_vendor_package_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_rrc_vendor_location_id AS min_rrc_vendor_location_id,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_ddc_vendor_nbr AS min_ddc_vendor_nbr,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_ddc_vendor_package_qty AS min_ddc_vendor_package_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_ddc_vendor_location_id AS min_ddc_vendor_location_id,
	work__import_minimum_vendor::min_vendor_nbr AS min_import_vendor_nbr,
	work__import_minimum_vendor::min_vendor_package_qty AS min_import_vendor_package_qty,
	work__import_minimum_vendor::min_vendor_location_id AS min_import_vendor_location_id,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::rim_last_record_creation_dt AS rim_last_record_creation_dt;


join_gen_candidate_sears_warehouse_rrc_vendor_ddc_min_i2k =
        JOIN
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min
        BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr))
        LEFT OUTER,
        smith__idrp_i2k_sears_rebuy_vendor_package_current
        BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr));

work__idrp_sourced_sears_warehouse_step1 = 
	FOREACH
	join_gen_candidate_sears_warehouse_rrc_vendor_ddc_min_i2k
	GENERATE
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::sears_division_nbr AS sears_division_nbr,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::sears_item_nbr AS sears_item_nbr,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::sears_sku_nbr AS sears_sku_nbr,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::sears_location_id AS sears_location_id,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::location_id AS location_id,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::location_level_cd AS location_level_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::location_format_type_cd AS location_format_type_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::location_owner_cd AS location_owner_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::dos_original_source_nbr AS dos_original_source_nbr,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::dos_source_nbr AS dos_source_nbr,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::dos_source_package_qty AS dos_source_package_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::dos_source_location_id AS dos_source_location_id,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::dos_source_location_level_cd AS dos_source_location_level_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::srim_source_nbr AS srim_source_nbr,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::srim_source_location_id AS srim_source_location_id,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::srim_source_location_level_cd AS srim_source_location_level_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::srim_status_cd AS srim_status_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::active_ind AS active_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::srim_source_package_qty AS srim_source_package_qty,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_active_ind AS item_active_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::shc_item_id AS shc_item_id,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::ksn_id AS ksn_id,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::special_retail_order_system_ind AS special_retail_order_system_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::dot_com_allocation_ind AS dot_com_allocation_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::distribution_type_cd AS distribution_type_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::special_order_candidate_ind AS special_order_candidate_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_emp_ind AS item_emp_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::easy_order_ind AS easy_order_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::rapid_item_ind AS rapid_item_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::constrained_item_ind AS constrained_item_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::sears_import_ind AS sears_import_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::idrp_item_type_desc AS idrp_item_type_desc,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::sams_migration_ind AS sams_migration_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::emp_to_jit_ind AS emp_to_jit_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::rim_flow_ind AS rim_flow_ind,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::cross_merchandising_cd AS cross_merchandising_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::stock_type_cd AS stock_type_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_reserve_cd AS item_reserve_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::non_stock_source_cd AS non_stock_source_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::product_condition_cd AS product_condition_cd,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_on_order_qty AS item_on_order_qty,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_reserve_qty AS item_reserve_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_back_order_qty AS item_back_order_qty,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_next_period_future_order_qty AS item_next_period_future_order_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_last_receive_dt AS item_last_receive_dt,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::item_last_ship_dt AS item_last_ship_dt,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_rrc_vendor_nbr AS min_rrc_vendor_nbr,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_rrc_vendor_package_qty AS min_rrc_vendor_package_qty,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_rrc_vendor_location_id AS min_rrc_vendor_location_id,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_ddc_vendor_nbr AS min_ddc_vendor_nbr,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_ddc_vendor_package_qty AS min_ddc_vendor_package_qty,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_ddc_vendor_location_id AS min_ddc_vendor_location_id,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_import_vendor_nbr AS min_import_vendor_nbr,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_import_vendor_package_qty AS min_import_vendor_package_qty,
        gen_candidate_sears_warehouse_rrc_vendor_ddc_min::min_import_vendor_location_id AS min_import_vendor_location_id,
	smith__idrp_i2k_sears_rebuy_vendor_package_current::sears_vendor_duns_nbr AS i2k_source_vendor_nbr,
	smith__idrp_i2k_sears_rebuy_vendor_package_current::source_package_qty AS i2k_source_package_qty,
	smith__idrp_i2k_sears_rebuy_vendor_package_current::location_id AS i2k_source_vendor_location_id,
	smith__idrp_i2k_sears_rebuy_vendor_package_current::vendor_package_id AS i2k_source_vendor_package_id,
        ((idrp_item_type_desc =='RAPID' OR idrp_item_type_desc =='IMPORT' )?dos_source_location_level_cd:((idrp_item_type_desc !='RAPID' AND idrp_item_type_desc !='IMPORT' AND srim_source_location_level_cd IS NOT NULL AND (TRIM(srim_source_location_level_cd))!='' )? srim_source_location_level_cd :dos_source_location_level_cd)) AS test_source_location_level_cd,
	gen_candidate_sears_warehouse_rrc_vendor_ddc_min::rim_last_record_creation_dt AS rim_last_record_creation_dt;


/*
# CR 3166 and 3187 - the checks for source field is NOT NULL in the below logic should also treat and empty string as a NULL value
# Logic incorporated as (AND TRIM(srim_source_nbr)!='')
# Derivation of 
#  sears_source_location_nbr
#  source_location_id
#  source_package_qty
#  purchase_order_vendor_location_id
#  source_system_cd
*/

work__idrp_sourced_sears_warehouse_step2 = 
	FOREACH
	work__idrp_sourced_sears_warehouse_step1
	GENERATE
	sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        sears_location_id,
        location_id,
        location_level_cd,
        location_format_type_cd,
        location_owner_cd,
        dos_original_source_nbr,
	dos_source_nbr,
        dos_source_package_qty,
        dos_source_location_id,
        dos_source_location_level_cd,
        srim_source_nbr,
        srim_source_location_id,
        srim_source_location_level_cd,
((TRIM(location_format_type_cd)=='RRC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW')? min_rrc_vendor_location_id :
(TRIM(location_format_type_cd)=='DDC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' ? min_ddc_vendor_location_id:
((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC') AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') ? (srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0  AND TRIM(srim_source_nbr) != '' ? srim_source_location_id :dos_source_location_id) :(TRIM(location_format_type_cd)=='RRC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT' ? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? i2k_source_vendor_location_id : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? min_import_vendor_location_id : '' )) :(TRIM(location_format_type_cd)=='DDC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT' ? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? i2k_source_vendor_location_id : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? min_import_vendor_location_id : '' )) :(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (NOT(TRIM(idrp_item_type_desc)=='RAPID' OR TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC' OR TRIM(idrp_item_type_desc)=='IMPORT'))?srim_source_location_id:
((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC') AND TRIM(test_source_location_level_cd)=='WAREHOUSE'? ' ' :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW'? min_rrc_vendor_location_id :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='DD'? min_ddc_vendor_location_id :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='STK' AND TRIM(distribution_type_cd)=='TW'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr)!='' ? srim_source_location_id : min_rrc_vendor_location_id):
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='STK' AND TRIM(distribution_type_cd)=='DD'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr)!='' ? srim_source_location_id : min_ddc_vendor_location_id):
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='NONSTK' AND TRIM(distribution_type_cd)=='TW'? min_rrc_vendor_location_id :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='NONSTK' AND TRIM(distribution_type_cd)=='DD'? min_ddc_vendor_location_id :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT' ? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0  AND TRIM(i2k_source_vendor_nbr) != '' ? i2k_source_vendor_location_id : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? min_import_vendor_location_id : '' )) :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='WAREHOUSE' AND TRIM(stock_type_cd)=='STK'? ' ' :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='WAREHOUSE' AND TRIM(stock_type_cd)=='NONSTK'? ' ' :
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID'?(min_rrc_vendor_nbr IS NOT NULL? min_rrc_vendor_location_id : srim_source_location_id):
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 ? i2k_source_vendor_location_id : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? min_import_vendor_location_id : '' )):
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND (NOT(TRIM(idrp_item_type_desc)=='IMPORT' OR TRIM(idrp_item_type_desc)=='RAPID'))? srim_source_location_id :
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='WAREHOUSE'? ' ' :
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)!='IMPORT'?srim_source_location_id:
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'?(i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 ? i2k_source_vendor_location_id : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? min_import_vendor_location_id : '' )):
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='WAREHOUSE'? ' ':
(((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC') AND TRIM(test_source_location_level_cd)=='VENDOR' AND NOT(TRIM(idrp_item_type_desc)=='IMPORT' OR TRIM(idrp_item_type_desc)=='DOMESTIC' OR TRIM(idrp_item_type_desc)=='RAPID' OR TRIM(idrp_item_type_desc)=='CONSTRAINED')) ? srim_source_location_id : purchase_order_vendor_location_id)
))))))))))))))))))))))) AS purchase_order_vendor_location_id,
	srim_status_cd, 
	active_ind, 
	srim_source_package_qty, 
	item_active_ind, 
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
	stock_type_cd, 
	item_reserve_cd, 
	non_stock_source_cd, 
	product_condition_cd, 
	item_next_period_on_hand_qty, 
	item_on_order_qty, 
	item_reserve_qty, 
	item_back_order_qty, 
	item_next_period_future_order_qty,
	item_next_period_in_transit_qty,
	item_last_receive_dt,
	item_last_ship_dt, 
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
	  ((TRIM(location_format_type_cd)=='RRC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW')? min_rrc_vendor_nbr :
(TRIM(location_format_type_cd)=='DDC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' ? min_ddc_vendor_nbr :
((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC')
        AND
  TRIM(test_source_location_level_cd)=='VENDOR'
        AND
  (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC')
?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr) != '' ? srim_source_nbr:dos_source_nbr):
(TRIM(location_format_type_cd)=='RRC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT' ? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? 'TF32-15' : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'TF32-15' : '' ))  :
(TRIM(location_format_type_cd)=='DDC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT' ? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? 'TF32-15' : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'TF32-15' : '' )) :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (NOT(TRIM(idrp_item_type_desc)=='RAPID' OR TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC' OR TRIM(idrp_item_type_desc)=='IMPORT'))?srim_source_nbr:
((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC') AND TRIM(test_source_location_level_cd)=='WAREHOUSE'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr) != '' ? srim_source_nbr :dos_source_nbr) :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW'?min_rrc_vendor_nbr :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='DD'?min_ddc_vendor_nbr :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='STK' AND TRIM(distribution_type_cd)=='TW'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr)!='' ? srim_source_nbr:min_rrc_vendor_nbr):
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='STK' AND TRIM(distribution_type_cd)=='DD'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr)!='' ? srim_source_nbr:min_ddc_vendor_nbr):
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='NONSTK' AND TRIM(distribution_type_cd)=='TW'?min_rrc_vendor_nbr:
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='NONSTK' AND TRIM(distribution_type_cd)=='DD'?min_ddc_vendor_nbr:
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT' ?(i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? 'TF32-15' : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'TF32-15' : '' )):
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='WAREHOUSE' AND TRIM(stock_type_cd)=='STK'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr)!='' ? srim_source_nbr:dos_source_nbr):
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='WAREHOUSE' AND TRIM(stock_type_cd)=='NONSTK'?dos_source_nbr :
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID'?(min_rrc_vendor_nbr IS NOT NULL?min_rrc_vendor_nbr :srim_source_nbr):
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'?(i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? 'TF32-15' : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'TF32-15' : '' )):
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND (NOT(TRIM(idrp_item_type_desc)=='IMPORT' OR TRIM(idrp_item_type_desc)=='RAPID'))?srim_source_nbr:
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='WAREHOUSE'?srim_source_nbr:
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)!='IMPORT'?srim_source_nbr:
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'?(i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? 'TF32-15' : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'TF32-15' : '' )):
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='WAREHOUSE'?srim_source_nbr : (((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC') AND TRIM(test_source_location_level_cd)=='VENDOR' AND NOT(TRIM(idrp_item_type_desc)=='IMPORT' OR TRIM(idrp_item_type_desc)=='DOMESTIC' OR TRIM(idrp_item_type_desc)=='RAPID' OR TRIM(idrp_item_type_desc)=='CONSTRAINED')) ? srim_source_nbr : '')))))))))))))))))))))))) AS sears_source_location_nbr,

((TRIM(location_format_type_cd)=='RRC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW')? min_rrc_vendor_location_id  :
(TRIM(location_format_type_cd)=='DDC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' ? min_ddc_vendor_location_id :
((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC')
        AND
  TRIM(test_source_location_level_cd)=='VENDOR'
        AND
  (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC')
?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr) != '' ? srim_source_location_id :dos_source_location_id):
(TRIM(location_format_type_cd)=='RRC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? 'TF32-15' : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'TF32-15' : '' )) :
(TRIM(location_format_type_cd)=='DDC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT' ? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? 'TF32-15' : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'TF32-15' : '' )) :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (NOT(TRIM(idrp_item_type_desc)=='RAPID' OR TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC' OR TRIM(idrp_item_type_desc)=='IMPORT'))?srim_source_location_id:
((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC') AND TRIM(test_source_location_level_cd)=='WAREHOUSE'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr) != '' ? srim_source_location_id:dos_source_location_id) :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW'?min_rrc_vendor_location_id  :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='DD'?min_ddc_vendor_location_id :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='STK' AND TRIM(distribution_type_cd)=='TW'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr)!='' ? srim_source_location_id:min_rrc_vendor_location_id):
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='STK' AND TRIM(distribution_type_cd)=='DD'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr)!='' ? srim_source_location_id:min_ddc_vendor_location_id):
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='NONSTK' AND TRIM(distribution_type_cd)=='TW'?min_rrc_vendor_location_id :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='NONSTK' AND TRIM(distribution_type_cd)=='DD'?min_ddc_vendor_location_id:
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'?(i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? 'TF32-15' : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'TF32-15' : '' )) :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='WAREHOUSE' AND TRIM(stock_type_cd)=='STK'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr)!=''?srim_source_location_id:dos_source_location_id):
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='WAREHOUSE' AND TRIM(stock_type_cd)=='NONSTK'?dos_source_location_id:
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID'?(min_rrc_vendor_nbr IS NOT NULL?min_rrc_vendor_location_id:srim_source_location_id):
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'?(i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? 'TF32-15' : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'TF32-15' : '' )) :
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND (NOT(TRIM(idrp_item_type_desc)=='IMPORT' OR TRIM(idrp_item_type_desc)=='RAPID'))?srim_source_location_id:
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='WAREHOUSE'?srim_source_location_id :
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)!='IMPORT'?srim_source_location_id:
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'?(i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? 'TF32-15' : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'TF32-15' : '' )) :
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='WAREHOUSE'?srim_source_location_id:(((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC') AND TRIM(test_source_location_level_cd)=='VENDOR' AND NOT(TRIM(idrp_item_type_desc)=='IMPORT' OR TRIM(idrp_item_type_desc)=='DOMESTIC' OR TRIM(idrp_item_type_desc)=='RAPID' OR TRIM(idrp_item_type_desc)=='CONSTRAINED')) ? srim_source_location_id  : '')))))))))))))))))))))))) AS source_location_id,

((TRIM(location_format_type_cd)=='RRC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW')? min_rrc_vendor_package_qty :
(TRIM(location_format_type_cd)=='DDC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' ? min_ddc_vendor_package_qty :
((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC')
        AND
  TRIM(test_source_location_level_cd)=='VENDOR'
        AND
  (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC')
? srim_source_package_qty :
(TRIM(location_format_type_cd)=='RRC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT' ? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? i2k_source_package_qty:((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? min_import_vendor_package_qty : '' )) :
(TRIM(location_format_type_cd)=='DDC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? i2k_source_package_qty:((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? min_import_vendor_package_qty : '' )) :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (NOT(TRIM(idrp_item_type_desc)=='RAPID' OR TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC' OR TRIM(idrp_item_type_desc)=='IMPORT'))?srim_source_package_qty:
((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC') AND TRIM(test_source_location_level_cd)=='WAREHOUSE'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr) != '' ? srim_source_package_qty:dos_source_package_qty) :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW'? min_rrc_vendor_package_qty :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='DD'? min_ddc_vendor_package_qty :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='STK' AND TRIM(distribution_type_cd)=='TW'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr)!='' ? srim_source_package_qty : min_rrc_vendor_package_qty):
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='STK' AND TRIM(distribution_type_cd)=='DD'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr)!='' ? srim_source_package_qty:min_ddc_vendor_package_qty):
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='NONSTK' AND TRIM(distribution_type_cd)=='TW'? min_rrc_vendor_package_qty :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='NONSTK' AND TRIM(distribution_type_cd)=='DD'? min_ddc_vendor_package_qty :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT' ? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? i2k_source_package_qty : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? min_import_vendor_package_qty : '' )) :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='WAREHOUSE' AND TRIM(stock_type_cd)=='STK'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr)!=''? srim_source_package_qty : dos_source_package_qty):
---IPS-3972: start
--(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='WAREHOUSE' AND TRIM(stock_type_cd)=='NONSTK'?dos_source_package_qty:  ---IPS-3972: commented old code
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='WAREHOUSE' AND TRIM(stock_type_cd)=='NONSTK' AND ( TrimLeadingZeros(sears_division_nbr)=='22' or TrimLeadingZeros(sears_division_nbr)=='26' or TrimLeadingZeros(sears_division_nbr)=='46' )   ? srim_source_package_qty:								 ---IPS-3972: added
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='WAREHOUSE' AND TRIM(stock_type_cd)=='NONSTK' AND (NOT ( TrimLeadingZeros(sears_division_nbr)=='22' or TrimLeadingZeros(sears_division_nbr)=='26' or TrimLeadingZeros(sears_division_nbr)=='46' ) ) ? dos_source_package_qty:								  ---IPS-3972: added
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID'?(min_rrc_vendor_nbr IS NOT NULL? min_rrc_vendor_package_qty:srim_source_package_qty):
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? i2k_source_package_qty : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? min_import_vendor_package_qty : '' )):
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND (NOT(TRIM(idrp_item_type_desc)=='IMPORT' OR TRIM(idrp_item_type_desc)=='RAPID'))? srim_source_package_qty:
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='WAREHOUSE'?srim_source_package_qty :
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)!='IMPORT'?srim_source_package_qty:
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'?(i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? i2k_source_package_qty : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? min_import_vendor_package_qty : '' )):
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='WAREHOUSE'?srim_source_package_qty:(((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC') AND TRIM(test_source_location_level_cd)=='VENDOR' AND NOT(TRIM(idrp_item_type_desc)=='IMPORT' OR TRIM(idrp_item_type_desc)=='DOMESTIC' OR TRIM(idrp_item_type_desc)=='RAPID' OR TRIM(idrp_item_type_desc)=='CONSTRAINED')) ? srim_source_package_qty : ''))))))))
)					---IPS-3972: added
)					---IPS-3972: added
--)					---IPS-3972: commented old code : end
))))))))))))))) AS source_package_qty,

((TRIM(location_format_type_cd)=='RRC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW')? 'MINRRC' :
(TRIM(location_format_type_cd)=='DDC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' ? 'MINDDC':
((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC')
        AND
  TRIM(test_source_location_level_cd)=='VENDOR'
        AND
  (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC')
? (srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0  AND TRIM(srim_source_nbr) != '' ? 'SRIM' :'DOS') :
(TRIM(location_format_type_cd)=='RRC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? 'I2K': ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'IMP' : '' )) :
(TRIM(location_format_type_cd)=='DDC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT' ? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? 'I2K':((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'IMP' : '' )) :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (NOT(TRIM(idrp_item_type_desc)=='RAPID' OR TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC' OR TRIM(idrp_item_type_desc)=='IMPORT'))? 'SRIM' :
((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC') AND TRIM(test_source_location_level_cd)=='WAREHOUSE'? (srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr) != '' ? 'SRIM':'DOS') :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='TW'? 'MINRRC' :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID' AND TRIM(distribution_type_cd)=='DD'? 'MINDDC' :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='STK' AND TRIM(distribution_type_cd)=='TW'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0  AND TRIM(srim_source_nbr)!='' ? 'SRIM' : 'MINRRC'):
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='STK' AND TRIM(distribution_type_cd)=='DD'?(srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0 AND TRIM(srim_source_nbr)!='' ? 'SRIM' : 'MINDDC'):
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='NONSTK' AND TRIM(distribution_type_cd)=='TW'? 'MINRRC' :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND (TRIM(idrp_item_type_desc)=='CONSTRAINED' OR TRIM(idrp_item_type_desc)=='DOMESTIC') AND TRIM(stock_type_cd)=='NONSTK' AND TRIM(distribution_type_cd)=='DD'? 'MINDDC' :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT' ? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? 'I2K': ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'IMP' : '' )) :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='WAREHOUSE' AND TRIM(stock_type_cd)=='STK'? (srim_source_nbr IS NOT NULL AND (long)srim_source_nbr!=0  AND TRIM(srim_source_nbr)!=''? 'SRIM':'DOS') :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='WAREHOUSE' AND TRIM(stock_type_cd)=='NONSTK'? 'DOS':
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='RAPID'?(min_rrc_vendor_nbr IS NOT NULL? 'MINRRC' : 'SRIM'):
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 ? 'I2K' :((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'IMP' : '' )):
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND (NOT(TRIM(idrp_item_type_desc)=='IMPORT' OR TRIM(idrp_item_type_desc)=='RAPID'))? 'SRIM' :
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='WAREHOUSE'? 'SRIM' :
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)!='IMPORT'?'SRIM':
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'?(i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 ? 'I2K' : ((min_import_vendor_location_id IS NOT NULL AND TRIM(min_import_vendor_location_id) != '') ? 'IMP' : '' )):
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='WAREHOUSE'? 'SRIM':(((TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='RRC') AND TRIM(test_source_location_level_cd)=='VENDOR' AND NOT(TRIM(idrp_item_type_desc)=='IMPORT' OR TRIM(idrp_item_type_desc)=='DOMESTIC' OR TRIM(idrp_item_type_desc)=='RAPID' OR TRIM(idrp_item_type_desc)=='CONSTRAINED')) ? 'SRIM' : '')))))))))))))))))))))))) AS source_system_cd
,

(TRIM(location_format_type_cd)=='RRC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT' ? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? i2k_source_vendor_package_id: '') :
(TRIM(location_format_type_cd)=='DDC' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT' ? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? i2k_source_vendor_package_id:'') :
(TRIM(location_format_type_cd)=='MDO' AND TRIM(test_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 AND TRIM(i2k_source_vendor_nbr) != '' ? i2k_source_vendor_package_id:'') :
(TRIM(location_format_type_cd)=='CDFC' AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'? (i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 ? i2k_source_vendor_package_id : ''):
((NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC')) AND TRIM(srim_source_location_level_cd)=='VENDOR' AND TRIM(idrp_item_type_desc)=='IMPORT'?(i2k_source_vendor_nbr IS NOT NULL AND (long)i2k_source_vendor_nbr!=0 ? i2k_source_vendor_package_id : '' ):
''))))) AS vendor_package_id,
rim_last_record_creation_dt;


work__idrp_sourced_sears_warehouse_step2 = FOREACH work__idrp_sourced_sears_warehouse_step2 GENERATE
        sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        sears_location_id,
        location_id,
        location_level_cd,
        location_format_type_cd,
        location_owner_cd,
        dos_original_source_nbr,
        dos_source_nbr,
        dos_source_location_id,
        dos_source_location_level_cd,
        srim_source_nbr,
        srim_source_location_id,
        srim_source_location_level_cd,
(((TRIM(source_location_id)=='' or source_location_id is null) and idrp_item_type_desc !='IMPORT')
	?((location_format_type_cd == 'RRC' OR location_format_type_cd == 'DDC' OR location_format_type_cd == 'MDO') 
		? ((dos_source_location_level_cd=='VENDOR' OR ((TRIM(dos_source_location_level_cd)=='') AND srim_source_location_level_cd=='VENDOR')) 
			? dos_source_location_id 
			:'')
		:((srim_source_location_level_cd=='VENDOR')
			? srim_source_location_id
			:''))
    : purchase_order_vendor_location_id) AS purchase_order_vendor_location_id,
        srim_status_cd,
        active_ind,
        srim_source_package_qty,
        item_active_ind,
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
        stock_type_cd,
        item_reserve_cd,
        non_stock_source_cd,
        product_condition_cd,
        item_next_period_on_hand_qty,
        item_on_order_qty,
        item_reserve_qty,
        item_back_order_qty,
        item_next_period_future_order_qty,
        item_next_period_in_transit_qty,
        item_last_receive_dt,
        item_last_ship_dt,
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
(((TRIM(source_location_id)=='' or source_location_id is null) and idrp_item_type_desc !='IMPORT')
	?((location_format_type_cd == 'RRC' OR location_format_type_cd == 'DDC' OR location_format_type_cd == 'MDO') 
		? dos_source_nbr
		: srim_source_nbr)
	:sears_source_location_nbr) AS sears_source_location_nbr,

(((TRIM(source_location_id)=='' or source_location_id is null) and idrp_item_type_desc !='IMPORT')
	?((location_format_type_cd == 'RRC' OR location_format_type_cd == 'DDC' OR location_format_type_cd == 'MDO') 
		? dos_source_location_id
		: srim_source_location_id)
	:source_location_id) AS source_location_id,
	
(((TRIM(source_location_id)=='' or source_location_id is null) and idrp_item_type_desc !='IMPORT')
		? dos_source_package_qty
		: source_package_qty) AS source_package_qty,
		
(((TRIM(source_location_id)=='' or source_location_id is null) and idrp_item_type_desc !='IMPORT')
	?((location_format_type_cd == 'RRC' OR location_format_type_cd == 'DDC' OR location_format_type_cd == 'MDO') 
		? 'DOS'
		:'SRIM') 
	:source_system_cd) AS source_system_cd,		
	vendor_package_id,
	rim_last_record_creation_dt;


join_sourced_sears_warehouse_step2_location_xref  = 
	JOIN
	work__idrp_sourced_sears_warehouse_step2
	BY(TrimLeadingZeros(source_location_id)),
	gen_work__idrp_sears_location_xref 
	BY(TrimLeadingZeros(location_id));


work__idrp_sourced_sears_warehouse_step3 = 
	FOREACH
	join_sourced_sears_warehouse_step2_location_xref
	GENERATE
	work__idrp_sourced_sears_warehouse_step2::sears_division_nbr,
        work__idrp_sourced_sears_warehouse_step2::sears_item_nbr,
        work__idrp_sourced_sears_warehouse_step2::sears_sku_nbr,
        work__idrp_sourced_sears_warehouse_step2::sears_location_id,
        work__idrp_sourced_sears_warehouse_step2::location_id,
        work__idrp_sourced_sears_warehouse_step2::location_level_cd,
        work__idrp_sourced_sears_warehouse_step2::location_format_type_cd,
        work__idrp_sourced_sears_warehouse_step2::location_owner_cd,
        work__idrp_sourced_sears_warehouse_step2::dos_original_source_nbr,
	work__idrp_sourced_sears_warehouse_step2::dos_source_nbr,
        work__idrp_sourced_sears_warehouse_step2::dos_source_location_id,
        work__idrp_sourced_sears_warehouse_step2::dos_source_location_level_cd,
        work__idrp_sourced_sears_warehouse_step2::srim_source_nbr,
        work__idrp_sourced_sears_warehouse_step2::srim_source_location_id,
        work__idrp_sourced_sears_warehouse_step2::srim_source_location_level_cd,
	work__idrp_sourced_sears_warehouse_step2::purchase_order_vendor_location_id,
        work__idrp_sourced_sears_warehouse_step2::srim_status_cd AS srim_status_cd,
        work__idrp_sourced_sears_warehouse_step2::active_ind AS active_ind,
        work__idrp_sourced_sears_warehouse_step2::srim_source_package_qty AS srim_source_package_qty,
        work__idrp_sourced_sears_warehouse_step2::item_active_ind AS item_active_ind,
        work__idrp_sourced_sears_warehouse_step2::shc_item_id AS shc_item_id,
        work__idrp_sourced_sears_warehouse_step2::ksn_id AS ksn_id,
        work__idrp_sourced_sears_warehouse_step2::special_retail_order_system_ind AS special_retail_order_system_ind,
        work__idrp_sourced_sears_warehouse_step2::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        work__idrp_sourced_sears_warehouse_step2::dot_com_allocation_ind AS dot_com_allocation_ind,
        work__idrp_sourced_sears_warehouse_step2::distribution_type_cd AS distribution_type_cd,
        work__idrp_sourced_sears_warehouse_step2::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
        work__idrp_sourced_sears_warehouse_step2::special_order_candidate_ind AS special_order_candidate_ind,
        work__idrp_sourced_sears_warehouse_step2::item_emp_ind AS item_emp_ind,
        work__idrp_sourced_sears_warehouse_step2::easy_order_ind AS easy_order_ind,
        work__idrp_sourced_sears_warehouse_step2::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
        work__idrp_sourced_sears_warehouse_step2::rapid_item_ind AS rapid_item_ind,
        work__idrp_sourced_sears_warehouse_step2::constrained_item_ind AS constrained_item_ind,
        work__idrp_sourced_sears_warehouse_step2::sears_import_ind AS sears_import_ind,
        work__idrp_sourced_sears_warehouse_step2::idrp_item_type_desc AS idrp_item_type_desc, 
	work__idrp_sourced_sears_warehouse_step2::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd, 
	work__idrp_sourced_sears_warehouse_step2::sams_migration_ind AS sams_migration_ind,
        work__idrp_sourced_sears_warehouse_step2::emp_to_jit_ind AS emp_to_jit_ind,
        work__idrp_sourced_sears_warehouse_step2::rim_flow_ind AS rim_flow_ind,
        work__idrp_sourced_sears_warehouse_step2::cross_merchandising_cd AS cross_merchandising_cd,
        work__idrp_sourced_sears_warehouse_step2::stock_type_cd AS stock_type_cd,
        work__idrp_sourced_sears_warehouse_step2::item_reserve_cd AS item_reserve_cd,
        work__idrp_sourced_sears_warehouse_step2::non_stock_source_cd AS non_stock_source_cd,
        work__idrp_sourced_sears_warehouse_step2::product_condition_cd AS product_condition_cd,
        work__idrp_sourced_sears_warehouse_step2::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
        work__idrp_sourced_sears_warehouse_step2::item_on_order_qty AS item_on_order_qty,
        work__idrp_sourced_sears_warehouse_step2::item_reserve_qty AS item_reserve_qty,
        work__idrp_sourced_sears_warehouse_step2::item_back_order_qty AS item_back_order_qty,
        work__idrp_sourced_sears_warehouse_step2::item_next_period_future_order_qty AS item_next_period_future_order_qty,
	work__idrp_sourced_sears_warehouse_step2::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
        work__idrp_sourced_sears_warehouse_step2::item_last_receive_dt AS item_last_receive_dt,
        work__idrp_sourced_sears_warehouse_step2::item_last_ship_dt AS item_last_ship_dt,
	work__idrp_sourced_sears_warehouse_step2::min_rrc_vendor_nbr AS min_rrc_vendor_nbr,
        work__idrp_sourced_sears_warehouse_step2::min_rrc_vendor_package_qty AS min_rrc_vendor_package_qty,
        work__idrp_sourced_sears_warehouse_step2::min_rrc_vendor_location_id AS min_rrc_vendor_location_id,
        work__idrp_sourced_sears_warehouse_step2::min_ddc_vendor_nbr AS min_ddc_vendor_nbr,
        work__idrp_sourced_sears_warehouse_step2::min_ddc_vendor_package_qty AS min_ddc_vendor_package_qty,
        work__idrp_sourced_sears_warehouse_step2::min_ddc_vendor_location_id AS min_ddc_vendor_location_id,
        work__idrp_sourced_sears_warehouse_step2::min_import_vendor_nbr AS min_import_vendor_nbr,
        work__idrp_sourced_sears_warehouse_step2::min_import_vendor_package_qty AS min_import_vendor_package_qty,
        work__idrp_sourced_sears_warehouse_step2::min_import_vendor_location_id AS min_import_vendor_location_id,
        work__idrp_sourced_sears_warehouse_step2::i2k_source_vendor_nbr AS i2k_source_vendor_nbr,
        work__idrp_sourced_sears_warehouse_step2::i2k_source_package_qty AS i2k_source_package_qty,
        work__idrp_sourced_sears_warehouse_step2::i2k_source_vendor_location_id AS i2k_source_vendor_location_id,
        work__idrp_sourced_sears_warehouse_step2::i2k_source_vendor_package_id AS i2k_source_vendor_package_id,
	work__idrp_sourced_sears_warehouse_step2::sears_source_location_nbr AS sears_source_location_nbr,
	work__idrp_sourced_sears_warehouse_step2::source_location_id AS source_location_id,
	work__idrp_sourced_sears_warehouse_step2::source_package_qty AS source_package_qty,
	work__idrp_sourced_sears_warehouse_step2::source_system_cd AS source_system_cd,
	work__idrp_sourced_sears_warehouse_step2::vendor_package_id AS vendor_package_id, 
	gen_work__idrp_sears_location_xref::location_level_cd AS source_location_level_cd,
	(TRIM(gen_work__idrp_sears_location_xref::location_level_cd)=='VENDOR'?source_package_qty:' ') AS vendor_package_carton_qty,
	work__idrp_sourced_sears_warehouse_step2::rim_last_record_creation_dt AS rim_last_record_creation_dt;


work__idrp_sourced_sears_warehouse = 
	FOREACH
	work__idrp_sourced_sears_warehouse_step3
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
        srim_status_cd,
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
	' ' AS original_source_nbr:chararray,
        item_active_ind,
        stock_type_cd,
	item_reserve_cd,
	non_stock_source_cd,
        item_next_period_on_hand_qty,
        item_on_order_qty,
        item_reserve_qty,
        item_back_order_qty,
        item_next_period_future_order_qty,
        item_next_period_in_transit_qty,
        item_last_receive_dt,
        item_last_ship_dt,
	rim_last_record_creation_dt;

STORE work__idrp_sourced_sears_warehouse
INTO '$WORK__IDRP_SOURCED_SEARS_WAREHOUSE_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');
	
