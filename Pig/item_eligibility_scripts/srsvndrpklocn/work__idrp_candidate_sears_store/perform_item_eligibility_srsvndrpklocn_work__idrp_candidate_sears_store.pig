/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_work__idrp_candidate_sears_store.pig
# AUTHOR NAME:         Neera Singh
# CREATION DATE:       Tue Jul 08 09:37:58 EST 2014
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
#  10/10/2014    Arjun Dabhade    Added TrimLeadingZeros as per Spira#3138
#  22/10/2014	Priyanka Gurjar   eliminate easy order filter against CR 3205
#  13/11/2014	Siddhivinayak	  CR#3351 Translate the warehouse numbers on the work__idrp_dummy_vend_whse_ref table to their corresponding sears location warehouse number Code change at Line 202 to 221
#  23/04/2015  Sushauvik Deb      CR#4294 Unexpected cross merchandising attribute of N/A causing errors
#  11/05/2015  Sushauvik Deb      CR#4407 Update to online authorization based on warehouse sizing attributes to Sears Vendor Pack Loc
#  12/05/201   Suresh             CR4369
#  28/08/2015  Meghana D		  CR 5079 Modify the candidate warehouse and candidate store logic to use the gold__item_aprk_current table to determine which items are Import  
#  30-08-2016	Pankaj Gupta	 IPS -700 Applied skewed join
#  01-19-2017   Srujan Dussa     IPS-779 . Adding rim_last_record_create_dt from gold__inventory_rim_daily_current to be included in the Extract File to Shared Items
#  #  01/19/2017   Srujan Dussa     IPS-1058 - Correcting issue where store 9300 was not being set as eligible for Warehouse Sizing Attributes WG8800, WG8801, WG8808, WG8809, and WG8810
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

SET default_parallel 99;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

/*********Loading gold__inventory_rim_daily_current*****************************************************************************/

gold__inventory_rim_daily_current =
        LOAD '$GOLD__INVENTORY_RIM_DAILY_CURRENT_LOCATION'
        USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
        $GOLD__INVENTORY_RIM_DAILY_CURRENT_SCHEMA
       );


/***********Loading work__idrp_dummy_vend_whse_ref******************************************************************************/

work__idrp_dummy_vend_whse_ref = 
	LOAD '$WORK__IDRP_DUMMY_VEND_WHSE_REF_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
	    $WORK__IDRP_DUMMY_VEND_WHSE_REF_SCHEMA
	  );


/**********Laoding Table work__idrp_sears_location_xref***********************************************************************/

work__idrp_sears_location_xref = 
	LOAD '$WORK__IDRP_SEARS_LOCATION_XREF_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
	   $WORK__IDRP_SEARS_LOCATION_XREF_SCHEMA
	 );

gen_work__idrp_sears_location_xref = 
	FOREACH
	work__idrp_sears_location_xref
	GENERATE
	location_id,
	TrimLeadingZeros(sears_location_id) AS sears_location_id,
	location_level_cd,
	location_owner_cd,
	location_format_type_cd,
	cross_merchandise_store_type_cd;

fltr_gen_work__idrp_sears_location_xref = 
	FILTER
	gen_work__idrp_sears_location_xref
	BY(TRIM(location_level_cd)=='STORE');

/**********lOADING smith__idrp_ksn_attribute_current ***************************************************************************/

smith__idrp_ksn_attribute_current = 
	LOAD '$SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
	   $SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_SCHEMA
	   );

gen_smith__idrp_ksn_attribute_current = 
	FOREACH
	smith__idrp_ksn_attribute_current
	GENERATE
	ksn_id,
	sears_division_nbr,
	TrimLeadingZeros(sears_item_nbr) AS sears_item_nbr,
	sears_sku_nbr,
	shc_item_id,
	ksn_purchase_status_cd,
	special_retail_order_system_ind,
	shc_item_corporate_owner_cd,
	dot_com_allocation_ind,
	distribution_type_cd,
	only_rsu_distribution_channel_ind,
	special_order_candidate_ind,
	item_emp_ind,
	easy_order_ind,
	sams_migration_ind,
	IsNull(warehouse_sizing_attribute_cd,'') AS warehouse_sizing_attribute_cd,
	cross_merchandising_attribute_cd,
	rapid_item_ind,
	constrained_item_ind,
	sears_import_ind,
	TRIM(idrp_item_type_desc) AS idrp_item_type_desc,
	emp_to_jit_ind,
	rim_flow_ind,
	cross_merchandising_cd,
	IsNull(shop_your_way_attribute_cd,'') AS shop_your_way_attribute_cd;

/*********Loading gold__inventory_sears_dc_item_facility_current******************************************************************/

gold__inventory_sears_dc_item_facility_current =
	LOAD '$GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
	     $GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT_SCHEMA
	   ); 

gen_gold__inventory_sears_dc_item_facility_current = 
	FOREACH
	gold__inventory_sears_dc_item_facility_current
	GENERATE
	sears_division_nbr,
	TrimLeadingZeros(sears_item_nbr) AS sears_item_nbr,
	sears_sku_nbr,
	non_stock_source_cd,
	vendor_nbr,
	dos_warehouse_nbr;
	
--CR 5079
	
/*********Loading gold__item_aprk_current*************************************/

gold__item_aprk_current = 
	LOAD '$GOLD__ITEM_APRK_CURRENT_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
	AS ($GOLD__ITEM_APRK_CURRENT_SCHEMA);
	
gen_gold__item_aprk_current = 
	FOREACH gold__item_aprk_current
	GENERATE
	duns_nbr,
	aprk_type_cd,
	duns_owner_cd,
	import_ind;
	
gold__item_aprk_current_filter = 
	FILTER gen_gold__item_aprk_current 
	BY TRIM(aprk_type_cd) == 'ORD' AND
	   TRIM(duns_owner_cd) == 'S' AND
	   TRIM(import_ind) == 'Y';
	
work__aprk_import_vendors = 
	FOREACH gold__item_aprk_current_filter
	GENERATE
		SUBSTRING(duns_nbr, 1, 10) AS sears_vendor_nbr,
		'1' AS import_ind;

/********generating work__idrp_processing_day********************************************************************************/

work__idrp_candidate_sears_store_step1=
        FOREACH
        gold__inventory_rim_daily_current
        GENERATE
	division_nbr AS sears_division_nbr,
	item_nbr AS sears_item_nbr,
	sku_nbr AS sears_sku_nbr,
	store_nbr AS sears_location_id,
	IsNull(status_cd,'') AS rim_status_cd,
	TrimLeadingZeros(source_nbr) AS rim_source_nbr,
	store_pack_size_qty AS source_package_qty,
        last_record_creation_dt AS rim_last_record_creation_dt;

/********JOIN gold__inventory_sears_dc_item_facility_current & work__idrp_candidate_sears_store_step1*************************/


fltr_gold__inventory_sears_dc_item_facility_current = 
	FILTER
	gen_gold__inventory_sears_dc_item_facility_current
	BY(TRIM(non_stock_source_cd)=='STK');



join_candidate_sears_store_step1_dc_item_facility_current = 
	JOIN
	work__idrp_candidate_sears_store_step1
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr), TrimLeadingZeros(rim_source_nbr))
	LEFT OUTER,
	fltr_gold__inventory_sears_dc_item_facility_current
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr), TrimLeadingZeros(dos_warehouse_nbr));


work__idrp_candidate_sears_store_step2a = 
	FOREACH
	join_candidate_sears_store_step1_dc_item_facility_current
	GENERATE
	work__idrp_candidate_sears_store_step1::sears_division_nbr AS sears_division_nbr,
	work__idrp_candidate_sears_store_step1::sears_item_nbr AS sears_item_nbr,
	work__idrp_candidate_sears_store_step1::sears_sku_nbr AS sears_sku_nbr,
	work__idrp_candidate_sears_store_step1::sears_location_id AS sears_location_id,
	work__idrp_candidate_sears_store_step1::rim_status_cd AS rim_status_cd,
	(fltr_gold__inventory_sears_dc_item_facility_current::vendor_nbr IS NOT NULL?
	  fltr_gold__inventory_sears_dc_item_facility_current::vendor_nbr:work__idrp_candidate_sears_store_step1::rim_source_nbr) AS rim_source_nbr,
	work__idrp_candidate_sears_store_step1::source_package_qty AS source_package_qty,
        work__idrp_candidate_sears_store_step1::rim_last_record_creation_dt AS rim_last_record_creation_dt;

	
work__idrp_dummy_vend_whse_with_sears_nbr = 
	JOIN work__idrp_dummy_vend_whse_ref 
		by warehouse_nbr ,
			gen_work__idrp_sears_location_xref 
			by location_id;

gen_work__idrp_dummy_vend_whse_with_sears_nbr = 
	foreach work__idrp_dummy_vend_whse_with_sears_nbr 
		generate
			gen_work__idrp_sears_location_xref::sears_location_id as sears_warehouse_nbr,
			work__idrp_dummy_vend_whse_ref::vendor_nbr as vendor_nbr;
	
													
join_candidate_sears_store_step2a_whse_ref = 
	JOIN
		work__idrp_candidate_sears_store_step2a
			BY((int)rim_source_nbr) LEFT OUTER,
				gen_work__idrp_dummy_vend_whse_with_sears_nbr 
					BY((int)vendor_nbr);

			
work__idrp_candidate_sears_store_step2b = 
	FOREACH
	join_candidate_sears_store_step2a_whse_ref
	GENERATE
	work__idrp_candidate_sears_store_step2a::sears_division_nbr AS sears_division_nbr,
	work__idrp_candidate_sears_store_step2a::sears_item_nbr AS sears_item_nbr,
	work__idrp_candidate_sears_store_step2a::sears_sku_nbr AS sears_sku_nbr,
	work__idrp_candidate_sears_store_step2a::sears_location_id AS sears_location_id,
	work__idrp_candidate_sears_store_step2a::rim_status_cd AS rim_status_cd,
	work__idrp_candidate_sears_store_step2a::source_package_qty AS source_package_qty,
	(gen_work__idrp_dummy_vend_whse_with_sears_nbr::sears_warehouse_nbr IS NULL ? work__idrp_candidate_sears_store_step2a::rim_source_nbr : gen_work__idrp_dummy_vend_whse_with_sears_nbr::sears_warehouse_nbr) AS rim_source_nbr,
	(gen_work__idrp_dummy_vend_whse_with_sears_nbr::sears_warehouse_nbr IS NOT NULL? work__idrp_candidate_sears_store_step2a::rim_source_nbr:'0') AS rim_original_source_nbr,
        work__idrp_candidate_sears_store_step2a::rim_last_record_creation_dt AS rim_last_record_creation_dt;



join_sears_store_step2b_ksn_attribute_current = 
	JOIN
	work__idrp_candidate_sears_store_step2b
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr)),
	gen_smith__idrp_ksn_attribute_current
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr));


work__idrp_candidate_sears_store_step3 = 
	FOREACH
	join_sears_store_step2b_ksn_attribute_current
	GENERATE
	work__idrp_candidate_sears_store_step2b::sears_division_nbr AS sears_division_nbr,
	work__idrp_candidate_sears_store_step2b::sears_item_nbr AS sears_item_nbr,
	work__idrp_candidate_sears_store_step2b::sears_sku_nbr AS sears_sku_nbr,
	TrimLeadingZeros(work__idrp_candidate_sears_store_step2b::sears_location_id) AS sears_location_id,
	work__idrp_candidate_sears_store_step2b::rim_status_cd AS rim_status_cd,
	work__idrp_candidate_sears_store_step2b::source_package_qty AS source_package_qty,
	work__idrp_candidate_sears_store_step2b::rim_source_nbr AS rim_source_nbr,
	work__idrp_candidate_sears_store_step2b::rim_original_source_nbr AS rim_original_source_nbr,
	gen_smith__idrp_ksn_attribute_current::ksn_id AS ksn_id,
	gen_smith__idrp_ksn_attribute_current::shc_item_id AS shc_item_id,
	gen_smith__idrp_ksn_attribute_current::special_retail_order_system_ind AS special_retail_order_system_ind,
	gen_smith__idrp_ksn_attribute_current::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
	gen_smith__idrp_ksn_attribute_current::dot_com_allocation_ind AS dot_com_allocation_ind,
	gen_smith__idrp_ksn_attribute_current::distribution_type_cd AS distribution_type_cd,
	gen_smith__idrp_ksn_attribute_current::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
	gen_smith__idrp_ksn_attribute_current::special_order_candidate_ind AS special_order_candidate_ind,
	gen_smith__idrp_ksn_attribute_current::item_emp_ind AS item_emp_ind,
	gen_smith__idrp_ksn_attribute_current::easy_order_ind AS easy_order_ind, 
	gen_smith__idrp_ksn_attribute_current::sams_migration_ind AS sams_migration_ind,
	gen_smith__idrp_ksn_attribute_current::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
	gen_smith__idrp_ksn_attribute_current::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd, 
	gen_smith__idrp_ksn_attribute_current::rapid_item_ind AS rapid_item_ind,
	gen_smith__idrp_ksn_attribute_current::constrained_item_ind AS constrained_item_ind,
	gen_smith__idrp_ksn_attribute_current::sears_import_ind AS sears_import_ind,
	gen_smith__idrp_ksn_attribute_current::idrp_item_type_desc AS idrp_item_type_desc,
	gen_smith__idrp_ksn_attribute_current::emp_to_jit_ind AS emp_to_jit_ind,
	gen_smith__idrp_ksn_attribute_current::rim_flow_ind AS rim_flow_ind,
	gen_smith__idrp_ksn_attribute_current::cross_merchandising_cd AS cross_merchandising_cd,
	gen_smith__idrp_ksn_attribute_current::shop_your_way_attribute_cd AS shop_your_way_attribute_cd,
        work__idrp_candidate_sears_store_step2b::rim_last_record_creation_dt AS rim_last_record_creation_dt;

join_sears_store_step3_location_xref = 
	JOIN
	work__idrp_candidate_sears_store_step3
	BY(TrimLeadingZeros(sears_location_id)),
	fltr_gen_work__idrp_sears_location_xref 
	BY(TrimLeadingZeros(sears_location_id));


work__idrp_candidate_sears_store_step4 = 
	FOREACH
	join_sears_store_step3_location_xref
	GENERATE
	work__idrp_candidate_sears_store_step3::sears_division_nbr AS sears_division_nbr,
	work__idrp_candidate_sears_store_step3::sears_item_nbr AS sears_item_nbr,
	work__idrp_candidate_sears_store_step3::sears_sku_nbr AS sears_sku_nbr,
	work__idrp_candidate_sears_store_step3::sears_location_id AS sears_location_id,
	work__idrp_candidate_sears_store_step3::rim_status_cd AS rim_status_cd,
	work__idrp_candidate_sears_store_step3::source_package_qty AS source_package_qty,
	work__idrp_candidate_sears_store_step3::rim_source_nbr AS rim_source_nbr,
	work__idrp_candidate_sears_store_step3::rim_original_source_nbr AS rim_original_source_nbr,
	work__idrp_candidate_sears_store_step3::ksn_id AS ksn_id,
	work__idrp_candidate_sears_store_step3::shc_item_id AS shc_item_id,
	work__idrp_candidate_sears_store_step3::special_retail_order_system_ind AS special_retail_order_system_ind,
	work__idrp_candidate_sears_store_step3::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
	work__idrp_candidate_sears_store_step3::dot_com_allocation_ind AS dot_com_allocation_ind,
	work__idrp_candidate_sears_store_step3::distribution_type_cd AS distribution_type_cd,
	work__idrp_candidate_sears_store_step3::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
	work__idrp_candidate_sears_store_step3::special_order_candidate_ind AS special_order_candidate_ind,
	work__idrp_candidate_sears_store_step3::item_emp_ind AS item_emp_ind,
	work__idrp_candidate_sears_store_step3::easy_order_ind AS easy_order_ind,
	work__idrp_candidate_sears_store_step3::sams_migration_ind AS sams_migration_ind,
	work__idrp_candidate_sears_store_step3::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
	work__idrp_candidate_sears_store_step3::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
	work__idrp_candidate_sears_store_step3::rapid_item_ind AS rapid_item_ind,
	work__idrp_candidate_sears_store_step3::constrained_item_ind AS constrained_item_ind,
	work__idrp_candidate_sears_store_step3::sears_import_ind AS sears_import_ind,
	work__idrp_candidate_sears_store_step3::idrp_item_type_desc AS idrp_item_type_desc,
	work__idrp_candidate_sears_store_step3::emp_to_jit_ind AS emp_to_jit_ind,
	work__idrp_candidate_sears_store_step3::rim_flow_ind AS rim_flow_ind,
	work__idrp_candidate_sears_store_step3::cross_merchandising_cd AS cross_merchandising_cd,
	work__idrp_candidate_sears_store_step3::shop_your_way_attribute_cd AS shop_your_way_attribute_cd,
	fltr_gen_work__idrp_sears_location_xref::location_id AS location_id,
	fltr_gen_work__idrp_sears_location_xref::location_owner_cd AS location_owner_cd,
	fltr_gen_work__idrp_sears_location_xref::location_level_cd AS location_level_cd,
	fltr_gen_work__idrp_sears_location_xref::location_format_type_cd AS location_format_type_cd,
	fltr_gen_work__idrp_sears_location_xref::cross_merchandise_store_type_cd AS cross_merchandise_store_type_cd,
        work__idrp_candidate_sears_store_step3::rim_last_record_creation_dt AS rim_last_record_creation_dt;

	
filter_gen_work__idrp_sears_location_xref = 
	FILTER
	gen_work__idrp_sears_location_xref
	BY(TRIM(location_level_cd)!='STORE');
	
join_work__idrp_candidate_sears_store_step4_location_xref = 
	JOIN
        work__idrp_candidate_sears_store_step4
        BY(TrimLeadingZeros(rim_source_nbr))
	LEFT OUTER,
        filter_gen_work__idrp_sears_location_xref
        BY(TrimLeadingZeros(sears_location_id)) USING 'SKEWED';
		
work__idrp_candidate_sears_store_step5a = 
	FOREACH
	join_work__idrp_candidate_sears_store_step4_location_xref
	GENERATE
	work__idrp_candidate_sears_store_step4::sears_division_nbr AS sears_division_nbr,
	work__idrp_candidate_sears_store_step4::sears_item_nbr AS sears_item_nbr,
	work__idrp_candidate_sears_store_step4::sears_sku_nbr AS sears_sku_nbr,
	work__idrp_candidate_sears_store_step4::sears_location_id AS sears_location_id,
	work__idrp_candidate_sears_store_step4::rim_status_cd AS rim_status_cd,
	work__idrp_candidate_sears_store_step4::source_package_qty AS source_package_qty,
	work__idrp_candidate_sears_store_step4::rim_source_nbr AS rim_source_nbr,
	work__idrp_candidate_sears_store_step4::rim_original_source_nbr AS rim_original_source_nbr,
	work__idrp_candidate_sears_store_step4::ksn_id AS ksn_id,
	work__idrp_candidate_sears_store_step4::shc_item_id AS shc_item_id,
	work__idrp_candidate_sears_store_step4::special_retail_order_system_ind AS special_retail_order_system_ind,
	work__idrp_candidate_sears_store_step4::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
	work__idrp_candidate_sears_store_step4::dot_com_allocation_ind AS dot_com_allocation_ind,
	work__idrp_candidate_sears_store_step4::distribution_type_cd AS distribution_type_cd,
	work__idrp_candidate_sears_store_step4::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
	work__idrp_candidate_sears_store_step4::special_order_candidate_ind AS special_order_candidate_ind,
	work__idrp_candidate_sears_store_step4::item_emp_ind AS item_emp_ind,
	work__idrp_candidate_sears_store_step4::easy_order_ind AS easy_order_ind,
	work__idrp_candidate_sears_store_step4::sams_migration_ind AS sams_migration_ind,
	work__idrp_candidate_sears_store_step4::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
	work__idrp_candidate_sears_store_step4::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
	work__idrp_candidate_sears_store_step4::rapid_item_ind AS rapid_item_ind,
	work__idrp_candidate_sears_store_step4::constrained_item_ind AS constrained_item_ind,
	work__idrp_candidate_sears_store_step4::sears_import_ind AS sears_import_ind,
	work__idrp_candidate_sears_store_step4::idrp_item_type_desc AS idrp_item_type_desc,
	work__idrp_candidate_sears_store_step4::emp_to_jit_ind AS emp_to_jit_ind,
	work__idrp_candidate_sears_store_step4::rim_flow_ind AS rim_flow_ind,
	work__idrp_candidate_sears_store_step4::cross_merchandising_cd AS cross_merchandising_cd,
	work__idrp_candidate_sears_store_step4::shop_your_way_attribute_cd AS shop_your_way_attribute_cd,
	work__idrp_candidate_sears_store_step4::location_id AS location_id,
	work__idrp_candidate_sears_store_step4::location_owner_cd AS location_owner_cd,
	work__idrp_candidate_sears_store_step4::location_level_cd AS location_level_cd,
	work__idrp_candidate_sears_store_step4::location_format_type_cd AS location_format_type_cd,
	work__idrp_candidate_sears_store_step4::cross_merchandise_store_type_cd AS cross_merchandise_store_type_cd,
	filter_gen_work__idrp_sears_location_xref::location_id AS source_location_id,
	(TRIM(filter_gen_work__idrp_sears_location_xref::location_level_cd)=='VENDOR'?filter_gen_work__idrp_sears_location_xref::location_id:' ') AS purchase_order_vendor_location_id,
	filter_gen_work__idrp_sears_location_xref::location_level_cd AS source_location_level_cd,
        work__idrp_candidate_sears_store_step4::rim_last_record_creation_dt AS rim_last_record_creation_dt;



SPLIT work__idrp_candidate_sears_store_step5a
INTO work__idrp_candidate_sears_store_missing_source_error_data IF(source_location_id IS NULL),
work__idrp_candidate_sears_store_step5b IF(source_location_id IS NOT NULL);

work__idrp_candidate_sears_store_missing_source_error = foreach work__idrp_candidate_sears_store_missing_source_error_data 
														generate
														sears_division_nbr,
                                                        sears_item_nbr,
                                                        sears_sku_nbr,
                                                        sears_location_id AS sears_location_nbr,
                                                        rim_source_nbr AS sears_source_nbr,
                                                        'RIM' as sears_source_system_cd,
                                                        'source vendor not found in eligible loc table' as error_message;
														
STORE work__idrp_candidate_sears_store_missing_source_error 
INTO '$WORK__IDRP_CANDIDATE_SEARS_STORE_MISSING_SOURCE_ERROR_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


--CR 4294 Code Implemented
--CR4369 Code Implemented

fltr_idrp_candidate_sears_store_step5b = 
	FILTER
	work__idrp_candidate_sears_store_step5b
       BY((NOT(TRIM(rim_status_cd)=='X' OR TRIM(rim_status_cd)=='F' OR TRIM(rim_status_cd)=='T'))
		AND
	    ((((cross_merchandising_cd IS NULL OR TRIM(cross_merchandising_cd)=='') AND TRIM(location_owner_cd)=='S'))
		OR
		(TRIM(cross_merchandising_cd)=='SK3000'
		      AND
			((TRIM(location_owner_cd)=='S' AND (TRIM(cross_merchandise_store_type_cd) IS NULL OR TRIM(cross_merchandise_store_type_cd)=='')) OR (TRIM(location_owner_cd)=='K' AND TRIM(cross_merchandise_store_type_cd)=='K'))
		)
		      OR
			(TRIM(cross_merchandising_cd)=='EMP2JIT' AND TRIM(location_owner_cd)=='S')
		      OR
			(TRIM(cross_merchandising_cd)=='RIMFLOW' AND TRIM(location_owner_cd)=='S')
			 OR
            (TRIM(cross_merchandising_cd)=='N/A'AND TRIM(location_owner_cd)=='S')
             OR
              (TRIM(cross_merchandising_cd)=='SK1400' AND TRIM(location_owner_cd)=='S')
                   OR
                      (   (TRIM(cross_merchandising_cd)=='KM1000' 
                         OR TRIM(cross_merchandising_cd)=='KM4001' 
                         OR TRIM(cross_merchandising_cd)=='KM4005' 
                         OR TRIM(cross_merchandising_cd)=='KM4009' 
                         OR TRIM(cross_merchandising_cd)=='KM5000' 
                         OR TRIM(cross_merchandising_cd)=='KM9000' 
                        ) 
                           AND 
                              (TRIM(location_owner_cd)=='S' )  
                            AND 
                            (TRIM(cross_merchandise_store_type_cd) IS NULL OR TRIM(cross_merchandise_store_type_cd)=='' )  
                             )
                            )
                            );  

SPLIT fltr_idrp_candidate_sears_store_step5b
INTO rec_non_sears_internet_store IF(TRIM(location_format_type_cd)!='SINT' OR location_format_type_cd IS NULL),
rec_sears_internet_store IF(TRIM(location_format_type_cd)=='SINT');

work__idrp_candidate_sears_store_non_sears = 
	FOREACH
	rec_non_sears_internet_store
	GENERATE
	sears_division_nbr,
	TrimLeadingZeros(sears_item_nbr) AS sears_item_nbr,
	sears_sku_nbr,
	sears_location_id,
	location_id,
	location_level_cd,
	location_format_type_cd,
	location_owner_cd,
	rim_original_source_nbr,
	rim_source_nbr,
	source_location_id,
	source_location_level_cd,
	purchase_order_vendor_location_id,
	rim_status_cd,	
	(((TRIM(rim_status_cd)=='C' OR TRIM(rim_status_cd)=='L' OR TRIM(rim_status_cd)=='E' OR  TRIM(rim_status_cd)=='R' OR TRIM(rim_status_cd)=='P' OR TRIM(rim_status_cd)=='S'
		OR (TRIM(rim_status_cd)=='Z' AND (int)location_id==9372))) ?'Y':'N') AS active_ind,
	source_package_qty,
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
    (IsNull(idrp_item_type_desc,'') == ''
		? ''
		: idrp_item_type_desc) AS idrp_item_type_desc,
	cross_merchandising_attribute_cd,
	sams_migration_ind,
	emp_to_jit_ind,
	rim_flow_ind,
	cross_merchandising_cd,
	cross_merchandise_store_type_cd,
        rim_last_record_creation_dt;

--CR 4407 Implemented
work__idrp_candidate_sears_store_sears = 
        FOREACH
        rec_sears_internet_store
        GENERATE
        sears_division_nbr,
        TrimLeadingZeros(sears_item_nbr) AS sears_item_nbr,
        sears_sku_nbr,
        sears_location_id,
        location_id,
        location_level_cd,
        location_format_type_cd,
        location_owner_cd,
        rim_original_source_nbr,
        rim_source_nbr,
        source_location_id,
        source_location_level_cd,
        purchase_order_vendor_location_id,
        rim_status_cd,
        ( ( (TRIM(rim_status_cd)=='C' OR TRIM(rim_status_cd)=='L' OR TRIM(rim_status_cd)=='E' OR TRIM(rim_status_cd)=='R' OR TRIM(rim_status_cd)=='P' 
	   	OR TRIM(rim_status_cd)=='S' )
          AND 
	   
	   ( 
	 	((int)location_id==9300 AND
			   (( (IsNull(warehouse_sizing_attribute_cd,'') != '') AND (TRIM(warehouse_sizing_attribute_cd)=='WG8804' OR TRIM(warehouse_sizing_attribute_cd)=='WG8807' OR TRIM(warehouse_sizing_attribute_cd)=='WG8800' OR TRIM(warehouse_sizing_attribute_cd)=='WG8808' OR TRIM(warehouse_sizing_attribute_cd)=='WG8809' OR TRIM(warehouse_sizing_attribute_cd)=='WG8810')) 
				   --OR TRIM(warehouse_sizing_attribute_cd)=='WG8809' OR TRIM(warehouse_sizing_attribute_cd)=='WG8810' ))
		    	     OR 
			     (((IsNull(warehouse_sizing_attribute_cd,'') != '') AND TRIM(warehouse_sizing_attribute_cd)=='WG8801') AND ((IsNull(shop_your_way_attribute_cd,'') == '') OR TRIM(shop_your_way_attribute_cd)!='OS0001'))
			   )) 
		OR
		((NOT((int)location_id==9300 OR (int)location_id==9305)) 
		AND 
		(TRIM(rim_status_cd)=='C' OR TRIM(rim_status_cd)=='L' OR TRIM(rim_status_cd)=='E' OR TRIM(rim_status_cd)=='R' OR TRIM(rim_status_cd)=='P' OR TRIM(rim_status_cd)=='S')
			
	   ) ) ) ? 'Y':'N') AS active_ind,
        source_package_qty,
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
        (IsNull(idrp_item_type_desc,'') == ''
			? ''
			: idrp_item_type_desc) AS idrp_item_type_desc,
        cross_merchandising_attribute_cd,
        sams_migration_ind,
        emp_to_jit_ind,
        rim_flow_ind,
        cross_merchandising_cd,
        cross_merchandise_store_type_cd,
        rim_last_record_creation_dt;

work__idrp_candidate_sears_store_1 = 
	UNION
	work__idrp_candidate_sears_store_non_sears,
	work__idrp_candidate_sears_store_sears;

--CR 5079

work__idrp_candidate_sears_store_filter = 
	FILTER work__idrp_candidate_sears_store_1
	BY active_ind == 'Y';

work__idrp_candidate_active_store = 
	FOREACH work__idrp_candidate_sears_store_filter
	GENERATE
		sears_division_nbr,	
		sears_item_nbr,	
		sears_sku_nbr,	
		rim_source_nbr;

join_aprk_candidate_active_store = 
	JOIN  work__aprk_import_vendors BY ((int)sears_vendor_nbr),
		  work__idrp_candidate_active_store BY ((int)rim_source_nbr);
		  
gen_aprk_candidate_active_store =
	FOREACH join_aprk_candidate_active_store
	GENERATE
		work__idrp_candidate_active_store::sears_division_nbr AS sears_division_nbr,	
		work__idrp_candidate_active_store::sears_item_nbr AS sears_item_nbr,	
		work__idrp_candidate_active_store::sears_sku_nbr AS sears_sku_nbr;		

work__idrp_import_items = DISTINCT gen_aprk_candidate_active_store;

join_candidate_sears_store_import_items = 
	JOIN work__idrp_candidate_sears_store_1 BY (sears_division_nbr, sears_item_nbr, sears_sku_nbr) LEFT OUTER,
		 work__idrp_import_items BY (sears_division_nbr, sears_item_nbr, sears_sku_nbr); 
		 
work__idrp_candidate_sears_store =
	FOREACH join_candidate_sears_store_import_items
	GENERATE
        work__idrp_candidate_sears_store_1::sears_division_nbr AS sears_division_nbr,
        work__idrp_candidate_sears_store_1::sears_item_nbr AS sears_item_nbr,
        work__idrp_candidate_sears_store_1::sears_sku_nbr AS sears_sku_nbr,
        sears_location_id,
        location_id,
        location_level_cd,
        location_format_type_cd,
        location_owner_cd,
        rim_original_source_nbr,
        rim_source_nbr,
        source_location_id,
        source_location_level_cd,
        purchase_order_vendor_location_id,
        rim_status_cd,
        active_ind,
        source_package_qty,
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
        ((IsNull(work__idrp_import_items::sears_division_nbr,'') != '')
			? '1'
			: '0') AS sears_import_ind,
		(((IsNull(work__idrp_import_items::sears_division_nbr,'') != '') AND
		(idrp_item_type_desc != 'RSOS' AND idrp_item_type_desc != 'EASY ORDER'))
			? 'IMPORT'
			: idrp_item_type_desc) AS idrp_item_type_desc,
        cross_merchandising_attribute_cd,
        sams_migration_ind,
        emp_to_jit_ind,
        rim_flow_ind,
        cross_merchandising_cd,
        cross_merchandise_store_type_cd,
        rim_last_record_creation_dt;	

STORE work__idrp_candidate_sears_store
INTO '$WORK__IDRP_CANDIDATE_SEARS_STORE_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
