/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_online_fulfillment_initialization_smith__idrp_online_billable_weight_1.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Thu Nov 28 00:18:47 EST 2013
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
#        DATE           BY                       MODIFICATION
#        22/01/0215		Siddhivinayak Karpe	     CR#3628 Source Changed from smith__idrp_ie_item_combined_hierarchy_all_current to 	 
#                                                work__idrp_item_hierarchy_combined_all_current
#        27/01/2015     Sushauvik Deb            CR#3622 IE Item - Missing OBU billable weight file
#        02/03/2015     Meghana Dhage            CR#3890 Add filter conditions to OBU billable weight file to exclude marketplace seller products
#	
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

REGISTER $UDF_JAR;
SET default_parallel $NUM_PARALLEL;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

-----------------------Loads--------------------------------------

work__idrp_obu_wcs_file_load = 
	LOAD '$SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
			AS ($SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_SCHEMA);


smith__item_combined_hierarchy_current_data = 
	LOAD '$WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
			AS ($WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_SCHEMA);

-----------------------Applying required filters and generating required columns--------------------------------------

smith__item_combined_hierarchy_current_data_req = 
	FOREACH smith__item_combined_hierarchy_current_data 
	GENERATE 
		ksn_id,
		shc_item_id,
		sears_division_nbr, 
		sears_item_nbr, 
		sears_sku_nbr;
		
work__idrp_obu_wcs_file_data = 
	FOREACH work__idrp_obu_wcs_file_load
	GENERATE
		UPPER(item_store_nm) AS item_store_nm,
		UPPER(TRIM(service_level_cd)) AS service_level_cd,
		product_id as product_id,
		TRIM(ksn_id) AS ksn_id,
		ups_billable_weight_qty AS ups_billable_weight,
		SUBSTRING(product_id,0,3) AS sears_division_nbr_wcs,
		SUBSTRING(product_id,3,8) AS sears_item_nbr_wcs,		
		SUBSTRING(product_id,8,11) AS sears_sku_nbr_wcs;
		
work__idrp_obu_wcs_file_data_kmart = 
	FILTER work__idrp_obu_wcs_file_data
	BY item_store_nm == 'KMART' AND service_level_cd == 'GROUND' AND (ksn_id != '' AND ksn_id IS NOT NULL);
	
join_kmart_combined_hierarchy = 
	JOIN work__idrp_obu_wcs_file_data_kmart
		BY (int)ksn_id,
		 smith__item_combined_hierarchy_current_data_req 
		BY (int)ksn_id;
		
join_kmart_combined_hierarchy_gen = 
	FOREACH join_kmart_combined_hierarchy
	GENERATE
		shc_item_id AS item_id,
		ups_billable_weight AS ups_billable_weight;
	
work__idrp_obu_wcs_file_data_sears =
	FILTER work__idrp_obu_wcs_file_data
	BY item_store_nm == 'SEARS' AND service_level_cd == 'GROUND' AND 
	   (IsNumeric(product_id));	

join_sears_combined_hierarchy = 
	JOIN work__idrp_obu_wcs_file_data_sears
		BY ((int)sears_division_nbr_wcs, (int)sears_item_nbr_wcs, (int)sears_sku_nbr_wcs),
		 smith__item_combined_hierarchy_current_data_req
		BY ((int)sears_division_nbr, (int)sears_item_nbr, (int)sears_sku_nbr);
		
join_sears_combined_hierarchy_gen = 
	FOREACH join_sears_combined_hierarchy
	GENERATE
		shc_item_id AS item_id,
		ups_billable_weight AS ups_billable_weight;
		
union_kmart_sears = 
	UNION join_kmart_combined_hierarchy_gen,
		  join_sears_combined_hierarchy_gen;
		  
group_union_kmart_sears = 
	GROUP union_kmart_sears
	BY (item_id);
	
max_ups_billable_weight_qty = 
		FOREACH group_union_kmart_sears
			{
				ordered_data = ORDER union_kmart_sears BY ups_billable_weight DESC;
				first_row = LIMIT ordered_data 1;
				GENERATE FLATTEN(first_row);
			};
				
SPLIT max_ups_billable_weight_qty INTO
	error_data IF (((int)ups_billable_weight) == 0 OR ups_billable_weight IS NULL),
	valid_data IF (((int)ups_billable_weight) != 0 AND IsNull(ups_billable_weight,'') != '');
	
gen_error_data = 
	FOREACH error_data
	GENERATE
		'$CURRENT_TIMESTAMP' AS load_ts,
		item_id AS item_id,
		'' AS ksn_id,
		'' AS sears_division_nbr,
		'' AS sears_item_nbr,
		'' AS sears_sku_nbr,
		'' AS web_sku_id,
		'' AS package_id,
		ups_billable_weight AS error_value,
		'Invalid UPS Billable Weight' AS error_desc,
		'$batchid' AS idrp_batch_id;

gen_valid_data = 
	FOREACH valid_data 
	GENERATE 
        '$CURRENT_TIMESTAMP' AS load_ts,
        item_id AS item_id,
        ups_billable_weight AS ups_billable_weight,
        '' AS temporary_billable_weight,
        '$CURRENT_TIMESTAMP' AS last_change_ts,
		'$batchid' AS idrp_batch_id;

		
STORE gen_valid_data 
	INTO '$TEMP_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

STORE gen_error_data 
	INTO '$WORK_ERROR_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
