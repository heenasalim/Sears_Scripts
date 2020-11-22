/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_obu_billable_weight_daily_format.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Thu Mar 27 03:47:07 EDT 2014
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
#        DATE           BY            MODIFICATION
#		 2015-02-12     Meghana		  CR 3266
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

--register the jar containing all PIG UDFs
REGISTER $UDF_JAR;
SET default_parallel $NUM_PARALLEL;

--trim spaces around string
DEFINE TRIM_STRING $TRIM_STRING ;

--trim leading zeros
DEFINE TRIM_INTEGER $TRIM_INTEGER ;

--trim leading and trailing zeros
DEFINE TRIM_DECIMAL $TRIM_DECIMAL ;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

--load incoming data
incoming_data = 
    LOAD '$SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_INCOMING_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
    AS ( $SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_INCOMING_SCHEMA );
	
--load existing data
existing_data = 
    LOAD '$SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
    AS ( $SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_SCHEMA );

--apply formatting to each field              
formatted_data = 
    FOREACH incoming_data
    GENERATE
			TRIM(item_store_nm) AS item_store_nm,
			TRIM(business_unit_desc) AS business_unit_desc,
			TRIM(division_nbr) AS division_nbr,
			TRIM(division_desc) AS division_desc,
			TRIM(line_nbr) AS line_nbr,
			TRIM(line_desc) AS line_desc,
			TRIM(catalog_entry_id) AS catalog_entry_id,
			TRIM(product_id) AS product_id,
			TRIM(ksn_id) AS ksn_id,
			TRIM(default_fulfillment_type_id) AS default_fulfillment_type_id,
			TRIM(service_level_cd) AS service_level_cd,
			TRIM(zone_cd) AS zone_cd,
			TRIM(ups_billable_weight_qty) AS ups_billable_weight_qty,
			TRIM(shipped_cost_amt) AS shipped_cost_amt,
			TRIM(last_modified_ts) AS last_modified_ts,
			TRIM(online_ind) AS online_ind,
			TRIM(create_ts) AS create_ts,
			TRIM(last_update_ts) AS last_update_ts,
			TRIM(item_store_id) AS item_store_id;
			
incoming_join_existing = 
	JOIN formatted_data BY (item_store_nm, catalog_entry_id, product_id, service_level_cd, zone_cd)
		FULL OUTER,
		 existing_data BY (item_store_nm, catalog_entry_id, product_id, service_level_cd, zone_cd);
		 
inserts = FILTER incoming_join_existing
		  BY (existing_data::catalog_entry_id IS NULL);
		  
old_data = FILTER incoming_join_existing
		   BY (formatted_data::catalog_entry_id IS NULL); 
		   
updates = FILTER incoming_join_existing
		  BY     (existing_data::item_store_nm == formatted_data::item_store_nm)
		     AND (existing_data::catalog_entry_id == formatted_data::catalog_entry_id)
		     AND (existing_data::product_id == formatted_data::product_id)
		     AND (existing_data::service_level_cd == formatted_data::service_level_cd)
		     AND (existing_data::zone_cd == formatted_data::zone_cd)
			 AND ((existing_data::business_unit_desc != formatted_data::business_unit_desc)
			 OR   (existing_data::division_nbr != formatted_data::division_nbr)
			 OR   (existing_data::division_desc != formatted_data::division_desc)
			 OR   (existing_data::line_nbr != formatted_data::line_nbr)
			 OR   (existing_data::line_desc != formatted_data::line_desc)			 
			 OR   (existing_data::default_fulfillment_type_id != formatted_data::default_fulfillment_type_id)				 	
			 OR   (existing_data::ups_billable_weight_qty != formatted_data::ups_billable_weight_qty)	
			 OR   (existing_data::shipped_cost_amt != formatted_data::shipped_cost_amt)	
			 OR   (existing_data::last_modified_ts != formatted_data::last_modified_ts)	
			 OR   (existing_data::online_ind != formatted_data::online_ind)	
			 OR   (existing_data::create_ts != formatted_data::create_ts)	
			 OR   (existing_data::last_update_ts != formatted_data::last_update_ts)	
			 OR   (existing_data::item_store_id != formatted_data::item_store_id));

inserts_gen = 
	FOREACH inserts
	GENERATE
		'$CURRENT_TIMESTAMP' AS load_ts,
		formatted_data::item_store_nm AS item_store_nm,
		formatted_data::business_unit_desc AS business_unit_desc,
		formatted_data::division_nbr AS division_nbr,
		formatted_data::division_desc AS division_desc,
		formatted_data::line_nbr AS line_nbr,
		formatted_data::line_desc AS line_desc,
		formatted_data::catalog_entry_id AS catalog_entry_id,
		formatted_data::product_id AS product_id,
		formatted_data::ksn_id AS ksn_id,
		formatted_data::default_fulfillment_type_id AS default_fulfillment_type_id,
		formatted_data::service_level_cd AS service_level_cd,
		formatted_data::zone_cd AS zone_cd,
		formatted_data::ups_billable_weight_qty AS ups_billable_weight_qty,
		formatted_data::shipped_cost_amt AS shipped_cost_amt,
		formatted_data::last_modified_ts AS last_modified_ts,
		formatted_data::online_ind AS online_ind,
		formatted_data::create_ts AS create_ts,
		formatted_data::last_update_ts AS last_update_ts,
		formatted_data::item_store_id AS item_store_id,
		'$batchid' AS IDRP_BATCH_ID;
		
old_data_gen = 
	FOREACH old_data
	GENERATE
		'$CURRENT_TIMESTAMP' AS load_ts,
		existing_data::item_store_nm AS item_store_nm,
		existing_data::business_unit_desc AS business_unit_desc,
		existing_data::division_nbr AS division_nbr,
		existing_data::division_desc AS division_desc,
		existing_data::line_nbr AS line_nbr,
		existing_data::line_desc AS line_desc,
		existing_data::catalog_entry_id AS catalog_entry_id,
		existing_data::product_id AS product_id,
		existing_data::ksn_id AS ksn_id,
		existing_data::default_fulfillment_type_id AS default_fulfillment_type_id,
		existing_data::service_level_cd AS service_level_cd,
		existing_data::zone_cd AS zone_cd,
		existing_data::ups_billable_weight_qty AS ups_billable_weight_qty,
		existing_data::shipped_cost_amt AS shipped_cost_amt,
		existing_data::last_modified_ts AS last_modified_ts,
		existing_data::online_ind AS online_ind,
		existing_data::create_ts AS create_ts,
		existing_data::last_update_ts AS last_update_ts,
		existing_data::item_store_id AS item_store_id,
		'$batchid' AS IDRP_BATCH_ID;
		
updates_gen = 
	FOREACH updates
	GENERATE
		'$CURRENT_TIMESTAMP' AS load_ts,
		formatted_data::item_store_nm AS item_store_nm,
		formatted_data::business_unit_desc AS business_unit_desc,
		formatted_data::division_nbr AS division_nbr,
		formatted_data::division_desc AS division_desc,
		formatted_data::line_nbr AS line_nbr,
		formatted_data::line_desc AS line_desc,
		formatted_data::catalog_entry_id AS catalog_entry_id,
		formatted_data::product_id AS product_id,
		formatted_data::ksn_id AS ksn_id,
		formatted_data::default_fulfillment_type_id AS default_fulfillment_type_id,
		formatted_data::service_level_cd AS service_level_cd,
		formatted_data::zone_cd AS zone_cd,
		formatted_data::ups_billable_weight_qty AS ups_billable_weight_qty,
		formatted_data::shipped_cost_amt AS shipped_cost_amt,
		formatted_data::last_modified_ts AS last_modified_ts,
		formatted_data::online_ind AS online_ind,
		formatted_data::create_ts AS create_ts,
		formatted_data::last_update_ts AS last_update_ts,
		formatted_data::item_store_id AS item_store_id,
		'$batchid' AS IDRP_BATCH_ID;

smith__idrp_obu_billable_weight_daily = 
	UNION inserts_gen,
		  old_data_gen,
		  updates_gen;
		  
			 
--store output data
STORE smith__idrp_obu_billable_weight_daily 
INTO '$SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_WORK_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
