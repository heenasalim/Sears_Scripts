/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_ie_master_data_work__idrp_ksn_core_bridge_item.pig
# AUTHOR NAME:         Khim Mehta
# CREATION DATE:       Wed 11 June 12:52:57 CST 2015
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

SET default_parallel $NUM_PARALLEL;
REGISTER  $UDF_JAR;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

--------------------------------------------------------------------------

SMITH__IDRP_IE_BATCHDATE_DATA 		= LOAD '$SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_SCHEMA);

SMITH__IDRP_IE_BATCHDATE_PROCESS_TS = FOREACH SMITH__IDRP_IE_BATCHDATE_DATA GENERATE processing_ts AS processing_ts;

LOAD_SMITH__ITEM_HIERARCHY_COMBINED_ALL_CURRENT = LOAD '$SMITH__ITEM_HIERARCHY_COMBINED_ALL_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
									AS ($SMITH__ITEM_HIERARCHY_COMBINED_ALL_CURRENT_SCHEMA);
									
SMITH__ITEM_HIERARCHY_COMBINED_ALL_CURRENT_DATA = FOREACH LOAD_SMITH__ITEM_HIERARCHY_COMBINED_ALL_CURRENT 
													  GENERATE 	ksn_id as ksn_id,
															 	ksn_id_effective_ts as effective_ts,
																ksn_id_expiration_ts as expiration_ts,
																sears_division_nbr as sears_division_nbr, 
																sears_item_nbr as sears_item_nbr;

LOAD_CORE_BRIDGE_ITEM = LOAD '$GOLD__ITEM_CORE_BRIDGE_ITEM_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') as ($GOLD__ITEM_CORE_BRIDGE_ITEM_SCHEMA);

CORE_BRIDGE_ITEM_DATA = FOREACH LOAD_CORE_BRIDGE_ITEM GENERATE sears_division_nbr as sears_division_nbr, 
															   sears_item_nbr as sears_item_nbr,
															   TRIM(cost_pointing_method_cd) AS cost_pointing_method_cd,
															   effective_ts as effective_ts,
															   expiration_ts as expiration_ts,
															   TRIM(item_order_system_cd) AS item_order_system_cd;

CORE_BRIDGE_ITEM_CROSS_IMA_TS = CROSS CORE_BRIDGE_ITEM_DATA, SMITH__IDRP_IE_BATCHDATE_PROCESS_TS;




CORE_BRIDGE_ITEM_FILTER = FILTER CORE_BRIDGE_ITEM_CROSS_IMA_TS BY (effective_ts <= processing_ts) AND (processing_ts <= expiration_ts);

CORE_BRIDGE_ITEM_FILTER = FOREACH CORE_BRIDGE_ITEM_FILTER GENERATE sears_division_nbr, 
															   sears_item_nbr AS sears_item_nbr,
															   cost_pointing_method_cd AS cost_pointing_method_cd,
															   effective_ts AS effective_ts,
															   expiration_ts AS expiration_ts,
															   processing_ts AS processing_ts,
															   item_order_system_cd AS item_order_system_cd;

NON_EXAS_KSN_CORE_BRIDGE_ITEM_JOIN  = JOIN SMITH__ITEM_HIERARCHY_COMBINED_ALL_CURRENT_DATA BY (sears_division_nbr,sears_item_nbr) , CORE_BRIDGE_ITEM_FILTER BY (sears_division_nbr,sears_item_nbr);  


NON_EXAS_KSN_CORE_BRIDGE_ITEM = FOREACH NON_EXAS_KSN_CORE_BRIDGE_ITEM_JOIN GENERATE  
										SMITH__ITEM_HIERARCHY_COMBINED_ALL_CURRENT_DATA::ksn_id AS ksn_id,
										CORE_BRIDGE_ITEM_FILTER::cost_pointing_method_cd AS cost_pointing_method_cd,
										CORE_BRIDGE_ITEM_FILTER::item_order_system_cd AS item_order_system_cd;

LOAD_GOLD_ITEM_EXPLO_ASSRTMNT = LOAD '$GOLD__ITEM_EXPLODING_ASSORTMENT_ACTIVE_LOCATION' using PigStorage('$FIELD_DELIMITER_CONTROL_A') as ($GOLD__ITEM_EXPLODING_ASSORTMENT_SCHEMA);

GOLD__ITEM_EXAS_DATA = FOREACH LOAD_GOLD_ITEM_EXPLO_ASSRTMNT
								generate effective_ts,
									expiration_ts,
									external_ksn_id, 
									internal_ksn_id;

GOLD__ITEM_EXAS_CROSS_IMA_TS = cross GOLD__ITEM_EXAS_DATA, SMITH__IDRP_IE_BATCHDATE_PROCESS_TS;



GOLD__ITEM_EXAS_FILTER = filter GOLD__ITEM_EXAS_CROSS_IMA_TS by (effective_ts <= processing_ts) AND (processing_ts <= expiration_ts);

GOLD__ITEM_EXAS_KSN = 	foreach GOLD__ITEM_EXAS_FILTER 	generate external_ksn_id, internal_ksn_id;

DISTINCT_GOLD__ITEM_EXAS_KSN = distinct GOLD__ITEM_EXAS_KSN;

SAMS_NON_EXAS_KSN_CORE_BRIDGE_ITEM = filter NON_EXAS_KSN_CORE_BRIDGE_ITEM by item_order_system_cd == 'SAMS';

JOIN_SAMS_NON_EXAS_KSN = join SAMS_NON_EXAS_KSN_CORE_BRIDGE_ITEM  by (int)TRIM(ksn_id), DISTINCT_GOLD__ITEM_EXAS_KSN by (int)TRIM(internal_ksn_id);

SAMS_EXAS = foreach JOIN_SAMS_NON_EXAS_KSN 	generate external_ksn_id;

DISTINCT_SAMS_EXAS = distinct SAMS_EXAS;

EXAS_KSN_CORE_BRIDGE_ITEM  = 	FOREACH 	DISTINCT_SAMS_EXAS GENERATE 
									 		external_ksn_id  AS ksn_id,
									 		'' AS cost_pointing_method_cd,
									 		'SAMS' AS item_order_system_cd;
									 
FULL_KSN_CORE_BRIDGE_ITEM = JOIN NON_EXAS_KSN_CORE_BRIDGE_ITEM  BY (int)TRIM(ksn_id) FULL outer, EXAS_KSN_CORE_BRIDGE_ITEM BY (int)TRIM(ksn_id);
									 
									 
WORK__IDRP_KSN_CORE_BRIDGE_ITEM = 	foreach	FULL_KSN_CORE_BRIDGE_ITEM generate 
									'$CURRENT_TIMESTAMP' as load_ts,
									(NON_EXAS_KSN_CORE_BRIDGE_ITEM::ksn_id IS NULL OR IsNull(NON_EXAS_KSN_CORE_BRIDGE_ITEM::ksn_id,'') == '' ? EXAS_KSN_CORE_BRIDGE_ITEM::ksn_id : NON_EXAS_KSN_CORE_BRIDGE_ITEM::ksn_id) AS ksn_id,
							    	(NON_EXAS_KSN_CORE_BRIDGE_ITEM::ksn_id IS NULL OR IsNull(NON_EXAS_KSN_CORE_BRIDGE_ITEM::ksn_id,'') == '' ? EXAS_KSN_CORE_BRIDGE_ITEM::cost_pointing_method_cd: NON_EXAS_KSN_CORE_BRIDGE_ITEM::cost_pointing_method_cd) AS cost_pointing_method_cd,
									(NON_EXAS_KSN_CORE_BRIDGE_ITEM::ksn_id IS NULL OR IsNull(NON_EXAS_KSN_CORE_BRIDGE_ITEM::ksn_id,'') == '' ? EXAS_KSN_CORE_BRIDGE_ITEM::item_order_system_cd : NON_EXAS_KSN_CORE_BRIDGE_ITEM::item_order_system_cd) AS item_order_system_cd,
									'$batchid' AS batch_id;
STORE WORK__IDRP_KSN_CORE_BRIDGE_ITEM INTO '$WORK__IDRP_KSN_CORE_BRIDGE_ITEM_LOCATION'   USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

