/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_online_fulfillment_initialization_smith__idrp_online_fulfillment_2.pig
# AUTHOR NAME:         Mudit Mangal
# CREATION DATE:       Mon July 07 05:25:42 EST 2014
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
#		 2015-03-30   Meghana		CR 3703
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

smith__idrp_online_fulfillment_history_data = 
	LOAD '$SMITH__IDRP_ONLINE_FULFILLMENT_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
			AS ($SMITH__IDRP_ONLINE_FULFILLMENT_SCHEMA);

smith__idrp_eligible_item_current = 
	LOAD '$SMITH__IDRP_ELIGIBLE_ITEM_CURRENT_LOCATION'
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
			AS ($SMITH__IDRP_ELIGIBLE_ITEM_CURRENT_SCHEMA);

smith__idrp_eligible_item_current_req = 
	FOREACH smith__idrp_eligible_item_current 
	GENERATE 
		shc_item_id AS item_id,
		sears_temporary_online_fulfillment_type_cd AS sears_temporary_online_fulfillment_type_cd,
		kmart_temporary_online_fulfillment_type_cd AS kmart_temporary_online_fulfillment_type_cd;

smith__idrp_online_fulfillment_current_data = LOAD 
	'$TEMP_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
			AS ($SMITH__IDRP_ONLINE_FULFILLMENT_SCHEMA);


/****************Merging the history and current tables ************************/

merge_data = 
	JOIN smith__idrp_online_fulfillment_history_data 
		BY item_id FULL OUTER, 
		 smith__idrp_online_fulfillment_current_data 
		BY item_id;

SPLIT merge_data INTO 
	no_change_data IF          
	((IsNull(smith__idrp_online_fulfillment_history_data::item_id,'')!='' AND IsNull(smith__idrp_online_fulfillment_current_data::item_id,'')!='') AND
        (smith__idrp_online_fulfillment_history_data::item_id == smith__idrp_online_fulfillment_current_data::item_id) AND 
	(smith__idrp_online_fulfillment_history_data::sears_default_online_fulfillment_type_cd == smith__idrp_online_fulfillment_current_data::sears_default_online_fulfillment_type_cd) AND
	(smith__idrp_online_fulfillment_history_data::sears_default_online_fulfillment_type_cd_ts == smith__idrp_online_fulfillment_current_data::sears_default_online_fulfillment_type_cd_ts) AND
	(smith__idrp_online_fulfillment_history_data::kmart_default_online_fulfillment_type_cd == smith__idrp_online_fulfillment_current_data::kmart_default_online_fulfillment_type_cd) AND
	(smith__idrp_online_fulfillment_history_data::kmart_default_online_fulfillment_type_cd_ts == smith__idrp_online_fulfillment_current_data::kmart_default_online_fulfillment_type_cd_ts) AND	
	(smith__idrp_online_fulfillment_history_data::web_exclusive_ind == smith__idrp_online_fulfillment_current_data::web_exclusive_ind)), 

    changed_data IF 
	((IsNull(smith__idrp_online_fulfillment_history_data::item_id,'')!='' AND IsNull(smith__idrp_online_fulfillment_current_data::item_id,'')!='') AND
        (smith__idrp_online_fulfillment_history_data::item_id == smith__idrp_online_fulfillment_current_data::item_id) AND  
	((smith__idrp_online_fulfillment_history_data::sears_default_online_fulfillment_type_cd !=  
	 smith__idrp_online_fulfillment_current_data::sears_default_online_fulfillment_type_cd) OR
	 (smith__idrp_online_fulfillment_history_data::sears_default_online_fulfillment_type_cd_ts != smith__idrp_online_fulfillment_current_data::sears_default_online_fulfillment_type_cd_ts) OR
	 (smith__idrp_online_fulfillment_history_data::kmart_default_online_fulfillment_type_cd != smith__idrp_online_fulfillment_current_data::kmart_default_online_fulfillment_type_cd) OR
	 (smith__idrp_online_fulfillment_history_data::kmart_default_online_fulfillment_type_cd_ts != smith__idrp_online_fulfillment_current_data::kmart_default_online_fulfillment_type_cd_ts) OR
	 (smith__idrp_online_fulfillment_history_data::web_exclusive_ind != smith__idrp_online_fulfillment_current_data::web_exclusive_ind))),
					  
    old_data IF (IsNull(smith__idrp_online_fulfillment_history_data::item_id,'') != '' AND 
				 IsNull(smith__idrp_online_fulfillment_current_data::item_id,'') == ''),
    new_data IF (IsNull(smith__idrp_online_fulfillment_history_data::item_id,'') == '' AND 
				 IsNull(smith__idrp_online_fulfillment_current_data::item_id,'') != '');

no_change_data_gen = 
	FOREACH no_change_data 
	GENERATE 
		smith__idrp_online_fulfillment_history_data::load_ts AS load_ts,
		smith__idrp_online_fulfillment_history_data::item_id AS item_id,
		smith__idrp_online_fulfillment_history_data::ksn_id AS ksn_id,
		smith__idrp_online_fulfillment_history_data::sears_division_nbr AS sears_division_nbr,
		smith__idrp_online_fulfillment_history_data::sears_item_nbr AS sears_item_nbr,
		smith__idrp_online_fulfillment_history_data::sears_sku_nbr AS sears_sku_nbr,
		smith__idrp_online_fulfillment_history_data::web_sku_id AS web_sku_id,
		smith__idrp_online_fulfillment_history_data::upc_nbr AS upc_nbr,
		smith__idrp_online_fulfillment_history_data::sears_default_online_fulfillment_type_cd AS sears_default_online_fulfillment_type_cd,
		smith__idrp_online_fulfillment_history_data::sears_default_online_fulfillment_type_cd_ts AS sears_default_online_fulfillment_type_cd_ts,
		smith__idrp_online_fulfillment_history_data::sears_temporary_online_fulfillment_type_cd AS sears_temporary_online_fulfillment_type_cd,
		smith__idrp_online_fulfillment_history_data::kmart_default_online_fulfillment_type_cd AS kmart_default_online_fulfillment_type_cd,
		smith__idrp_online_fulfillment_history_data::kmart_default_online_fulfillment_type_cd_ts AS kmart_default_online_fulfillment_type_cd_ts,
		smith__idrp_online_fulfillment_history_data::kmart_temporary_online_fulfillment_type_cd AS kmart_temporary_online_fulfillment_type_cd,
		smith__idrp_online_fulfillment_history_data::web_exclusive_ind AS web_exclusive_ind,		
		'$batchid' AS idrp_batch_id;


changed_data_gen = 
	FOREACH changed_data 
	GENERATE 
		smith__idrp_online_fulfillment_history_data::load_ts AS load_ts,
		smith__idrp_online_fulfillment_history_data::item_id AS item_id,
		smith__idrp_online_fulfillment_history_data::ksn_id AS ksn_id,
		smith__idrp_online_fulfillment_history_data::sears_division_nbr AS sears_division_nbr,
		smith__idrp_online_fulfillment_history_data::sears_item_nbr AS sears_item_nbr,
		smith__idrp_online_fulfillment_history_data::sears_sku_nbr AS sears_sku_nbr,
		smith__idrp_online_fulfillment_history_data::web_sku_id AS web_sku_id,
		smith__idrp_online_fulfillment_history_data::upc_nbr AS upc_nbr,
		smith__idrp_online_fulfillment_current_data::sears_default_online_fulfillment_type_cd AS sears_default_online_fulfillment_type_cd,
		'$CURRENT_TIMESTAMP' AS sears_default_online_fulfillment_type_cd_ts,
		smith__idrp_online_fulfillment_current_data::sears_temporary_online_fulfillment_type_cd AS sears_temporary_online_fulfillment_type_cd,
		smith__idrp_online_fulfillment_current_data::kmart_default_online_fulfillment_type_cd AS kmart_default_online_fulfillment_type_cd,
		'$CURRENT_TIMESTAMP' AS kmart_default_online_fulfillment_type_cd_ts,
		smith__idrp_online_fulfillment_current_data::kmart_temporary_online_fulfillment_type_cd AS kmart_temporary_online_fulfillment_type_cd,
		smith__idrp_online_fulfillment_current_data::web_exclusive_ind AS web_exclusive_ind,
		'$batchid' AS idrp_batch_id;


old_data_gen = 
	FOREACH old_data 
	GENERATE 
		smith__idrp_online_fulfillment_history_data::load_ts AS load_ts,
		smith__idrp_online_fulfillment_history_data::item_id AS item_id,
		smith__idrp_online_fulfillment_history_data::ksn_id AS ksn_id,
		smith__idrp_online_fulfillment_history_data::sears_division_nbr AS sears_division_nbr,
		smith__idrp_online_fulfillment_history_data::sears_item_nbr AS sears_item_nbr,
		smith__idrp_online_fulfillment_history_data::sears_sku_nbr AS sears_sku_nbr,
		smith__idrp_online_fulfillment_history_data::web_sku_id AS web_sku_id,
		smith__idrp_online_fulfillment_history_data::upc_nbr AS upc_nbr,		
		(smith__idrp_online_fulfillment_history_data::sears_default_online_fulfillment_type_cd != 'NONE'
			? 'NONE'
			: smith__idrp_online_fulfillment_history_data::sears_default_online_fulfillment_type_cd) AS sears_default_online_fulfillment_type_cd,
		(smith__idrp_online_fulfillment_history_data::sears_default_online_fulfillment_type_cd != 'NONE'
			? '$CURRENT_TIMESTAMP'
			: smith__idrp_online_fulfillment_history_data::sears_default_online_fulfillment_type_cd_ts) AS sears_default_online_fulfillment_type_cd_ts,
		smith__idrp_online_fulfillment_history_data::sears_temporary_online_fulfillment_type_cd AS sears_temporary_online_fulfillment_type_cd,
		(smith__idrp_online_fulfillment_history_data::kmart_default_online_fulfillment_type_cd != 'NONE'
			? 'NONE'
			: smith__idrp_online_fulfillment_history_data::kmart_default_online_fulfillment_type_cd) AS kmart_default_online_fulfillment_type_cd,
		(smith__idrp_online_fulfillment_history_data::kmart_default_online_fulfillment_type_cd != 'NONE' 	
			? '$CURRENT_TIMESTAMP'
			: smith__idrp_online_fulfillment_history_data::kmart_default_online_fulfillment_type_cd_ts) AS kmart_default_online_fulfillment_type_cd_ts,
		smith__idrp_online_fulfillment_history_data::kmart_temporary_online_fulfillment_type_cd AS kmart_temporary_online_fulfillment_type_cd,
		smith__idrp_online_fulfillment_history_data::web_exclusive_ind AS web_exclusive_ind,		
		'$batchid' AS idrp_batch_id;

new_data_gen = 
	FOREACH new_data 
	GENERATE 
        smith__idrp_online_fulfillment_current_data::load_ts AS load_ts,
        smith__idrp_online_fulfillment_current_data::item_id AS item_id,
        smith__idrp_online_fulfillment_current_data::ksn_id AS ksn_id,
        smith__idrp_online_fulfillment_current_data::sears_division_nbr AS sears_division_nbr,
        smith__idrp_online_fulfillment_current_data::sears_item_nbr AS sears_item_nbr,
        smith__idrp_online_fulfillment_current_data::sears_sku_nbr AS sears_sku_nbr,
        smith__idrp_online_fulfillment_current_data::web_sku_id AS web_sku_id,
        smith__idrp_online_fulfillment_current_data::upc_nbr AS upc_nbr,
		smith__idrp_online_fulfillment_current_data::sears_default_online_fulfillment_type_cd AS sears_default_online_fulfillment_type_cd,
		smith__idrp_online_fulfillment_current_data::sears_default_online_fulfillment_type_cd_ts AS sears_default_online_fulfillment_type_cd_ts,
		smith__idrp_online_fulfillment_current_data::sears_temporary_online_fulfillment_type_cd AS sears_temporary_online_fulfillment_type_cd,
		smith__idrp_online_fulfillment_current_data::kmart_default_online_fulfillment_type_cd AS kmart_default_online_fulfillment_type_cd,
		smith__idrp_online_fulfillment_current_data::kmart_default_online_fulfillment_type_cd_ts AS kmart_default_online_fulfillment_type_cd_ts,
		smith__idrp_online_fulfillment_current_data::kmart_temporary_online_fulfillment_type_cd AS kmart_temporary_online_fulfillment_type_cd,
		smith__idrp_online_fulfillment_current_data::web_exclusive_ind AS web_exclusive_ind,
		'$batchid' AS idrp_batch_id;



---------Joining on item table to add new items in the online_fulfillment table----------------------

join_item_new_data = 
	JOIN new_data_gen 
		BY item_id LEFT OUTER, 
		 smith__idrp_eligible_item_current_req 
		BY item_id;

join_item_new_data_gen = 
	FOREACH join_item_new_data 
	GENERATE 
        new_data_gen::load_ts AS load_ts,
        new_data_gen::item_id AS item_id,
        new_data_gen::ksn_id AS ksn_id,
        new_data_gen::sears_division_nbr AS sears_division_nbr,
        new_data_gen::sears_item_nbr AS sears_item_nbr,
        new_data_gen::sears_sku_nbr AS sears_sku_nbr,
        new_data_gen::web_sku_id AS web_sku_id,
        new_data_gen::upc_nbr AS upc_nbr,
		new_data_gen::sears_default_online_fulfillment_type_cd AS sears_default_online_fulfillment_type_cd,
		'$CURRENT_TIMESTAMP' AS sears_default_online_fulfillment_type_cd_ts,
        (IsNull(smith__idrp_eligible_item_current_req::sears_temporary_online_fulfillment_type_cd,'') != ''
			? smith__idrp_eligible_item_current_req::sears_temporary_online_fulfillment_type_cd 
			: new_data_gen::sears_temporary_online_fulfillment_type_cd) AS sears_temporary_online_fulfillment_type_cd,		
		new_data_gen::kmart_default_online_fulfillment_type_cd AS kmart_default_online_fulfillment_type_cd,
		'$CURRENT_TIMESTAMP' AS kmart_default_online_fulfillment_type_cd_ts,		
        (IsNull(smith__idrp_eligible_item_current_req::kmart_temporary_online_fulfillment_type_cd,'') != ''
			? smith__idrp_eligible_item_current_req::kmart_temporary_online_fulfillment_type_cd 
			: new_data_gen::kmart_temporary_online_fulfillment_type_cd) AS kmart_temporary_online_fulfillment_type_cd,	
		new_data_gen::web_exclusive_ind AS web_exclusive_ind,
		'$batchid' AS idrp_batch_id;

								 
final_output_data = UNION 
	no_change_data_gen,
	changed_data_gen,
	old_data_gen,
	join_item_new_data_gen;



STORE final_output_data INTO 
	'$WORK_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
