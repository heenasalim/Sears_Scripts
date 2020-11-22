/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_online_fulfillment_initialization_smith__idrp_online_billable_weight_2.pig
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
#        DATE         BY            MODIFICATION
#
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

REGISTER $UDF_JAR;
SET default_parallel $NUM_PARALLEL

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

----------------------------------------Loads----------------------------------------------------

smith__idrp_eligible_item_current_data = LOAD '$SMITH__IDRP_ELIGIBLE_ITEM_CURRENT_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($SMITH__IDRP_ELIGIBLE_ITEM_CURRENT_SCHEMA);

smith__idrp_online_billable_weight_data_old = LOAD '$SMITH__IDRP_ONLINE_BILLABLE_WEIGHT_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($SMITH__IDRP_ONLINE_BILLABLE_WEIGHT_SCHEMA);

work__idrp_online_billable_weight_data_new = LOAD '$TEMP_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($SMITH__IDRP_ONLINE_BILLABLE_WEIGHT_SCHEMA);
		

-------------------------------------Generating required columns-------------------------------

smith__idrp_eligible_item_new_data_req = 
	FOREACH smith__idrp_eligible_item_current_data 
	GENERATE
        shc_item_id AS item_id,
        temporary_ups_billable_weight_qty AS temporary_ups_billable_weight;


-------Merging previous and current online_billable_weights

merge_data = 
	JOIN smith__idrp_online_billable_weight_data_old 
		BY item_id FULL OUTER, 
		 work__idrp_online_billable_weight_data_new 
		BY item_id;

SPLIT merge_data 
	INTO no_change_data 
		IF (smith__idrp_online_billable_weight_data_old::item_id == work__idrp_online_billable_weight_data_new::item_id AND 
		    smith__idrp_online_billable_weight_data_old::ups_billable_weight == 
			work__idrp_online_billable_weight_data_new::ups_billable_weight), 
		 changed_data 
		IF (smith__idrp_online_billable_weight_data_old::item_id == work__idrp_online_billable_weight_data_new::item_id AND   
		    smith__idrp_online_billable_weight_data_old::ups_billable_weight != 
			work__idrp_online_billable_weight_data_new::ups_billable_weight),
		 old_data 
		IF (IsNull(smith__idrp_online_billable_weight_data_old::item_id,'') != '' AND 
		    IsNull(work__idrp_online_billable_weight_data_new::item_id,'') == ''), 
         new_data 
		IF (IsNull(smith__idrp_online_billable_weight_data_old::item_id,'') == '' AND 
		    IsNull(work__idrp_online_billable_weight_data_new::item_id,'') != '');


no_change_data_gen = 
	FOREACH no_change_data 
	GENERATE 
        smith__idrp_online_billable_weight_data_old::load_ts AS load_ts,
        smith__idrp_online_billable_weight_data_old::item_id AS item_id,
        smith__idrp_online_billable_weight_data_old::ups_billable_weight AS ups_billable_weight,
        smith__idrp_online_billable_weight_data_old::temporary_billable_weight AS temporary_billable_weight,
        smith__idrp_online_billable_weight_data_old::last_change_ts AS last_change_ts,
		'$batchid' AS idrp_batch_id;

changed_data_gen = 
	FOREACH changed_data 
	GENERATE 
        smith__idrp_online_billable_weight_data_old::load_ts AS load_ts,
        smith__idrp_online_billable_weight_data_old::item_id AS item_id,
        work__idrp_online_billable_weight_data_new::ups_billable_weight AS ups_billable_weight,
        smith__idrp_online_billable_weight_data_old::temporary_billable_weight AS temporary_billable_weight,
        '$CURRENT_TIMESTAMP' AS last_change_ts,
		'$batchid' AS idrp_batch_id;

old_data_gen = 
	FOREACH old_data 
	GENERATE 
        smith__idrp_online_billable_weight_data_old::load_ts AS load_ts,
        smith__idrp_online_billable_weight_data_old::item_id AS item_id,
        smith__idrp_online_billable_weight_data_old::ups_billable_weight AS ups_billable_weight,
        smith__idrp_online_billable_weight_data_old::temporary_billable_weight AS temporary_billable_weight,
        smith__idrp_online_billable_weight_data_old::last_change_ts AS last_change_ts,
		'$batchid' AS idrp_batch_id;

new_data_gen = 
	FOREACH new_data 
	GENERATE 
        work__idrp_online_billable_weight_data_new::load_ts AS load_ts,
        work__idrp_online_billable_weight_data_new::item_id AS item_id,
        work__idrp_online_billable_weight_data_new::ups_billable_weight AS ups_billable_weight,
        work__idrp_online_billable_weight_data_new::temporary_billable_weight AS temporary_billable_weight,
        work__idrp_online_billable_weight_data_new::last_change_ts AS last_change_ts,
		'$batchid' AS idrp_batch_id;


-------Joining on item table to add new items in the online_billable_weights 

join_item_new_data = 
	JOIN new_data_gen 
		BY item_id LEFT OUTER, 
		 smith__idrp_eligible_item_new_data_req 
		BY item_id;

join_item_new_data_gen = 
	FOREACH join_item_new_data 
	GENERATE 
        new_data_gen::load_ts AS load_ts,
        new_data_gen::item_id AS item_id,
        new_data_gen::ups_billable_weight AS ups_billable_weight,
        (IsNull(smith__idrp_eligible_item_new_data_req::temporary_ups_billable_weight,'') != '' 
			? smith__idrp_eligible_item_new_data_req::temporary_ups_billable_weight 
			: new_data_gen::temporary_billable_weight ) AS temporary_billable_weight,
        new_data_gen::last_change_ts AS last_change_ts,
		'$batchid' AS idrp_batch_id;
							 

final_output_data = 
	UNION no_change_data_gen,
		  changed_data_gen,
		  old_data_gen,
		  join_item_new_data_gen;

---Store final Output

STORE final_output_data INTO '$WORK_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
