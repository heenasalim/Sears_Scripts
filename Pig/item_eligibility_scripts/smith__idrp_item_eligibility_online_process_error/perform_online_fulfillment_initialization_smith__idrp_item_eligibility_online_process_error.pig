/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_online_fulfillment_initialization_smith__idrp_item_eligibility_online_process_error.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Mon Dec 09 04:02:41 EST 2013
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



/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

#error_item_current = LOAD '$ERROR_ITEM_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ITEM_ELIGIBILITY_ONLINE_PROCESS_ERROR_SCHEMA);

error_item_loc = LOAD '$ERROR_ITEM_LOC_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ITEM_ELIGIBILITY_ONLINE_PROCESS_ERROR_SCHEMA);

error_online_fulfillment = LOAD '$ERROR_ONLINE_FULFILLMENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ITEM_ELIGIBILITY_ONLINE_PROCESS_ERROR_SCHEMA);

error_online_billable_weight = LOAD '$ERROR_ONLINE_BILLABLE_WEIGHT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ITEM_ELIGIBILITY_ONLINE_PROCESS_ERROR_SCHEMA);


final_error_union = UNION error_item_loc,error_online_fulfillment,error_online_billable_weight, error_item_current;

final_data = DISTINCT final_error_union;

STORE final_data INTO '$SMITH__IDRP_ITEM_ELIGIBILITY_ONLINE_PROCESS_ERROR_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
