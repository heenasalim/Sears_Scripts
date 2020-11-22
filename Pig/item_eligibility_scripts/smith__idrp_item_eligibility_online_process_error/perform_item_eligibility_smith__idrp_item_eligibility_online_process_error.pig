/*
###############################################################################
#<>                           START HEADER DOCUMENT                         <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_item_eligibility_online_process_error.pig
# AUTHOR NAME:         Mudit Mangal
# CREATION DATE:       07-07-2014 06:19
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>
#
#
#
# DEPENDENCIES: None
# RESTARTABLE:  N/A
#
#
# REV LIST:
#        DATE         BY            MODIFICATION
#
#
#
###############################################################################
#<<                 START COMMON HEADER CODE - DO NOT MANUALLY EDIT         >>#
###############################################################################
*/

-- Register the jar containing all PIG UDFs
REGISTER $UDF_JAR;


/*
###############################################################################
#<<                           START CUSTOM HEADER CODE                      >>#
###############################################################################
*/



error_item_current = LOAD '$ERROR_ITEM_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ITEM_ELIGIBILITY_ONLINE_PROCESS_ERROR_SCHEMA);

error_smith_online_error = LOAD '$ERROR_SMITH_ONLINE_ERROR_TEMP_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ITEM_ELIGIBILITY_ONLINE_PROCESS_ERROR_SCHEMA);

error_smith_online_error = FOREACH error_smith_online_error GENERATE

load_ts,
item_id,
ksn_id,
sears_division_nbr,
sears_item_nbr,
sears_sku_nbr,
websku,
package_id,
error_value,
error_desc,
'$batchid' AS idrp_batch_id;

final_error_union = UNION error_smith_online_error, error_item_current;

final_data = DISTINCT final_error_union;

STORE final_data INTO '$SMITH__IDRP_ITEM_ELIGIBILITY_ONLINE_PROCESS_ERROR_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');




/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/