/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibilitysmith__idrp_obu_default_fulfillment_kmart_daily_format.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Fri Feb 14 05:09:36 EST 2014
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

--register the jar containing all PIG UDFs
REGISTER $UDF_JAR;

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

--load existing data
existing_data = 
    LOAD '$SMITH__IDRP_OBU_DEFAULT_FULFILLMENT_KMART_DAILY_INCOMING_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
    AS ( $SMITH__IDRP_OBU_DEFAULT_FULFILLMENT_KMART_DAILY_INCOMING_SCHEMA );

--apply formatting to each field              
formatted_data = 
    FOREACH existing_data
    GENERATE 
        '$CURRENT_TIMESTAMP' AS load_ts,
        TRIM_STRING(ksn_id),
        TRIM_STRING(web_sku_id),
        TRIM_STRING(upc_nbr),
        TRIM_STRING(default_fulfillment_type_cd),
        TRIM_STRING(first_online_ts),
		TRIM_STRING(web_exclusive_ind),
		'$batchid' AS idrp_batch_id ;
               
--store formatted data
STORE formatted_data 
INTO '$SMITH__IDRP_OBU_DEFAULT_FULFILLMENT_KMART_DAILY_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
