/*
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_mips1_smith__idrp_item_dc_fill_preference_format.pig
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       19-09-2013 10:37
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>
#
#
#
# DEPENDENCIES: <<TODO>>
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
existing_data = LOAD '$SMITH__IDRP_ITEM_DC_FILL_PREFERENCE_INCOMING_LOCATION'
           USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
           AS (
                ITEM_ID:chararray, 
		DC_LOCN_NBR:chararray, 
		BEG_DT:chararray, 
		END_DT:chararray, 
		EFF_TS:chararray, 
		EXPIR_TS:chararray, 
		KSN_ID:chararray, 
		LAST_CHG_USER_ID:chararray
              );

--apply formatting to each field
formatted_data = FOREACH existing_data
                 GENERATE
		      '$CURRENT_TIMESTAMP' AS load_ts,
                      TRIM_STRING(ITEM_ID) AS item_id,
                      TRIM_STRING(DC_LOCN_NBR) AS dc_location_nbr,
                      TRIM_STRING(BEG_DT) AS store_order_fill_preference_begin_dt,
                      TRIM_STRING(END_DT) AS store_order_fill_preference_end_dt,
                      TRIM_STRING(EFF_TS) AS effective_ts,
                      TRIM_STRING(EXPIR_TS) AS expiration_ts,
                      TRIM_STRING(KSN_ID) AS ksn_id,
                      TRIM_STRING(LAST_CHG_USER_ID) AS last_change_user_id,
					  '$batchid'
                 ;

--store formatted data
STORE formatted_data
INTO '$SMITH__IDRP_ITEM_DC_FILL_PREFERENCE_WORK_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
