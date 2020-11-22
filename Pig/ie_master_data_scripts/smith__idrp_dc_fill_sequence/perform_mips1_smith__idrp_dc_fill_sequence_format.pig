/*
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_mips1_smith__idrp_dc_fill_sequence_format.pig
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       19-09-2013 10:15
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

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

--load existing data
existing_data = LOAD '$SMITH__IDRP_DC_FILL_SEQUENCE_INCOMING_LOCATION'
           USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
           AS ( 
                ITEM_ID:chararray,
		SEQ_NBR:chararray,
		LOCN_NBR:chararray,
		KSN_PACK_ID:chararray
              );

--apply formatting to each field
formatted_data = FOREACH existing_data
                 GENERATE
		      '$CURRENT_TIMESTAMP' AS load_ts,
                      TRIM(ITEM_ID) AS item_id,
                      TRIM(SEQ_NBR) AS store_order_fill_sequence,
                      TRIM(LOCN_NBR) AS dc_location_nbr,
                      TRIM(KSN_PACK_ID) AS ksn_package_id,
					  '$batchid'
                 ;

--store formatted data
STORE formatted_data
INTO '$SMITH__IDRP_DC_FILL_SEQUENCE_WORK_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
