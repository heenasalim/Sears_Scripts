/*
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_build_master_smith__idrp_item_eligibility_batchdate.pig
# AUTHOR NAME:         Nava Jyothi Samudrala
# CREATION DATE:       11-06-2014 10:38
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

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

----------------Load the table for smith__idrp_batchdate---------------------------------	
				
smith__idrp_batchdate = 
		LOAD '$SMITH__IDRP_BATCH_DATE_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
			AS ($SMITH__IDRP_BATCH_DATE_SCHEMA);
		
--apply formatting to each field
smith__idrp_item_eligibility_batchdate = FOREACH smith__idrp_batchdate
                 GENERATE
		      	'$CURRENT_TIMESTAMP' AS load_ts,
                	AddOrRemoveDaysToDate(batch_dt, 1) AS batch_dt,
                   	CONCAT( FormatDate('$CURRENT_TIMESTAMP'), ' 05:00:00') AS processing_ts,
					'$batchid';

--store formatted data
STORE smith__idrp_item_eligibility_batchdate
INTO '$SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_WORK_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
