/*
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_mips1_smith__idrp_replenishment_store_parameter_format.pig
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
existing_data = LOAD '$SMITH__IDRP_REPLENISHMENT_STORE_PARAMETER_CONVERTED_WORK_LOCATION'
           USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
           AS (
               DEPT_NBR:chararray,
	       CATG_NBR:chararray, 
	       STORE_NBR:chararray,
	       REPLENISHMENT_SYSTEM_CD:chararray
              );

--apply formatting to each field
formatted_data = FOREACH existing_data
                 GENERATE
		      '$CURRENT_TIMESTAMP' AS load_ts,
		      TRIM_STRING(STORE_NBR) AS store_location_nbr,
                      TRIM_STRING(DEPT_NBR) AS shc_division_nbr,
                      TRIM_STRING(CATG_NBR) AS category_nbr,
                      TRIM_STRING(REPLENISHMENT_SYSTEM_CD) AS replenishment_system_cd,
					  '$batchid'
                      ;

--store formatted data
STORE formatted_data
INTO '$SMITH__IDRP_REPLENISHMENT_STORE_PARAMETER_FORMATTED_WORK_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
