/*
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_mips1_smith__idrp_store_source_directive_format.pig
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       19-09-2013 10:36
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
existing_data = LOAD '$SMITH__IDRP_STORE_SOURCE_DIRECTIVE_INCOMING_LOCATION'
           USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
           AS (
                ITEM_ID:chararray,
		DIR_ID:chararray, 
		DTL_NBR:chararray, 
		EFF_TS:chararray, 
		EXPIR_TS:chararray, 
		LAST_CHG_USER_ID:chararray, 
		LOCN_NBR:chararray, 
		MDL_ID:chararray, 
		MDL_PRTY_NBR:chararray, 
		REORD_MTHD_CD:chararray, 
		BEG_DT:chararray,
		STR_SRC_DIR_ALT_ID:chararray,
		TYPE_CD:chararray, 
		END_DT:chararray,
		SRC_ITEM_ID:chararray,  
		VEND_PACK_ID:chararray, 
		LAST_APPR_PROC_ID:chararray
              );

--apply formatting to each field
formatted_data = FOREACH existing_data
                 GENERATE
                     '$CURRENT_TIMESTAMP' AS load_ts,
                     TRIM_STRING(ITEM_ID) AS item_id,
		     DIR_ID AS source_directive_id,
                     DTL_NBR AS source_directive_dtl_nbr,
                     TRIM_STRING(EFF_TS) AS effective_ts,
                     TRIM_STRING(EXPIR_TS) AS expiration_ts,
		     TRIM_STRING(LAST_CHG_USER_ID) AS last_change_user_id,
                     TRIM_STRING(LOCN_NBR) AS location_nbr,
		     TRIM_STRING(MDL_ID) AS model_id,
		     MDL_PRTY_NBR AS model_priority_nbr,
		     TRIM_STRING(REORD_MTHD_CD) AS reorder_method_cd,
		     TRIM_STRING(BEG_DT) AS source_begin_dt,
		     STR_SRC_DIR_ALT_ID AS source_directive_alternate_id,
		     TRIM_STRING(TYPE_CD) AS source_directive_type_cd,
		     TRIM_STRING(END_DT) AS source_end_dt,
         	     TRIM_STRING(SRC_ITEM_ID) AS source_item_id,
		     TRIM_STRING(VEND_PACK_ID) AS vendor_package_id,
		     LAST_APPR_PROC_ID AS last_approved_process_id,
				'$batchid'
                 ;

--store formatted data
STORE formatted_data
INTO '$SMITH__IDRP_STORE_SOURCE_DIRECTIVE_WORK_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
