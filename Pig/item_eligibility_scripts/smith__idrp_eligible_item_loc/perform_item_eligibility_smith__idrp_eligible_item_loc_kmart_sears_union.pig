/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_loc_kmart_sears_union.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Thu Jan 02 00:25:57 EST 2014
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

SET default_parallel $NUM_PARALLEL;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

kmart_data = 
    LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOC_KMART_UNION_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);


sears_data = 
    LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOC_SEARS_IMPORT_CENTER_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);


item_loc_data = 
        UNION kmart_data,
		      sears_data;


STORE item_loc_data 
INTO '$WORK__IDRP_ELIGIBLE_ITEM_LOC_KMART_SEARS_UNION_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
