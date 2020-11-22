/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_markdown_ksn_location_current.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Wed Apr 23 02:51:42 EDT 2014
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

work__idrp_markdown_ksn_location_current_data =
       LOAD '$WORK__DYNAMIC_PRICING_PRCM_INFOREM_SEARS_KMART_DISTINCT_LOCATION'
       USING PigStorage('$FIELD_DELIMITER_TAB')
       AS ($WORK__DYNAMIC_PRICING_PRCM_INFOREM_SEARS_KMART_DISTINCT_SCHEMA);


smith__idrp_markdown_ksn_location_current = 
       FOREACH work__idrp_markdown_ksn_location_current_data 
       GENERATE
               '$CURRENT_TIMESTAMP' AS load_ts,
               (int)ksn_id AS ksn_id,
               (int)store_location_nbr AS store_location_nbr,
               '$batchid' AS idrp_batch_id;

STORE smith__idrp_markdown_ksn_location_current 
INTO '$SMITH__IDRP_MARKDOWN_KSN_LOCATION_CURRENT_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
