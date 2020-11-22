/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_smith__idrp_scan_based_trading_sears_location_current.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Thu Jul 31 02:41:03 EDT 2014
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

smith__idrp_scan_based_trading_sears_location_current_incoming = 
     LOAD '$SMITH__IDRP_SCAN_BASED_TRADING_SEARS_LOCATION_CURRENT_INCOMING_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($SMITH__IDRP_SCAN_BASED_TRADING_SEARS_LOCATION_CURRENT_INCOMING_SCHEMA);


smith__idrp_scan_based_trading_sears_location_current = 
     FOREACH smith__idrp_scan_based_trading_sears_location_current_incoming
     GENERATE 
             '$CURRENT_TIMESTAMP' AS load_ts,
             sears_division_nbr AS sears_division_nbr,
             sears_item_nbr AS sears_item_nbr,
             sears_store_nbr AS sears_store_nbr,
             sears_scan_based_trading_duns_nbr AS sears_scan_based_trading_duns_nbr,
             data_center_filled_ind AS data_center_filled_ind; 


STORE smith__idrp_scan_based_trading_sears_location_current 
INTO '$SMITH__IDRP_SCAN_BASED_TRADING_SEARS_LOCATION_CURRENT_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
