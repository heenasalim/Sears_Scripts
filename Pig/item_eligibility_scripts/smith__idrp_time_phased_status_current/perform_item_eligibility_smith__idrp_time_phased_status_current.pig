/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_time_phased_status_current.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Wed Jun 18 10:00:48 EDT 2014
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

work__idrp_time_phased_status_current = 
      LOAD '$WORK__IDRP_TIME_PHASED_STATUS_CURRENT_INCOMING_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_PIPE') 
      AS ($WORK__IDRP_TIME_PHASED_STATUS_CURRENT_SCHEMA);


smith__idrp_time_phased_status_current_data = 
     FOREACH work__idrp_time_phased_status_current
     GENERATE
     '$CURRENT_TIMESTAMP' AS load_ts,
     TRIM(shc_item_id) AS shc_item_id,
     TRIM(sears_division_nbr) AS sears_division_nbr,
     TRIM(sears_item_nbr) AS sears_item_nbr,
     TRIM(sears_sku_nbr) AS sears_sku_nbr,
     TRIM(location_id) AS location_id,
     TRIM(sears_location_id) AS sears_location_id,
     TRIM(idrp_status_cd) AS idrp_status_cd,
     TRIM(start_dt) AS start_dt,
     TRIM(end_dt) AS end_dt,
     TRIM(user_id) AS user_id,
     TRIM(idrp_status_source_cd) AS idrp_status_source_cd,
     '$batchid' AS idrp_batch_id;

STORE smith__idrp_time_phased_status_current_data 
INTO '$SMITH__IDRP_TIME_PHASED_STATUS_CURRENT_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
