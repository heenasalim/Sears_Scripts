/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_item_rpt_grp_format.pig
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       Mon Oct 14 05:08:43 EDT 2013
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



--load existing data
existing_data = LOAD '$WORK__IDRP_ITEM_RPT_GRP_INCOMING_LOCATION' 
           USING PigStorage('$FIELD_DELIMITER_PIPE')
           AS ( 
                $WORK__IDRP_ITEM_RPT_GRP_SCHEMA 
              );

--apply formatting to each field              
formatted_data = FOREACH existing_data
                 GENERATE 
                      TRIM(item_id),
                      TRIM(rpt_grp_id),
                      TRIM(rpt_grp_seq_nbr),
                      TRIM(creat_ts),
                      TRIM(last_chg_user_id),
                      TRIM(last_chg_ts),
                      itm_rpt_grp_alt_id,
                      TRIM(delt_dt)
                 ;
               
--store formatted data
STORE formatted_data 
INTO '$WORK__IDRP_ITEM_RPT_GRP_WORK_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/