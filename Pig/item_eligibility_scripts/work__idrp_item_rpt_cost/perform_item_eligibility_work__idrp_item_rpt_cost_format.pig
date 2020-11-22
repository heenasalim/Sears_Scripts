/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_item_rpt_cost_format.pig
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       Mon Oct 14 05:08:50 EDT 2013
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
existing_data = LOAD '$WORK__IDRP_ITEM_RPT_COST_INCOMING_LOCATION' 
           USING PigStorage('$FIELD_DELIMITER_PIPE')
           AS ( 
                $WORK__IDRP_ITEM_RPT_COST_SCHEMA 
              );

--apply formatting to each field              
formatted_data = FOREACH existing_data
                 GENERATE 
                      TRIM(item_id),
                      TRIM(fisc_wk_end_dt),
                      TRIM(corp_90dy_avg_cost),
                      TRIM(pr_90dy_avg_cost),
                      TRIM(vi_90dy_avg_cost),
                      TRIM(gu_90dy_avg_cost),
                      TRIM(hi_90dy_avg_cost),
                      TRIM(stsd_90dy_avg_cost),
                      TRIM(corp_ptd_avg_cost),
                      TRIM(pr_ptd_avg_cost),
                      TRIM(vi_ptd_avg_cost),
                      TRIM(gu_ptd_avg_cost),
                      TRIM(hi_ptd_avg_cost),
                      TRIM(stsd_ptd_avg_cost)
                 ;
               
--store formatted data
STORE formatted_data 
INTO '$WORK__IDRP_ITEM_RPT_COST_WORK_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/