/*
################################################################################
#       Script Name   : PerformCollectImpactAdPlanPromoDataScoop_Sears_Stat.sql
#       Author        : Reggie Zamanski
#       Date created  : 05/12/2017
#       JIRA #        : IPS-1680  
#       Description   : Collect statistic after the load of
#                       'Offer Manager to Ad Plan' Data Into Temporary Promotional  
#                       table.
#
################################################################################
################################################################################
#      Changed By     :  
#      Changed Date   : xx-xx-2xxx
#      JIRA #         : IPS-xxxx
#
#      Changed - Modified ...
#
################################################################################
*/
/***************************************************************
* CHECK FOR FASTLOAD ERRORS                                    *
***************************************************************/

 SELECT ERRORCODE
       ,ERRORFIELDNAME
       ,COUNT(*)
   FROM $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN_err1
  GROUP BY 1,2
  ORDER BY 1,2;
.IF ERRORCODE = 3807 THEN .GOTO CHECK_ERR2
.IF ERRORCODE =    0 THEN .GOTO ABEND_JOB
.IF ERRORCODE <>   0 THEN .GOTO EXIT ERRORCODE

.LABEL CHECK_ERR2
 SELECT COUNT(*) FROM $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN_err2;
.IF ERRORCODE = 3807 THEN .GOTO GET_STATS
.IF ERRORCODE =    0 THEN .GOTO ABEND_JOB
.IF ERRORCODE <>   0 THEN .GOTO EXIT ERRORCODE

.LABEL ABEND_JOB
 SELECT 'ERROR OCCURRED IN FASTLOAD//ERROR TABLE EXISTS'
        (TITLE 'TASK STATUS');
.EXIT 801

/***************************************************************
* COLLECT STATISTICS                                           *
***************************************************************/

.LABEL GET_STATS

COLLECT STATS $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN
     COLUMN(SRS_DIV_NO, SRS_ITEM_NO);

.IF ERRORCODE  = 0 THEN .EXIT 0; 	 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
.QUIT;
 