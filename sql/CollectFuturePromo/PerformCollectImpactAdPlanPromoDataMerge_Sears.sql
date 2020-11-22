/*
################################################################################
#       Script Name   : PerformCollectImpactAdPlanPromoDataMerge_Sears.sql
#       Author        : Reggie Zamanski
#       Date created  : 07/22/2017
#       JIRA #        : IPS-1680  
#       Description   : Merge Offer Manager Event Information with Sears Ad Plan
#                       data to production Teradata table -     
#                       idrp_work_tbls.DPRPTAX_ADP_XTC_WRK_ALL_PPS.
#
################################################################################
################################################################################
#      Changed By     :  
#      Changed Date   : xx-xx-2xxx
#      JIRA #         : IPS-xxxx
#
#      Changed - 
#
################################################################################
*/
/*******************************************************************************
* DELETE ALL ROWS FROM IDRP_WORK_TBLS.DPRPTAX_ADP_XTC_WRK_ALL_PPS THAT ARE     *
* CURRENTLY LOCATED IN TABLE $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN_SUM.         *
********************************************************************************/

 DELETE FROM IDRP_WORK_TBLS.DPRPTAX_ADP_XTC_WRK_ALL_PPS
    WHERE (MKT_YR_NO, MKT_MTH_NO, WK_NO, DIV_NO, ITM_NO, PLN_LN_NO, SEQ_NO, STA_DT, STP_DT) 
          IN
             (SELECT SUBSTRING(B.MTH_NO FROM 6  FOR 4)    AS MKT_YR_NO,
			         SUBSTRING(B.MTH_NO FROM 10 FOR 2)    AS MKT_MTH_NO,			  
                     B.WOM                                AS WK_NO,	
                     A.SRS_DIV_NO                         AS DIV_NO,					 
                     A.SRS_ITEM_NO                        AS ITM_NO,
                     A.SRS_LN_NO                          AS PLN_LN_NO,
                     '000'                                AS SEQ_NO,   
                     A.OFFER_START_DT                     AS STA_DT,					 
                     A.OFFER_END_DT                       AS STP_DT	   
                FROM $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN_SUM  A
                    ,SPRS_DW_VIEWS.WEEKS_RETAIL                B
                    ,DATAVIEW.CORP_DAY                         C
               WHERE A.OFFER_START_DT     = C.DAY_DT
                 AND B.WK_END_DT          = C.CALNDR_WK_END_DT
               GROUP BY 1,2,3,4,5,6,7,8,9); 
  
.IF ERRORCODE <>   0 THEN .GOTO EXIT ERRORCODE

/***************************************************************************
* DELETE ALL ROWS FROM IDRP_WORK_TBLS.DPRPTAX_ADP_XTC_WRK_ALL_PPS          *
* WHERE THE DIVISON IS ON THE ROLLOUT TABLE AND THE ROLLOUT DATE IS LESS   *
* THAN THE EVENT START DATE.                                               *
****************************************************************************/

 DELETE FROM IDRP_WORK_TBLS.DPRPTAX_ADP_XTC_WRK_ALL_PPS
    WHERE (DIV_NO, STA_DT) 
          IN
             (SELECT A.DIV_NO                  AS DIV_NO,
                     A.STA_DT                  AS STA_DT                   
                FROM IDRP_WORK_TBLS.DPRPTAX_ADP_XTC_WRK_ALL_PPS  A
				    ,(SELECT DIV_NO,
                             MIN(ROLLOUT_DATE) AS ROLLOUT_DT 
						FROM IMPCT_VIEWS.SEARS_ROLLOUT
				       GROUP BY DIV_NO)                          B
               WHERE B.DIV_NO                   = A.DIV_NO
			     AND B.ROLLOUT_DT               < A.STA_DT   
               GROUP BY 1,2); 
  
.IF ERRORCODE <>   0 THEN .GOTO EXIT ERRORCODE
 
 /******************************************************************************
* INSERT ALL ROWS INTO IDRP_WORK_TBLS.DPRPTAX_ADP_XTC_WRK_ALL_PPS THAT ARE     *
* CURRENTLY LOCATED IN TABLE $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN_SUM.         *
********************************************************************************/
 
 INSERT INTO IDRP_WORK_TBLS.DPRPTAX_ADP_XTC_WRK_ALL_PPS

  SELECT MKT_YR_NO, 
         MKT_MTH_NO,
         WK_NO,	
         DIV_NO,		
         PLN_LN_NO,
         SEQ_NO, 
         MED_CD1 || MED_CD2 || MED_CD34 AS MED_CD,
         SBL_NO,
         CLS_NO,
         ITM_NO,
         RNG_NO,
         RNG_TYP_CD,
         PRD_DS,
		 SLS_TYP_CD,
		 EST_SLD_QT,
		 EST_SLD_AM,
		 MED_TYP_CD,	
		 PIC_FL,
		 PRO_PPS_TX,
		 REG_EST_SLD_QT,
		 REG_EST_SLD_AM,
		 CLR_EST_SLD_QT,
		 CLR_EST_SLD_AM,	
		 STA_DT,			
		 STP_DT,
         MSG_STY_TX		
	FROM
        (SELECT SUBSTRING(B.MTH_NO FROM 6  FOR 4)               AS MKT_YR_NO,
			    SUBSTRING(B.MTH_NO FROM 10 FOR 2)               AS MKT_MTH_NO,
                B.WOM                                           AS WK_NO,	
                A.SRS_DIV_NO                                    AS DIV_NO,					          
	            A.SRS_LN_NO                                     AS PLN_LN_NO,
	            '000'                                           AS SEQ_NO, 
                (CASE WHEN B.WOM = 1
                      THEN '1'
                      WHEN B.WOM = 2 
                      THEN '2'
                      WHEN B.WOM = 3 
                      THEN '3' 
                      WHEN B.WOM = 4
                      THEN '4'
                      ELSE '5'
			     END)                                 (char(1)) AS MED_CD1,
		        (CASE WHEN (A.CHANNEL_DESC      LIKE 'Unadvertise%'  
				   OR       A.CHANNEL_TYPE_DESC LIKE 'Unadvertise%')
		              THEN 'U'
		        	  ELSE 'A'                              
		          END)                                          AS MED_CD2,
		       
	            (CASE WHEN D.RANK_NBR = '1'   THEN '1A'
                      WHEN D.RANK_NBR = '2'   THEN '1B'
                      WHEN D.RANK_NBR = '3'   THEN '1C'
                      WHEN D.RANK_NBR = '4'   THEN '1D'
                      WHEN D.RANK_NBR = '5'   THEN '1E'
                      WHEN D.RANK_NBR = '6'   THEN '1F'
                      WHEN D.RANK_NBR = '7'   THEN '1G'
                      WHEN D.RANK_NBR = '8'   THEN '1H'
                      WHEN D.RANK_NBR = '9'   THEN '1I'
                      WHEN D.RANK_NBR = '10'  THEN '1J'
                      WHEN D.RANK_NBR = '11'  THEN '1K'
                      WHEN D.RANK_NBR = '12'  THEN '1L'
                      WHEN D.RANK_NBR = '13'  THEN '1M'
                      WHEN D.RANK_NBR = '14'  THEN '1N'
                      WHEN D.RANK_NBR = '15'  THEN '1O'
                      WHEN D.RANK_NBR = '16'  THEN '1P'
                      WHEN D.RANK_NBR = '17'  THEN '1Q'
                      WHEN D.RANK_NBR = '18'  THEN '1R'
                      WHEN D.RANK_NBR = '19'  THEN '1S'
                      WHEN D.RANK_NBR = '20'  THEN '1T'
                      WHEN D.RANK_NBR = '21'  THEN '1U'
                      WHEN D.RANK_NBR = '22'  THEN '1V'
                      WHEN D.RANK_NBR = '23'  THEN '1W'
                      WHEN D.RANK_NBR = '24'  THEN '1X'
                      WHEN D.RANK_NBR = '25'  THEN '1Y'
                      WHEN D.RANK_NBR = '26'  THEN '1Z'
				      WHEN D.RANK_NBR = '27'  THEN '2A'
                      WHEN D.RANK_NBR = '28'  THEN '2B'
                      WHEN D.RANK_NBR = '29'  THEN '2C'
                      WHEN D.RANK_NBR = '30'  THEN '2D'
                      WHEN D.RANK_NBR = '31'  THEN '2E'
                      WHEN D.RANK_NBR = '32'  THEN '2F'
                      WHEN D.RANK_NBR = '33'  THEN '2G'
                      WHEN D.RANK_NBR = '34'  THEN '2H'
                      WHEN D.RANK_NBR = '35'  THEN '2I'
                      WHEN D.RANK_NBR = '36'  THEN '2J'
                      WHEN D.RANK_NBR = '37'  THEN '2K'
                      WHEN D.RANK_NBR = '38'  THEN '2L'
                      WHEN D.RANK_NBR = '39'  THEN '2M'
                      WHEN D.RANK_NBR = '40'  THEN '2N'
                      WHEN D.RANK_NBR = '41'  THEN '2O'
                      WHEN D.RANK_NBR = '42'  THEN '2P'
                      WHEN D.RANK_NBR = '43'  THEN '2Q'
                      WHEN D.RANK_NBR = '44'  THEN '2R'
                      WHEN D.RANK_NBR = '45'  THEN '2S'
                      WHEN D.RANK_NBR = '46'  THEN '2T'
                      WHEN D.RANK_NBR = '47'  THEN '2U'
                      WHEN D.RANK_NBR = '48'  THEN '2V'
                      WHEN D.RANK_NBR = '49'  THEN '2W'
                      WHEN D.RANK_NBR = '50'  THEN '2X'
                      WHEN D.RANK_NBR = '51'  THEN '2Y'
                      WHEN D.RANK_NBR = '52'  THEN '2Z'
					  WHEN D.RANK_NBR = '53'  THEN '3A'
                      WHEN D.RANK_NBR = '54'  THEN '3B'
                      WHEN D.RANK_NBR = '55'  THEN '3C'
                      WHEN D.RANK_NBR = '56'  THEN '3D'
                      WHEN D.RANK_NBR = '57'  THEN '3E'
                      WHEN D.RANK_NBR = '58'  THEN '3F'
                      WHEN D.RANK_NBR = '59'  THEN '3G'
                      WHEN D.RANK_NBR = '60'  THEN '3H'
                      WHEN D.RANK_NBR = '61'  THEN '3I'
                      WHEN D.RANK_NBR = '62'  THEN '3J'
                      WHEN D.RANK_NBR = '63'  THEN '3K'
                      WHEN D.RANK_NBR = '64'  THEN '3L'
                      WHEN D.RANK_NBR = '65'  THEN '3M'
                      WHEN D.RANK_NBR = '66'  THEN '3N'
                      WHEN D.RANK_NBR = '67'  THEN '3O'
                      WHEN D.RANK_NBR = '68'  THEN '3P'
                      WHEN D.RANK_NBR = '69'  THEN '3Q'
                      WHEN D.RANK_NBR = '70'  THEN '3R'
                      WHEN D.RANK_NBR = '71'  THEN '3S'
                      WHEN D.RANK_NBR = '72'  THEN '3T'
                      WHEN D.RANK_NBR = '73'  THEN '3U'
                      WHEN D.RANK_NBR = '74'  THEN '3V'
                      WHEN D.RANK_NBR = '75'  THEN '3W'
                      WHEN D.RANK_NBR = '76'  THEN '3X'
                      WHEN D.RANK_NBR = '77'  THEN '3Y'
                      WHEN D.RANK_NBR = '78'  THEN '3Z'
					  WHEN D.RANK_NBR = '79'  THEN '4A'
                      WHEN D.RANK_NBR = '80'  THEN '4B'
                      WHEN D.RANK_NBR = '81'  THEN '4C'
                      WHEN D.RANK_NBR = '82'  THEN '4D'
                      WHEN D.RANK_NBR = '83'  THEN '4E'
                      WHEN D.RANK_NBR = '84'  THEN '4F'
                      WHEN D.RANK_NBR = '85'  THEN '4G'
                      WHEN D.RANK_NBR = '86'  THEN '4H'
                      WHEN D.RANK_NBR = '87'  THEN '4I'
                      WHEN D.RANK_NBR = '88'  THEN '4J'
                      WHEN D.RANK_NBR = '89'  THEN '4K'
                      WHEN D.RANK_NBR = '90'  THEN '4L'
                      WHEN D.RANK_NBR = '91'  THEN '4M'
                      WHEN D.RANK_NBR = '92'  THEN '4N'
                      WHEN D.RANK_NBR = '93'  THEN '4O'
                      WHEN D.RANK_NBR = '94'  THEN '4P'
                      WHEN D.RANK_NBR = '95'  THEN '4Q'
                      WHEN D.RANK_NBR = '96'  THEN '4R'
                      WHEN D.RANK_NBR = '97'  THEN '4S'
                      WHEN D.RANK_NBR = '98'  THEN '4T'
                      WHEN D.RANK_NBR = '99'  THEN '4U'
                      WHEN D.RANK_NBR = '100' THEN '4V'
                      WHEN D.RANK_NBR = '101' THEN '4W'
                      WHEN D.RANK_NBR = '102' THEN '4X'
                      WHEN D.RANK_NBR = '103' THEN '4Y'
                      WHEN D.RANK_NBR = '104' THEN '4Z'
					                          ELSE '5A'
		           END)                                          AS MED_CD34, 		 
		         A.SRS_SLN_NO                                    AS SBL_NO,
		         A.SRS_CLS_NO                                    AS CLS_NO,
		         A.SRS_ITEM_NO                                   AS ITM_NO,
		         A.RNG_NO                                        AS RNG_NO,
		         ' '                                             AS RNG_TYP_CD,
		         ' '                                             AS PRD_DS,
		        (CASE WHEN (A.CHANNEL_DESC LIKE 'Unadvertise%'  OR 
		                    A.CHANNEL_TYPE_DESC LIKE 'Unadvertise%')
		              THEN 'UNADV'
		              ELSE 'ADV  '                              
		          END)                                          AS SLS_TYP_CD,
                 A.MERCHANT_UPLIFT_QTY                          AS EST_SLD_QT,
                 '0'                                            AS EST_SLD_AM,	
                (CASE WHEN (A.CHANNEL_DESC LIKE 'Unadvertise%'  OR 
		                    A.CHANNEL_TYPE_DESC LIKE 'Unadvertise%')
		        	   THEN 'UNADV'
		        	   ELSE 'ADV  '                              
		           END)                                         AS MED_TYP_CD,	
                  ' '                                           AS PIC_FL,
                  (CASE WHEN A.DEAL_TYPE_ID = 1 
                        THEN '$0.00off'
                        WHEN A.DEAL_TYPE_ID = 2
                        THEN PROMO_PRC_AMT
                        WHEN A.DEAL_TYPE_ID = 3
                        THEN DEAL_VALUE_AMT||'%off'
                        WHEN A.DEAL_TYPE_ID = 4
                        THEN '$'||DEAL_VALUE_AMT||'off'
                        WHEN A.DEAL_TYPE_ID = 5
                        THEN 'Buy'||DEAL_QTY||'for'||DEAL_VALUE_AMT
                        ELSE PROMO_PRC_AMT
                    END)                 
                                                                AS PRO_PPS_TX,                 
                  A.MARGIN_PROJECTED_SALES_QTY                  AS REG_EST_SLD_QT,
                  '0'                                           AS REG_EST_SLD_AM,
                  '0'                                           AS CLR_EST_SLD_QT,
                  '0'                                           AS CLR_EST_SLD_AM,		 
                  A.OFFER_START_DT                              AS STA_DT,					 
                  A.OFFER_END_DT                                AS STP_DT,
                  ' '                                           AS MSG_STY_TX		
		           
           FROM $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN_SUM        A
               ,SPRS_DW_VIEWS.WEEKS_RETAIL                      B
               ,DATAVIEW.CORP_DAY                               C
               ,(SELECT ACTIVITY_ID
                       ,OFFER_START_DT
                       ,CSUM( 1
                       ,ACTIVITY_ID 
                       ,OFFER_START_DT)        AS RANK_NBR
				   FROM
				   (SELECT ACTIVITY_ID
                          ,MIN(OFFER_START_DT) AS OFFER_START_DT
                   FROM $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN_SUM
                   GROUP BY 1)                                  Z 
                                )                               D  
          WHERE A.OFFER_START_DT = C.DAY_DT
            AND B.WK_END_DT      = C.CALNDR_WK_END_DT
            AND A.ACTIVITY_ID    = D.ACTIVITY_ID)               E
     ;
  
.IF ERRORCODE <>   0 THEN .GOTO EXIT ERRORCODE

 
/***************************************************************
*                     COLLECT STATISTICS                       *
***************************************************************/

.LABEL GET_STATS

COLLECT STATS IDRP_WORK_TBLS.DPRPTAX_ADP_XTC_WRK_ALL_PPS
     INDEX (MKT_YR_NO, MKT_MTH_NO, WK_NO, DIV_NO, PLN_LN_NO, SEQ_NO);


.IF ERRORCODE  = 0 THEN .EXIT 0; 	 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
.QUIT;
 