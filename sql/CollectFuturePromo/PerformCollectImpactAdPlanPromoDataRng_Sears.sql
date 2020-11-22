/*
################################################################################
#     Script Name   : PerformCollectImpactAdPlanPromoDataRng_Sears.sql
#     Author        : Reggie Zamanski
#     Date created  : 08/12/2017
#     JIRA #        : IPS-1680  
#     Description   : This script removes lower order Product_Group_type_Cd  
#                     rows for any Div/Item/Location/Start_Dt/End_DT, and sum's 
#                     quantities. 
#
################################################################################
################################################################################
#    Changed By     :  
#    Changed Date   : xx-xx-2xxx
#    JIRA #         : IPS-xxxx
#
#    Changed - 
#
################################################################################
*/

/***************************************************************************
* DROP TABLE IDRP_OFFERMGR_ADPLAN_RNG_WRK, TABLE AND RECREATE IT FOR       *
* CURRENT PROCESSING.                                                      * 
****************************************************************************/

 DROP TABLE IDRP_WORK_TBLS.IDRP_OFFERMGR_ADPLAN_RNG_WRK;

 CREATE SET TABLE IDRP_WORK_TBLS.IDRP_OFFERMGR_ADPLAN_RNG_WRK,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      MKT_YR_NO         SMALLINT NOT NULL,
      MKT_MTH_NO        SMALLINT NOT NULL,
      WK_NO             INTEGER NOT NULL,
      DIV_NO            SMALLINT NOT NULL,
      PLN_LN_NO         SMALLINT NOT NULL,
      RNG_NO            CHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      RNG_TYP_CD        CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      PRD_NO            CHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      PRO_TYP_CD        CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      PRC_AM            DECIMAL(7,2),
      PRC_BRK_QT        SMALLINT,
      PRC_DNT_VAL_AM    DECIMAL(7,2),
      PRC_DNT_VAL_PC    DECIMAL(5,2),
      EST_SLD_QT        INTEGER NOT NULL,
      REG_EST_SLD_QT    INTEGER NOT NULL,
      MASTER_OFFER_ID   DECIMAL(11,0),	
      PRODUCT_GROUP_ID  DECIMAL(11,0))
PRIMARY INDEX (MKT_YR_NO, MKT_MTH_NO, WK_NO, DIV_NO, PLN_LN_NO, PRD_NO);

/****************************************************************************
* INSERT SUMMED ROWS INTO DPRP_WORK_TBLS.IDRP_OFFERMANAGER_ADPLAN_SUM       *
* THAT ARE CURRENTLY LOCATED IN TABLE $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN  *
****************************************************************************/
 
 INSERT INTO IDRP_WORK_TBLS.IDRP_OFFERMGR_ADPLAN_RNG_WRK
 
 SELECT SUBSTRING(B.MTH_NO FROM 6  FOR 4)               AS MKT_YR_NO
       ,SUBSTRING(B.MTH_NO FROM 10 FOR 2)               AS MKT_MTH_NO
       ,B.WOM                                           AS WK_NO	
       ,A.SRS_DIV_NO                                    AS DIV_NO					          
	   ,A.SRS_LN_NO                                     AS PLN_LN_NO 
       ,CAST(E.RNG_NO AS CHAR(5))                       AS RNG_NO 
	   ,'O'                                             AS RNG_TYP_CD
	   ,A.SRS_ITEM_NO                                   AS PRD_NO
       ,(CASE WHEN A.DEAL_TYPE_ID = 2
              THEN 'DA' 
              WHEN A.DEAL_TYPE_ID = 3
              THEN 'PO'
              WHEN A.DEAL_TYPE_ID = 4
              THEN 'DO'
              ELSE ''
          END)                                          AS PRO_TYP_CD
       ,(CASE WHEN A.DEAL_TYPE_ID = 2
              THEN PROMO_PRC_AMT 
              ELSE ''
          END)                                          AS PRC_AM 
	   ,0                                               AS PRC_BRK_QT
	   ,(CASE WHEN A.DEAL_TYPE_ID = 4
              THEN DEAL_VALUE_AMT 
              ELSE ''
          END)                                          AS PRC_DNT_VAL_AM 
       ,(CASE WHEN A.DEAL_TYPE_ID = 3
              THEN DEAL_VALUE_AMT 
              ELSE ''
          END)                                          AS PRC_DNT_VAL_PC
	    
	   ,MAX(MERCHANT_UPLIFT_QTY)                        AS EST_SLD_QT
	   ,MAX(MARGIN_PROJECTED_SALES_QTY)                 AS REG_EST_SLD_QT 	 
	   ,A.MASTER_OFFER_ID                               AS MASTER_OFFER_ID	
       ,A.PRODUCT_GROUP_ID                              AS PRODUCT_GROUP_ID
  FROM $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN                      A
              ,SPRS_DW_VIEWS.WEEKS_RETAIL                        B
              ,DATAVIEW.CORP_DAY                                 C
              ,(SELECT MASTER_OFFER_ID              
                      ,PRODUCT_GROUP_ID
                      ,CSUM( 1
                      ,MASTER_OFFER_ID              
                      ,PRODUCT_GROUP_ID)                AS RNG_NO  
                  FROM (SELECT MASTER_OFFER_ID
                              ,PRODUCT_GROUP_ID				  
                  FROM $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN
				 WHERE PRODUCT_GROUP_TYPE_CD IN ('PQRY', 'SLST')
				 GROUP BY 1,2)                                   D
                              )                                  E				 
 WHERE A.OFFER_START_DT         = C.DAY_DT
   AND B.WK_END_DT              = C.CALNDR_WK_END_DT
   AND A.MASTER_OFFER_ID        = E.MASTER_OFFER_ID           
   AND A.PRODUCT_GROUP_ID       = E.PRODUCT_GROUP_ID
   AND A.PRODUCT_GROUP_TYPE_CD IN ('PQRY', 'SLST')
  
   AND (A.SRS_DIV_NO, A.SRS_LN_NO, A.SRS_SLN_NO, A.SRS_CLS_NO, A.SRS_ITEM_NO, 
        A.OFFER_START_DT, A.OFFER_END_DT, A.PRODUCT_GROUP_TYPE_CD) 
          IN
               (SELECT SRS_DIV_NO   
                      ,SRS_LN_NO 
                      ,SRS_SLN_NO
                      ,SRS_CLS_NO
                      ,SRS_ITEM_NO
                      ,OFFER_START_DT              
                      ,OFFER_END_DT	
                      ,(CASE WHEN PRODUCT_RANK = 1
                             THEN 'DVIT'
                             WHEN PRODUCT_RANK = 2
                             THEN 'SLST'	
                             WHEN PRODUCT_RANK = 3
                             THEN 'PQRY'
                             WHEN PRODUCT_RANK = 4
                             THEN 'SCLS'		
                             WHEN PRODUCT_RANK = 5
                             THEN 'SSLN'
                             WHEN PRODUCT_RANK = 6
                             THEN 'SLIN'
                             ELSE '    '
                         END)                                      AS PRODUCT_GROUP_TYPE_CD
                  FROM	 
    	              (SELECT SRS_DIV_NO 
      	                     ,SRS_LN_NO 
                             ,SRS_SLN_NO
                             ,SRS_CLS_NO
                             ,SRS_ITEM_NO
                             ,OFFER_START_DT              
                             ,OFFER_END_DT	
                             ,MIN(CASE WHEN PRODUCT_GROUP_TYPE_CD = 'DVIT'
                                       THEN 1
                                       WHEN PRODUCT_GROUP_TYPE_CD = 'SLST'
                                       THEN 2		
                                       WHEN PRODUCT_GROUP_TYPE_CD = 'PQRY'
                                       THEN 3
                                       WHEN PRODUCT_GROUP_TYPE_CD = 'SCLS'
                                       THEN 4		
                                       WHEN PRODUCT_GROUP_TYPE_CD = 'SSLN'
                                       THEN 5
                                       WHEN PRODUCT_GROUP_TYPE_CD = 'SLIN'
                                       THEN 6
                                       ELSE 7
                                   END)                            AS PRODUCT_RANK
                         FROM	$TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN
                        GROUP BY 1,2,3,4,5,6,7)	                 F                     
    	       GROUP BY 1,2,3,4,5,6,7,8)                          
   
   
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,16,17;
  
.IF ERRORCODE <>   0 THEN .GOTO EXIT ERRORCODE

 
/***************************************************************
*                     COLLECT STATISTICS                       *
***************************************************************/

.LABEL GET_STATS

COLLECT STATS IDRP_WORK_TBLS.IDRP_OFFERMGR_ADPLAN_RNG_WRK
     INDEX (MKT_YR_NO, MKT_MTH_NO, WK_NO, DIV_NO, PLN_LN_NO, PRD_NO);


.IF ERRORCODE  = 0 THEN .EXIT 0; 	 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
.QUIT;
 		   