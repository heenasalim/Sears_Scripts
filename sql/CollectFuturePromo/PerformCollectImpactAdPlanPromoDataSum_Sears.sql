/*
################################################################################
#     Script Name   : PerformCollectImpactAdPlanPromoDataSum_Sears.sql
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
* DROP TABLE IDRP_OFFERMANAGER_ADPLAN_SUM TABLE AND RECREATE IT FOR        *
* CURRENT PROCESSING.                                                      * 
****************************************************************************/

 DROP TABLE $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN_SUM;
 
 CREATE SET TABLE $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN_SUM, 
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (SRS_DIV_NO                   CHAR(3)   CHARACTER SET LATIN NOT CASESPECIFIC,
      SRS_ITEM_NO                  CHAR(5)   CHARACTER SET LATIN NOT CASESPECIFIC,
      SRS_LN_NO                    CHAR(2)   CHARACTER SET LATIN NOT CASESPECIFIC,
      SRS_SLN_NO                   CHAR(2)   CHARACTER SET LATIN NOT CASESPECIFIC,
      SRS_CLS_NO                   CHAR(3)   CHARACTER SET LATIN NOT CASESPECIFIC,
      OFFER_START_DT               DATE FORMAT 'YYYY-MM-DD' NOT NULL,
      OFFER_END_DT                 DATE FORMAT 'YYYY-MM-DD' NOT NULL,
      CHANNEL_DESC                 CHAR(50)  CHARACTER SET LATIN NOT CASESPECIFIC,
      CHANNEL_TYPE_DESC            CHAR(50)  CHARACTER SET LATIN NOT CASESPECIFIC,
      PRODUCT_GROUP_TYPE_CD        CHAR(4)   CHARACTER SET LATIN NOT CASESPECIFIC,
      MERCHANT_UPLIFT_QTY          DECIMAL(11,0),  
      MARGIN_PROJECTED_SALES_QTY   DECIMAL(11,0), 
      ACTIVITY_ID                  DECIMAL(11,0), 
      PROMO_PRC_AMT                DECIMAL(11,2),   
      DEAL_TYPE_ID                 DECIMAL(3,0),  
      DEAL_VALUE_AMT               DECIMAL(11,2), 
      DEAL_QTY                     DECIMAL(5,0),
      FMT_ID                       CHAR(1)   CHARACTER SET LATIN NOT CASESPECIFIC,
      RNG_NO                       CHAR(5)   CHARACTER SET LATIN NOT CASESPECIFIC)
 PRIMARY INDEX (SRS_DIV_NO, SRS_ITEM_NO);
 
 .IF ERRORCODE <>   0 THEN .GOTO EXIT ERRORCODE
 
/****************************************************************************
* INSERT SUMMED ROWS INTO DPRP_WORK_TBLS.IDRP_OFFERMANAGER_ADPLAN_SUM       *
* THAT ARE CURRENTLY LOCATED IN TABLE $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN  *
****************************************************************************/
 
 INSERT INTO $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN_SUM

 SELECT E.SRS_DIV_NO                  
       ,(CASE WHEN D.PRODUCT_GROUP_TYPE_CD IN ('DVIT', 'PQRY', 'SLST')
              THEN E.SRS_ITEM_NO
              ELSE NULL
          END)                                                   AS SRS_ITEM_NO                 
       ,E.SRS_LN_NO                   
       ,(CASE WHEN D.PRODUCT_GROUP_TYPE_CD IN ('DVIT', 'SLST', 'PQRY', 'SCLS', 'SSLN')
              THEN E.SRS_SLN_NO
              ELSE NULL
          END)                                                   AS SRS_SLN_NO  
       ,(CASE WHEN D.PRODUCT_GROUP_TYPE_CD IN ('DVIT', 'SLST', 'PQRY', 'SCLS')                
              THEN E.SRS_CLS_NO
              ELSE NULL
          END)                                                   AS SRS_CLS_NO                   
       ,E.OFFER_START_DT              
       ,E.OFFER_END_DT 
       ,MIN(D.CHANNEL_DESC)                                      AS CHANNEL_DESC            
       ,MIN(D.CHANNEL_TYPE_DESC)                                 AS CHANNEL_TYPE_DESC
       ,D.PRODUCT_GROUP_TYPE_CD  
       ,MAX(CASE WHEN D.PRODUCT_GROUP_TYPE_CD = 'DVIT'
              THEN F.MERCHANT_UPLIFT_QTY
              ELSE G.MERCHANT_UPLIFT_QTY
          END)                                                   AS MERCHANT_UPLIFT_QTY        
       ,MAX(CASE WHEN D.PRODUCT_GROUP_TYPE_CD = 'DVIT'
              THEN F.MARGIN_PROJECTED_SALES_QTY
              ELSE G.MARGIN_PROJECTED_SALES_QTY
          END)                                                   AS MARGIN_PROJECTED_SALES_QTY
       ,MIN(D.ACTIVITY_ID)                                       AS ACTIVITY_ID
       ,MIN(D.PROMO_PRC_AMT)                                     AS PROMO_PRC_AMT                   
       ,MIN(D.DEAL_TYPE_ID)                                      AS DEAL_TYPE_ID                  
       ,MIN(E.DEAL_VALUE_AMT)                                    AS DEAL_VALUE_AMT                 
       ,MIN(E.DEAL_QTY)                                          AS DEAL_QTY
       ,E.FMT_ID  
       ,(CASE WHEN E.PRODUCT_GROUP_TYPE_CD IN ('PQRY', 'SLST')
             THEN J.RNG_NO
             ELSE '     '                                        
          END)                                                   AS RNG_NO 
 FROM $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN                        E
   LEFT OUTER JOIN	
      (SELECT MASTER_OFFER_ID                               	
             ,PRODUCT_GROUP_ID
             ,RNG_NO
         FROM IDRP_WORK_TBLS.IDRP_OFFERMGR_ADPLAN_RNG_WRK
        GROUP BY 1,2,3)                                           J
      ON E.MASTER_OFFER_ID                    = J.MASTER_OFFER_ID 
     AND E.PRODUCT_GROUP_ID                   = J.PRODUCT_GROUP_ID 
     ,(SELECT A.SRS_DIV_NO                  
             ,A.SRS_ITEM_NO                 
             ,A.SRS_LN_NO                   
             ,A.SRS_SLN_NO                  
             ,A.SRS_CLS_NO                  
             ,A.OFFER_START_DT              
             ,A.OFFER_END_DT     
             ,C.PRODUCT_GROUP_TYPE_CD
             ,MIN(A.CHANNEL_DESC)                                AS CHANNEL_DESC            
             ,MIN(A.CHANNEL_TYPE_DESC)                           AS CHANNEL_TYPE_DESC
             ,MIN(A.PROMO_PRC_AMT)                               AS PROMO_PRC_AMT
             ,MIN(A.ACTIVITY_ID)                                 AS ACTIVITY_ID
             ,MIN(A.DEAL_TYPE_ID)                                AS DEAL_TYPE_ID                        
        FROM $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN                 A        
            ,(SELECT SRS_DIV_NO   
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
                        GROUP BY 1,2,3,4,5,6,7)	                  B	   
                        
    	       GROUP BY 1,2,3,4,5,6,7,8)                          C
    	       
      WHERE A.SRS_DIV_NO                = C.SRS_DIV_NO  
        AND A.SRS_LN_NO                 = C.SRS_LN_NO         
        AND A.SRS_SLN_NO                = C.SRS_SLN_NO      
        AND A.SRS_CLS_NO                = C.SRS_CLS_NO 
    	AND	A.SRS_ITEM_NO               = C.SRS_ITEM_NO                    
    	AND	A.OFFER_START_DT            = C.OFFER_START_DT                 
    	AND	A.OFFER_END_DT              = C.OFFER_END_DT	                 
    	AND	A.PRODUCT_GROUP_TYPE_CD     = C.PRODUCT_GROUP_TYPE_CD
      GROUP BY 1,2,3,4,5,6,7,8)                                   D
  LEFT OUTER JOIN  
      (SELECT SRS_DIV_NO                  
             ,SRS_ITEM_NO                 
             ,OFFER_START_DT              
             ,OFFER_END_DT 
             ,SUM(MERCHANT_UPLIFT_QTY)                           AS MERCHANT_UPLIFT_QTY	 
    	     ,SUM(MARGIN_PROJECTED_SALES_QTY)                    AS MARGIN_PROJECTED_SALES_QTY 
        FROM $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN
       WHERE PRODUCT_GROUP_TYPE_CD      = 'DVIT' 
       GROUP BY 1,2,3,4)                                          F
      ON D.SRS_DIV_NO                   = F.SRS_DIV_NO                                    		
     AND D.SRS_ITEM_NO                  = F.SRS_ITEM_NO                    
     AND D.OFFER_START_DT               = F.OFFER_START_DT                 
     AND D.OFFER_END_DT                 = F.OFFER_END_DT
   LEFT OUTER JOIN	
      (SELECT SRS_DIV_NO  
             ,SRS_LN_NO 
             ,SRS_SLN_NO
             ,SRS_CLS_NO
             ,SRS_ITEM_NO                 
             ,OFFER_START_DT              
             ,OFFER_END_DT 
             ,MAX(MERCHANT_UPLIFT_QTY)                           AS MERCHANT_UPLIFT_QTY	 
    	     ,MAX(MARGIN_PROJECTED_SALES_QTY)                    AS MARGIN_PROJECTED_SALES_QTY 
        FROM $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN 
       WHERE PRODUCT_GROUP_TYPE_CD NOT  = 'DVIT'
       GROUP BY 1,2,3,4,5,6,7)                                    G   
      ON D.SRS_DIV_NO                   = G.SRS_DIV_NO 
     AND D.SRS_LN_NO                    = G.SRS_LN_NO         
     AND D.SRS_SLN_NO                   = G.SRS_SLN_NO      
     AND D.SRS_CLS_NO                   = G.SRS_CLS_NO 
     AND D.SRS_ITEM_NO                  = G.SRS_ITEM_NO                    
     AND D.OFFER_START_DT               = G.OFFER_START_DT         
     AND D.OFFER_END_DT                 = G.OFFER_END_DT   
     ,(SELECT SRS_DIV_NO   
             ,SRS_LN_NO 
             ,SRS_SLN_NO
             ,SRS_CLS_NO
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
               GROUP BY 1,2,3,4,5,6)	                          H	
        GROUP BY 1,2,3,4,5,6,7)                                   I   
        
 WHERE D.SRS_DIV_NO                     = E.SRS_DIV_NO 
   AND D.SRS_LN_NO                      = E.SRS_LN_NO         
   AND D.SRS_SLN_NO                     = E.SRS_SLN_NO      
   AND D.SRS_CLS_NO                     = E.SRS_CLS_NO 
   AND D.SRS_ITEM_NO                    = E.SRS_ITEM_NO                    
   AND D.OFFER_END_DT                   = E.OFFER_END_DT	                 
   AND D.OFFER_START_DT                 = E.OFFER_START_DT                 
   AND D.PRODUCT_GROUP_TYPE_CD          = E.PRODUCT_GROUP_TYPE_CD
   AND D.DEAL_TYPE_ID                   = E.DEAL_TYPE_ID  
   AND (D.SRS_DIV_NO                    = I.SRS_DIV_NO 
   AND D.SRS_LN_NO                      = I.SRS_LN_NO         
   AND D.SRS_SLN_NO                     = I.SRS_SLN_NO      
   AND D.SRS_CLS_NO                     = I.SRS_CLS_NO                       
   AND D.OFFER_END_DT                   = I.OFFER_END_DT	                 
   AND D.OFFER_START_DT                 = I.OFFER_START_DT 
   AND D.PRODUCT_GROUP_TYPE_CD          = I.PRODUCT_GROUP_TYPE_CD
    OR D.PRODUCT_GROUP_TYPE_CD         IN ('DVIT', 'PQRY', 'SLST'))
 GROUP BY 1,2,3,4,5,6,7,10,18,19;	
		
		
/***************************************************************
* COLLECT STATISTICS                                           *
***************************************************************/

.LABEL GET_STATS

 COLLECT STATS $TD_DB_WORK.IDRP_OFFERMANAGER_ADPLAN_SUM
     INDEX (SRS_DIV_NO, SRS_ITEM_NO);

.IF ERRORCODE  = 0 THEN .EXIT 0; 	 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
.QUIT;
 		