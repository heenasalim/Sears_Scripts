/*
###############################################################################################
#<>                                   HEADER                          			            <>#
###############################################################################################

# SCRIPT NAME:         perform_ie_master_data_smith__idrp_collections_carton_pack_xref_current.pig
# AUTHOR NAME:         Priyanka Gurjar
# CREATION DATE:       Tue June 10 15:44:42 CST 2014
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>
#
# DEPENDENCIES: <<TODO>>
#
# REV LIST:
#        DATE 06-10-2014        BY Priyanka Gurjar           Creation

##############################################################################################
#<<                                DECLARE                        				           >>#
##############################################################################################
*/

REGISTER $UDF_JAR;

SET default_parallel $NUM_PARALLEL;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

------------------------------------Load relevant input data files------------------------------------

LOAD_SMITH_IDRP_ITEM_ELIGIBILITY_BATCHDATE = LOAD '$SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_LOCATION' USING PigStorage ('$FIELD_DELIMITER_CONTROL_A') as ($SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_SCHEMA);

SMITH_IDRP_ITEM_ELIGIBILITY_BATCHDATE = foreach  LOAD_SMITH_IDRP_ITEM_ELIGIBILITY_BATCHDATE generate processing_ts;

LOAD_SMITH_IDRP_VEND_PACK_COMBINED =   
 LOAD '$SMITH__IDRP_VEND_PACK_COMBINED_LOCATION' USING PigStorage ('$FIELD_DELIMITER_CONTROL_A') as ($SMITH__IDRP_VEND_PACK_COMBINED_SCHEMA);
																	
LOAD_GOLD_ITEM_PACKAGE_COLLECTION =  
 LOAD '$GOLD__ITEM_PACKAGE_COLLECTION_LOCATION' USING PigStorage ('$FIELD_DELIMITER_CONTROL_A') as ($GOLD__ITEM_PACKAGE_COLLECTION_SCHEMA);
		    												
LOAD_GOLD_ITEM_PACKAGE_CURRENT = 
 LOAD '$GOLD__ITEM_PACKAGE_CURRENT_LOCATION' USING PigStorage ('$FIELD_DELIMITER_CONTROL_A') as ($GOLD__ITEM_PACKAGE_CURRENT_SCHEMA);
															
----------CROSS REFERENCE PACKAGE ELIGIBILITY BATCHDATE PROCESS----------------------------------------

GENERATE_GOLD_ITEM_PACKAGE_COLLECTION = foreach LOAD_GOLD_ITEM_PACKAGE_COLLECTION generate 
												external_package_id,
												internal_package_id,
												CONCAT_MULTIPLE(SUBSTRING(effective_ts,0,10),' ', SUBSTRING(effective_ts,11,19)) as effective_ts,
												CONCAT_MULTIPLE(SUBSTRING(expiration_ts,0,10),' ', SUBSTRING(expiration_ts,12,19)) as expiration_ts,
												internal_qty;

CROSS_JN_ELIGIBILITY_BATCH_PROCESS = CROSS GENERATE_GOLD_ITEM_PACKAGE_COLLECTION, SMITH_IDRP_ITEM_ELIGIBILITY_BATCHDATE;
	
----------FILTER GOLD ITEM PACKAGE COLLECTION FOR CURRENT DATA----------------------------------------

FLTR_GOLD_ITEM_PACKAGE_COLLTN_CURRENT_DATA = FILTER CROSS_JN_ELIGIBILITY_BATCH_PROCESS BY (processing_ts >= effective_ts) AND (processing_ts <= expiration_ts);

-----------SPLIT GOLD ITEM PACKAGE CURRENT INTO 'RCRT' AND ('EACH','ECRT')----------------------------

GENERATE_GOLD_ITEM_PACKAGE_CURRENT_LOCATION = foreach LOAD_GOLD_ITEM_PACKAGE_CURRENT generate
												package_id,
												effective_ts,
												expiration_ts,
												package_type_cd,
												package_type_desc,
												ksn_id;
	
SPLIT GENERATE_GOLD_ITEM_PACKAGE_CURRENT_LOCATION INTO FLTR_GOLD_ITEM_PACKAGE_CURRENT_RCRT IF package_type_cd == 'RCRT' ,				
											  FLTR_GOLD_ITEM_PACKAGE_CURRENT_EACH_ECRT IF (package_type_cd == 'EACH' OR package_type_cd == 'ECRT');

													
-----------OUTPUT OF EXTERNAL CARTON (RETAIL CARTON) PACKAGES----------------------------------------------------------------------------------------------- 

JN_WORK_IDRP_CARTON_COLLECTIONS =  join FLTR_GOLD_ITEM_PACKAGE_COLLTN_CURRENT_DATA BY external_package_id,
                                FLTR_GOLD_ITEM_PACKAGE_CURRENT_RCRT BY package_id;			
								
WORK_IDRP_CARTON_COLLECTIONS = foreach JN_WORK_IDRP_CARTON_COLLECTIONS generate 
										ksn_id as external_ksn_id,
										external_package_id as external_package_id,
										internal_package_id as internal_package_id,
										internal_qty as internal_qty;  



-----------OUTPUT OF INNER PACKAGES--------------------------------------------------------------------

JN_WORK_IDRP_CARTON_EACHES_COLLECTIONS = join WORK_IDRP_CARTON_COLLECTIONS BY internal_package_id,
                                         FLTR_GOLD_ITEM_PACKAGE_CURRENT_EACH_ECRT BY package_id;		
										 
WORK_IDRP_CARTON_EACHES_COLLECTIONS = foreach JN_WORK_IDRP_CARTON_EACHES_COLLECTIONS generate 
												WORK_IDRP_CARTON_COLLECTIONS::external_ksn_id as external_ksn_id,
												WORK_IDRP_CARTON_COLLECTIONS::external_package_id as external_package_id,
												WORK_IDRP_CARTON_COLLECTIONS::internal_package_id as internal_package_id,
												WORK_IDRP_CARTON_COLLECTIONS::internal_qty as internal_qty,
												FLTR_GOLD_ITEM_PACKAGE_CURRENT_EACH_ECRT::ksn_id as internal_ksn_id;

															
------------VENDOR PACK INFORMATION FOR  BOTH EXTERNAL AND INTERNAL KSNS--------------------------------

------------CROSS REFERENCE VENDOR ELIGIBILITY BATCHDATE PROCESS----------------------------------------
	
GEN_SMITH_IDRP_VEND_PACK_COMBINED = foreach LOAD_SMITH_IDRP_VEND_PACK_COMBINED generate 
											aprk_id,
											effective_dt,
											CONCAT_MULTIPLE(SUBSTRING(effective_ts,0,10),' ', SUBSTRING(effective_ts,11,19)) as effective_ts,
											CONCAT_MULTIPLE(SUBSTRING(expiration_ts,0,10),' ', SUBSTRING(expiration_ts,11,19)) as expiration_ts,
											(IsNull(gtin_usage_cd,' ')) as gtin_usage_cd,
											ksn_id,
											package_id,
											shc_item_id,
											vendor_package_id,
											purchase_status_cd;	
											
CROSS_JN_VENDOR_ELIGIBILITY_BATCH_PROCESS = CROSS GEN_SMITH_IDRP_VEND_PACK_COMBINED, SMITH_IDRP_ITEM_ELIGIBILITY_BATCHDATE;
		
----------FILTER CURRENT VENDOR ELIGIBILITY PROCESS DATA--------------------------------------------

FLTR_VENDOR_ELIGIBILITY_CURRENT_DATA = FILTER CROSS_JN_VENDOR_ELIGIBILITY_BATCH_PROCESS by (processing_ts >= effective_ts) AND (processing_ts <=expiration_ts);

-----------OUTPUT VENDOR CARTON EACHES EXTERNAL AND INTERNAL COLLECTION----------------------------
	
JN_WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS = JOIN FLTR_VENDOR_ELIGIBILITY_CURRENT_DATA by (ksn_id, package_id),
							   WORK_IDRP_CARTON_EACHES_COLLECTIONS by (external_ksn_id, external_package_id);
			
WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS = FOREACH JN_WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS generate 
																	external_ksn_id as external_ksn_id,
																	external_package_id as external_package_id,
																	internal_package_id as internal_package_id,
																	internal_qty as internal_qty,
																	internal_ksn_id as internal_ksn_id,
																	shc_item_id as external_shc_item_id,
																	vendor_package_id as external_vendor_package_id,
																	aprk_id as aprk_id,
																	gtin_usage_cd as gtin_usage_cd;
																													
---------OUTPUT WORK IDRP CARTON EACHES EXTINT APRKGTIN AND GTIN ITEM COLLECTION-----------------------

JN_WORK_IDRP_CARTON_EACHES_EXTINT_APRKGTIN_GTIN_ITEM_COLLECTIONS = join WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS by (internal_ksn_id,internal_package_id,gtin_usage_cd),
										  FLTR_VENDOR_ELIGIBILITY_CURRENT_DATA by (ksn_id,package_id,gtin_usage_cd);
										  									  
SPLIT JN_WORK_IDRP_CARTON_EACHES_EXTINT_APRKGTIN_GTIN_ITEM_COLLECTIONS INTO PRIORITY1_ITEM_COLLECTION IF (WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::aprk_id == GEN_SMITH_IDRP_VEND_PACK_COMBINED::aprk_id),
PRIORITY2_ITEM_COLLECTION IF (WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::aprk_id != GEN_SMITH_IDRP_VEND_PACK_COMBINED::aprk_id);

WORK_IDRP_CARTON_EACHES_EXTINT_APRKGTIN_ITEM_COLLECTIONS = FOREACH PRIORITY1_ITEM_COLLECTION generate
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::external_ksn_id as external_ksn_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::external_package_id as external_package_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::internal_package_id as internal_package_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::internal_qty as internal_qty,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::internal_ksn_id as internal_ksn_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::external_shc_item_id as external_shc_item_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::external_vendor_package_id as external_vendor_package_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::aprk_id as aprk_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::gtin_usage_cd as gtin_usage_cd,
													GEN_SMITH_IDRP_VEND_PACK_COMBINED::shc_item_id as internal_shc_item_id,
													GEN_SMITH_IDRP_VEND_PACK_COMBINED::vendor_package_id as internal_vendor_package_id,
                                                    FLTR_VENDOR_ELIGIBILITY_CURRENT_DATA::GEN_SMITH_IDRP_VEND_PACK_COMBINED::purchase_status_cd as purchase_status_cd,
													'1' as priority;
																	
WORK_IDRP_CARTON_EACHES_EXTINT_GTIN_ITEM_COLLECTIONS = FOREACH PRIORITY2_ITEM_COLLECTION generate
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::external_ksn_id as external_ksn_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::external_package_id as external_package_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::internal_package_id as internal_package_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::internal_qty as internal_qty,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::internal_ksn_id as internal_ksn_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::external_shc_item_id as external_shc_item_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::external_vendor_package_id as external_vendor_package_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::aprk_id as aprk_id,
													WORK_IDRP_CARTON_EACHES_EXTIN_COLLECTIONS::gtin_usage_cd as gtin_usage_cd,
													GEN_SMITH_IDRP_VEND_PACK_COMBINED::shc_item_id as internal_shc_item_id,
													GEN_SMITH_IDRP_VEND_PACK_COMBINED::vendor_package_id as internal_vendor_package_id,	
													FLTR_VENDOR_ELIGIBILITY_CURRENT_DATA::GEN_SMITH_IDRP_VEND_PACK_COMBINED::purchase_status_cd as purchase_status_cd,
													'2' as priority;
													
------------------------UNION EXTERNAL AND INTERNAL COLLECTION-------------------------------------------------

UNION_E	= 	UNION WORK_IDRP_CARTON_EACHES_EXTINT_APRKGTIN_ITEM_COLLECTIONS, 
			      WORK_IDRP_CARTON_EACHES_EXTINT_GTIN_ITEM_COLLECTIONS;
				  
GROUP_E	= GROUP UNION_E BY (external_vendor_package_id);

UNQ_WORK_IDRP_COLLECTIONS_CARTON_PACK_XREF_PRIORITY = FOREACH GROUP_E {sorted = ORDER UNION_E BY priority, purchase_status_cd;
										unq = LIMIT sorted 1;
										GENERATE FLATTEN(unq);};

WORK_IDRP_COLLECTIONS_CARTON_PACK_XREF_PRIORITY = foreach UNQ_WORK_IDRP_COLLECTIONS_CARTON_PACK_XREF_PRIORITY generate
															'$CURRENT_TIMESTAMP' AS load_ts,
															external_vendor_package_id,
															external_shc_item_id,
															external_ksn_id,															
															external_package_id ,
															internal_shc_item_id,
															internal_ksn_id,
															internal_vendor_package_id,
															internal_package_id  ,
															internal_qty ,
															aprk_id, 
															gtin_usage_cd,
															'$batchid' as batchid;

-----------FINAL OUTPUT FILE------------------------------------------------------------------------------------
/* Storing store smith__idrp_collections_carton_pack_xref_current from above */

STORE WORK_IDRP_COLLECTIONS_CARTON_PACK_XREF_PRIORITY INTO '$output_hdfs_path' USING PigStorage('$FIELD_DELIMITER_CONTROL_A'); 

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/