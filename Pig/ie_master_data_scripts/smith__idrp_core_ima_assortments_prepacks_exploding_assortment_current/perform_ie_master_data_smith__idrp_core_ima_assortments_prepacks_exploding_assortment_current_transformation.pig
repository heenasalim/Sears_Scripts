/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_ie_master_data_smith__idrp_core_ima_assortments_prepacks_exploding_assortment_current_transformation.pig 
# AUTHOR NAME:         Srikanth Reddy Annadi
# CREATION DATE:       18-06-2014
# CURRENT REVISION NO: 1
#
# DESCRIPTION:	
#
#
# Param files - 
#
#
# DEPENDENCIES: 
#
#
# REV LIST:
#        DATE         BY               MODIFICATION
#        8-07-2014    Meghana Dhage    CR 2759 and Defect 2770
#
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

REGISTER $UDF_JAR;
SET default_parallel $num_parallel;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

-- Load Smith__idrp_ie_item_hierarchy_combined_all_current

LOAD_ITEM_COMBINED_HIER_ALL_CURR = load '$WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_LOCATION' using PigStorage('$work__idrp_item_hierarchy_combined_all_current_delimiter') as ($WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_SCHEMA);

ITEM_COMBINED_HIER_ALL_CURR_DATA = foreach LOAD_ITEM_COMBINED_HIER_ALL_CURR generate  ksn_id,
																					    shc_item_id,
																					    dotcom_allocation_ind as Component_online_order_eligibility_ind;
 																										  
--Load smith__idrp_vend_pack_combined

LOAD_SMITH_IDRP_VEND_PACK_COMBINED = load '$SMITH__IDRP_VEND_PACK_COMBINED_LOCATION' using PigStorage('$smith__idrp_vend_pack_combined_delimiter') as ($SMITH__IDRP_VEND_PACK_COMBINED_SCHEMA);

SMITH_IDRP_VEND_PACK_COMBINED_DATA = foreach LOAD_SMITH_IDRP_VEND_PACK_COMBINED generate  vendor_package_id ,
																					  order_duns_nbr, 
																					  flow_type_cd,
																					  owner_cd;
																					  														  
-- Load Gold__item_exploding_assortment

LOAD_GOLD_ITEM_EXPLO_ASSRTMNT = load '$GOLD__ITEM_EXPLODING_ASSORTMENT_ACTIVE_LOCATION' using PigStorage('$Gold__item_exploding_assortment_delimiter') as ($GOLD__ITEM_EXPLODING_ASSORTMENT_SCHEMA);

GOLD_ITEM_EXPLO_ASSRTMNT_DATA = foreach LOAD_GOLD_ITEM_EXPLO_ASSRTMNT generate  load_ts,
                                       external_vendor_package_id as exploding_assortment_vendor_package_id,
                                       internal_ksn_id,
                                       effective_ts,
                                       expiration_ts,
                                       external_ksn_id,
                                       internal_qty,
                                       internal_cost_amt,
                                       alternate_exploding_assortment_id,
                                       last_change_user_id,
                                       internal_vendor_package_id as component_vendor_package_id,
                                       record_status;
									   
--Load work__idrp_sears_exploding_assortment_master_final

LOAD_WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL = load '$WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_LOCATION' using PigStorage('$work__idrp_sears_exploding_assortment_master_final_delimiter') as ($WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_SCHEMA);

WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA = foreach LOAD_WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL generate 
																								TrimLeadingZeros(exploding_assortment_ksn_id) as exploding_assortment_ksn_id,
																								sears_division_nbr,
																								TrimLeadingZeros(sears_item_nbr) as sears_item_nbr,
																								sears_sku_nbr,
																								TrimLeadingZeros(order_duns_nbr) as order_duns_nbr,
																								exploding_assortment_type_cd,
																								rimflow_jit_ind,
																								component_count_nbr,
																								component_sears_division_nbr,
																								TrimLeadingZeros(component_sears_item_nbr) as component_sears_item_nbr,
																								component_sears_sku_nbr,
																								TrimLeadingZeros(component_ksn_id) as component_ksn_id,
																								TrimLeadingZeros(component_qty) as component_qty;												
-- Load smith__idrp_item_eligibility_batchdate 

LOAD_SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE = load '$SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_LOCATION' using
PigStorage('$smith__idrp_item_eligibility_batchdate_delimiter') as ($SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_SCHEMA);

SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE = foreach LOAD_SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE generate processing_ts;
																								
----Transformations---------------

JOINSET_1 = join WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA by exploding_assortment_ksn_id , ITEM_COMBINED_HIER_ALL_CURR_DATA by ksn_id ;

work__idrp_core_ima_exas_step2 = foreach JOINSET_1 generate shc_item_id,
												exploding_assortment_ksn_id,
												sears_division_nbr,
												sears_item_nbr,
												sears_sku_nbr,
												order_duns_nbr,
												exploding_assortment_type_cd,
												rimflow_jit_ind,
												component_count_nbr,
												component_sears_division_nbr,
												component_sears_item_nbr,
												component_sears_sku_nbr,
												component_ksn_id,
												component_qty;
																								
JOIN_GOLD_ITEM_EXPLO_ASSRTMNT_BATCHDATE = CROSS GOLD_ITEM_EXPLO_ASSRTMNT_DATA, SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE;

FILTER_GOLD_ITEM_EXPLO_ASSRTMNT_BATCHDATE = filter JOIN_GOLD_ITEM_EXPLO_ASSRTMNT_BATCHDATE by (processing_ts >= effective_ts AND processing_ts <= expiration_ts);
											
JOINSET_2 =  join work__idrp_core_ima_exas_step2 by (exploding_assortment_ksn_id, component_ksn_id, component_qty) , FILTER_GOLD_ITEM_EXPLO_ASSRTMNT_BATCHDATE by (external_ksn_id, internal_ksn_id, internal_qty);

work__idrp_core_ima_exas_step3 = foreach JOINSET_2 generate shc_item_id,
										exploding_assortment_ksn_id,
										sears_division_nbr,
										sears_item_nbr,
										sears_sku_nbr,
										order_duns_nbr,
										exploding_assortment_type_cd,
										rimflow_jit_ind,
										component_count_nbr,
										component_sears_division_nbr,
										component_sears_item_nbr,
										component_sears_sku_nbr,
										component_ksn_id,
										component_qty,
										exploding_assortment_vendor_package_id,
										component_vendor_package_id;
																				
FILTER_SMITH_IDRP_VEND_PACK_COMBINED_DATA = filter SMITH_IDRP_VEND_PACK_COMBINED_DATA by owner_cd == 'S';
																			
JOINSET_3 = join work__idrp_core_ima_exas_step3 by (exploding_assortment_vendor_package_id) , FILTER_SMITH_IDRP_VEND_PACK_COMBINED_DATA by (vendor_package_id);

work__idrp_core_ima_exas_step4 = foreach JOINSET_3 generate shc_item_id,
												exploding_assortment_ksn_id,
												sears_division_nbr,
												sears_item_nbr,
												sears_sku_nbr,
												FILTER_SMITH_IDRP_VEND_PACK_COMBINED_DATA::order_duns_nbr as order_duns_nbr,
												exploding_assortment_type_cd,
												rimflow_jit_ind,
												component_count_nbr,
												component_sears_division_nbr,
												component_sears_item_nbr,
												component_sears_sku_nbr,
												component_ksn_id,
												component_qty,
												exploding_assortment_vendor_package_id,
												component_vendor_package_id,
												FILTER_SMITH_IDRP_VEND_PACK_COMBINED_DATA::flow_type_cd as flow_type_cd; 
																							
JOINSET_4 = join work__idrp_core_ima_exas_step4 by component_ksn_id , ITEM_COMBINED_HIER_ALL_CURR_DATA by ksn_id;

work__idrp_core_ima_exas_step5 = foreach JOINSET_4 generate
										work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::work__idrp_core_ima_exas_step2::ITEM_COMBINED_HIER_ALL_CURR_DATA::shc_item_id as shc_item_id,
										exploding_assortment_ksn_id,
										sears_division_nbr,
										sears_item_nbr,
										sears_sku_nbr,
										order_duns_nbr,
										exploding_assortment_type_cd,
										rimflow_jit_ind,
										component_count_nbr,
										component_sears_division_nbr,
										component_sears_item_nbr,
										component_sears_sku_nbr,
										component_ksn_id,
										component_qty,
										exploding_assortment_vendor_package_id,
										component_vendor_package_id,
										flow_type_cd,
										ITEM_COMBINED_HIER_ALL_CURR_DATA::Component_online_order_eligibility_ind as Component_online_order_eligibility_ind;
																				
GROUP_JOINSET_4_DATA_GEN = GROUP work__idrp_core_ima_exas_step5 by exploding_assortment_ksn_id ;

FLTN_GROUP_JOINSET_4_DATA_GEN = foreach GROUP_JOINSET_4_DATA_GEN 
												{
													ASC_SORTED = ORDER work__idrp_core_ima_exas_step5 by sears_division_nbr,sears_item_nbr,sears_sku_nbr;
													 TOP1  = LIMIT ASC_SORTED 1 ;
													 GENERATE FLATTEN(TOP1);
											};
																		
JOINSET_5 = join work__idrp_core_ima_exas_step5 by (exploding_assortment_ksn_id,sears_division_nbr,sears_item_nbr,sears_sku_nbr) , FLTN_GROUP_JOINSET_4_DATA_GEN by (exploding_assortment_ksn_id,sears_division_nbr,sears_item_nbr,sears_sku_nbr) ;

work__idrp_core_ima_exas_step6 = foreach JOINSET_5 generate 
										work__idrp_core_ima_exas_step5::shc_item_id as shc_item_id ,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::work__idrp_core_ima_exas_step2::WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA::exploding_assortment_ksn_id as exploding_assortment_ksn_id,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::work__idrp_core_ima_exas_step2::WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA::sears_division_nbr as sears_division_nbr,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::work__idrp_core_ima_exas_step2::WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA::sears_item_nbr as sears_item_nbr,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::work__idrp_core_ima_exas_step2::WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA::sears_sku_nbr as sears_sku_nbr,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::order_duns_nbr as order_duns_nbr,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::work__idrp_core_ima_exas_step2::WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA::exploding_assortment_type_cd,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::work__idrp_core_ima_exas_step2::WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA::rimflow_jit_ind as rimflow_jit_ind,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::work__idrp_core_ima_exas_step2::WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA::component_count_nbr as component_count_nbr,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::work__idrp_core_ima_exas_step2::WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA::component_sears_division_nbr as component_sears_division_nbr,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::work__idrp_core_ima_exas_step2::WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA::component_sears_item_nbr as component_sears_item_nbr,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::work__idrp_core_ima_exas_step2::WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA::component_sears_sku_nbr as component_sears_sku_nbr,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::work__idrp_core_ima_exas_step2::WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA::component_ksn_id as component_ksn_id,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::work__idrp_core_ima_exas_step2::WORK_IDRP_SEARS_EXPLO_ASSRTMNT_MASTER_FINAL_DATA::component_qty as component_qty,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::FILTER_GOLD_ITEM_EXPLO_ASSRTMNT_BATCHDATE::GOLD_ITEM_EXPLO_ASSRTMNT_DATA::exploding_assortment_vendor_package_id as exploding_assortment_vendor_package_id,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::work__idrp_core_ima_exas_step3::FILTER_GOLD_ITEM_EXPLO_ASSRTMNT_BATCHDATE::GOLD_ITEM_EXPLO_ASSRTMNT_DATA::component_vendor_package_id as component_vendor_package_id,
										work__idrp_core_ima_exas_step5::work__idrp_core_ima_exas_step4::flow_type_cd as flow_type_cd,
										work__idrp_core_ima_exas_step5::Component_online_order_eligibility_ind as Component_online_order_eligibility_ind;
																														
distinct_work__idrp_core_ima_exas_step6	= DISTINCT work__idrp_core_ima_exas_step6;	
																		
smith__idrp_core_ima_assortments_prepacks_exploding_assortment_current = foreach distinct_work__idrp_core_ima_exas_step6 generate 
												GetCurrentDate() as load_ts,
												shc_item_id as item_id ,
												exploding_assortment_ksn_id,
												exploding_assortment_vendor_package_id as vendor_pack_id,
												flow_type_cd,
												sears_division_nbr,
												sears_item_nbr,
												sears_sku_nbr,
												exploding_assortment_type_cd,
												rimflow_jit_ind,
												component_count_nbr,
												component_sears_division_nbr,
												component_sears_item_nbr,
												component_sears_sku_nbr,
												component_ksn_id,
												component_vendor_package_id as component_vendor_pack_id,
												component_qty,
												Component_online_order_eligibility_ind,
												'$batchid'  as idrp_batch_id;
																																			
---------- LOADREADY FILE ---------------------
-----------------------------------------------

rmf $output_hdfs_path;

Store smith__idrp_core_ima_assortments_prepacks_exploding_assortment_current into '$output_hdfs_path' using PigStorage('$output_table_delimiter');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
												