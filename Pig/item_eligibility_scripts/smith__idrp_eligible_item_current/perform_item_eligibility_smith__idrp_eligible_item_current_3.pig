/*
###############################################################################
#<>                           START HEADER DOCUMENT                         <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_current.pig
# AUTHOR NAME:         Mudit Mangal
# CREATION DATE:       07-07-2014 06:20
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>
#
#
#
# DEPENDENCIES: None
# RESTARTABLE:  N/A
#
#
# REV LIST:
#        DATE         BY            MODIFICATION
#12/1/2015		Siddhivinayak Karpe	CR#3325 When a Sears item is 4 characters in length, the fields IE_ITEM.SRS_DIV_ITM_SKU and  
#                                   IE_ITEM.SMT_SRS_DIV_ITM_SKU will be right-padded to 5 characters
#13/01/2015		Siddhivinayak Karpe CR#3079 Modify Hadoop script to extract the SQ product attribute only (remove case logic). The sales 
#                                   performance code code will be assigned in the extension script
#22/01/2015     Sushauvik Deb       CR#3640 Change the order of join to get the vendor package and package dimension values
#22/01/2015     Sushauvik Deb       CR#3642 Trim spaces in non_stock_source_cd while checking for a non-blank value to set the centrally    
#                                   stocked indicator
#11/02/2015     Meghana Dhage       CR#3170 Change the order of the join to get brand_ds after join with package_id (Line 1260 -1344)
#04/05/2015     Sushauvik Deb       CR#3451 Remove space from SRS_DIV_ITM_SKU
#06/11/2019     Heena Shaikh        CR#4007 Add Single Item Replenishment flag to IE_ITEM
#
###############################################################################
#<<                 START COMMON HEADER CODE - DO NOT MANUALLY EDIT         >>#
###############################################################################
*/

-- Register the jar containing all PIG UDFs
REGISTER $UDF_JAR;
SET default_parallel $NUM_PARALLEL;
DEFINE TrimLeadingZeros com.searshc.supplychain.idrp.udf.TrimLeadingZeros();
DEFINE AddDays com.searshc.supplychain.idrp.udf.AddOrRemoveDaysToDate();


/******************************* LOAD FOR ALL TABLES AND FILES REQUIRED ***********************************/

smith__idrp_ksn_attribute_current = LOAD '$SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_SCHEMA);
	
work__idrp_item_hierarchy_combined_all_current =     LOAD '$WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_LOCATION'     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_SCHEMA);	

gold__item_shc_hierarchy_current = LOAD '$GOLD__ITEM_SHC_HIERARCHY_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_SHC_HIERARCHY_CURRENT_SCHEMA);	
	
gold__inventory_sears_dc_item_facility_current_data = LOAD '$GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT_SCHEMA);

gold__item_attribute_relate_current_data = LOAD '$GOLD__ITEM_ATTRIBUTE_RELATE_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_ATTRIBUTE_RELATE_CURRENT_SCHEMA);
	
gold__item_package_current_data = LOAD '$GOLD__ITEM_PACKAGE_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_PACKAGE_CURRENT_SCHEMA);

smith__idrp_item_eligibility_batchdate_load  = LOAD '$SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_LOCATION'	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_SCHEMA);

smith__idrp_item_eligibility_batchdate_data = FOREACH smith__idrp_item_eligibility_batchdate_load
											  GENERATE processing_ts;
	
smith__idrp_vend_pack_combined_data = LOAD '$SMITH__IDRP_VEND_PACK_COMBINED_LOCATION'	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_VEND_PACK_COMBINED_SCHEMA);
	
smith__idrp_i2k_valid_rebuy_vendor_package_ids_current_data = LOAD 	'$smith__idrp_i2k_valid_rebuy_vendor_package_ids_current_location'	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($smith__idrp_i2k_valid_rebuy_vendor_package_ids_current_schema);	
---------------------------------------------------------------------------------------------------------------------------------------------

join_ksn_item = JOIN smith__idrp_ksn_attribute_current BY ksn_id, work__idrp_item_hierarchy_combined_all_current BY ksn_id ;

work__idrp_eligible_item_ksn_attribute_step1 = FOREACH join_ksn_item GENERATE

smith__idrp_ksn_attribute_current::ksn_id AS ksn_id ,
smith__idrp_ksn_attribute_current::shc_item_id AS shc_item_id , 
work__idrp_item_hierarchy_combined_all_current::sears_business_nbr AS sears_business_nbr ,
work__idrp_item_hierarchy_combined_all_current::sears_business_desc AS sears_business_desc,
work__idrp_item_hierarchy_combined_all_current::sears_division_nbr AS sears_division_nbr,
work__idrp_item_hierarchy_combined_all_current::sears_division_desc AS sears_division_desc,
work__idrp_item_hierarchy_combined_all_current::sears_line_nbr AS sears_line_nbr,
work__idrp_item_hierarchy_combined_all_current::sears_line_desc AS sears_line_desc,
work__idrp_item_hierarchy_combined_all_current::sears_sub_line_nbr AS sears_sub_line_nbr,
work__idrp_item_hierarchy_combined_all_current::sears_sub_line_desc AS sears_sub_line_desc,
work__idrp_item_hierarchy_combined_all_current::sears_class_nbr AS sears_class_nbr,
work__idrp_item_hierarchy_combined_all_current::sears_class_desc AS sears_class_desc,
work__idrp_item_hierarchy_combined_all_current::sears_item_nbr AS sears_item_nbr,
work__idrp_item_hierarchy_combined_all_current::sears_sku_nbr AS sears_sku_nbr ,
CONCAT(work__idrp_item_hierarchy_combined_all_current::sears_division_nbr,CONCAT('-',work__idrp_item_hierarchy_combined_all_current::sears_item_nbr)) AS sears_division_item_id,
(CONCAT(work__idrp_item_hierarchy_combined_all_current::sears_division_nbr,CONCAT('-',CONCAT(CONCAT(work__idrp_item_hierarchy_combined_all_current::sears_item_nbr,'-'),work__idrp_item_hierarchy_combined_all_current::sears_sku_nbr)))) AS sears_division_item_sku_id,
smith__idrp_ksn_attribute_current::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
smith__idrp_ksn_attribute_current::distribution_type_cd AS distribution_type_cd,
smith__idrp_ksn_attribute_current::special_order_candidate_ind AS special_order_candidate_ind,
smith__idrp_ksn_attribute_current::item_emp_ind AS item_emp_ind,
smith__idrp_ksn_attribute_current::easy_order_ind AS easy_order_ind,
smith__idrp_ksn_attribute_current::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
smith__idrp_ksn_attribute_current::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
smith__idrp_ksn_attribute_current::rapid_item_ind AS rapid_item_ind ,
smith__idrp_ksn_attribute_current::constrained_item_ind AS constrained_item_ind,
smith__idrp_ksn_attribute_current::idrp_item_type_desc AS idrp_item_type_desc ;
----------------------------------------------------------------------------------------------------


gold__item_shc_hierarchy_current_data = FOREACH  gold__item_shc_hierarchy_current GENERATE

ksn_id AS ksn_id, 
similar_ksn_id AS similar_ksn_id, 
similar_ksn_pct AS similar_ksn_pct ; 

join_gold_smith = JOIN 	gold__item_shc_hierarchy_current_data BY similar_ksn_id , work__idrp_item_hierarchy_combined_all_current BY ksn_id ;

join_gold_smith_step1 = JOIN work__idrp_eligible_item_ksn_attribute_step1 BY ksn_id LEFT OUTER, join_gold_smith BY gold__item_shc_hierarchy_current_data::ksn_id ;

work__idrp_eligible_item_ksn_attribute_step2 = FOREACH 	join_gold_smith_step1 GENERATE
work__idrp_eligible_item_ksn_attribute_step1::ksn_id AS ksn_id ,
work__idrp_eligible_item_ksn_attribute_step1::shc_item_id AS shc_item_id , 
work__idrp_eligible_item_ksn_attribute_step1::sears_business_nbr AS sears_business_nbr ,
work__idrp_eligible_item_ksn_attribute_step1::sears_business_desc AS sears_business_desc,
work__idrp_eligible_item_ksn_attribute_step1::sears_division_nbr AS sears_division_nbr,
work__idrp_eligible_item_ksn_attribute_step1::sears_division_desc AS sears_division_desc,
work__idrp_eligible_item_ksn_attribute_step1::sears_line_nbr AS sears_line_nbr,
work__idrp_eligible_item_ksn_attribute_step1::sears_line_desc AS sears_line_desc,
work__idrp_eligible_item_ksn_attribute_step1::sears_sub_line_nbr AS sears_sub_line_nbr,
work__idrp_eligible_item_ksn_attribute_step1::sears_sub_line_desc AS sears_sub_line_desc,
work__idrp_eligible_item_ksn_attribute_step1::sears_class_nbr AS sears_class_nbr,
work__idrp_eligible_item_ksn_attribute_step1::sears_class_desc AS sears_class_desc,
work__idrp_eligible_item_ksn_attribute_step1::sears_item_nbr AS sears_item_nbr,
work__idrp_eligible_item_ksn_attribute_step1::sears_sku_nbr AS sears_sku_nbr ,
work__idrp_eligible_item_ksn_attribute_step1::sears_division_item_id AS sears_division_item_id,
work__idrp_eligible_item_ksn_attribute_step1::sears_division_item_sku_id AS sears_division_item_sku_id,
work__idrp_eligible_item_ksn_attribute_step1::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
work__idrp_eligible_item_ksn_attribute_step1::distribution_type_cd AS distribution_type_cd,
work__idrp_eligible_item_ksn_attribute_step1::special_order_candidate_ind AS special_order_candidate_ind,
work__idrp_eligible_item_ksn_attribute_step1::item_emp_ind AS item_emp_ind,
work__idrp_eligible_item_ksn_attribute_step1::easy_order_ind AS easy_order_ind,
work__idrp_eligible_item_ksn_attribute_step1::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
work__idrp_eligible_item_ksn_attribute_step1::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
work__idrp_eligible_item_ksn_attribute_step1::rapid_item_ind AS rapid_item_ind ,
work__idrp_eligible_item_ksn_attribute_step1::constrained_item_ind AS constrained_item_ind,
work__idrp_eligible_item_ksn_attribute_step1::idrp_item_type_desc AS idrp_item_type_desc,
join_gold_smith::work__idrp_item_hierarchy_combined_all_current::shc_item_id AS ima_sim_to_shc_item_id,
join_gold_smith::work__idrp_item_hierarchy_combined_all_current::shc_item_desc AS ima_sim_to_shc_item_id_desc,
((chararray)((double)join_gold_smith::gold__item_shc_hierarchy_current_data::similar_ksn_pct/100)) AS ima_sim_to_factor_qty,
join_gold_smith::work__idrp_item_hierarchy_combined_all_current::sears_division_nbr AS sim_to_sears_division_nbr,
join_gold_smith::work__idrp_item_hierarchy_combined_all_current::sears_division_desc AS sim_to_sears_division_desc,
join_gold_smith::work__idrp_item_hierarchy_combined_all_current::sears_item_nbr AS sim_to_sears_item_nbr,
join_gold_smith::work__idrp_item_hierarchy_combined_all_current::sears_sku_nbr AS sim_to_sears_sku_nbr,
(CONCAT(join_gold_smith::work__idrp_item_hierarchy_combined_all_current::sears_division_nbr,CONCAT('-',CONCAT(CONCAT(join_gold_smith::work__idrp_item_hierarchy_combined_all_current::sears_item_nbr,'-'),join_gold_smith::work__idrp_item_hierarchy_combined_all_current::sears_sku_nbr)))) AS sim_to_sears_division_item_sku_id,
(CONCAT(join_gold_smith::work__idrp_item_hierarchy_combined_all_current::sears_division_nbr,CONCAT('-',join_gold_smith::work__idrp_item_hierarchy_combined_all_current::sears_item_nbr))) AS sim_to_sears_division_item_id ;

-----------------------------------------------------------------------------------------------------------------------------------------------

gen_gold__inventory_sears_dc_item_facility_current_table = 
    FOREACH gold__inventory_sears_dc_item_facility_current_data 
    GENERATE 
        sears_division_nbr AS in_division_nbr,
        sears_item_nbr AS in_item_nbr,
        sears_sku_nbr AS in_sku_nbr, 
		non_stock_source_cd ;
	
filter_gold__inventory_sears_dc_item_facility_current_table = 
    FILTER gen_gold__inventory_sears_dc_item_facility_current_table 
	BY TRIM(non_stock_source_cd) != '';
	
grp_filter_gold__inventory_sears_dc_item_facility_current_table = 
    GROUP filter_gold__inventory_sears_dc_item_facility_current_table 
	      BY (in_division_nbr,in_item_nbr,in_sku_nbr);
	
flatten_grp_filter_gold__inventory_sears_dc_item_facility_current_table = 
    FOREACH grp_filter_gold__inventory_sears_dc_item_facility_current_table 
            { a = LIMIT $1 1;
              GENERATE flatten(a);
            };

			
join_inv_item_shc_combil_div_sku_itm = 
    JOIN work__idrp_eligible_item_ksn_attribute_step2 BY ((long)sears_division_nbr,(long)sears_item_nbr,(long)sears_sku_nbr) 
         LEFT OUTER ,
	 flatten_grp_filter_gold__inventory_sears_dc_item_facility_current_table BY ((long)in_division_nbr,(long)in_item_nbr,(long)in_sku_nbr);		


work__idrp_eligible_item_ksn_attribute_step3  = FOREACH join_inv_item_shc_combil_div_sku_itm GENERATE

ksn_id AS ksn_id ,
shc_item_id AS shc_item_id , 
sears_business_nbr AS sears_business_nbr ,
sears_business_desc AS sears_business_desc,
sears_division_nbr AS sears_division_nbr,
sears_division_desc AS sears_division_desc,
sears_line_nbr AS sears_line_nbr,
sears_line_desc AS sears_line_desc,
sears_sub_line_nbr AS sears_sub_line_nbr,
sears_sub_line_desc AS sears_sub_line_desc,
sears_class_nbr AS sears_class_nbr,
sears_class_desc AS sears_class_desc,
sears_item_nbr AS sears_item_nbr,
sears_sku_nbr AS sears_sku_nbr ,
sears_division_item_id AS sears_division_item_id,
sears_division_item_sku_id AS sears_division_item_sku_id,
shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
distribution_type_cd AS distribution_type_cd,
special_order_candidate_ind AS special_order_candidate_ind,
item_emp_ind AS item_emp_ind,
easy_order_ind AS easy_order_ind,
warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
rapid_item_ind AS rapid_item_ind ,
constrained_item_ind AS constrained_item_ind,
idrp_item_type_desc AS idrp_item_type_desc,
ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
sim_to_sears_division_desc AS sim_to_sears_division_desc,
sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
(non_stock_source_cd IS NULL OR TRIM(non_stock_source_cd) == '' ? '0' : '1' ) AS centrally_stocked_ind;
-----------------------------------------------------------------------------------------------------------------------------------------------

/***** FILTERING GOLD__ITEM_ATTRIBUTE_RELATE_CURRENT ON ATTRIBUTE_IDS AND PERFORMING CR-798 ****/

gold__item_attribute_relate_current_data_filter =
    FILTER gold__item_attribute_relate_current_data
        BY  (
            ksn_id IS NOT NULL
            AND
            (
                attribute_id == '30'
                OR
                attribute_id == '50'
                OR
                attribute_id == '90'
                OR
                attribute_id == '360'
                OR
                attribute_id == '400'
                OR
                attribute_id == '420'
                OR
                (
                    attribute_id == '270'
                    AND
                    attribute_relate_level_cd == 'K'
                )
                OR
                attribute_id == '720'
                OR
                attribute_id == '730'
                OR
                attribute_id == '1610'
		OR
                attribute_id == '3710'                                --IPS 4007
             )
           AND
           (
             '$CURRENT_TIMESTAMP' >= effective_ts 
              AND 
              '$CURRENT_TIMESTAMP' <= expiration_ts 
           )
           );

gen_attribute_relate_curr_tbl_ksn_valu = 
    FOREACH gold__item_attribute_relate_current_data_filter
	GENERATE 
        ksn_id AS attr_ksn_id,
        value_definition_tx,
        sub_attribute_id,
        item_id AS attr_item_id,
        attribute_relate_id AS attr_relate_id,
		attribute_relate_level_cd AS attribute_relate_level_cd,
        attribute_id,
        value_nm,
        package_id AS attr_package_id,
        attribute_relate_alternate_id AS attribute_relate_alternate_id;
		
SPLIT gen_attribute_relate_curr_tbl_ksn_valu 
INTO 
   gen_attribute_relate_curr_tbl_ksn_valu_30   IF attribute_id == '30',
   gen_attribute_relate_curr_tbl_ksn_valu_50   IF attribute_id == '50',
   gen_attribute_relate_curr_tbl_ksn_valu_90   IF attribute_id == '90',
   gen_attribute_relate_curr_tbl_ksn_valu_360  IF attribute_id == '360',
   gen_attribute_relate_curr_tbl_ksn_valu_400  IF attribute_id == '400',
   gen_attribute_relate_curr_tbl_ksn_valu_420  IF attribute_id == '420',
   gen_attribute_relate_curr_tbl_ksn_valu_720  IF attribute_id == '720',
   gen_attribute_relate_curr_tbl_ksn_valu_730  IF attribute_id == '730' ,
   gen_attribute_relate_curr_tbl_ksn_valu_1610 IF attribute_id == '1610',
   gen_attribute_relate_curr_tbl_ksn_valu_270  IF attribute_id == '270' ,	
   gen_attribute_relate_curr_tbl_ksn_valu_3710 IF attribute_id == '3710';   -- IPS 4007

gen_attribute_relate_curr_tbl_ksn_valu_720 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_720 
    GENERATE 
        value_definition_tx AS value_definition_tx_720,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_720,
        attr_ksn_id AS attr_ksn_id_720;

group_data_by_ksn_id_to_find_obn_830_dur  =
    GROUP gen_attribute_relate_curr_tbl_ksn_valu_720
    BY attr_ksn_id_720;		


flatten_data_to_find_obn_830_dur =
        FOREACH group_data_by_ksn_id_to_find_obn_830_dur
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER gen_attribute_relate_curr_tbl_ksn_valu_720 BY attribute_relate_alternate_id_720 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };
	
		
join_with_attr_id_720 = 
    JOIN work__idrp_eligible_item_ksn_attribute_step3 BY ksn_id 
         LEFT OUTER ,
	 flatten_data_to_find_obn_830_dur BY attr_ksn_id_720 PARALLEL $NUM_PARALLEL;


work__idrp_eligible_item_ksn_attribute_step4b = 
    FOREACH join_with_attr_id_720
    GENERATE 
ksn_id AS ksn_id ,
shc_item_id AS shc_item_id , 
sears_business_nbr AS sears_business_nbr ,
sears_business_desc AS sears_business_desc,
sears_division_nbr AS sears_division_nbr,
sears_division_desc AS sears_division_desc,
sears_line_nbr AS sears_line_nbr,
sears_line_desc AS sears_line_desc,
sears_sub_line_nbr AS sears_sub_line_nbr,
sears_sub_line_desc AS sears_sub_line_desc,
sears_class_nbr AS sears_class_nbr,
sears_class_desc AS sears_class_desc,
sears_item_nbr AS sears_item_nbr,
sears_sku_nbr AS sears_sku_nbr ,
sears_division_item_id AS sears_division_item_id,
sears_division_item_sku_id AS sears_division_item_sku_id,
shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
distribution_type_cd AS distribution_type_cd,
special_order_candidate_ind AS special_order_candidate_ind,
item_emp_ind AS item_emp_ind,
easy_order_ind AS easy_order_ind,
warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
rapid_item_ind AS rapid_item_ind ,
constrained_item_ind AS constrained_item_ind,
idrp_item_type_desc AS idrp_item_type_desc,
ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
sim_to_sears_division_desc AS sim_to_sears_division_desc,
sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
centrally_stocked_ind AS centrally_stocked_ind,
(attr_ksn_id_720 IS NULL ? '0':(value_definition_tx_720 IS NULL OR value_definition_tx_720 == '' ? '0' : (SUBSTRING(value_definition_tx_720,0,2) == 'RQ' ? (chararray)((int)(SUBSTRING(value_definition_tx_720,(int)(SIZE(value_definition_tx_720)-2),(int)SIZE(value_definition_tx_720))) * 10080) : '0'))) AS outbound_830_duration_nbr,
(attr_ksn_id_720 IS NULL ? '0':( (value_definition_tx_720 IS NULL OR value_definition_tx_720 == '' ? '0' : (SUBSTRING(value_definition_tx_720,0,2) == 'RQ' ? (chararray)((int)(SUBSTRING(value_definition_tx_720,(int)(SIZE(value_definition_tx_720)-2),(int)SIZE(value_definition_tx_720))) * 10080) : '0')) IS NULL OR (value_definition_tx_720 IS NULL OR value_definition_tx_720 == '' ? '0' : (SUBSTRING(value_definition_tx_720,0,2) == 'RQ' ? (chararray)((int)(SUBSTRING(value_definition_tx_720,(int)(SIZE(value_definition_tx_720)-2),(int)SIZE(value_definition_tx_720))) * 10080) : '0')) == '0' ? '0' : '1')) AS outbound_830_duration_ind;
--------------------------------------------------------------------------------------------------------------------------------

gen_attribute_relate_curr_tbl_ksn_valu_730 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_730 
    GENERATE 
        value_definition_tx AS value_definition_tx_730,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_730,
        attr_ksn_id AS attr_ksn_id_730;

group_data_by_ksn_id_to_find_rpd_frz_dur  =
    GROUP gen_attribute_relate_curr_tbl_ksn_valu_730
    BY attr_ksn_id_730;

flatten_data_to_find_rpd_frz_dur =
        FOREACH group_data_by_ksn_id_to_find_rpd_frz_dur
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER gen_attribute_relate_curr_tbl_ksn_valu_730 BY attribute_relate_alternate_id_730 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };

		
join_with_attr_id_730 = 
    JOIN work__idrp_eligible_item_ksn_attribute_step4b BY ksn_id 
         LEFT OUTER ,
	 flatten_data_to_find_rpd_frz_dur BY attr_ksn_id_730 PARALLEL $NUM_PARALLEL;

work__idrp_eligible_item_ksn_attribute_step5b = 
    FOREACH join_with_attr_id_730
    GENERATE 
ksn_id AS ksn_id ,
shc_item_id AS shc_item_id , 
sears_business_nbr AS sears_business_nbr ,
sears_business_desc AS sears_business_desc,
sears_division_nbr AS sears_division_nbr,
sears_division_desc AS sears_division_desc,
sears_line_nbr AS sears_line_nbr,
sears_line_desc AS sears_line_desc,
sears_sub_line_nbr AS sears_sub_line_nbr,
sears_sub_line_desc AS sears_sub_line_desc,
sears_class_nbr AS sears_class_nbr,
sears_class_desc AS sears_class_desc,
sears_item_nbr AS sears_item_nbr,
sears_sku_nbr AS sears_sku_nbr ,
sears_division_item_id AS sears_division_item_id,
sears_division_item_sku_id AS sears_division_item_sku_id,
shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
distribution_type_cd AS distribution_type_cd,
special_order_candidate_ind AS special_order_candidate_ind,
item_emp_ind AS item_emp_ind,
easy_order_ind AS easy_order_ind,
warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
rapid_item_ind AS rapid_item_ind ,
constrained_item_ind AS constrained_item_ind,
idrp_item_type_desc AS idrp_item_type_desc,
ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
sim_to_sears_division_desc AS sim_to_sears_division_desc,
sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
centrally_stocked_ind AS centrally_stocked_ind,
outbound_830_duration_nbr AS outbound_830_duration_nbr,
outbound_830_duration_ind AS outbound_830_duration_ind,   
(attr_ksn_id_730 IS NULL ? '0':(value_definition_tx_730 IS NULL OR value_definition_tx_730 == '' ? '0' : (SUBSTRING(value_definition_tx_730,0,4) == 'RR00' OR  SUBSTRING(value_definition_tx_730,0,4) == 'RR10'  ?  (chararray)((int)SUBSTRING(value_definition_tx_730,(int)(SIZE(value_definition_tx_730)-2),(int)SIZE(value_definition_tx_730)) * 10080) : '0'))) AS rapid_freeze_duration_nbr;
-------------------------------------------------------------------------------------------------------------------------------------------

gen_attribute_relate_curr_tbl_ksn_valu_400 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_400 
    GENERATE  
        value_definition_tx,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_400,
        attr_ksn_id AS attr_ksn_id_400;


group_data_by_ksn_id_to_find_sls_pfm_seg_cd  =
    GROUP gen_attribute_relate_curr_tbl_ksn_valu_400 
    BY attr_ksn_id_400;

flatten_data_to_find_sls_pfm_seg_cd = 
	FOREACH group_data_by_ksn_id_to_find_sls_pfm_seg_cd 
	    {
		    sort_data_desc_on_attr_relate_alt_id = ORDER gen_attribute_relate_curr_tbl_ksn_valu_400 BY attribute_relate_alternate_id_400 DESC ;
			take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
			GENERATE FLATTEN (take_first_row);
            };

		
join_with_attr_id_400 = 
    JOIN work__idrp_eligible_item_ksn_attribute_step5b BY ksn_id 
         LEFT OUTER ,
	 flatten_data_to_find_sls_pfm_seg_cd BY attr_ksn_id_400 PARALLEL $NUM_PARALLEL;

work__idrp_eligible_item_ksn_attribute_step7b = FOREACH join_with_attr_id_400
    GENERATE 
ksn_id AS ksn_id ,
shc_item_id AS shc_item_id , 
sears_business_nbr AS sears_business_nbr ,
sears_business_desc AS sears_business_desc,
sears_division_nbr AS sears_division_nbr,
sears_division_desc AS sears_division_desc,
sears_line_nbr AS sears_line_nbr,
sears_line_desc AS sears_line_desc,
sears_sub_line_nbr AS sears_sub_line_nbr,
sears_sub_line_desc AS sears_sub_line_desc,
sears_class_nbr AS sears_class_nbr,
sears_class_desc AS sears_class_desc,
sears_item_nbr AS sears_item_nbr,
sears_sku_nbr AS sears_sku_nbr ,
sears_division_item_id AS sears_division_item_id,
sears_division_item_sku_id AS sears_division_item_sku_id,
shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
distribution_type_cd AS distribution_type_cd,
special_order_candidate_ind AS special_order_candidate_ind,
item_emp_ind AS item_emp_ind,
easy_order_ind AS easy_order_ind,
warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
rapid_item_ind AS rapid_item_ind ,
constrained_item_ind AS constrained_item_ind,
idrp_item_type_desc AS idrp_item_type_desc,
ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
sim_to_sears_division_desc AS sim_to_sears_division_desc,
sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
centrally_stocked_ind AS centrally_stocked_ind,
outbound_830_duration_nbr AS outbound_830_duration_nbr,
outbound_830_duration_ind AS outbound_830_duration_ind,   
rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr,
value_definition_tx AS sales_performance_segment_cd,
(attr_ksn_id_400 IS NULL ? 'SHARED':(value_definition_tx IS NULL ? 'SHARED' : (value_definition_tx == 'SQ5106' ? 'ICS' : (value_definition_tx == 'SQ5107' ? 'TGI' : (value_definition_tx == 'SQ5125' ? 'INT' : 'SHARED' ))))) AS format_exclusive_cd;
-----------------------------------------------------------------------------------------------------------------------------------------------

gen_attribute_relate_curr_tbl_ksn_valu_360 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_360 
    GENERATE  
        attribute_relate_alternate_id AS attribute_relate_alternate_id_360,
        value_definition_tx AS value_definition_tx_360,
        attr_ksn_id AS attr_ksn_id_360;

		
group_data_by_ksn_id_to_find_itm_pgm  =
    GROUP gen_attribute_relate_curr_tbl_ksn_valu_360
    BY attr_ksn_id_360;

flatten_data_to_find_itm_pgm =
        FOREACH group_data_by_ksn_id_to_find_itm_pgm
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER gen_attribute_relate_curr_tbl_ksn_valu_360 BY attribute_relate_alternate_id_360 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };

		
join_with_attr_id_360 = 
    JOIN work__idrp_eligible_item_ksn_attribute_step7b BY ksn_id 
         LEFT OUTER ,
	 flatten_data_to_find_itm_pgm BY attr_ksn_id_360 PARALLEL $NUM_PARALLEL;

work__idrp_eligible_item_ksn_attribute_step8b = 
    FOREACH join_with_attr_id_360
    GENERATE 
ksn_id AS ksn_id ,
shc_item_id AS shc_item_id , 
sears_business_nbr AS sears_business_nbr ,
sears_business_desc AS sears_business_desc,
sears_division_nbr AS sears_division_nbr,
sears_division_desc AS sears_division_desc,
sears_line_nbr AS sears_line_nbr,
sears_line_desc AS sears_line_desc,
sears_sub_line_nbr AS sears_sub_line_nbr,
sears_sub_line_desc AS sears_sub_line_desc,
sears_class_nbr AS sears_class_nbr,
sears_class_desc AS sears_class_desc,
sears_item_nbr AS sears_item_nbr,
sears_sku_nbr AS sears_sku_nbr ,
sears_division_item_id AS sears_division_item_id,
sears_division_item_sku_id AS sears_division_item_sku_id,
shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
distribution_type_cd AS distribution_type_cd,
special_order_candidate_ind AS special_order_candidate_ind,
item_emp_ind AS item_emp_ind,
easy_order_ind AS easy_order_ind,
warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
rapid_item_ind AS rapid_item_ind ,
constrained_item_ind AS constrained_item_ind,
idrp_item_type_desc AS idrp_item_type_desc,
ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
sim_to_sears_division_desc AS sim_to_sears_division_desc,
sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
centrally_stocked_ind AS centrally_stocked_ind,
outbound_830_duration_nbr AS outbound_830_duration_nbr,
outbound_830_duration_ind AS outbound_830_duration_ind,   
rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr,
sales_performance_segment_cd AS sales_performance_segment_cd,
format_exclusive_cd AS format_exclusive_cd,
(attr_ksn_id_360 IS NULL ? '': value_definition_tx_360) AS item_program_cd;
 	    
-----------------------------------------------------------------------------------------------------------------

gen_attribute_relate_curr_tbl_ksn_valu_420 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_420 
    GENERATE  
        value_definition_tx AS value_definition_tx_420,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_420,
        attr_ksn_id AS attr_ksn_id_420;

group_data_by_ksn_id_to_find_key_pgm  =
    GROUP gen_attribute_relate_curr_tbl_ksn_valu_420
    BY attr_ksn_id_420;

flatten_data_to_find_key_pgm =
        FOREACH group_data_by_ksn_id_to_find_key_pgm
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER gen_attribute_relate_curr_tbl_ksn_valu_420 BY attribute_relate_alternate_id_420 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };
		
		
join_with_attr_id_420 = 
    JOIN work__idrp_eligible_item_ksn_attribute_step8b BY ksn_id 
         LEFT OUTER ,
	 flatten_data_to_find_key_pgm BY attr_ksn_id_420 PARALLEL $NUM_PARALLEL;

		 
work__idrp_eligible_item_ksn_attribute_step9b = 
    FOREACH join_with_attr_id_420
    GENERATE 
ksn_id AS ksn_id ,
shc_item_id AS shc_item_id , 
sears_business_nbr AS sears_business_nbr ,
sears_business_desc AS sears_business_desc,
sears_division_nbr AS sears_division_nbr,
sears_division_desc AS sears_division_desc,
sears_line_nbr AS sears_line_nbr,
sears_line_desc AS sears_line_desc,
sears_sub_line_nbr AS sears_sub_line_nbr,
sears_sub_line_desc AS sears_sub_line_desc,
sears_class_nbr AS sears_class_nbr,
sears_class_desc AS sears_class_desc,
sears_item_nbr AS sears_item_nbr,
sears_sku_nbr AS sears_sku_nbr ,
sears_division_item_id AS sears_division_item_id,
sears_division_item_sku_id AS sears_division_item_sku_id,
shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
distribution_type_cd AS distribution_type_cd,
special_order_candidate_ind AS special_order_candidate_ind,
item_emp_ind AS item_emp_ind,
easy_order_ind AS easy_order_ind,
warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
rapid_item_ind AS rapid_item_ind ,
constrained_item_ind AS constrained_item_ind,
idrp_item_type_desc AS idrp_item_type_desc,
ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
sim_to_sears_division_desc AS sim_to_sears_division_desc,
sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
centrally_stocked_ind AS centrally_stocked_ind,
outbound_830_duration_nbr AS outbound_830_duration_nbr,
outbound_830_duration_ind AS outbound_830_duration_ind,   
rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr,
sales_performance_segment_cd AS sales_performance_segment_cd,
format_exclusive_cd AS format_exclusive_cd,
item_program_cd AS item_program_cd,
(attr_ksn_id_420 IS NULL ? '': value_definition_tx_420 )AS key_program_cd;
 ------------------------------------------------------------------------------------------------------

gen_attribute_relate_curr_tbl_ksn_valu_30 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_30 
    GENERATE  
        value_nm AS value_nm_30,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_30,
        attr_ksn_id AS attr_ksn_id_30;

group_data_by_ksn_id_to_find_size  =
    GROUP gen_attribute_relate_curr_tbl_ksn_valu_30
    BY attr_ksn_id_30;

flatten_data_to_find_size =
        FOREACH group_data_by_ksn_id_to_find_size
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER gen_attribute_relate_curr_tbl_ksn_valu_30 BY attribute_relate_alternate_id_30 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };
		
		
join_with_attr_id_30 = 
    JOIN work__idrp_eligible_item_ksn_attribute_step9b BY ksn_id 
         LEFT OUTER ,
         flatten_data_to_find_size BY attr_ksn_id_30 PARALLEL $NUM_PARALLEL;


work__idrp_eligible_item_ksn_attribute_step10b = 
    FOREACH join_with_attr_id_30
    GENERATE 
ksn_id AS ksn_id ,
shc_item_id AS shc_item_id , 
sears_business_nbr AS sears_business_nbr ,
sears_business_desc AS sears_business_desc,
sears_division_nbr AS sears_division_nbr,
sears_division_desc AS sears_division_desc,
sears_line_nbr AS sears_line_nbr,
sears_line_desc AS sears_line_desc,
sears_sub_line_nbr AS sears_sub_line_nbr,
sears_sub_line_desc AS sears_sub_line_desc,
sears_class_nbr AS sears_class_nbr,
sears_class_desc AS sears_class_desc,
sears_item_nbr AS sears_item_nbr,
sears_sku_nbr AS sears_sku_nbr ,
sears_division_item_id AS sears_division_item_id,
sears_division_item_sku_id AS sears_division_item_sku_id,
shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
distribution_type_cd AS distribution_type_cd,
special_order_candidate_ind AS special_order_candidate_ind,
item_emp_ind AS item_emp_ind,
easy_order_ind AS easy_order_ind,
warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
rapid_item_ind AS rapid_item_ind ,
constrained_item_ind AS constrained_item_ind,
idrp_item_type_desc AS idrp_item_type_desc,
ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
sim_to_sears_division_desc AS sim_to_sears_division_desc,
sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
centrally_stocked_ind AS centrally_stocked_ind,
outbound_830_duration_nbr AS outbound_830_duration_nbr,
outbound_830_duration_ind AS outbound_830_duration_ind,   
rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr,
sales_performance_segment_cd AS sales_performance_segment_cd,
format_exclusive_cd AS format_exclusive_cd,
item_program_cd AS item_program_cd,
key_program_cd AS key_program_cd,
(attr_ksn_id_30 IS NULL ? '': value_nm_30) AS size_nbr;
 ------------------------------------------------------------------------------------------------------------------------------------------

gen_attribute_relate_curr_tbl_ksn_valu_90 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_90 
    GENERATE  
        value_nm AS value_nm_90,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_90,
        attr_ksn_id AS attr_ksn_id_90;

group_data_by_ksn_id_to_find_style  =
    GROUP gen_attribute_relate_curr_tbl_ksn_valu_90
    BY attr_ksn_id_90;

flatten_data_to_find_style =
        FOREACH group_data_by_ksn_id_to_find_style
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER gen_attribute_relate_curr_tbl_ksn_valu_90 BY attribute_relate_alternate_id_90 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };
		
join_with_attr_id_90 = 
    JOIN work__idrp_eligible_item_ksn_attribute_step10b BY ksn_id 
         LEFT OUTER ,
	 flatten_data_to_find_style BY attr_ksn_id_90 PARALLEL $NUM_PARALLEL;

work__idrp_eligible_item_ksn_attribute_step11b = 
    FOREACH join_with_attr_id_90
    GENERATE 
ksn_id AS ksn_id ,
shc_item_id AS shc_item_id , 
sears_business_nbr AS sears_business_nbr ,
sears_business_desc AS sears_business_desc,
sears_division_nbr AS sears_division_nbr,
sears_division_desc AS sears_division_desc,
sears_line_nbr AS sears_line_nbr,
sears_line_desc AS sears_line_desc,
sears_sub_line_nbr AS sears_sub_line_nbr,
sears_sub_line_desc AS sears_sub_line_desc,
sears_class_nbr AS sears_class_nbr,
sears_class_desc AS sears_class_desc,
sears_item_nbr AS sears_item_nbr,
sears_sku_nbr AS sears_sku_nbr ,
sears_division_item_id AS sears_division_item_id,
sears_division_item_sku_id AS sears_division_item_sku_id,
shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
distribution_type_cd AS distribution_type_cd,
special_order_candidate_ind AS special_order_candidate_ind,
item_emp_ind AS item_emp_ind,
easy_order_ind AS easy_order_ind,
warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
rapid_item_ind AS rapid_item_ind ,
constrained_item_ind AS constrained_item_ind,
idrp_item_type_desc AS idrp_item_type_desc,
ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
sim_to_sears_division_desc AS sim_to_sears_division_desc,
sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
centrally_stocked_ind AS centrally_stocked_ind,
outbound_830_duration_nbr AS outbound_830_duration_nbr,
outbound_830_duration_ind AS outbound_830_duration_ind,   
rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr,
sales_performance_segment_cd AS sales_performance_segment_cd,
format_exclusive_cd AS format_exclusive_cd,
item_program_cd AS item_program_cd,
key_program_cd AS key_program_cd,
size_nbr AS size_nbr,
(attr_ksn_id_90 IS NULL ?'': value_nm_90) AS style_nbr;
--------------------------------------------------------------------------------------------------------------

join_work_smith_11b = JOIN work__idrp_eligible_item_ksn_attribute_step11b BY ksn_id LEFT OUTER, work__idrp_item_hierarchy_combined_all_current BY ksn_id ;

work__idrp_eligible_item_ksn_attribute_step12 = FOREACH join_work_smith_11b GENERATE

work__idrp_eligible_item_ksn_attribute_step11b::ksn_id AS ksn_id ,
work__idrp_eligible_item_ksn_attribute_step11b::shc_item_id AS shc_item_id , 
work__idrp_eligible_item_ksn_attribute_step11b::sears_business_nbr AS sears_business_nbr ,
work__idrp_eligible_item_ksn_attribute_step11b::sears_business_desc AS sears_business_desc,
work__idrp_eligible_item_ksn_attribute_step11b::sears_division_nbr AS sears_division_nbr,
work__idrp_eligible_item_ksn_attribute_step11b::sears_division_desc AS sears_division_desc,
work__idrp_eligible_item_ksn_attribute_step11b::sears_line_nbr AS sears_line_nbr,
work__idrp_eligible_item_ksn_attribute_step11b::sears_line_desc AS sears_line_desc,
work__idrp_eligible_item_ksn_attribute_step11b::sears_sub_line_nbr AS sears_sub_line_nbr,
work__idrp_eligible_item_ksn_attribute_step11b::sears_sub_line_desc AS sears_sub_line_desc,
work__idrp_eligible_item_ksn_attribute_step11b::sears_class_nbr AS sears_class_nbr,
work__idrp_eligible_item_ksn_attribute_step11b::sears_class_desc AS sears_class_desc,
work__idrp_eligible_item_ksn_attribute_step11b::sears_item_nbr AS sears_item_nbr,
work__idrp_eligible_item_ksn_attribute_step11b::sears_sku_nbr AS sears_sku_nbr ,
work__idrp_eligible_item_ksn_attribute_step11b::sears_division_item_id AS sears_division_item_id,
work__idrp_eligible_item_ksn_attribute_step11b::sears_division_item_sku_id AS sears_division_item_sku_id,
work__idrp_eligible_item_ksn_attribute_step11b::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
work__idrp_eligible_item_ksn_attribute_step11b::distribution_type_cd AS distribution_type_cd,
work__idrp_eligible_item_ksn_attribute_step11b::special_order_candidate_ind AS special_order_candidate_ind,
work__idrp_eligible_item_ksn_attribute_step11b::item_emp_ind AS item_emp_ind,
work__idrp_eligible_item_ksn_attribute_step11b::easy_order_ind AS easy_order_ind,
work__idrp_eligible_item_ksn_attribute_step11b::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
work__idrp_eligible_item_ksn_attribute_step11b::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
work__idrp_eligible_item_ksn_attribute_step11b::rapid_item_ind AS rapid_item_ind ,
work__idrp_eligible_item_ksn_attribute_step11b::constrained_item_ind AS constrained_item_ind,
work__idrp_eligible_item_ksn_attribute_step11b::idrp_item_type_desc AS idrp_item_type_desc,
work__idrp_eligible_item_ksn_attribute_step11b::ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
work__idrp_eligible_item_ksn_attribute_step11b::ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
work__idrp_eligible_item_ksn_attribute_step11b::ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
work__idrp_eligible_item_ksn_attribute_step11b::sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
work__idrp_eligible_item_ksn_attribute_step11b::sim_to_sears_division_desc AS sim_to_sears_division_desc,
work__idrp_eligible_item_ksn_attribute_step11b::sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
work__idrp_eligible_item_ksn_attribute_step11b::sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
work__idrp_eligible_item_ksn_attribute_step11b::sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
work__idrp_eligible_item_ksn_attribute_step11b::sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
work__idrp_eligible_item_ksn_attribute_step11b::centrally_stocked_ind AS centrally_stocked_ind,
work__idrp_eligible_item_ksn_attribute_step11b::outbound_830_duration_nbr AS outbound_830_duration_nbr,
work__idrp_eligible_item_ksn_attribute_step11b::outbound_830_duration_ind AS outbound_830_duration_ind,   
work__idrp_eligible_item_ksn_attribute_step11b::rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr,
work__idrp_eligible_item_ksn_attribute_step11b::sales_performance_segment_cd AS sales_performance_segment_cd,
work__idrp_eligible_item_ksn_attribute_step11b::format_exclusive_cd AS format_exclusive_cd,
work__idrp_eligible_item_ksn_attribute_step11b::item_program_cd AS item_program_cd,
work__idrp_eligible_item_ksn_attribute_step11b::key_program_cd AS key_program_cd,
work__idrp_eligible_item_ksn_attribute_step11b::size_nbr AS size_nbr,
work__idrp_eligible_item_ksn_attribute_step11b::style_nbr AS style_nbr,
work__idrp_item_hierarchy_combined_all_current::season_cd AS season_cd ,
work__idrp_item_hierarchy_combined_all_current::season_year_nbr AS season_year_nbr ,
work__idrp_item_hierarchy_combined_all_current::sub_season_id AS sub_season_id ;
-----------------------------------------------------------------------------------------------------------------------------------------------

gen_attribute_relate_curr_tbl_ksn_valu_50 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_50 
    GENERATE  
        value_nm AS value_nm_50,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_50,
	    attr_ksn_id AS attr_ksn_id_50;


group_data_by_ksn_id_to_find_color_ds  =
    GROUP gen_attribute_relate_curr_tbl_ksn_valu_50
    BY attr_ksn_id_50;

flatten_data_to_find_color_ds =
        FOREACH group_data_by_ksn_id_to_find_color_ds
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER gen_attribute_relate_curr_tbl_ksn_valu_50 BY attribute_relate_alternate_id_50 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };
	
join_with_attr_id_50 = 
    JOIN work__idrp_eligible_item_ksn_attribute_step12 BY ksn_id 
         LEFT OUTER ,
		 flatten_data_to_find_color_ds BY attr_ksn_id_50 PARALLEL $NUM_PARALLEL;

work__idrp_eligible_item_ksn_attribute_step14b = 
    FOREACH join_with_attr_id_50
    GENERATE 
ksn_id AS ksn_id ,
shc_item_id AS shc_item_id , 
sears_business_nbr AS sears_business_nbr ,
sears_business_desc AS sears_business_desc,
sears_division_nbr AS sears_division_nbr,
sears_division_desc AS sears_division_desc,
sears_line_nbr AS sears_line_nbr,
sears_line_desc AS sears_line_desc,
sears_sub_line_nbr AS sears_sub_line_nbr,
sears_sub_line_desc AS sears_sub_line_desc,
sears_class_nbr AS sears_class_nbr,
sears_class_desc AS sears_class_desc,
sears_item_nbr AS sears_item_nbr,
sears_sku_nbr AS sears_sku_nbr ,
sears_division_item_id AS sears_division_item_id,
sears_division_item_sku_id AS sears_division_item_sku_id,
shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
distribution_type_cd AS distribution_type_cd,
special_order_candidate_ind AS special_order_candidate_ind,
item_emp_ind AS item_emp_ind,
easy_order_ind AS easy_order_ind,
warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
rapid_item_ind AS rapid_item_ind ,
constrained_item_ind AS constrained_item_ind,
idrp_item_type_desc AS idrp_item_type_desc,
ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
sim_to_sears_division_desc AS sim_to_sears_division_desc,
sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
centrally_stocked_ind AS centrally_stocked_ind,
outbound_830_duration_nbr AS outbound_830_duration_nbr,
outbound_830_duration_ind AS outbound_830_duration_ind,   
rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr,
sales_performance_segment_cd AS sales_performance_segment_cd,
format_exclusive_cd AS format_exclusive_cd,
item_program_cd AS item_program_cd,
key_program_cd AS key_program_cd,
size_nbr AS size_nbr,
style_nbr AS style_nbr,
season_cd AS season_cd ,
season_year_nbr AS season_year_nbr ,
sub_season_id AS sub_season_id ,
(attr_ksn_id_50 IS NULL ?'':value_nm_50) AS color_ds;
---------------------------------------------------------------------------------------------------------------------------------------
		
gen_attribute_relate_curr_tbl_ksn_valu_1610 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_1610 
    GENERATE 
        value_nm AS value_nm_1610,
        value_definition_tx AS value_definition_tx_1610,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_1610,
        attr_ksn_id AS attr_ksn_id_1610;


group_data_by_ksn_id_to_find_tire_size_ds  =
    GROUP gen_attribute_relate_curr_tbl_ksn_valu_1610
    BY attr_ksn_id_1610;

flatten_data_to_find_tire_size_ds =
        FOREACH group_data_by_ksn_id_to_find_tire_size_ds
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER gen_attribute_relate_curr_tbl_ksn_valu_1610 BY attribute_relate_alternate_id_1610 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };
		
		
join_with_attr_id_1610 = 
    JOIN work__idrp_eligible_item_ksn_attribute_step14b BY ksn_id 
         LEFT OUTER ,
         flatten_data_to_find_tire_size_ds BY attr_ksn_id_1610 PARALLEL $NUM_PARALLEL;
	 
work__idrp_eligible_item_ksn_attribute_step15b = 
    FOREACH join_with_attr_id_1610
    GENERATE 
ksn_id AS ksn_id ,
shc_item_id AS shc_item_id , 
sears_business_nbr AS sears_business_nbr ,
sears_business_desc AS sears_business_desc,
sears_division_nbr AS sears_division_nbr,
sears_division_desc AS sears_division_desc,
sears_line_nbr AS sears_line_nbr,
sears_line_desc AS sears_line_desc,
sears_sub_line_nbr AS sears_sub_line_nbr,
sears_sub_line_desc AS sears_sub_line_desc,
sears_class_nbr AS sears_class_nbr,
sears_class_desc AS sears_class_desc,
sears_item_nbr AS sears_item_nbr,
sears_sku_nbr AS sears_sku_nbr ,
sears_division_item_id AS sears_division_item_id,
sears_division_item_sku_id AS sears_division_item_sku_id,
shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
distribution_type_cd AS distribution_type_cd,
special_order_candidate_ind AS special_order_candidate_ind,
item_emp_ind AS item_emp_ind,
easy_order_ind AS easy_order_ind,
warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
rapid_item_ind AS rapid_item_ind ,
constrained_item_ind AS constrained_item_ind,
idrp_item_type_desc AS idrp_item_type_desc,
ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
sim_to_sears_division_desc AS sim_to_sears_division_desc,
sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
centrally_stocked_ind AS centrally_stocked_ind,
outbound_830_duration_nbr AS outbound_830_duration_nbr,
outbound_830_duration_ind AS outbound_830_duration_ind,   
rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr,
sales_performance_segment_cd AS sales_performance_segment_cd,
format_exclusive_cd AS format_exclusive_cd,
item_program_cd AS item_program_cd,
key_program_cd AS key_program_cd,
size_nbr AS size_nbr,
style_nbr AS style_nbr,
season_cd AS season_cd ,
season_year_nbr AS season_year_nbr ,
sub_season_id AS sub_season_id ,
color_ds AS color_ds,       
(attr_ksn_id_1610 IS NULL ?'':value_nm_1610 )AS tire_size_ds;
---------------------------------------------------------------------------------------------------------------------------------------------

gen_attribute_relate_curr_tbl_ksn_valu_270 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_270
    GENERATE 
        value_nm AS value_nm,
		value_definition_tx,
        attr_package_id AS attr_package_id_270,
		attr_ksn_id AS attr_ksn_id_270,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_270;

group_data_by_package_id_to_find_sears_price  =
    GROUP gen_attribute_relate_curr_tbl_ksn_valu_270
    BY attr_ksn_id_270;

flatten_data_to_find_sears_price =
        FOREACH group_data_by_package_id_to_find_sears_price
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER gen_attribute_relate_curr_tbl_ksn_valu_270 BY attribute_relate_alternate_id_270 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };
		
		
join_with_attr_id_270 = 
    JOIN work__idrp_eligible_item_ksn_attribute_step15b BY ksn_id
	 LEFT OUTER ,
	 flatten_data_to_find_sears_price BY attr_ksn_id_270 PARALLEL $NUM_PARALLEL;


work__idrp_eligible_item_ksn_attribute_step16b = 
    FOREACH join_with_attr_id_270 
    GENERATE
ksn_id AS ksn_id ,
shc_item_id AS shc_item_id , 
sears_business_nbr AS sears_business_nbr ,
sears_business_desc AS sears_business_desc,
sears_division_nbr AS sears_division_nbr,
sears_division_desc AS sears_division_desc,
sears_line_nbr AS sears_line_nbr,
sears_line_desc AS sears_line_desc,
sears_sub_line_nbr AS sears_sub_line_nbr,
sears_sub_line_desc AS sears_sub_line_desc,
sears_class_nbr AS sears_class_nbr,
sears_class_desc AS sears_class_desc,
sears_item_nbr AS sears_item_nbr,
sears_sku_nbr AS sears_sku_nbr ,
sears_division_item_id AS sears_division_item_id,
sears_division_item_sku_id AS sears_division_item_sku_id,
shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
distribution_type_cd AS distribution_type_cd,
special_order_candidate_ind AS special_order_candidate_ind,
item_emp_ind AS item_emp_ind,
easy_order_ind AS easy_order_ind,
warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
rapid_item_ind AS rapid_item_ind ,
constrained_item_ind AS constrained_item_ind,
idrp_item_type_desc AS idrp_item_type_desc,
ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
sim_to_sears_division_desc AS sim_to_sears_division_desc,
sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
centrally_stocked_ind AS centrally_stocked_ind,
outbound_830_duration_nbr AS outbound_830_duration_nbr,
outbound_830_duration_ind AS outbound_830_duration_ind,   
rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr,
sales_performance_segment_cd AS sales_performance_segment_cd,
format_exclusive_cd AS format_exclusive_cd,
item_program_cd AS item_program_cd,
key_program_cd AS key_program_cd,
size_nbr AS size_nbr,
style_nbr AS style_nbr,
season_cd AS season_cd ,
season_year_nbr AS season_year_nbr ,
sub_season_id AS sub_season_id ,
color_ds AS color_desc,       
tire_size_ds AS tire_size_desc,
(attr_ksn_id_270 IS NULL ?'':value_definition_tx) AS sears_price_type_desc ;

------------------------------------------------------------------ IPS-4007 Heena Salim -------------------------------------------------------------------------

gen_attribute_relate_curr_tbl_ksn_valu_3710 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_3710
    GENERATE 
        value_nm AS value_nm,
        attr_ksn_id AS attr_ksn_id_3710,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_3710;   --IPS 4007

group_attribute_relate_curr_tbl_ksn_valu_3710_by_ksn  =
    GROUP gen_attribute_relate_curr_tbl_ksn_valu_3710
    BY attr_ksn_id_3710;						       --IPS 4007

work__idrp_eligible_item_ksn_attribute_step17a =
        FOREACH group_attribute_relate_curr_tbl_ksn_valu_3710_by_ksn
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER gen_attribute_relate_curr_tbl_ksn_valu_3710 BY attribute_relate_alternate_id_3710 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };                                                                 --IPS 4007

           
join_with_attr_id_3710 = 
    JOIN work__idrp_eligible_item_ksn_attribute_step16b BY ksn_id
             LEFT OUTER ,
             work__idrp_eligible_item_ksn_attribute_step17a BY attr_ksn_id_3710 PARALLEL $NUM_PARALLEL; --IPS 4007


work__idrp_eligible_item_ksn_attribute_step17b = 
    FOREACH join_with_attr_id_3710 
    GENERATE
ksn_id AS ksn_id ,
shc_item_id AS shc_item_id , 
sears_business_nbr AS sears_business_nbr ,
sears_business_desc AS sears_business_desc,
sears_division_nbr AS sears_division_nbr,
sears_division_desc AS sears_division_desc,
sears_line_nbr AS sears_line_nbr,
sears_line_desc AS sears_line_desc,
sears_sub_line_nbr AS sears_sub_line_nbr,
sears_sub_line_desc AS sears_sub_line_desc,
sears_class_nbr AS sears_class_nbr,
sears_class_desc AS sears_class_desc,
sears_item_nbr AS sears_item_nbr,
sears_sku_nbr AS sears_sku_nbr ,
sears_division_item_id AS sears_division_item_id,
sears_division_item_sku_id AS sears_division_item_sku_id,
shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd ,
distribution_type_cd AS distribution_type_cd,
special_order_candidate_ind AS special_order_candidate_ind,
item_emp_ind AS item_emp_ind,
easy_order_ind AS easy_order_ind,
warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
rapid_item_ind AS rapid_item_ind ,
constrained_item_ind AS constrained_item_ind,
idrp_item_type_desc AS idrp_item_type_desc,
ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
sim_to_sears_division_desc AS sim_to_sears_division_desc,
sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
centrally_stocked_ind AS centrally_stocked_ind,
outbound_830_duration_nbr AS outbound_830_duration_nbr,
outbound_830_duration_ind AS outbound_830_duration_ind,
rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr,
sales_performance_segment_cd AS sales_performance_segment_cd,
format_exclusive_cd AS format_exclusive_cd,
item_program_cd AS item_program_cd,
key_program_cd AS key_program_cd,
size_nbr AS size_nbr,
style_nbr AS style_nbr,
season_cd AS season_cd ,
season_year_nbr AS season_year_nbr ,
sub_season_id AS sub_season_id ,
color_desc,
tire_size_desc,
sears_price_type_desc,
(attr_ksn_id_3710 IS NULL ? 0 : (work__idrp_eligible_item_ksn_attribute_step17a::take_first_row::value_nm == 'YES' ? 1 : 0)) AS single_item_replen_ind;  --IPS 4007

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
			
join_replenishment_day_and_vend_pack_combined =
	CROSS 	
		smith__idrp_item_eligibility_batchdate_data,	
		smith__idrp_vend_pack_combined_data;

filter_on_timestamp_join_replenishment_day_and_vend_pack_combined = 		
    FILTER join_replenishment_day_and_vend_pack_combined 
	BY (TRIM(smith__idrp_item_eligibility_batchdate_data::processing_ts) >= TRIM(smith__idrp_vend_pack_combined_data::effective_ts) AND
        TRIM(smith__idrp_item_eligibility_batchdate_data::processing_ts) <= TRIM(smith__idrp_vend_pack_combined_data::expiration_ts));

work__idrp_filtered_vend_packs = 
    FOREACH filter_on_timestamp_join_replenishment_day_and_vend_pack_combined
    GENERATE	
		vendor_package_id AS vendor_package_id,
        aprk_id AS aprk_id,
        carton_per_layer_qty AS carton_per_layer_qty,
        flow_type_cd AS flow_type_cd,
        import_cd AS import_cd,
        ksn_id AS ksn_id,
        ksn_package_id AS ksn_package_id,
        layer_per_pallet_qty AS layer_per_pallet_qty,
        order_duns_nbr AS order_duns_nbr,
        owner_cd AS owner_cd,
        purchase_status_cd AS purchase_status_cd,
        purchase_status_dt AS purchase_status_dt,
        service_area_restriction_model_id AS service_area_restriction_model_id,
        vendor_carton_qty AS vendor_carton_qty,
        vendor_stock_nbr AS vendor_stock_nbr,
		(shc_item_id is NULL ? '': shc_item_id) AS shc_item_id,
		ksn_purchase_status_cd as ksn_purchase_status_cd,
	dotcom_allocation_ind as dotcom_allocation_ind,
        package_id AS package_id;
--CR 3640 Change to Inner Join From Left Outer Join

join2 = JOIN work__idrp_eligible_item_ksn_attribute_step17b BY ksn_id, work__idrp_filtered_vend_packs BY ksn_id ;

work__idrp_eligible_item_vend_pack_attribute_step1 = FOREACH join2 GENERATE

work__idrp_eligible_item_ksn_attribute_step17b::shc_item_id AS shc_item_id ,
work__idrp_eligible_item_ksn_attribute_step17b::ksn_id AS ksn_id ,
work__idrp_filtered_vend_packs::vendor_package_id AS vendor_package_id ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_business_nbr AS sears_business_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_business_desc AS sears_business_desc ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_division_nbr AS sears_division_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_division_desc AS sears_division_desc ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_line_nbr AS sears_line_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_line_desc AS sears_line_desc ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_sub_line_nbr AS sears_sub_line_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_sub_line_desc AS sears_sub_line_desc ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_class_nbr AS sears_class_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_class_desc AS sears_class_desc ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_item_nbr AS sears_item_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_sku_nbr AS sears_sku_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_division_item_id AS sears_division_item_id ,
work__idrp_eligible_item_ksn_attribute_step17b::sears_division_item_sku_id AS sears_division_item_sku_id ,
work__idrp_eligible_item_ksn_attribute_step17b::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd ,
work__idrp_eligible_item_ksn_attribute_step17b::distribution_type_cd AS distribution_type_cd ,
work__idrp_eligible_item_ksn_attribute_step17b::special_order_candidate_ind AS special_order_candidate_ind ,
work__idrp_eligible_item_ksn_attribute_step17b::item_emp_ind AS item_emp_ind ,
work__idrp_eligible_item_ksn_attribute_step17b::easy_order_ind AS easy_order_ind ,
work__idrp_eligible_item_ksn_attribute_step17b::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd ,
work__idrp_eligible_item_ksn_attribute_step17b::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd ,
work__idrp_eligible_item_ksn_attribute_step17b::rapid_item_ind AS rapid_item_ind ,
work__idrp_eligible_item_ksn_attribute_step17b::constrained_item_ind AS constrained_item_ind ,
work__idrp_eligible_item_ksn_attribute_step17b::idrp_item_type_desc AS idrp_item_type_desc ,
work__idrp_eligible_item_ksn_attribute_step17b::ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id ,
work__idrp_eligible_item_ksn_attribute_step17b::ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc ,
work__idrp_eligible_item_ksn_attribute_step17b::ima_sim_to_factor_qty AS ima_sim_to_factor_qty ,
work__idrp_eligible_item_ksn_attribute_step17b::sim_to_sears_division_nbr AS sim_to_sears_division_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::sim_to_sears_division_desc AS sim_to_sears_division_desc ,
work__idrp_eligible_item_ksn_attribute_step17b::sim_to_sears_item_nbr AS sim_to_sears_item_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id ,
work__idrp_eligible_item_ksn_attribute_step17b::sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
work__idrp_eligible_item_ksn_attribute_step17b::centrally_stocked_ind AS centrally_stocked_ind ,
work__idrp_eligible_item_ksn_attribute_step17b::outbound_830_duration_nbr AS outbound_830_duration_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::outbound_830_duration_ind AS outbound_830_duration_ind ,
work__idrp_eligible_item_ksn_attribute_step17b::rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::sales_performance_segment_cd AS sales_performance_segment_cd ,
work__idrp_eligible_item_ksn_attribute_step17b::format_exclusive_cd AS format_exclusive_cd ,
work__idrp_eligible_item_ksn_attribute_step17b::item_program_cd AS item_program_cd ,
work__idrp_eligible_item_ksn_attribute_step17b::key_program_cd AS key_program_cd ,
work__idrp_eligible_item_ksn_attribute_step17b::size_nbr AS size_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::style_nbr AS style_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::season_cd AS season_cd ,
work__idrp_eligible_item_ksn_attribute_step17b::season_year_nbr AS season_year_nbr ,
work__idrp_eligible_item_ksn_attribute_step17b::sub_season_id AS sub_season_id ,
work__idrp_eligible_item_ksn_attribute_step17b::work__idrp_eligible_item_ksn_attribute_step16b::color_desc,
work__idrp_eligible_item_ksn_attribute_step17b::work__idrp_eligible_item_ksn_attribute_step16b::tire_size_desc AS tire_size_desc ,
work__idrp_eligible_item_ksn_attribute_step17b::work__idrp_eligible_item_ksn_attribute_step16b::sears_price_type_desc AS sears_price_type_desc,
work__idrp_eligible_item_ksn_attribute_step17b::single_item_replen_ind,    -- IPS-4007
work__idrp_filtered_vend_packs::package_id AS package_id;

------------------------------------------------------------------------------------------------------------------------------

gen_gold__item_package_current_data = 
    FOREACH gold__item_package_current_data
	GENERATE
	    package_id,
		ksn_id,
		uom_cd,
		package_cube_volume_inch_qty,
		package_weight_pounds_qty;

--CR 3640 Change the order of join to get the vendor package and package dimension values
		
join_step1_package = JOIN work__idrp_eligible_item_vend_pack_attribute_step1 BY package_id LEFT OUTER , gen_gold__item_package_current_data BY package_id ;

work__idrp_eligible_item_vend_pack_attribute_step2 = FOREACH join_step1_package GENERATE

work__idrp_eligible_item_vend_pack_attribute_step1::ksn_id AS ksn_id ,
work__idrp_eligible_item_vend_pack_attribute_step1::shc_item_id AS shc_item_id ,
work__idrp_eligible_item_vend_pack_attribute_step1::vendor_package_id AS vendor_package_id , 
work__idrp_eligible_item_vend_pack_attribute_step1::sears_business_nbr AS sears_business_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_business_desc AS sears_business_desc,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_division_nbr AS sears_division_nbr,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_division_desc AS sears_division_desc,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_line_nbr AS sears_line_nbr,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_line_desc AS sears_line_desc,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_sub_line_nbr AS sears_sub_line_nbr,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_sub_line_desc AS sears_sub_line_desc,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_class_nbr AS sears_class_nbr,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_class_desc AS sears_class_desc,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_item_nbr AS sears_item_nbr,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_sku_nbr AS sears_sku_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_division_item_id AS sears_division_item_id,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_division_item_sku_id AS sears_division_item_sku_id,
work__idrp_eligible_item_vend_pack_attribute_step1::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
work__idrp_eligible_item_vend_pack_attribute_step1::distribution_type_cd AS distribution_type_cd,
work__idrp_eligible_item_vend_pack_attribute_step1::special_order_candidate_ind AS special_order_candidate_ind,
work__idrp_eligible_item_vend_pack_attribute_step1::item_emp_ind AS item_emp_ind,
work__idrp_eligible_item_vend_pack_attribute_step1::easy_order_ind AS easy_order_ind,
work__idrp_eligible_item_vend_pack_attribute_step1::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
work__idrp_eligible_item_vend_pack_attribute_step1::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
work__idrp_eligible_item_vend_pack_attribute_step1::rapid_item_ind AS rapid_item_ind ,
work__idrp_eligible_item_vend_pack_attribute_step1::constrained_item_ind AS constrained_item_ind,
work__idrp_eligible_item_vend_pack_attribute_step1::idrp_item_type_desc AS idrp_item_type_desc,
work__idrp_eligible_item_vend_pack_attribute_step1::ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
work__idrp_eligible_item_vend_pack_attribute_step1::ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
work__idrp_eligible_item_vend_pack_attribute_step1::ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
work__idrp_eligible_item_vend_pack_attribute_step1::sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
work__idrp_eligible_item_vend_pack_attribute_step1::sim_to_sears_division_desc AS sim_to_sears_division_desc,
work__idrp_eligible_item_vend_pack_attribute_step1::sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
work__idrp_eligible_item_vend_pack_attribute_step1::sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
work__idrp_eligible_item_vend_pack_attribute_step1::sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
work__idrp_eligible_item_vend_pack_attribute_step1::sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
work__idrp_eligible_item_vend_pack_attribute_step1::centrally_stocked_ind AS centrally_stocked_ind,
work__idrp_eligible_item_vend_pack_attribute_step1::outbound_830_duration_nbr AS outbound_830_duration_nbr,
work__idrp_eligible_item_vend_pack_attribute_step1::outbound_830_duration_ind AS outbound_830_duration_ind,   
work__idrp_eligible_item_vend_pack_attribute_step1::rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr,
work__idrp_eligible_item_vend_pack_attribute_step1::sales_performance_segment_cd AS sales_performance_segment_cd,
work__idrp_eligible_item_vend_pack_attribute_step1::format_exclusive_cd AS format_exclusive_cd,
work__idrp_eligible_item_vend_pack_attribute_step1::item_program_cd AS item_program_cd,
work__idrp_eligible_item_vend_pack_attribute_step1::key_program_cd AS key_program_cd,
work__idrp_eligible_item_vend_pack_attribute_step1::size_nbr AS size_nbr,
work__idrp_eligible_item_vend_pack_attribute_step1::style_nbr AS style_nbr,
work__idrp_eligible_item_vend_pack_attribute_step1::season_cd AS season_cd ,
work__idrp_eligible_item_vend_pack_attribute_step1::season_year_nbr AS season_year_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step1::sub_season_id AS sub_season_id ,
work__idrp_eligible_item_vend_pack_attribute_step1::work__idrp_eligible_item_ksn_attribute_step17b::work__idrp_eligible_item_ksn_attribute_step16b::color_desc AS color_desc,       
work__idrp_eligible_item_vend_pack_attribute_step1::tire_size_desc AS tire_size_desc,
work__idrp_eligible_item_vend_pack_attribute_step1::sears_price_type_desc AS sears_price_type_desc,
work__idrp_eligible_item_vend_pack_attribute_step1::work__idrp_eligible_item_ksn_attribute_step17b::single_item_replen_ind,  -- IPS-4007
work__idrp_eligible_item_vend_pack_attribute_step1::package_id AS package_id,
((IsNull(gen_gold__item_package_current_data::package_id,'') != '') ? gen_gold__item_package_current_data::uom_cd : '' ) AS uom_cd,
((IsNull(gen_gold__item_package_current_data::package_id,'') != '')? gen_gold__item_package_current_data::package_cube_volume_inch_qty : '' ) AS package_cube_volume_inch_qty,
((IsNull(gen_gold__item_package_current_data::package_id,'') != '')? gen_gold__item_package_current_data::package_weight_pounds_qty : '' ) AS package_weight_pounds_qty;

-------------------------------------------------------------------------------------------------------------------------------------------

--CR 3170
gold__item_attribute_relate_current_data_filter_10 =
    FILTER gold__item_attribute_relate_current_data
        BY  (IsNull(package_id,'') != ''
            AND
            attribute_id == '10'
            AND
            (
             '$CURRENT_TIMESTAMP' >= effective_ts 
              AND 
              '$CURRENT_TIMESTAMP' <= expiration_ts 
            )
            );

gen_attribute_relate_curr_tbl_ksn_valu_10 = 
    FOREACH gold__item_attribute_relate_current_data_filter_10
    GENERATE 
        value_nm AS value_nm,
        package_id AS attr_package_id_10,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_10;

group_data_by_package_id_to_find_brand_ds  =
    GROUP gen_attribute_relate_curr_tbl_ksn_valu_10
    BY attr_package_id_10;

work__idrp_eligible_item_vend_pack_attribute_step3a =
        FOREACH group_data_by_package_id_to_find_brand_ds
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER gen_attribute_relate_curr_tbl_ksn_valu_10 BY attribute_relate_alternate_id_10 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };

join_with_attr_id_10 = 
    JOIN work__idrp_eligible_item_vend_pack_attribute_step2 BY package_id
	 LEFT OUTER ,
	 work__idrp_eligible_item_vend_pack_attribute_step3a BY attr_package_id_10 PARALLEL $NUM_PARALLEL;

work__idrp_eligible_item_vend_pack_attribute_step3b = 
	FOREACH join_with_attr_id_10
	GENERATE 
work__idrp_eligible_item_vend_pack_attribute_step2::ksn_id AS ksn_id ,
work__idrp_eligible_item_vend_pack_attribute_step2::shc_item_id AS shc_item_id ,
work__idrp_eligible_item_vend_pack_attribute_step2::vendor_package_id AS vendor_package_id , 
work__idrp_eligible_item_vend_pack_attribute_step2::sears_business_nbr AS sears_business_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_business_desc AS sears_business_desc,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_division_nbr AS sears_division_nbr,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_division_desc AS sears_division_desc,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_line_nbr AS sears_line_nbr,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_line_desc AS sears_line_desc,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_sub_line_nbr AS sears_sub_line_nbr,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_sub_line_desc AS sears_sub_line_desc,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_class_nbr AS sears_class_nbr,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_class_desc AS sears_class_desc,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_item_nbr AS sears_item_nbr,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_sku_nbr AS sears_sku_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_division_item_id AS sears_division_item_id,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_division_item_sku_id AS sears_division_item_sku_id,
work__idrp_eligible_item_vend_pack_attribute_step2::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd , 
work__idrp_eligible_item_vend_pack_attribute_step2::distribution_type_cd AS distribution_type_cd,
work__idrp_eligible_item_vend_pack_attribute_step2::special_order_candidate_ind AS special_order_candidate_ind,
work__idrp_eligible_item_vend_pack_attribute_step2::item_emp_ind AS item_emp_ind,
work__idrp_eligible_item_vend_pack_attribute_step2::easy_order_ind AS easy_order_ind,
work__idrp_eligible_item_vend_pack_attribute_step2::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
work__idrp_eligible_item_vend_pack_attribute_step2::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
work__idrp_eligible_item_vend_pack_attribute_step2::rapid_item_ind AS rapid_item_ind ,
work__idrp_eligible_item_vend_pack_attribute_step2::constrained_item_ind AS constrained_item_ind,
work__idrp_eligible_item_vend_pack_attribute_step2::idrp_item_type_desc AS idrp_item_type_desc,
work__idrp_eligible_item_vend_pack_attribute_step2::ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id,
work__idrp_eligible_item_vend_pack_attribute_step2::ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc,
work__idrp_eligible_item_vend_pack_attribute_step2::ima_sim_to_factor_qty AS ima_sim_to_factor_qty,
work__idrp_eligible_item_vend_pack_attribute_step2::sim_to_sears_division_nbr AS sim_to_sears_division_nbr,
work__idrp_eligible_item_vend_pack_attribute_step2::sim_to_sears_division_desc AS sim_to_sears_division_desc,
work__idrp_eligible_item_vend_pack_attribute_step2::sim_to_sears_item_nbr AS sim_to_sears_item_nbr,
work__idrp_eligible_item_vend_pack_attribute_step2::sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr,
work__idrp_eligible_item_vend_pack_attribute_step2::sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id,
work__idrp_eligible_item_vend_pack_attribute_step2::sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
work__idrp_eligible_item_vend_pack_attribute_step2::centrally_stocked_ind AS centrally_stocked_ind,
work__idrp_eligible_item_vend_pack_attribute_step2::outbound_830_duration_nbr AS outbound_830_duration_nbr,
work__idrp_eligible_item_vend_pack_attribute_step2::outbound_830_duration_ind AS outbound_830_duration_ind,   
work__idrp_eligible_item_vend_pack_attribute_step2::rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr,
work__idrp_eligible_item_vend_pack_attribute_step2::sales_performance_segment_cd AS sales_performance_segment_cd,
work__idrp_eligible_item_vend_pack_attribute_step2::format_exclusive_cd AS format_exclusive_cd,
work__idrp_eligible_item_vend_pack_attribute_step2::item_program_cd AS item_program_cd,
work__idrp_eligible_item_vend_pack_attribute_step2::key_program_cd AS key_program_cd,
work__idrp_eligible_item_vend_pack_attribute_step2::size_nbr AS size_nbr,
work__idrp_eligible_item_vend_pack_attribute_step2::style_nbr AS style_nbr,
work__idrp_eligible_item_vend_pack_attribute_step2::season_cd AS season_cd ,
work__idrp_eligible_item_vend_pack_attribute_step2::season_year_nbr AS season_year_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step2::sub_season_id AS sub_season_id ,
((IsNull(work__idrp_eligible_item_vend_pack_attribute_step3a::take_first_row::attr_package_id_10,'') != '' ) ? value_nm : '' ) AS brand_desc ,
work__idrp_eligible_item_vend_pack_attribute_step2::color_desc AS color_desc,       
work__idrp_eligible_item_vend_pack_attribute_step2::tire_size_desc AS tire_size_desc,
work__idrp_eligible_item_vend_pack_attribute_step2::sears_price_type_desc AS sears_price_type_desc,
work__idrp_eligible_item_vend_pack_attribute_step2::work__idrp_eligible_item_vend_pack_attribute_step1::work__idrp_eligible_item_ksn_attribute_step17b::single_item_replen_ind ,   -- IPS-4007
work__idrp_eligible_item_vend_pack_attribute_step2::uom_cd AS uom_cd,
work__idrp_eligible_item_vend_pack_attribute_step2::package_cube_volume_inch_qty AS package_cube_volume_inch_qty,
work__idrp_eligible_item_vend_pack_attribute_step2::package_weight_pounds_qty AS package_weight_pounds_qty;	 
-----------------------------------------------------------------------------------------------------------------------------------------	
		
join3 = JOIN work__idrp_eligible_item_vend_pack_attribute_step3b BY vendor_package_id LEFT OUTER,  smith__idrp_i2k_valid_rebuy_vendor_package_ids_current_data BY vendor_pack_id ;

work__idrp_eligible_item_vend_pack_attribute_step4 = FOREACH join3 GENERATE
work__idrp_eligible_item_vend_pack_attribute_step3b::shc_item_id AS shc_item_id ,
work__idrp_eligible_item_vend_pack_attribute_step3b::ksn_id AS ksn_id ,
work__idrp_eligible_item_vend_pack_attribute_step3b::vendor_package_id AS vendor_package_id ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_business_nbr AS sears_business_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_business_desc AS sears_business_desc ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_division_nbr AS sears_division_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_division_desc AS sears_division_desc ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_line_nbr AS sears_line_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_line_desc AS sears_line_desc ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_sub_line_nbr AS sears_sub_line_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_sub_line_desc AS sears_sub_line_desc ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_class_nbr AS sears_class_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_class_desc AS sears_class_desc ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_item_nbr AS sears_item_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_sku_nbr AS sears_sku_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_division_item_id AS sears_division_item_id ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_division_item_sku_id AS sears_division_item_sku_id ,
work__idrp_eligible_item_vend_pack_attribute_step3b::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd ,
work__idrp_eligible_item_vend_pack_attribute_step3b::distribution_type_cd AS distribution_type_cd ,
work__idrp_eligible_item_vend_pack_attribute_step3b::special_order_candidate_ind AS special_order_candidate_ind ,
work__idrp_eligible_item_vend_pack_attribute_step3b::item_emp_ind AS item_emp_ind ,
work__idrp_eligible_item_vend_pack_attribute_step3b::easy_order_ind AS easy_order_ind ,
work__idrp_eligible_item_vend_pack_attribute_step3b::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd ,
work__idrp_eligible_item_vend_pack_attribute_step3b::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd ,
work__idrp_eligible_item_vend_pack_attribute_step3b::rapid_item_ind AS rapid_item_ind ,
work__idrp_eligible_item_vend_pack_attribute_step3b::constrained_item_ind AS constrained_item_ind ,
work__idrp_eligible_item_vend_pack_attribute_step3b::idrp_item_type_desc AS idrp_item_type_desc ,
work__idrp_eligible_item_vend_pack_attribute_step3b::ima_sim_to_shc_item_id AS ima_sim_to_shc_item_id ,
work__idrp_eligible_item_vend_pack_attribute_step3b::ima_sim_to_shc_item_id_desc AS ima_sim_to_shc_item_id_desc ,
work__idrp_eligible_item_vend_pack_attribute_step3b::ima_sim_to_factor_qty AS ima_sim_to_factor_qty ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sim_to_sears_division_nbr AS sim_to_sears_division_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sim_to_sears_division_desc AS sim_to_sears_division_desc ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sim_to_sears_item_nbr AS sim_to_sears_item_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sim_to_sears_sku_nbr AS sim_to_sears_sku_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sim_to_sears_division_item_sku_id AS sim_to_sears_division_item_sku_id ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sim_to_sears_division_item_id AS sim_to_sears_division_item_id ,
work__idrp_eligible_item_vend_pack_attribute_step3b::centrally_stocked_ind AS centrally_stocked_ind ,
work__idrp_eligible_item_vend_pack_attribute_step3b::outbound_830_duration_nbr AS outbound_830_duration_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::outbound_830_duration_ind AS outbound_830_duration_ind ,
work__idrp_eligible_item_vend_pack_attribute_step3b::rapid_freeze_duration_nbr AS rapid_freeze_duration_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sales_performance_segment_cd AS sales_performance_segment_cd ,
work__idrp_eligible_item_vend_pack_attribute_step3b::format_exclusive_cd AS format_exclusive_cd ,
work__idrp_eligible_item_vend_pack_attribute_step3b::item_program_cd AS item_program_cd ,
work__idrp_eligible_item_vend_pack_attribute_step3b::key_program_cd AS key_program_cd ,
work__idrp_eligible_item_vend_pack_attribute_step3b::size_nbr AS size_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::style_nbr AS style_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::season_cd AS season_cd ,
work__idrp_eligible_item_vend_pack_attribute_step3b::season_year_nbr AS season_year_nbr ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sub_season_id AS sub_season_id ,
work__idrp_eligible_item_vend_pack_attribute_step3b::brand_desc AS brand_desc ,
work__idrp_eligible_item_vend_pack_attribute_step3b::color_desc AS color_desc ,
work__idrp_eligible_item_vend_pack_attribute_step3b::tire_size_desc AS tire_size_desc ,
work__idrp_eligible_item_vend_pack_attribute_step3b::sears_price_type_desc AS sears_price_type_desc ,
work__idrp_eligible_item_vend_pack_attribute_step3b::uom_cd AS uom_cd ,
work__idrp_eligible_item_vend_pack_attribute_step3b::package_cube_volume_inch_qty AS package_cube_volume_inch_qty ,
work__idrp_eligible_item_vend_pack_attribute_step3b::package_weight_pounds_qty AS package_weight_pounds_qty ,
(smith__idrp_i2k_valid_rebuy_vendor_package_ids_current_data::vendor_pack_id IS NULL OR smith__idrp_i2k_valid_rebuy_vendor_package_ids_current_data::vendor_pack_id == '' ? '0':'1') AS import_rebuy_ind ,
work__idrp_eligible_item_vend_pack_attribute_step3b::work__idrp_eligible_item_vend_pack_attribute_step2::work__idrp_eligible_item_vend_pack_attribute_step1::work__idrp_eligible_item_ksn_attribute_step17b::single_item_replen_ind AS single_item_replen_ind,  -- IPS 4007
'$batchid' AS batch_id  ;

----------------------------------------------------------------------------------------------------------------------------------------------

work__idrp_eligible_item_vend_pack_attribute_step4 = DISTINCT work__idrp_eligible_item_vend_pack_attribute_step4 ;

----------------------------------------------------------------------------------------------------------------------------------------------
STORE work__idrp_eligible_item_vend_pack_attribute_step4 INTO '$WORK__IDRP_ELIGIBLE_ITEM_CURRENT_PART_3' USING PigStorage('$FIELD_DELIMITER_PIPE'); 
						 
/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
