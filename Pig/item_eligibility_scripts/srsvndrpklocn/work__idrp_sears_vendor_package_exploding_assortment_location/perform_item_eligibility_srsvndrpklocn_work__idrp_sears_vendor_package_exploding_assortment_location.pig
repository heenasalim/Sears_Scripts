/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_work__idrp_sears_vendor_package_exploding_assortment_location.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Wed Jul 30 03:08:41 EDT 2014
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
#        DATE         BY            		MODIFICATION
#16/10/2014			Siddhivinayak Karpe		CR#3118 Line No 120 & 122 Distinct Condition removed and Vendor PAck ID added in grouping condition
#14/11/2014         Meghana Dhage           CR#3356 Line no 89 Include cross_merchandising_cd = '' OR cross_merchandising_cd is NULL
#17/08/2015			Priyanka Gurjar			CR#4947 Added logic to convert Dummy vendors for EXAS rows and vend pack assignment need to account for TPW dummy vendors
#26/08/2015			Priyanka Gurjar			CR#4947b Limit the warehouse open in the Exploding Assortment logic to TPW (loc format type DC) and CDFC
#01/19/2017			Srujan Dussa			IPS-779 . Adding rim_last_record_create_dt from gold__inventory_rim_daily_current to be included in the Extract File to Shared Items.
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

SET default_parallel $NUM_PARALLEL;

REGISTER $UDF_JAR;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/


work__idrp_sears_vendor_package_location_data = 
     LOAD '$WORK__IDRP_SEARS_VENDOR_PACKAGE_LOCATION_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($WORK__IDRP_SEARS_VENDOR_PACKAGE_LOCATION_SCHEMA);


work__idrp_sears_location_xref_data = 
     LOAD '$WORK__IDRP_SEARS_LOCATION_XREF_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($WORK__IDRP_SEARS_LOCATION_XREF_SCHEMA);
	 
work__idrp_dummy_vend_whse_ref_data = 
	LOAD '$WORK__IDRP_DUMMY_VEND_WHSE_REF_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($WORK__IDRP_DUMMY_VEND_WHSE_REF_SCHEMA);	
	 
work__idrp_sears_location_xref_data = 
	LOAD '$WORK__IDRP_SEARS_LOCATION_XREF_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($WORK__IDRP_SEARS_LOCATION_XREF_SCHEMA);


smith__idrp_ksn_attribute_current_data = 
     LOAD '$SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_SCHEMA);


smith__idrp_eligible_loc_data = 
     LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA); 


smith__idrp_vend_pack_combined_data = 
     LOAD '$SMITH__IDRP_VEND_PACK_COMBINED_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($SMITH__IDRP_VEND_PACK_COMBINED_SCHEMA); 


smith__idrp_vend_pack_dc_combined_data = 
     LOAD '$SMITH__IDRP_VEND_PACK_DC_COMBINED_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($SMITH__IDRP_VEND_PACK_DC_COMBINED_SCHEMA);


smith__idrp_shc_item_combined_data = 
     LOAD '$SMITH__IDRP_SHC_ITEM_COMBINED_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($SMITH__IDRP_SHC_ITEM_COMBINED_SCHEMA); 


smith__idrp_core_ima_assortments_prepacks_exploding_assortment_current_data = 
     LOAD '$SMITH__IDRP_CORE_IMA_ASSORTMENTS_PREPACKS_EXPLODING_ASSORTMENT_CURRENT_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($SMITH__IDRP_CORE_IMA_ASSORTMENTS_PREPACKS_EXPLODING_ASSORTMENT_CURRENT_SCHEMA); 


-------CR4947-----------------------------

work__idrp_sears_vendor_package_location_data_fltr = 
     FILTER work__idrp_sears_vendor_package_location_data 
     BY ((location_level_cd=='STORE' or (location_level_cd=='WAREHOUSE' and (location_format_type_cd == 'DC' or location_format_type_cd == 'CDFC'))) AND active_ind=='Y' AND (IsNull(TRIM(cross_merchandising_cd),'') == '' OR TRIM(cross_merchandising_cd) !='EMP2JIT' OR (TRIM(cross_merchandising_cd)=='EMP2JIT' AND location_format_type_cd!='SINT') OR (TRIM(cross_merchandising_cd)=='EMP2JIT' AND location_format_type_cd=='SINT' AND (dotcom_orderable_cd=='S' OR dotcom_orderable_cd=='B'))));


smith__idrp_eligible_loc_data_fltr = 
       FILTER smith__idrp_eligible_loc_data 
       BY duns_type_cd=='ORD';


join_smith_work = 
     JOIN work__idrp_sears_vendor_package_location_data_fltr BY ((int)sears_division_nbr,TrimLeadingZeros(sears_item_nbr),(int)sears_sku_nbr), 
          smith__idrp_core_ima_assortments_prepacks_exploding_assortment_current_data BY ((int)component_sears_division_nbr,TrimLeadingZeros(component_sears_item_nbr),(int)component_sears_sku_nbr);


work__idrp_sears_vend_pack_exas_step1 = 
      FOREACH join_smith_work 
      GENERATE
              smith__idrp_core_ima_assortments_prepacks_exploding_assortment_current_data::item_id AS shc_item_id,
              smith__idrp_core_ima_assortments_prepacks_exploding_assortment_current_data::exploding_assortment_ksn_id AS exploding_assortment_ksn_id,
              smith__idrp_core_ima_assortments_prepacks_exploding_assortment_current_data::vendor_pack_id AS exploding_assortment_vendor_package_id,
              smith__idrp_core_ima_assortments_prepacks_exploding_assortment_current_data::flow_type_cd AS flowtype_cd,
              smith__idrp_core_ima_assortments_prepacks_exploding_assortment_current_data::sears_division_nbr AS sears_division_nbr,
              smith__idrp_core_ima_assortments_prepacks_exploding_assortment_current_data::sears_item_nbr AS sears_item_nbr,
              smith__idrp_core_ima_assortments_prepacks_exploding_assortment_current_data::sears_sku_nbr AS sears_sku_nbr,
              smith__idrp_core_ima_assortments_prepacks_exploding_assortment_current_data::component_count_nbr as component_count_nbr,
              work__idrp_sears_vendor_package_location_data_fltr::location_id as location_id,
              work__idrp_sears_vendor_package_location_data_fltr::location_format_type_cd as location_format_type_cd,
              work__idrp_sears_vendor_package_location_data_fltr::location_level_cd AS location_level_cd,
              work__idrp_sears_vendor_package_location_data_fltr::location_owner_cd AS location_owner_cd,
              work__idrp_sears_vendor_package_location_data_fltr::sears_location_id AS sears_location_id,
	          work__idrp_sears_vendor_package_location_data_fltr::rim_last_record_creation_dt AS rim_last_record_creation_dt;


group_data = GROUP work__idrp_sears_vend_pack_exas_step1 BY (sears_division_nbr,sears_item_nbr,sears_sku_nbr,location_id,exploding_assortment_vendor_package_id);

work__idrp_sears_vend_pack_exas_step2 = foreach group_data generate FLATTEN (work__idrp_sears_vend_pack_exas_step1) , COUNT(work__idrp_sears_vend_pack_exas_step1) AS eligible_component_count_nbr;


work__idrp_sears_vend_pack_exas_step2 = DISTINCT work__idrp_sears_vend_pack_exas_step2;

work__idrp_sears_vend_pack_exas_step3_fltr = 
      FILTER work__idrp_sears_vend_pack_exas_step2 
      BY (int)component_count_nbr==(int)eligible_component_count_nbr;


work__idrp_sears_vend_pack_exas_step3 = 
      FOREACH work__idrp_sears_vend_pack_exas_step3_fltr 
      GENERATE 
              shc_item_id AS shc_item_id,
              exploding_assortment_ksn_id AS exploding_assortment_ksn_id,
              exploding_assortment_vendor_package_id AS exploding_assortment_vendor_package_id,
              flowtype_cd AS flowtype_cd,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              component_count_nbr AS component_count_nbr,
              location_id AS location_id,
              location_format_type_cd AS location_format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              sears_location_id AS sears_location_id,
	          rim_last_record_creation_dt AS rim_last_record_creation_dt;



outer_join_step3_vend_pack_comb = 
      JOIN work__idrp_sears_vend_pack_exas_step3 BY exploding_assortment_vendor_package_id LEFT OUTER,
           smith__idrp_vend_pack_combined_data BY vendor_package_id;


outer_join_above_vend_pack_dc_comb = 
      JOIN outer_join_step3_vend_pack_comb BY (exploding_assortment_vendor_package_id,location_id) LEFT OUTER,
           smith__idrp_vend_pack_dc_combined_data BY (vendor_package_id,location_nbr);


outer_join_above_shc_item = 
      JOIN outer_join_above_vend_pack_dc_comb BY outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::shc_item_id LEFT OUTER,
           smith__idrp_shc_item_combined_data BY shc_item_id;


outer_join_above_ksn_attr = 
      JOIN outer_join_above_shc_item BY outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::exploding_assortment_ksn_id LEFT OUTER,
           smith__idrp_ksn_attribute_current_data BY ksn_id;


work__idrp_sears_vend_pack_exas_step4 = 
      FOREACH outer_join_above_ksn_attr 
      GENERATE
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::shc_item_id AS shc_item_id,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::exploding_assortment_ksn_id AS exploding_assortment_ksn_id,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::exploding_assortment_vendor_package_id AS exploding_assortment_vendor_package_id,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::flowtype_cd AS flowtype_cd,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::sears_division_nbr AS sears_division_nbr,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::sears_item_nbr AS sears_item_nbr,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::sears_sku_nbr AS sears_sku_nbr,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::component_count_nbr AS component_count_nbr,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::location_id AS location_id,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::location_format_type_cd AS location_format_type_cd,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::location_level_cd AS location_level_cd,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::location_owner_cd AS location_owner_cd,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::sears_location_id AS sears_location_id,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::smith__idrp_vend_pack_combined_data::order_duns_nbr AS order_duns_nbr,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::smith__idrp_vend_pack_combined_data::purchase_status_cd AS vendor_package_purchase_status_cd,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::smith__idrp_vend_pack_combined_data::purchase_status_dt AS vendor_package_purchase_status_dt,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::smith__idrp_vend_pack_combined_data::flow_type_cd AS flow_type_cd,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::smith__idrp_vend_pack_combined_data::owner_cd AS vendor_package_owner_cd,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::smith__idrp_vend_pack_combined_data::flow_type_cd AS vendor_package_flow_type_cd,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::smith__idrp_vend_pack_combined_data::vendor_stock_nbr AS vendor_stock_nbr,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::smith__idrp_vend_pack_combined_data::ksn_package_id AS ksn_package_id,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::smith__idrp_vend_pack_combined_data::vendor_carton_qty AS source_pack_qty,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::smith__idrp_vend_pack_combined_data::vendor_carton_qty AS vendor_package_carton_qty,
              (outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::smith__idrp_vend_pack_combined_data::import_cd=='I' ? '1' : '0') AS import_ind,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::smith__idrp_vend_pack_dc_combined_data::ksn_pack_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::smith__idrp_vend_pack_dc_combined_data::substition_eligibile_ind AS substition_eligibile_ind,
              outer_join_above_shc_item::smith__idrp_shc_item_combined_data::purchase_status_cd AS item_purchase_status_cd,
              outer_join_above_shc_item::smith__idrp_shc_item_combined_data::can_carry_model_id AS can_carry_model_id,
              outer_join_above_shc_item::smith__idrp_shc_item_combined_data::idrp_order_method_cd AS allocation_replenishment_cd,
              smith__idrp_ksn_attribute_current_data::ksn_purchase_status_cd AS ksn_purchase_status_cd,
              smith__idrp_ksn_attribute_current_data::dot_com_allocation_ind AS dot_com_orderable_cd,
	          outer_join_above_shc_item::outer_join_above_vend_pack_dc_comb::outer_join_step3_vend_pack_comb::work__idrp_sears_vend_pack_exas_step3::rim_last_record_creation_dt AS rim_last_record_creation_dt;



join_step4_eligible_loc = 
     JOIN work__idrp_sears_vend_pack_exas_step4 BY order_duns_nbr,
          smith__idrp_eligible_loc_data_fltr BY TrimLeadingZeros(shc_vndr_nbr);



work__idrp_sears_vend_pack_exas_step5 = 
      FOREACH join_step4_eligible_loc 
      GENERATE 
              shc_item_id AS shc_item_id,
              exploding_assortment_ksn_id AS exploding_assortment_ksn_id,
              exploding_assortment_vendor_package_id AS exploding_assortment_vendor_package_id,
              flowtype_cd AS flowtype_cd,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              component_count_nbr AS component_count_nbr,
              location_id AS location_id,
              location_format_type_cd AS location_format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              sears_location_id AS sears_location_id,
              order_duns_nbr AS order_duns_nbr,
              vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              flow_type_cd AS flow_type_cd,
              vendor_package_owner_cd AS vendor_package_owner_cd,
              vendor_package_flow_type_cd AS vendor_package_flow_type_cd,
              vendor_stock_nbr AS vendor_stock_nbr,
              ksn_package_id AS ksn_package_id,
              source_pack_qty AS source_pack_qty,
              vendor_package_carton_qty AS vendor_package_carton_qty,
              import_ind AS import_ind,
              ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              substition_eligibile_ind AS substition_eligibile_ind,
              item_purchase_status_cd AS item_purchase_status_cd,
              can_carry_model_id AS can_carry_model_id,
              allocation_replenishment_cd AS allocation_replenishment_cd,
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              dot_com_orderable_cd AS dot_com_orderable_cd,
              smith__idrp_eligible_loc_data_fltr::loc AS source_location_id,
              smith__idrp_eligible_loc_data_fltr::loc AS purchase_order_vendor_location_id,
              smith__idrp_eligible_loc_data_fltr::loc_lvl_cd AS source_location_level_cd,
              smith__idrp_eligible_loc_data_fltr::srs_vndr_nbr AS sears_source_location_id,
	          rim_last_record_creation_dt AS rim_last_record_creation_dt;

-----------------------CR4947----------------------------------------
			  
jn_work__idrp_dummy_vend_whse_with_sears_nbr = JOIN work__idrp_sears_location_xref_data by (location_id),work__idrp_dummy_vend_whse_ref_data by (warehouse_nbr) using 'replicated';

work__idrp_dummy_vend_whse_with_sears_nbr = foreach jn_work__idrp_dummy_vend_whse_with_sears_nbr generate 
											work__idrp_dummy_vend_whse_ref_data::vendor_nbr as vendor_nbr,
											work__idrp_sears_location_xref_data::sears_location_id as sears_warehouse_nbr,
											work__idrp_dummy_vend_whse_ref_data::warehouse_nbr as shc_warehouse_nbr,
											work__idrp_sears_location_xref_data::location_level_cd as location_level_cd_xref;
											
jn_work__idrp_sears_vp_exas_step5a = JOIN work__idrp_sears_vend_pack_exas_step5 by (sears_source_location_id) LEFT OUTER,work__idrp_dummy_vend_whse_with_sears_nbr by (vendor_nbr) using 'replicated';	

work__idrp_sears_vp_exas_step5a = foreach jn_work__idrp_sears_vp_exas_step5a generate 
											shc_item_id AS shc_item_id,
											exploding_assortment_ksn_id AS exploding_assortment_ksn_id,
											exploding_assortment_vendor_package_id AS exploding_assortment_vendor_package_id,
											flowtype_cd AS flowtype_cd,
											sears_division_nbr AS sears_division_nbr,
											sears_item_nbr AS sears_item_nbr,
											sears_sku_nbr AS sears_sku_nbr,
											component_count_nbr AS component_count_nbr,
											location_id AS location_id,
											location_format_type_cd AS location_format_type_cd,
											location_level_cd AS location_level_cd,
											location_owner_cd AS location_owner_cd,
											sears_location_id AS sears_location_id,
											order_duns_nbr AS order_duns_nbr,
											vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
											vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
											flow_type_cd AS flow_type_cd,
											vendor_package_owner_cd AS vendor_package_owner_cd,
											vendor_package_flow_type_cd AS vendor_package_flow_type_cd,
											vendor_stock_nbr AS vendor_stock_nbr,
											ksn_package_id AS ksn_package_id,
											source_pack_qty AS source_pack_qty,
											vendor_package_carton_qty AS vendor_package_carton_qty,
											import_ind AS import_ind,
											ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
											substition_eligibile_ind AS substition_eligibile_ind,
											item_purchase_status_cd AS item_purchase_status_cd,
											can_carry_model_id AS can_carry_model_id,
											allocation_replenishment_cd AS allocation_replenishment_cd,
											ksn_purchase_status_cd AS ksn_purchase_status_cd,
											dot_com_orderable_cd AS dot_com_orderable_cd,
											purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
											(vendor_nbr is not null ? sears_warehouse_nbr : sears_source_location_id) as sears_source_location_id,
											(vendor_nbr is not null ? shc_warehouse_nbr : source_location_id) as source_location_id,
											(vendor_nbr is not null ? location_level_cd_xref : source_location_level_cd) as source_location_level_cd,
											(vendor_nbr is not null ? sears_source_location_id : '0') as original_source_nbr,
											rim_last_record_creation_dt AS rim_last_record_creation_dt;
											
SPLIT work__idrp_sears_vp_exas_step5a into 
			work__idrp_sears_vp_exas_store_tpw  if location_level_cd == 'STORE' and original_source_nbr!='0',
			work__idrp_sears_vp_exas_store_dc if location_level_cd == 'STORE' and original_source_nbr=='0' and flowtype_cd=='DC',
			work__idrp_sears_vp_exas_store_dsd if location_level_cd == 'STORE' and original_source_nbr=='0' and flowtype_cd!='DC',
			work__idrp_sears_vp_exas_whse if location_level_cd == 'WAREHOUSE' and original_source_nbr=='0';
			
gen_work__idrp_sears_vp_exas_store_tpw = foreach work__idrp_sears_vp_exas_store_tpw generate 	
											sears_division_nbr as sears_division_nbr,
											sears_item_nbr as sears_item_nbr,
											sears_sku_nbr as sears_sku_nbr,
											location_id as location_id,
											sears_source_location_id as sears_source_location_id,
											source_location_id as source_location_id,
											source_location_level_cd as source_location_level_cd,
											rim_last_record_creation_dt AS rim_last_record_creation_dt;
											
dis_gen_work__idrp_sears_vp_exas_store_tpw = DISTINCT gen_work__idrp_sears_vp_exas_store_tpw;
										 
work__idrp_sears_vp_exas_store_tpw_grp = foreach dis_gen_work__idrp_sears_vp_exas_store_tpw	generate			
											sears_division_nbr as sears_division_nbr,
											sears_item_nbr as sears_item_nbr,
											sears_sku_nbr as sears_sku_nbr,
											location_id as location_id,
											sears_source_location_id as sears_source_location_id,
											source_location_id as source_location_id,
											source_location_level_cd as source_location_level_cd,
											rim_last_record_creation_dt AS rim_last_record_creation_dt;
											
jn_work__idrp_sears_vp_exas_store_dc_tpw = JOIN work__idrp_sears_vp_exas_store_dc by (sears_division_nbr,sears_item_nbr,sears_sku_nbr,location_id) LEFT OUTER, work__idrp_sears_vp_exas_store_tpw_grp by  (sears_division_nbr,sears_item_nbr,sears_sku_nbr,location_id);



work__idrp_sears_vp_exas_store_dc_tpw	= foreach jn_work__idrp_sears_vp_exas_store_dc_tpw generate
											work__idrp_sears_vp_exas_store_dc::shc_item_id AS shc_item_id,
											work__idrp_sears_vp_exas_store_dc::exploding_assortment_ksn_id AS exploding_assortment_ksn_id,
											work__idrp_sears_vp_exas_store_dc::exploding_assortment_vendor_package_id AS exploding_assortment_vendor_package_id,
											work__idrp_sears_vp_exas_store_dc::flowtype_cd AS flowtype_cd,
											work__idrp_sears_vp_exas_store_dc::sears_division_nbr AS sears_division_nbr,
											work__idrp_sears_vp_exas_store_dc::sears_item_nbr AS sears_item_nbr,
											work__idrp_sears_vp_exas_store_dc::sears_sku_nbr AS sears_sku_nbr,
											work__idrp_sears_vp_exas_store_dc::component_count_nbr AS component_count_nbr,
											work__idrp_sears_vp_exas_store_dc::location_id AS location_id,
											work__idrp_sears_vp_exas_store_dc::location_format_type_cd AS location_format_type_cd,
											work__idrp_sears_vp_exas_store_dc::location_level_cd AS location_level_cd,
											work__idrp_sears_vp_exas_store_dc::location_owner_cd AS location_owner_cd,
											work__idrp_sears_vp_exas_store_dc::sears_location_id AS sears_location_id,
											work__idrp_sears_vp_exas_store_dc::order_duns_nbr AS order_duns_nbr,
											work__idrp_sears_vp_exas_store_dc::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
											work__idrp_sears_vp_exas_store_dc::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
											work__idrp_sears_vp_exas_store_dc::flow_type_cd AS flow_type_cd,
											work__idrp_sears_vp_exas_store_dc::vendor_package_owner_cd AS vendor_package_owner_cd,
											work__idrp_sears_vp_exas_store_dc::vendor_package_flow_type_cd AS vendor_package_flow_type_cd,
											work__idrp_sears_vp_exas_store_dc::vendor_stock_nbr AS vendor_stock_nbr,
											work__idrp_sears_vp_exas_store_dc::ksn_package_id AS ksn_package_id,
											work__idrp_sears_vp_exas_store_dc::source_pack_qty AS source_pack_qty,
											work__idrp_sears_vp_exas_store_dc::vendor_package_carton_qty AS vendor_package_carton_qty,
											work__idrp_sears_vp_exas_store_dc::import_ind AS import_ind,
											work__idrp_sears_vp_exas_store_dc::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
											work__idrp_sears_vp_exas_store_dc::substition_eligibile_ind AS substition_eligibile_ind,
											work__idrp_sears_vp_exas_store_dc::item_purchase_status_cd AS item_purchase_status_cd,
											work__idrp_sears_vp_exas_store_dc::can_carry_model_id AS can_carry_model_id,
											work__idrp_sears_vp_exas_store_dc::allocation_replenishment_cd AS allocation_replenishment_cd,
											work__idrp_sears_vp_exas_store_dc::ksn_purchase_status_cd AS ksn_purchase_status_cd,
											work__idrp_sears_vp_exas_store_dc::dot_com_orderable_cd AS dot_com_orderable_cd,
											work__idrp_sears_vp_exas_store_dc::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
											( work__idrp_sears_vp_exas_store_tpw_grp::sears_division_nbr is null
												? work__idrp_sears_vp_exas_store_dc::sears_source_location_id
												: work__idrp_sears_vp_exas_store_tpw_grp::sears_source_location_id) as sears_source_location_id,
											( work__idrp_sears_vp_exas_store_tpw_grp::sears_division_nbr is null
												? work__idrp_sears_vp_exas_store_dc::source_location_id
												: work__idrp_sears_vp_exas_store_tpw_grp::source_location_id) as source_location_id, 
											( work__idrp_sears_vp_exas_store_tpw_grp::sears_division_nbr is null
												? work__idrp_sears_vp_exas_store_dc::source_location_level_cd
												:work__idrp_sears_vp_exas_store_tpw_grp::source_location_level_cd) as source_location_level_cd,
											work__idrp_sears_vp_exas_store_dc::original_source_nbr as original_source_nbr,
											work__idrp_sears_vp_exas_store_dc::rim_last_record_creation_dt AS rim_last_record_creation_dt;					
											 
work__idrp_sears_vp_exas_step5b = UNION work__idrp_sears_vp_exas_store_dc_tpw,work__idrp_sears_vp_exas_store_dsd,work__idrp_sears_vp_exas_whse;	

grp_work__idrp_sears_vp_exas_step5b = GROUP work__idrp_sears_vp_exas_step5b by (sears_division_nbr,sears_item_nbr,sears_sku_nbr,location_id);

srt_work__selected_vendor_pack = foreach grp_work__idrp_sears_vp_exas_step5b 
									{
										sorted = ORDER work__idrp_sears_vp_exas_step5b by  vendor_package_purchase_status_cd,vendor_package_purchase_status_dt,exploding_assortment_vendor_package_id DESC;
										unq = LIMIT sorted 1;
										GENERATE FLATTEN (unq);
									};
									
work__selected_vendor_pack = foreach srt_work__selected_vendor_pack generate
										exploding_assortment_vendor_package_id as vendor_package_id,
										location_id as location_id;
										
jn_work__idrp_sears_vend_pack_exas_step5d = JOIN work__idrp_sears_vp_exas_step5b by (exploding_assortment_vendor_package_id,location_id) LEFT OUTER,work__selected_vendor_pack by (vendor_package_id,location_id);

work__idrp_sears_vend_pack_exas_step5d = foreach  jn_work__idrp_sears_vend_pack_exas_step5d generate
											work__idrp_sears_vp_exas_step5b::shc_item_id AS shc_item_id,
											work__idrp_sears_vp_exas_step5b::exploding_assortment_ksn_id AS exploding_assortment_ksn_id,
											work__idrp_sears_vp_exas_step5b::exploding_assortment_vendor_package_id AS exploding_assortment_vendor_package_id,
											work__idrp_sears_vp_exas_step5b::flowtype_cd AS flowtype_cd,
											work__idrp_sears_vp_exas_step5b::sears_division_nbr AS sears_division_nbr,
											work__idrp_sears_vp_exas_step5b::sears_item_nbr AS sears_item_nbr,
											work__idrp_sears_vp_exas_step5b::sears_sku_nbr AS sears_sku_nbr,
											work__idrp_sears_vp_exas_step5b::component_count_nbr AS component_count_nbr,
											work__idrp_sears_vp_exas_step5b::location_id AS location_id,
											work__idrp_sears_vp_exas_step5b::location_format_type_cd AS location_format_type_cd,
											work__idrp_sears_vp_exas_step5b::location_level_cd AS location_level_cd,
											work__idrp_sears_vp_exas_step5b::location_owner_cd AS location_owner_cd,
											work__idrp_sears_vp_exas_step5b::sears_location_id AS sears_location_id,
											work__idrp_sears_vp_exas_step5b::order_duns_nbr AS order_duns_nbr,
											work__idrp_sears_vp_exas_step5b::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
											work__idrp_sears_vp_exas_step5b::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
											work__idrp_sears_vp_exas_step5b::flow_type_cd AS flow_type_cd,
											work__idrp_sears_vp_exas_step5b::vendor_package_owner_cd AS vendor_package_owner_cd,
											work__idrp_sears_vp_exas_step5b::vendor_package_flow_type_cd AS vendor_package_flow_type_cd,
											work__idrp_sears_vp_exas_step5b::vendor_stock_nbr AS vendor_stock_nbr,
											work__idrp_sears_vp_exas_step5b::ksn_package_id AS ksn_package_id,
											work__idrp_sears_vp_exas_step5b::source_pack_qty AS source_pack_qty,
											work__idrp_sears_vp_exas_step5b::vendor_package_carton_qty AS vendor_package_carton_qty,
											work__idrp_sears_vp_exas_step5b::import_ind AS import_ind,
											work__idrp_sears_vp_exas_step5b::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
											work__idrp_sears_vp_exas_step5b::substition_eligibile_ind AS substition_eligibile_ind,
											work__idrp_sears_vp_exas_step5b::item_purchase_status_cd AS item_purchase_status_cd,
											work__idrp_sears_vp_exas_step5b::can_carry_model_id AS can_carry_model_id,
											work__idrp_sears_vp_exas_step5b::allocation_replenishment_cd AS allocation_replenishment_cd,
											work__idrp_sears_vp_exas_step5b::ksn_purchase_status_cd AS ksn_purchase_status_cd,
											work__idrp_sears_vp_exas_step5b::dot_com_orderable_cd AS dot_com_orderable_cd,
											work__idrp_sears_vp_exas_step5b::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
											work__idrp_sears_vp_exas_step5b::sears_source_location_id,
											work__idrp_sears_vp_exas_step5b::source_location_id,
											work__idrp_sears_vp_exas_step5b::source_location_level_cd,
											work__idrp_sears_vp_exas_step5b::original_source_nbr,
											'N'as replenishment_planning_ind,
											work__idrp_sears_vp_exas_step5b::rim_last_record_creation_dt AS rim_last_record_creation_dt;					 
											
work__idrp_sears_vendor_package_exploding_assortment_location = 
      FOREACH work__idrp_sears_vend_pack_exas_step5d 
      GENERATE 
              exploding_assortment_vendor_package_id AS vendor_package_id,
              location_id AS location_id,
              location_format_type_cd AS location_format_type_cd,
              location_level_cd AS location_level_cd,
              location_owner_cd AS location_owner_cd,
              'S' AS source_owner_cd,
              'Y' AS active_ind,
              '$CURRENT_DATE' AS active_ind_change_dt,
              allocation_replenishment_cd AS allocation_replenishment_cd,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              replenishment_planning_ind AS replenishment_planning_ind,
              'N' AS scan_based_trading_ind,
              source_location_id AS source_location_id,
              source_location_level_cd AS source_location_level_cd,
              source_pack_qty AS source_pack_qty,
              vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              flow_type_cd AS flow_type_cd,
              import_ind AS import_ind,
              '0' AS retail_carton_vendor_package_id,
              vendor_package_owner_cd AS vendor_package_owner_cd,
              vendor_stock_nbr AS vendor_stock_nbr,
              shc_item_id AS shc_item_id,
              item_purchase_status_cd AS item_purchase_status_cd,
              can_carry_model_id AS can_carry_model_id,
              '0' AS days_to_check_begin_day_qty,
              '365' AS days_to_check_end_day_qty,
              '' AS reorder_method_cd,
              exploding_assortment_ksn_id AS ksn_id,
              ksn_purchase_status_cd AS ksn_purchase_status_cd,
              '' AS cross_merchandising_cd,
              dot_com_orderable_cd AS dotcom_orderable_cd,
              '' AS kmart_markdown_ind,
              ksn_package_id AS ksn_package_id,
              ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
              '' AS dc_configuration_cd,
              '' AS substitution_eligible_ind,
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              sears_location_id AS sears_location_id,
              sears_source_location_id AS sears_source_location_id,
              '' AS rim_status_cd,
              '' AS stock_type_cd,
              '' AS non_stock_source_cd,
              '' AS dos_item_active_ind,
              '' AS item_reserve_cd,
              '$CURRENT_DATE' AS create_dt,
              '$CURRENT_DATE' AS last_update_dt,
              vendor_package_carton_qty AS vendor_package_carton_qty,
              '' AS special_retail_order_system_ind,
              '' AS shc_item_corporate_owner_cd,
              '' AS distribution_type_cd,
              '' AS only_rsu_distribution_channel_ind,
              '' AS special_order_candidate_ind,
              '' AS item_emp_ind,
              '' AS easy_order_ind,
              '' AS warehouse_sizing_attribute_cd,
              '' AS rapid_item_ind,
              '' AS constrained_item_ind,
              '' AS idrp_item_type_desc,
              '' AS cross_merchandising_attribute_cd,
              '' AS sams_migration_ind,
              '' AS emp_to_jit_ind,
              '' AS rim_flow_ind,
              '' AS source_system_cd,
              '' AS original_source_nbr,
              '' AS item_next_period_on_hand_qty,
              '' AS item_on_order_qty,
              '' AS item_reserve_qty,
              '' AS item_back_order_qty,
              '' AS item_next_period_future_order_qty,
              '' AS item_next_period_in_transit_qty,
              '' AS item_last_receive_dt,
              '' AS item_last_ship_dt,
	          rim_last_record_creation_dt AS rim_last_record_creation_dt,
              '$batchid' AS idrp_batch_id;


STORE work__idrp_sears_vendor_package_exploding_assortment_location 
INTO '$WORK__IDRP_SEARS_VENDOR_PACKAGE_EXPLODING_ASSORTMENT_LOCATION_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
