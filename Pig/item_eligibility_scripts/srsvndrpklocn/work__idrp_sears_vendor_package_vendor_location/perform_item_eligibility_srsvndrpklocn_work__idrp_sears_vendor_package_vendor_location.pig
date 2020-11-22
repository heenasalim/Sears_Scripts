/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_work__idrp_sears_vendor_package_vendor_location.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Thu Jul 31 01:36:08 EDT 2014
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
#		22-10-2014   Priyanka Gurjar CR3205 – eliminate easy order filter
#		29-10-2014	Priyanka Gurjar  CR 3204 - Missing atttributes on sears vendor pack location vendor level rows.Added the logic (refer line:87 to 194)
#		5/112014	Siddhivinayak Karpe	CR 3204 - Code Changed at line 116  group on shc_item_id & source_location_id and order by vendor Pack ID and limit 1
#		18/11/2014	Siddhivinayak Karpe	CR 3204 - Batch Date and Current Date Added in Final output
#       02/12/2014  Meghana Dhage       CR 3415 - Set '1' as vendor_package_carton_qty (Line no: 180)
#       01/19/2017  Srujan Dussa	IPS-779 . Adding rim_last_record_create_dt from gold__inventory_rim_daily_current to be included in the Extract File to Shared Items.
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

SET default_parallel $NUM_PARALLEL;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/


work__idrp_sears_vendor_package_location_data = 
      LOAD '$WORK__IDRP_SEARS_VENDOR_PACKAGE_LOCATION_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_SEARS_VENDOR_PACKAGE_LOCATION_SCHEMA);


work__idrp_sears_vendor_package_exploding_assortment_location_data = 
      LOAD '$WORK__IDRP_SEARS_VENDOR_PACKAGE_EXPLODING_ASSORTMENT_LOCATION_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_SEARS_VENDOR_PACKAGE_EXPLODING_ASSORTMENT_LOCATION_SCHEMA);


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


work__idrp_sears_vendor_package_location_data_fltr = 
      FILTER work__idrp_sears_vendor_package_location_data 
      BY TRIM(source_location_level_cd)=='VENDOR' AND (TRIM(active_ind)=='Y');


work__idrp_sears_vendor_package_exploding_assortment_location_data_fltr = 
      FILTER work__idrp_sears_vendor_package_exploding_assortment_location_data 
      BY TRIM(source_location_level_cd)=='VENDOR' AND (TRIM(active_ind)=='Y') ;


union_work_vp_locations = 
      UNION work__idrp_sears_vendor_package_location_data_fltr,
            work__idrp_sears_vendor_package_exploding_assortment_location_data_fltr;


group_union_work_vp_locations = 
  GROUP union_work_vp_locations 
  BY (shc_item_id,source_location_id,vendor_package_id);

count_union_work_vp_locations = 
  FOREACH group_union_work_vp_locations{ 
                    GENERATE 
						
						FLATTEN(union_work_vp_locations), 
						COUNT(union_work_vp_locations) AS vend_pack_row_cnt;
               };
			   
			   
group_count_union_work_vp_locations = 
	 GROUP count_union_work_vp_locations 
		BY (shc_item_id,source_location_id);
		
max_union_work_vp_locations	=	
  FOREACH group_count_union_work_vp_locations{ 
                    GENERATE 
						FLATTEN(count_union_work_vp_locations), 
						MAX(count_union_work_vp_locations.vend_pack_row_cnt) AS max_vend_pack_row_cnt;
               };

filter_max_union_work_vp_locations =
	FILTER max_union_work_vp_locations 
		BY (vend_pack_row_cnt == max_vend_pack_row_cnt);


group_max_union_work_vp_locations = group filter_max_union_work_vp_locations by (shc_item_id,source_location_id);

generate_max_union_work_vp_locations = foreach group_max_union_work_vp_locations
														{
															sorted = order filter_max_union_work_vp_locations by vendor_package_id ASC;
															lim = LIMIT sorted 1;
															generate FLATTEN(lim);
														};


work__idrp_sears_vend_pack_vendor_step1 = 
      FOREACH generate_max_union_work_vp_locations 
      GENERATE 
              vendor_package_id AS vendor_pack_id,
              source_location_id AS location_id,
              'VENDOR' AS location_format_type_cd,
              'VENDOR' AS location_level_cd,
              'S' AS location_owner_cd,
              'S' AS source_owner_cd,
              'Y' AS active_ind,
              '$CURRENT_DATE' AS active_ind_change_dt,
              allocation_replenishment_cd AS allocation_replenishment_cd,
              ' ' AS purchase_order_vendor_location_id,
              'Y' AS replenishment_planning_ind,
              'N' AS scan_based_trading_ind,
              '' AS source_location_id,
              '' AS source_location_level_cd,
              '1' AS source_package_qty,
              vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
              vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
              flow_type_cd AS flow_type_cd,
              import_ind AS import_ind,
              '' AS retail_carton_vendor_package_id,
               vendor_package_owner_cd AS vendor_package_owner_cd,
               vendor_stock_nbr AS vendor_stock_nbr,
               shc_item_id AS shc_item_id,
               item_purchase_status_cd AS item_purchase_status_cd,
               can_carry_model_id AS can_carry_model_id,
               days_to_check_begin_day_dt AS days_to_check_begin_day_qty,
               days_to_check_end_day_dt AS days_to_check_end_day_qty,
              '' AS reorder_method_cd,
               ksn_id AS ksn_id,
               ksn_purchase_status_cd AS ksn_purchase_status_cd,
               cross_merchandising_cd AS cross_merchandising_cd,
               dotcom_orderable_cd AS dotcom_orderable_cd,
              '' AS kmart_markdown_ind,
               ksn_package_id AS ksn_package_id,
              '' AS ksn_dc_package_purchase_status_cd,
              '' AS dc_configuration_cd,
              '' AS substitution_eligible_ind,
               sears_division_nbr AS sears_division_nbr,
               sears_item_nbr AS sears_item_nbr,
               sears_sku_nbr AS sears_sku_nbr,
               sears_source_location_id AS sears_location_id,
              '' AS sears_source_location_id,
              '' AS rim_status_cd,
              '' AS stock_type_cd,
              '' AS non_stock_source_cd,
              '' AS dos_item_active_ind,
              '' AS item_reserve_cd,
              '$CURRENT_DATE' AS create_dt,
              '$CURRENT_DATE' AS last_update_dt,
               '1' AS vendor_package_carton_qty,
               special_retail_order_system_ind AS special_retail_order_system_ind,
               shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
               distribution_type_cd AS distribution_type_cd,
               only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
               special_order_candidate_ind AS special_order_candidate_ind,
               item_emp_ind AS item_emp_ind,
               easy_order_ind AS easy_order_ind,
               warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
               rapid_item_ind AS rapid_item_ind,
               constrained_item_ind AS constrained_item_ind,
               idrp_item_type_desc AS idrp_item_type_desc,
               cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
               sams_migration_ind AS sams_migration_ind,
               emp_to_jit_ind AS emp_to_jit_ind,
               rim_flow_ind AS rim_flow_ind,
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


STORE work__idrp_sears_vend_pack_vendor_step1 
INTO '$WORK__IDRP_SEARS_VENDOR_PACKAGE_VENDOR_LOCATION_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
