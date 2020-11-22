/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_work__idrp_sourced_sears_location.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Thu Jul 24 06:23:19 EDT 2014
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
#        DATE    	BY                       	MODIFICATION
#15/10/2014         Siddhivinayak Karpe         Spira CR#3062 Code Changed at Line No 60,132,203,273,362
#21/10/2014         Siddhivinayak Karpe         Spira CR#3062 Code Changed at line 362
#28/10/2014         Siddhivinayak Karpe         Spira CR#3203 Code Changed at line 362
#10/11/2014         Siddhivinayak Karpe         Spira CR#3306 Code Changed at line nos 84,91,155,162,226,233,298,305
#01/19/2017         Srujan Dussa                IPS-779 . Adding rim_last_record_create_dt from gold__inventory_rim_daily_current to be included in the Extract File to Shared Items.
#08/02/2018        	Piyush Solanki          	IPS-3142: Create CLONE for Item Elig Sears Vendor Pack Location for AMZ/SHO
#
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

work__idrp_sourced_sears_warehouse_data =
      LOAD '$WORK__IDRP_SOURCED_SEARS_WAREHOUSE_LOCATION'
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($WORK__IDRP_SOURCED_SEARS_WAREHOUSE_SCHEMA);


work__idrp_sourced_sears_import_center_data =
      LOAD '$WORK__IDRP_SOURCED_SEARS_IMPORT_CENTER_LOCATION'
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($WORK__IDRP_SOURCED_SEARS_IMPORT_CENTER_SCHEMA);


work__idrp_sourced_sears_store_data =
      LOAD '$WORK__IDRP_SOURCED_SEARS_STORE_LOCATION'
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($WORK__IDRP_SOURCED_SEARS_STORE_SCHEMA);


work__idrp_sourced_sears_location_step1 =
      UNION work__idrp_sourced_sears_import_center_data,
            work__idrp_sourced_sears_warehouse_data,
            work__idrp_sourced_sears_store_data;


SPLIT work__idrp_sourced_sears_location_step1
INTO work_po_vend_assigned IF (IsNull(TRIM(purchase_order_vendor_location_id),'')!=''), work_po_vend_non_assigned OTHERWISE ;


outer_join =
     JOIN work_po_vend_non_assigned BY ((int)sears_division_nbr,TrimLeadingZeros(sears_item_nbr),(int)sears_sku_nbr,source_location_id) LEFT OUTER,
          work_po_vend_assigned BY ((int)sears_division_nbr,TrimLeadingZeros(sears_item_nbr),(int)sears_sku_nbr,location_id);


outer_join_gen =
     FOREACH outer_join
     GENERATE
             work_po_vend_non_assigned::sears_division_nbr AS sears_division_nbr,
             work_po_vend_non_assigned::sears_item_nbr AS sears_item_nbr,
             work_po_vend_non_assigned::sears_sku_nbr AS sears_sku_nbr,
             work_po_vend_non_assigned::sears_location_id AS sears_location_id,
             work_po_vend_non_assigned::location_id AS location_id,
             work_po_vend_non_assigned::location_level_cd AS location_level_cd,
             work_po_vend_non_assigned::location_format_type_cd AS location_format_type_cd,
             work_po_vend_non_assigned::location_owner_cd AS location_owner_cd,
             work_po_vend_non_assigned::sears_source_location_nbr AS sears_source_location_nbr,
             work_po_vend_non_assigned::source_location_id AS source_location_id,
             work_po_vend_non_assigned::source_location_level_cd AS source_location_level_cd,
             ((work_po_vend_assigned::sears_division_nbr IS NOT NULL) ? work_po_vend_assigned::purchase_order_vendor_location_id : work_po_vend_non_assigned::purchase_order_vendor_location_id) AS purchase_order_vendor_location_id,
             work_po_vend_non_assigned::rim_status_cd AS rim_status_cd,
             work_po_vend_non_assigned::active_ind AS active_ind,
             work_po_vend_non_assigned::source_package_qty AS source_package_qty,
             work_po_vend_non_assigned::shc_item_id AS shc_item_id,
             work_po_vend_non_assigned::ksn_id AS ksn_id,
             work_po_vend_non_assigned::vendor_package_id AS vendor_package_id,
             ((work_po_vend_assigned::sears_division_nbr IS NOT NULL) ? work_po_vend_assigned::vendor_package_carton_qty : work_po_vend_non_assigned::vendor_package_carton_qty) AS vendor_package_carton_qty,
             work_po_vend_non_assigned::special_retail_order_system_ind AS special_retail_order_system_ind,
             work_po_vend_non_assigned::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
             work_po_vend_non_assigned::dot_com_allocation_ind AS dot_com_allocation_ind,
             work_po_vend_non_assigned::distribution_type_cd AS distribution_type_cd,
             work_po_vend_non_assigned::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
             work_po_vend_non_assigned::special_order_candidate_ind AS special_order_candidate_ind,
             work_po_vend_non_assigned::item_emp_ind AS item_emp_ind,
             work_po_vend_non_assigned::easy_order_ind AS easy_order_ind,
             work_po_vend_non_assigned::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
             work_po_vend_non_assigned::rapid_item_ind AS rapid_item_ind,
             work_po_vend_non_assigned::constrained_item_ind AS constrained_item_ind,
             work_po_vend_non_assigned::sears_import_ind AS sears_import_ind,
             work_po_vend_non_assigned::idrp_item_type_desc AS idrp_item_type_desc,
             work_po_vend_non_assigned::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
             work_po_vend_non_assigned::sams_migration_ind AS sams_migration_ind,
             work_po_vend_non_assigned::emp_to_jit_ind AS emp_to_jit_ind,
             work_po_vend_non_assigned::rim_flow_ind AS rim_flow_ind,
             work_po_vend_non_assigned::cross_merchandising_cd AS cross_merchandising_cd,
             work_po_vend_non_assigned::source_system_cd AS source_system_cd,
             work_po_vend_non_assigned::original_source_nbr AS original_source_nbr,
             work_po_vend_non_assigned::item_active_ind AS item_active_ind,
             work_po_vend_non_assigned::stock_type_cd AS stock_type_cd,
             work_po_vend_non_assigned::item_reserve_cd AS item_reserve_cd,
             work_po_vend_non_assigned::non_stock_source_cd AS non_stock_source_cd,
             --work_po_vend_non_assigned::product_condition_cd AS product_condition_cd,
             work_po_vend_non_assigned::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             work_po_vend_non_assigned::item_on_order_qty AS item_on_order_qty,
             work_po_vend_non_assigned::item_reserve_qty AS item_reserve_qty,
             work_po_vend_non_assigned::item_back_order_qty AS item_back_order_qty,
             work_po_vend_non_assigned::item_next_period_future_order_qty AS item_next_period_future_order_qty,
             work_po_vend_non_assigned::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             work_po_vend_non_assigned::item_last_receive_dt AS item_last_receive_dt,
             work_po_vend_non_assigned::item_last_ship_dt AS item_last_ship_dt,
             work_po_vend_non_assigned::rim_last_record_creation_dt AS rim_last_record_creation_dt;


union_data =
     UNION work_po_vend_assigned,
           outer_join_gen;


SPLIT union_data
INTO work_po_vend_assigned_1 IF (IsNull(TRIM(purchase_order_vendor_location_id),'')!=''), work_po_vend_non_assigned_1 OTHERWISE ;


outer_join_work_1 =
     JOIN work_po_vend_non_assigned_1 BY ((int)sears_division_nbr,TrimLeadingZeros(sears_item_nbr),(int)sears_sku_nbr,source_location_id) LEFT OUTER,
          work_po_vend_assigned_1 BY ((int)sears_division_nbr,TrimLeadingZeros(sears_item_nbr),(int)sears_sku_nbr,location_id);


outer_join_work_1_gen =
     FOREACH outer_join_work_1
     GENERATE
             work_po_vend_non_assigned_1::sears_division_nbr AS sears_division_nbr,
             work_po_vend_non_assigned_1::sears_item_nbr AS sears_item_nbr,
             work_po_vend_non_assigned_1::sears_sku_nbr AS sears_sku_nbr,
             work_po_vend_non_assigned_1::sears_location_id AS sears_location_id,
             work_po_vend_non_assigned_1::location_id AS location_id,
             work_po_vend_non_assigned_1::location_level_cd AS location_level_cd,
             work_po_vend_non_assigned_1::location_format_type_cd AS location_format_type_cd,
             work_po_vend_non_assigned_1::location_owner_cd AS location_owner_cd,
             work_po_vend_non_assigned_1::sears_source_location_nbr AS sears_source_location_nbr,
             work_po_vend_non_assigned_1::source_location_id AS source_location_id,
             work_po_vend_non_assigned_1::source_location_level_cd AS source_location_level_cd,
             ((work_po_vend_assigned_1::sears_division_nbr IS NOT NULL) ? work_po_vend_assigned_1::purchase_order_vendor_location_id : work_po_vend_non_assigned_1::purchase_order_vendor_location_id) AS purchase_order_vendor_location_id,
             work_po_vend_non_assigned_1::rim_status_cd AS rim_status_cd,
             work_po_vend_non_assigned_1::active_ind AS active_ind,
             work_po_vend_non_assigned_1::source_package_qty AS source_package_qty,
             work_po_vend_non_assigned_1::shc_item_id AS shc_item_id,
             work_po_vend_non_assigned_1::ksn_id AS ksn_id,
             work_po_vend_non_assigned_1::vendor_package_id AS vendor_package_id,
             ((work_po_vend_assigned_1::sears_division_nbr IS NOT NULL) ? work_po_vend_assigned_1::vendor_package_carton_qty : work_po_vend_non_assigned_1::vendor_package_carton_qty) AS vendor_package_carton_qty,
             work_po_vend_non_assigned_1::special_retail_order_system_ind AS special_retail_order_system_ind,
             work_po_vend_non_assigned_1::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
             work_po_vend_non_assigned_1::dot_com_allocation_ind AS dot_com_allocation_ind,
             work_po_vend_non_assigned_1::distribution_type_cd AS distribution_type_cd,
             work_po_vend_non_assigned_1::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
             work_po_vend_non_assigned_1::special_order_candidate_ind AS special_order_candidate_ind,
             work_po_vend_non_assigned_1::item_emp_ind AS item_emp_ind,
             work_po_vend_non_assigned_1::easy_order_ind AS easy_order_ind,
             work_po_vend_non_assigned_1::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
             work_po_vend_non_assigned_1::rapid_item_ind AS rapid_item_ind,
             work_po_vend_non_assigned_1::constrained_item_ind AS constrained_item_ind,
             work_po_vend_non_assigned_1::sears_import_ind AS sears_import_ind,
             work_po_vend_non_assigned_1::idrp_item_type_desc AS idrp_item_type_desc,
             work_po_vend_non_assigned_1::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
             work_po_vend_non_assigned_1::sams_migration_ind AS sams_migration_ind,
             work_po_vend_non_assigned_1::emp_to_jit_ind AS emp_to_jit_ind,
             work_po_vend_non_assigned_1::rim_flow_ind AS rim_flow_ind,
             work_po_vend_non_assigned_1::cross_merchandising_cd AS cross_merchandising_cd,
             work_po_vend_non_assigned_1::source_system_cd AS source_system_cd,
             work_po_vend_non_assigned_1::original_source_nbr AS original_source_nbr,
             work_po_vend_non_assigned_1::item_active_ind AS item_active_ind,
             work_po_vend_non_assigned_1::stock_type_cd AS stock_type_cd,
             work_po_vend_non_assigned_1::item_reserve_cd AS item_reserve_cd,
             work_po_vend_non_assigned_1::non_stock_source_cd AS non_stock_source_cd,
             --work_po_vend_non_assigned_1::product_condition_cd AS product_condition_cd,
             work_po_vend_non_assigned_1::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             work_po_vend_non_assigned_1::item_on_order_qty AS item_on_order_qty,
             work_po_vend_non_assigned_1::item_reserve_qty AS item_reserve_qty,
             work_po_vend_non_assigned_1::item_back_order_qty AS item_back_order_qty,
             work_po_vend_non_assigned_1::item_next_period_future_order_qty AS item_next_period_future_order_qty,
             work_po_vend_non_assigned_1::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             work_po_vend_non_assigned_1::item_last_receive_dt AS item_last_receive_dt,
             work_po_vend_non_assigned_1::item_last_ship_dt AS item_last_ship_dt,
             work_po_vend_non_assigned_1::rim_last_record_creation_dt AS rim_last_record_creation_dt;


union_final_data =
     UNION work_po_vend_assigned_1,
           outer_join_work_1_gen;


SPLIT union_final_data
INTO work_po_vend_assigned_2 IF (IsNull(TRIM(purchase_order_vendor_location_id),'')!=''), work_po_vend_non_assigned_2 OTHERWISE ;


outer_join_work_2 =
     JOIN work_po_vend_non_assigned_2 BY ((int)sears_division_nbr,TrimLeadingZeros(sears_item_nbr),(int)sears_sku_nbr,source_location_id) LEFT OUTER,
          work_po_vend_assigned_2 BY ((int)sears_division_nbr,TrimLeadingZeros(sears_item_nbr),(int)sears_sku_nbr,location_id);


outer_join_work_2_gen =
     FOREACH outer_join_work_2
     GENERATE
             work_po_vend_non_assigned_2::sears_division_nbr AS sears_division_nbr,
             work_po_vend_non_assigned_2::sears_item_nbr AS sears_item_nbr,
             work_po_vend_non_assigned_2::sears_sku_nbr AS sears_sku_nbr,
             work_po_vend_non_assigned_2::sears_location_id AS sears_location_id,
             work_po_vend_non_assigned_2::location_id AS location_id,
             work_po_vend_non_assigned_2::location_level_cd AS location_level_cd,
             work_po_vend_non_assigned_2::location_format_type_cd AS location_format_type_cd,
             work_po_vend_non_assigned_2::location_owner_cd AS location_owner_cd,
             work_po_vend_non_assigned_2::sears_source_location_nbr AS sears_source_location_nbr,
             work_po_vend_non_assigned_2::source_location_id AS source_location_id,
             work_po_vend_non_assigned_2::source_location_level_cd AS source_location_level_cd,
             ((work_po_vend_assigned_2::sears_division_nbr IS NOT NULL) ? work_po_vend_assigned_2::purchase_order_vendor_location_id : work_po_vend_non_assigned_2::purchase_order_vendor_location_id) AS purchase_order_vendor_location_id,
             work_po_vend_non_assigned_2::rim_status_cd AS rim_status_cd,
             work_po_vend_non_assigned_2::active_ind AS active_ind,
             work_po_vend_non_assigned_2::source_package_qty AS source_package_qty,
             work_po_vend_non_assigned_2::shc_item_id AS shc_item_id,
             work_po_vend_non_assigned_2::ksn_id AS ksn_id,
             work_po_vend_non_assigned_2::vendor_package_id AS vendor_package_id,
             ((work_po_vend_assigned_2::sears_division_nbr IS NOT NULL) ? work_po_vend_assigned_2::vendor_package_carton_qty : work_po_vend_non_assigned_2::vendor_package_carton_qty) AS vendor_package_carton_qty,
             work_po_vend_non_assigned_2::special_retail_order_system_ind AS special_retail_order_system_ind,
             work_po_vend_non_assigned_2::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
             work_po_vend_non_assigned_2::dot_com_allocation_ind AS dot_com_allocation_ind,
             work_po_vend_non_assigned_2::distribution_type_cd AS distribution_type_cd,
             work_po_vend_non_assigned_2::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
             work_po_vend_non_assigned_2::special_order_candidate_ind AS special_order_candidate_ind,
             work_po_vend_non_assigned_2::item_emp_ind AS item_emp_ind,
             work_po_vend_non_assigned_2::easy_order_ind AS easy_order_ind,
             work_po_vend_non_assigned_2::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
             work_po_vend_non_assigned_2::rapid_item_ind AS rapid_item_ind,
             work_po_vend_non_assigned_2::constrained_item_ind AS constrained_item_ind,
             work_po_vend_non_assigned_2::sears_import_ind AS sears_import_ind,
             work_po_vend_non_assigned_2::idrp_item_type_desc AS idrp_item_type_desc,
             work_po_vend_non_assigned_2::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
             work_po_vend_non_assigned_2::sams_migration_ind AS sams_migration_ind,
             work_po_vend_non_assigned_2::emp_to_jit_ind AS emp_to_jit_ind,
             work_po_vend_non_assigned_2::rim_flow_ind AS rim_flow_ind,
             work_po_vend_non_assigned_2::cross_merchandising_cd AS cross_merchandising_cd,
             work_po_vend_non_assigned_2::source_system_cd AS source_system_cd,
             work_po_vend_non_assigned_2::original_source_nbr AS original_source_nbr,
             work_po_vend_non_assigned_2::item_active_ind AS item_active_ind,
             work_po_vend_non_assigned_2::stock_type_cd AS stock_type_cd,
             work_po_vend_non_assigned_2::item_reserve_cd AS item_reserve_cd,
             work_po_vend_non_assigned_2::non_stock_source_cd AS non_stock_source_cd,
             --work_po_vend_non_assigned_2::product_condition_cd AS product_condition_cd,
             work_po_vend_non_assigned_2::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             work_po_vend_non_assigned_2::item_on_order_qty AS item_on_order_qty,
             work_po_vend_non_assigned_2::item_reserve_qty AS item_reserve_qty,
             work_po_vend_non_assigned_2::item_back_order_qty AS item_back_order_qty,
             work_po_vend_non_assigned_2::item_next_period_future_order_qty AS item_next_period_future_order_qty,
             work_po_vend_non_assigned_2::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             work_po_vend_non_assigned_2::item_last_receive_dt AS item_last_receive_dt,
             work_po_vend_non_assigned_2::item_last_ship_dt AS item_last_ship_dt,
             work_po_vend_non_assigned_2::rim_last_record_creation_dt AS rim_last_record_creation_dt;


union_final_data_2 =
     UNION work_po_vend_assigned_2,
           outer_join_work_2_gen;


SPLIT union_final_data_2
INTO work_po_vend_assigned_3 IF (IsNull(TRIM(purchase_order_vendor_location_id),'')!=''), work_po_vend_non_assigned_3 OTHERWISE ;



outer_join_work_3 =
     JOIN work_po_vend_non_assigned_3 BY ((int)sears_division_nbr,TrimLeadingZeros(sears_item_nbr),(int)sears_sku_nbr,source_location_id) LEFT OUTER,
          work_po_vend_assigned_3 BY ((int)sears_division_nbr,TrimLeadingZeros(sears_item_nbr),(int)sears_sku_nbr,location_id);


outer_join_work_3_gen =
     FOREACH outer_join_work_3
     GENERATE
             work_po_vend_non_assigned_3::sears_division_nbr AS sears_division_nbr,
             work_po_vend_non_assigned_3::sears_item_nbr AS sears_item_nbr,
             work_po_vend_non_assigned_3::sears_sku_nbr AS sears_sku_nbr,
             work_po_vend_non_assigned_3::sears_location_id AS sears_location_id,
             work_po_vend_non_assigned_3::location_id AS location_id,
             work_po_vend_non_assigned_3::location_level_cd AS location_level_cd,
             work_po_vend_non_assigned_3::location_format_type_cd AS location_format_type_cd,
             work_po_vend_non_assigned_3::location_owner_cd AS location_owner_cd,
             work_po_vend_non_assigned_3::sears_source_location_nbr AS sears_source_location_nbr,
             work_po_vend_non_assigned_3::source_location_id AS source_location_id,
             work_po_vend_non_assigned_3::source_location_level_cd AS source_location_level_cd,
             ((work_po_vend_assigned_3::sears_division_nbr IS NOT NULL) ? work_po_vend_assigned_3::purchase_order_vendor_location_id : work_po_vend_non_assigned_3::purchase_order_vendor_location_id) AS purchase_order_vendor_location_id,
             work_po_vend_non_assigned_3::rim_status_cd AS rim_status_cd,
             work_po_vend_non_assigned_3::active_ind AS active_ind,
             work_po_vend_non_assigned_3::source_package_qty AS source_package_qty,
             work_po_vend_non_assigned_3::shc_item_id AS shc_item_id,
             work_po_vend_non_assigned_3::ksn_id AS ksn_id,
             work_po_vend_non_assigned_3::vendor_package_id AS vendor_package_id,
             ((work_po_vend_assigned_3::sears_division_nbr IS NOT NULL) ? work_po_vend_assigned_3::vendor_package_carton_qty : work_po_vend_non_assigned_3::vendor_package_carton_qty) AS vendor_package_carton_qty,
             work_po_vend_non_assigned_3::special_retail_order_system_ind AS special_retail_order_system_ind,
             work_po_vend_non_assigned_3::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
             work_po_vend_non_assigned_3::dot_com_allocation_ind AS dot_com_allocation_ind,
             work_po_vend_non_assigned_3::distribution_type_cd AS distribution_type_cd,
             work_po_vend_non_assigned_3::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
             work_po_vend_non_assigned_3::special_order_candidate_ind AS special_order_candidate_ind,
             work_po_vend_non_assigned_3::item_emp_ind AS item_emp_ind,
             work_po_vend_non_assigned_3::easy_order_ind AS easy_order_ind,
             work_po_vend_non_assigned_3::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
             work_po_vend_non_assigned_3::rapid_item_ind AS rapid_item_ind,
             work_po_vend_non_assigned_3::constrained_item_ind AS constrained_item_ind,
             work_po_vend_non_assigned_3::sears_import_ind AS sears_import_ind,
             work_po_vend_non_assigned_3::idrp_item_type_desc AS idrp_item_type_desc,
             work_po_vend_non_assigned_3::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
             work_po_vend_non_assigned_3::sams_migration_ind AS sams_migration_ind,
             work_po_vend_non_assigned_3::emp_to_jit_ind AS emp_to_jit_ind,
             work_po_vend_non_assigned_3::rim_flow_ind AS rim_flow_ind,
             work_po_vend_non_assigned_3::cross_merchandising_cd AS cross_merchandising_cd,
             work_po_vend_non_assigned_3::source_system_cd AS source_system_cd,
             work_po_vend_non_assigned_3::original_source_nbr AS original_source_nbr,
             work_po_vend_non_assigned_3::item_active_ind AS item_active_ind,
             work_po_vend_non_assigned_3::stock_type_cd AS stock_type_cd,
             work_po_vend_non_assigned_3::item_reserve_cd AS item_reserve_cd,
             work_po_vend_non_assigned_3::non_stock_source_cd AS non_stock_source_cd,
             --work_po_vend_non_assigned_3::product_condition_cd AS product_condition_cd,
             work_po_vend_non_assigned_3::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             work_po_vend_non_assigned_3::item_on_order_qty AS item_on_order_qty,
             work_po_vend_non_assigned_3::item_reserve_qty AS item_reserve_qty,
             work_po_vend_non_assigned_3::item_back_order_qty AS item_back_order_qty,
             work_po_vend_non_assigned_3::item_next_period_future_order_qty AS item_next_period_future_order_qty,
             work_po_vend_non_assigned_3::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             work_po_vend_non_assigned_3::item_last_receive_dt AS item_last_receive_dt,
             work_po_vend_non_assigned_3::item_last_ship_dt AS item_last_ship_dt,
             work_po_vend_non_assigned_3::rim_last_record_creation_dt AS rim_last_record_creation_dt;


union_final_data_3 =
     UNION work_po_vend_assigned_3,
           outer_join_work_3_gen;



work__idrp_sourced_sears_location =
     FOREACH union_final_data_3
     GENERATE
             sears_division_nbr AS sears_division_nbr,
             sears_item_nbr AS sears_item_nbr,
             sears_sku_nbr AS sears_sku_nbr,
             sears_location_id AS sears_location_id,
             location_id AS location_id,
             location_level_cd AS location_level_cd,
             location_format_type_cd AS location_format_type_cd,
             location_owner_cd AS location_owner_cd,
             sears_source_location_nbr AS sears_source_location_nbr,
             source_location_id AS source_location_id,
             source_location_level_cd AS source_location_level_cd,
             purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
             rim_status_cd AS rim_status_cd,
             (((IsNull(TRIM(purchase_order_vendor_location_id),'')=='') AND ((IsNull(TRIM(cross_merchandising_cd),'')=='') OR cross_merchandising_cd!='RIMFLOW') ) ? 'N' : active_ind) AS active_ind,
             source_package_qty AS source_package_qty,
             shc_item_id AS shc_item_id,
             ksn_id AS ksn_id,
             vendor_package_id AS vendor_package_id,
             vendor_package_carton_qty AS vendor_package_carton_qty,
             special_retail_order_system_ind AS special_retail_order_system_ind,
             shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
             dot_com_allocation_ind AS dot_com_allocation_ind,
             distribution_type_cd AS distribution_type_cd,
             only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
             special_order_candidate_ind AS special_order_candidate_ind,
             item_emp_ind AS item_emp_ind,
             easy_order_ind AS easy_order_ind,
             warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
             rapid_item_ind AS rapid_item_ind,
             constrained_item_ind AS constrained_item_ind,
             sears_import_ind AS sears_import_ind,
             idrp_item_type_desc AS idrp_item_type_desc,
             cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
             sams_migration_ind AS sams_migration_ind,
             emp_to_jit_ind AS emp_to_jit_ind,
             rim_flow_ind AS rim_flow_ind,
             cross_merchandising_cd AS cross_merchandising_cd,
             source_system_cd AS source_system_cd,
             original_source_nbr AS original_source_nbr,
             item_active_ind AS item_active_ind,
             stock_type_cd AS stock_type_cd,
             item_reserve_cd AS item_reserve_cd,
             non_stock_source_cd AS non_stock_source_cd,
             --product_condition_cd AS product_condition_cd,
             item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             item_on_order_qty AS item_on_order_qty,
             item_reserve_qty AS item_reserve_qty,
             item_back_order_qty AS item_back_order_qty,
             item_next_period_future_order_qty AS item_next_period_future_order_qty,
             item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             item_last_receive_dt AS item_last_receive_dt,
             item_last_ship_dt AS item_last_ship_dt,
                         rim_last_record_creation_dt AS rim_last_record_creation_dt;


--/******** IPS-3142: STEP 1: smith__idrp_ksn_attribute_current ********/

smith__idrp_ksn_attribute_current  =
        LOAD    '$SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_LOCATION'
        USING   PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS     	($SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_SCHEMA);

fil_idrp_ksn_attribute_current =
        FILTER smith__idrp_ksn_attribute_current by IsNull(amazon_brand_attribute_cd, ' ') != ' ';

--/******** IPS-3142: STEP 2: work__idrp_3pl_ddc_xref ********/

work__idrp_3pl_ddc_xref =
        LOAD    '$WORK__IDRP_3PL_DDC_XREF_LOCATION'
        USING   PigStorage('$FIELD_DELIMITER_PIPE')
        AS      ($WORK__IDRP_3PL_DDC_XREF_SCHEMA);

--/******** IPS-3142: STEP 3: join sourced_sears_location (sears_location_id) and xref (udc_srs_loc_ddc) ********/

join_sourced_sears_loc__xref =
        JOIN work__idrp_sourced_sears_location BY 	(sears_location_id),
                 work__idrp_3pl_ddc_xref BY     	(udc_srs_loc_ddc);

--/******** IPS-3142: STEP 4: Generate join_sourced_sears_loc__xref dataset, take XREF additional columns ********/

gen_join_sourced_sears_loc__xref =
    FOREACH  join_sourced_sears_loc__xref
    GENERATE work__idrp_sourced_sears_location::sears_division_nbr AS sears_division_nbr,
             work__idrp_sourced_sears_location::sears_item_nbr AS sears_item_nbr,
             work__idrp_sourced_sears_location::sears_sku_nbr AS sears_sku_nbr,
             work__idrp_sourced_sears_location::sears_location_id AS sears_location_id,
             work__idrp_sourced_sears_location::location_id AS location_id,
             work__idrp_3pl_ddc_xref::udc_srs_loc_ddc AS xref_udc_srs_loc_ddc,
             work__idrp_3pl_ddc_xref::udc_loc_3pl AS xref_udc_loc_3pl,
             work__idrp_sourced_sears_location::location_level_cd AS location_level_cd,
             work__idrp_sourced_sears_location::location_format_type_cd AS location_format_type_cd,
             work__idrp_sourced_sears_location::location_owner_cd AS location_owner_cd,
             work__idrp_sourced_sears_location::sears_source_location_nbr AS sears_source_location_nbr,
             work__idrp_sourced_sears_location::source_location_id AS source_location_id,
             work__idrp_sourced_sears_location::source_location_level_cd AS source_location_level_cd,
             work__idrp_sourced_sears_location::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
             work__idrp_sourced_sears_location::rim_status_cd AS rim_status_cd,
             work__idrp_sourced_sears_location::active_ind AS active_ind,
             work__idrp_sourced_sears_location::source_package_qty AS source_package_qty,
             work__idrp_sourced_sears_location::shc_item_id AS shc_item_id,
             work__idrp_sourced_sears_location::ksn_id AS ksn_id,
             work__idrp_sourced_sears_location::vendor_package_id AS vendor_package_id,
             work__idrp_sourced_sears_location::vendor_package_carton_qty AS vendor_package_carton_qty,
             work__idrp_sourced_sears_location::special_retail_order_system_ind AS special_retail_order_system_ind,
             work__idrp_sourced_sears_location::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
             work__idrp_sourced_sears_location::dot_com_allocation_ind AS dot_com_allocation_ind,
             work__idrp_sourced_sears_location::distribution_type_cd AS distribution_type_cd,
             work__idrp_sourced_sears_location::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
             work__idrp_sourced_sears_location::special_order_candidate_ind AS special_order_candidate_ind,
             work__idrp_sourced_sears_location::item_emp_ind AS item_emp_ind,
             work__idrp_sourced_sears_location::easy_order_ind AS easy_order_ind,
             work__idrp_sourced_sears_location::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
             work__idrp_sourced_sears_location::rapid_item_ind AS rapid_item_ind,
             work__idrp_sourced_sears_location::constrained_item_ind AS constrained_item_ind,
             work__idrp_sourced_sears_location::sears_import_ind AS sears_import_ind,
             work__idrp_sourced_sears_location::idrp_item_type_desc AS idrp_item_type_desc,
             work__idrp_sourced_sears_location::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
             work__idrp_sourced_sears_location::sams_migration_ind AS sams_migration_ind,
             work__idrp_sourced_sears_location::emp_to_jit_ind AS emp_to_jit_ind,
             work__idrp_sourced_sears_location::rim_flow_ind AS rim_flow_ind,
             work__idrp_sourced_sears_location::cross_merchandising_cd AS cross_merchandising_cd,
             work__idrp_sourced_sears_location::source_system_cd AS source_system_cd,
             work__idrp_sourced_sears_location::original_source_nbr AS original_source_nbr,
             work__idrp_sourced_sears_location::item_active_ind AS item_active_ind,
             work__idrp_sourced_sears_location::stock_type_cd AS stock_type_cd,
             work__idrp_sourced_sears_location::item_reserve_cd AS item_reserve_cd,
             work__idrp_sourced_sears_location::non_stock_source_cd AS non_stock_source_cd,
             work__idrp_sourced_sears_location::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             work__idrp_sourced_sears_location::item_on_order_qty AS item_on_order_qty,
             work__idrp_sourced_sears_location::item_reserve_qty AS item_reserve_qty,
             work__idrp_sourced_sears_location::item_back_order_qty AS item_back_order_qty,
             work__idrp_sourced_sears_location::item_next_period_future_order_qty AS item_next_period_future_order_qty,
             work__idrp_sourced_sears_location::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             work__idrp_sourced_sears_location::item_last_receive_dt AS item_last_receive_dt,
             work__idrp_sourced_sears_location::item_last_ship_dt AS item_last_ship_dt,
             work__idrp_sourced_sears_location::rim_last_record_creation_dt AS rim_last_record_creation_dt;

--/******** IPS-3142: STEP 5: join sourced_sears_loc__xref (ksn_id) and ksn_attribute_current (ksn_id) ********/

join_sourced_sears_loc__xref__ksn_attribute_current =
			JOIN gen_join_sourced_sears_loc__xref BY (ksn_id),
				 fil_idrp_ksn_attribute_current   BY (ksn_id);

--/******** IPS-3142: STEP 6: Generating Cloned Data ********/

work__idrp_sourced_sears_location_AMZ_CLONED_DATA = FOREACH join_sourced_sears_loc__xref__ksn_attribute_current GENERATE
				 gen_join_sourced_sears_loc__xref::sears_division_nbr AS sears_division_nbr,
				 gen_join_sourced_sears_loc__xref::sears_item_nbr AS sears_item_nbr,
				 gen_join_sourced_sears_loc__xref::sears_sku_nbr AS sears_sku_nbr,
				 gen_join_sourced_sears_loc__xref::xref_udc_loc_3pl AS sears_location_id,
				 gen_join_sourced_sears_loc__xref::xref_udc_loc_3pl AS location_id,
				 gen_join_sourced_sears_loc__xref::location_level_cd AS location_level_cd,
				 '3PL' AS location_format_type_cd,
				 gen_join_sourced_sears_loc__xref::location_owner_cd AS location_owner_cd,
				 gen_join_sourced_sears_loc__xref::sears_source_location_nbr AS sears_source_location_nbr,
				 gen_join_sourced_sears_loc__xref::source_location_id AS source_location_id,
				 gen_join_sourced_sears_loc__xref::source_location_level_cd AS source_location_level_cd,
				 gen_join_sourced_sears_loc__xref::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
				 gen_join_sourced_sears_loc__xref::rim_status_cd AS rim_status_cd,
				 gen_join_sourced_sears_loc__xref::active_ind AS active_ind,
				 gen_join_sourced_sears_loc__xref::source_package_qty AS source_package_qty,
				 gen_join_sourced_sears_loc__xref::shc_item_id AS shc_item_id,
				 gen_join_sourced_sears_loc__xref::ksn_id AS ksn_id,
				 gen_join_sourced_sears_loc__xref::vendor_package_id AS vendor_package_id,
				 gen_join_sourced_sears_loc__xref::vendor_package_carton_qty AS vendor_package_carton_qty,
				 gen_join_sourced_sears_loc__xref::special_retail_order_system_ind AS special_retail_order_system_ind,
				 gen_join_sourced_sears_loc__xref::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
				 gen_join_sourced_sears_loc__xref::dot_com_allocation_ind AS dot_com_allocation_ind,
				 gen_join_sourced_sears_loc__xref::distribution_type_cd AS distribution_type_cd,
				 gen_join_sourced_sears_loc__xref::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
				 gen_join_sourced_sears_loc__xref::special_order_candidate_ind AS special_order_candidate_ind,
				 gen_join_sourced_sears_loc__xref::item_emp_ind AS item_emp_ind,
				 gen_join_sourced_sears_loc__xref::easy_order_ind AS easy_order_ind,
				 gen_join_sourced_sears_loc__xref::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
				 gen_join_sourced_sears_loc__xref::rapid_item_ind AS rapid_item_ind,
				 gen_join_sourced_sears_loc__xref::constrained_item_ind AS constrained_item_ind,
				 gen_join_sourced_sears_loc__xref::sears_import_ind AS sears_import_ind,
				 gen_join_sourced_sears_loc__xref::idrp_item_type_desc AS idrp_item_type_desc,
				 gen_join_sourced_sears_loc__xref::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
				 gen_join_sourced_sears_loc__xref::sams_migration_ind AS sams_migration_ind,
				 gen_join_sourced_sears_loc__xref::emp_to_jit_ind AS emp_to_jit_ind,
				 gen_join_sourced_sears_loc__xref::rim_flow_ind AS rim_flow_ind,
				 gen_join_sourced_sears_loc__xref::cross_merchandising_cd AS cross_merchandising_cd,
				 gen_join_sourced_sears_loc__xref::source_system_cd AS source_system_cd,
				 gen_join_sourced_sears_loc__xref::original_source_nbr AS original_source_nbr,
				 gen_join_sourced_sears_loc__xref::item_active_ind AS item_active_ind,
				 gen_join_sourced_sears_loc__xref::stock_type_cd AS stock_type_cd,
				 gen_join_sourced_sears_loc__xref::item_reserve_cd AS item_reserve_cd,
				 gen_join_sourced_sears_loc__xref::non_stock_source_cd AS non_stock_source_cd,
				 '0' AS item_next_period_on_hand_qty,
				 '0' AS item_on_order_qty,
				 '0' AS item_reserve_qty,
				 '0' AS item_back_order_qty,
				 '0' AS item_next_period_future_order_qty,
				 '0' AS item_next_period_in_transit_qty,
				 '1970-10-01' AS item_last_receive_dt,
				 '1970-10-01' AS item_last_ship_dt,
				 '1970-10-01' AS rim_last_record_creation_dt;

--/******** IPS-3142: STEP 7: COMBINE original work__idrp_sourced_sears_location to new cloned_data and store COMBINED result ********/

original_sourced_sears_loc__amz_cloned_data = UNION work__idrp_sourced_sears_location, work__idrp_sourced_sears_location_AMZ_CLONED_DATA;


-- STORE work__idrp_sourced_sears_location              --IPS-3142: commented
STORE original_sourced_sears_loc__amz_cloned_data
INTO '$WORK__IDRP_SOURCED_SEARS_LOCATION_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/

