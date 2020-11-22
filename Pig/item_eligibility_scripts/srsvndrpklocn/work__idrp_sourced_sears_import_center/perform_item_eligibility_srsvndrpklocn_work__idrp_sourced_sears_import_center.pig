/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_work__idrp_sourced_sears_import_center.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Wed Jul 23 04:11:12 EDT 2014
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
#	1/19/2017     Srujan Dussa	IPS-779 . Adding rim_last_record_create_dt from gold__inventory_rim_daily_current to be included in the Extract File to Shared Items.
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

--register the jar containing all PIG UDFs
REGISTER $UDF_JAR;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

smith__idrp_ksn_attribute_current_data = 
      LOAD '$SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_SCHEMA);


work__idrp_sears_location_xref_data = 
      LOAD '$WORK__IDRP_SEARS_LOCATION_XREF_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_SEARS_LOCATION_XREF_SCHEMA);


work__idrp_sourced_sears_warehouse_data = 
      LOAD '$WORK__IDRP_SOURCED_SEARS_WAREHOUSE_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
      AS ($WORK__IDRP_SOURCED_SEARS_WAREHOUSE_SCHEMA);


work__idrp_sourced_sears_warehouse_data_fltr = 
      FILTER work__idrp_sourced_sears_warehouse_data 
      BY TRIM(idrp_item_type_desc)=='IMPORT' AND TRIM(source_location_id)=='TF32-15';


work__idrp_sourced_sears_warehouse_data_fltr_gen = 
      FOREACH work__idrp_sourced_sears_warehouse_data_fltr 
      GENERATE 
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              purchase_order_vendor_location_id AS source_location_id,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              source_package_qty AS source_package_qty,
              source_system_cd AS source_system_cd, 
              vendor_package_id AS vendor_package_id,
              shc_item_id AS shc_item_id,
              ksn_id AS ksn_id,
              vendor_package_carton_qty AS vendor_package_carton_qty,
	      rim_last_record_creation_dt AS rim_last_record_creation_dt;

work__idrp_sourced_sears_store_data = 
      LOAD '$WORK__IDRP_SOURCED_SEARS_STORE_LOCATION' 
      USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
      AS ($WORK__IDRP_SOURCED_SEARS_STORE_SCHEMA);


work__idrp_sourced_sears_store_data_fltr = 
      FILTER work__idrp_sourced_sears_store_data 
      BY TRIM(idrp_item_type_desc)=='IMPORT' AND TRIM(source_location_id)=='TF32-15';


work__idrp_sourced_sears_store_data_fltr_gen = 
      FOREACH work__idrp_sourced_sears_store_data_fltr 
      GENERATE 
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              purchase_order_vendor_location_id AS source_location_id,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              source_package_qty AS source_package_qty,
              source_system_cd AS source_system_cd,
              vendor_package_id AS vendor_package_id,
              shc_item_id AS shc_item_id,
              ksn_id AS ksn_id,
              vendor_package_carton_qty AS vendor_package_carton_qty,
	      rim_last_record_creation_dt AS rim_last_record_creation_dt;


union_warehouse_store_data = 
      UNION work__idrp_sourced_sears_warehouse_data_fltr_gen,
            work__idrp_sourced_sears_store_data_fltr_gen;


group_unin_data = 
      GROUP union_warehouse_store_data 
      BY (sears_division_nbr,sears_item_nbr,sears_sku_nbr);


cnt_loc_group_unin_data =
		FOREACH group_unin_data 
		GENERATE FLATTEN(union_warehouse_store_data) , COUNT(union_warehouse_store_data) AS loc_cnt;


cnt_loc_group_unin_data_gen =
		FOREACH cnt_loc_group_unin_data 
		GENERATE
			  sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              source_location_id AS source_location_id,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              source_package_qty AS source_package_qty,
              source_system_cd AS source_system_cd,
              vendor_package_id AS vendor_package_id,
              shc_item_id AS shc_item_id,
              ksn_id AS ksn_id,
              vendor_package_carton_qty AS vendor_package_carton_qty,
              loc_cnt AS loc_cnt:int,
	      rim_last_record_creation_dt AS rim_last_record_creation_dt;
	
regrp_cnt_loc_data = GROUP cnt_loc_group_unin_data_gen BY (shc_item_id);
	

group_unin_data_gen = 
      FOREACH regrp_cnt_loc_data{
                              data_record = ORDER cnt_loc_group_unin_data_gen BY loc_cnt DESC, source_package_qty, sears_division_nbr, sears_item_nbr, sears_sku_nbr  DESC;
                              first_record = LIMIT data_record 1;
                              GENERATE FLATTEN(first_record);
                             };


work__idrp_source_sears_import_center_step1 = 
      FOREACH group_unin_data_gen 
      GENERATE 
              sears_division_nbr AS sears_division_nbr,
              sears_item_nbr AS sears_item_nbr,
              sears_sku_nbr AS sears_sku_nbr,
              'TF32-15' AS sears_location_id,
              'TF32-15' AS location_id,
              'WAREHOUSE' AS location_level_cd,
              'IMPORT' AS location_format_type_cd,
              'S' AS location_owner_cd,
              purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              'Y' AS active_ind,
              shc_item_id AS shc_item_id,
              ksn_id AS ksn_id,
              source_location_id AS source_location_id,
              source_package_qty AS source_package_qty,
              source_system_cd AS source_system_cd,
              vendor_package_id AS vendor_package_id,
              vendor_package_carton_qty AS vendor_package_carton_qty,
              loc_cnt as loc_cnt,
	      rim_last_record_creation_dt as rim_last_record_creation_dt;

join_work_xref = 
     JOIN work__idrp_source_sears_import_center_step1 BY source_location_id,
          work__idrp_sears_location_xref_data BY location_id;


work__idrp_source_sears_import_center_step2 = 
      FOREACH join_work_xref
      GENERATE
              work__idrp_source_sears_import_center_step1::sears_division_nbr AS sears_division_nbr,
              work__idrp_source_sears_import_center_step1::sears_item_nbr AS sears_item_nbr,
              work__idrp_source_sears_import_center_step1::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_source_sears_import_center_step1::sears_location_id AS sears_location_id,
              work__idrp_source_sears_import_center_step1::location_id AS location_id,
              work__idrp_source_sears_import_center_step1::location_level_cd AS location_level_cd,
              work__idrp_source_sears_import_center_step1::location_format_type_cd AS location_format_type_cd,
              work__idrp_source_sears_import_center_step1::location_owner_cd AS location_owner_cd,
              work__idrp_source_sears_import_center_step1::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              work__idrp_source_sears_import_center_step1::active_ind AS active_ind,
              work__idrp_source_sears_import_center_step1::shc_item_id AS shc_item_id,
              work__idrp_source_sears_import_center_step1::ksn_id AS ksn_id,
              work__idrp_source_sears_import_center_step1::source_location_id AS source_location_id,
              work__idrp_source_sears_import_center_step1::source_package_qty AS source_package_qty,
              work__idrp_source_sears_import_center_step1::source_system_cd AS source_system_cd,
              work__idrp_source_sears_import_center_step1::vendor_package_id AS vendor_package_id,
              work__idrp_source_sears_import_center_step1::vendor_package_carton_qty AS vendor_package_carton_qty,
              work__idrp_sears_location_xref_data::sears_location_id AS sears_source_location_nbr,
              work__idrp_sears_location_xref_data::location_level_cd AS source_location_level_cd,
	      work__idrp_source_sears_import_center_step1::rim_last_record_creation_dt AS rim_last_record_creation_dt;


join_smith_work_import_cntr = 
     JOIN smith__idrp_ksn_attribute_current_data BY ((int)sears_division_nbr,TrimLeadingZeros(sears_item_nbr),(int)sears_sku_nbr),
          work__idrp_source_sears_import_center_step2 BY ((int)sears_division_nbr,TrimLeadingZeros(sears_item_nbr),(int)sears_sku_nbr);


work__idrp_source_sears_import_center_step3 = 
      FOREACH join_smith_work_import_cntr
      GENERATE
              work__idrp_source_sears_import_center_step2::sears_division_nbr AS sears_division_nbr,
              work__idrp_source_sears_import_center_step2::sears_item_nbr AS sears_item_nbr,
              work__idrp_source_sears_import_center_step2::sears_sku_nbr AS sears_sku_nbr,
              work__idrp_source_sears_import_center_step2::sears_location_id AS sears_location_id,
              work__idrp_source_sears_import_center_step2::location_id AS location_id,
              work__idrp_source_sears_import_center_step2::location_level_cd AS location_level_cd,
              work__idrp_source_sears_import_center_step2::location_format_type_cd AS location_format_type_cd,
              work__idrp_source_sears_import_center_step2::location_owner_cd AS location_owner_cd,
              work__idrp_source_sears_import_center_step2::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
              work__idrp_source_sears_import_center_step2::active_ind AS active_ind,
              work__idrp_source_sears_import_center_step2::shc_item_id AS shc_item_id,
              work__idrp_source_sears_import_center_step2::ksn_id AS ksn_id,
              work__idrp_source_sears_import_center_step2::source_location_id AS source_location_id,
              work__idrp_source_sears_import_center_step2::source_package_qty AS source_package_qty,
              work__idrp_source_sears_import_center_step2::source_system_cd AS source_system_cd,
              work__idrp_source_sears_import_center_step2::vendor_package_id AS vendor_package_id,
              work__idrp_source_sears_import_center_step2::vendor_package_carton_qty AS vendor_package_carton_qty,
              work__idrp_source_sears_import_center_step2::sears_source_location_nbr AS sears_source_location_nbr,
              work__idrp_source_sears_import_center_step2::source_location_level_cd AS source_location_level_cd,
              smith__idrp_ksn_attribute_current_data::special_retail_order_system_ind AS special_retail_order_system_ind,
              smith__idrp_ksn_attribute_current_data::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
              smith__idrp_ksn_attribute_current_data::dot_com_allocation_ind AS dot_com_allocation_ind,
              smith__idrp_ksn_attribute_current_data::distribution_type_cd AS distribution_type_cd,
              smith__idrp_ksn_attribute_current_data::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
              smith__idrp_ksn_attribute_current_data::special_order_candidate_ind AS special_order_candidate_ind,
              smith__idrp_ksn_attribute_current_data::item_emp_ind AS item_emp_ind,
              smith__idrp_ksn_attribute_current_data::easy_order_ind AS easy_order_ind,
              --smith__idrp_ksn_attribute_current_data::sams_migration_ind AS sams_migration_ind,
              smith__idrp_ksn_attribute_current_data::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
              smith__idrp_ksn_attribute_current_data::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
              smith__idrp_ksn_attribute_current_data::rapid_item_ind AS rapid_item_ind,
              smith__idrp_ksn_attribute_current_data::constrained_item_ind AS constrained_item_ind,
              smith__idrp_ksn_attribute_current_data::sears_import_ind AS sears_import_ind,
              smith__idrp_ksn_attribute_current_data::idrp_item_type_desc AS idrp_item_type_desc,
              smith__idrp_ksn_attribute_current_data::sams_migration_ind AS sams_migration_ind,
              smith__idrp_ksn_attribute_current_data::emp_to_jit_ind AS emp_to_jit_ind,
              smith__idrp_ksn_attribute_current_data::rim_flow_ind AS rim_flow_ind,
              smith__idrp_ksn_attribute_current_data::cross_merchandising_cd AS cross_merchandising_cd,
	      work__idrp_source_sears_import_center_step2::rim_last_record_creation_dt AS rim_last_record_creation_dt;



work__idrp_sourced_sears_import_center = 
      FOREACH work__idrp_source_sears_import_center_step3
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
              '' AS rim_status_cd,
              active_ind AS active_ind,
              source_package_qty  AS source_package_qty,
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
              '1' AS sears_import_ind,
              idrp_item_type_desc AS idrp_item_type_desc,
              cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
              sams_migration_ind AS sams_migration_ind,
              emp_to_jit_ind AS emp_to_jit_ind,
              rim_flow_ind AS rim_flow_ind,
              cross_merchandising_cd AS cross_merchandising_cd,
              source_system_cd  AS source_system_cd,
              ' ' AS original_source_nbr,
              ' ' AS item_active_ind,
              ' ' AS stock_type_cd,
              ' ' AS item_reserve_cd,
              ' ' AS non_stock_source_cd,
              --' ' AS product_condition_cd,
              ' ' AS item_next_period_on_hand_qty,
              ' ' AS item_on_order_qty,
              ' ' AS item_reserve_qty,
              ' ' AS item_back_order_qty,
              ' ' AS item_next_period_future_order_qty,
              ' ' AS item_next_period_in_transit_qty,
              ' ' AS item_last_receive_dt,
              ' ' AS item_last_ship_dt,
	      rim_last_record_creation_dt AS rim_last_record_creation_dt; 


STORE work__idrp_sourced_sears_import_center 
INTO '$WORK__IDRP_SOURCED_SEARS_IMPORT_CENTER_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
