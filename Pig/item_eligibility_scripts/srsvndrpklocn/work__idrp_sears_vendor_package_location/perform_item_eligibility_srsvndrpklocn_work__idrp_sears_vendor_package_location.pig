/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_work__idrp_sears_vendor_package_location.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Fri Jul 25 01:23:47 EDT 2014
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
#24/10/2014     Siddhivinayak Karpe     CR#3207 Added (vendor_package_id DESC) in order by clause Line changed 127,151,174,197
#28/10/2014             Priyanka Gurjar         CR#3240 Added constrain for empty sting and null at line:713 to populate replenishment_planning_ind
#14/11/2014             Siddhivinayak Karpe     CR#3327 Code change at line 269 270 & NULL Values handle for Vendor Pack ID AND UDF jar  register
#17/08/2015             Priyanka Gurjar         CR#4947 Added logic to convert Dummy vendors for EXAS rows and vend pack assignment need to account for TPW dummy vendors
#19/08/2015             Priyanka Gurjar         CR#5050 bypass EXAS items from DOS
#01/19/2017		Srujan Dussa		IPS-779 . Adding rim_last_record_create_dt from gold__inventory_rim_daily_current to be included in the Extract File to Shared Items.
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

smith__idrp_scan_based_trading_sears_location_current_data = 
     LOAD '$SMITH__IDRP_SCAN_BASED_TRADING_SEARS_LOCATION_CURRENT_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($SMITH__IDRP_SCAN_BASED_TRADING_SEARS_LOCATION_CURRENT_SCHEMA);


work__idrp_sourced_sears_location_data = 
     LOAD '$WORK__IDRP_SOURCED_SEARS_LOCATION_LOCATION'
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
     AS ($WORK__IDRP_SOURCED_SEARS_LOCATION_SCHEMA);


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


smith__idrp_eligible_loc_data = 
     LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);


smith__idrp_ksn_attribute_current_data = 
     LOAD '$SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_LOCATION' 
     USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
     AS ($SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_SCHEMA);



smith__idrp_vend_pack_combined_data_fltr = 
     FILTER smith__idrp_vend_pack_combined_data 
     BY TRIM(owner_cd)=='S';

smith__idrp_vend_pack_combined_data_fltr_gen = 
     FOREACH smith__idrp_vend_pack_combined_data_fltr 
     GENERATE 
             order_duns_nbr AS order_duns_nbr,
             purchase_status_cd AS purchase_status_cd,
             purchase_status_dt AS purchase_status_dt,
             ksn_id AS ksn_id,
             vendor_carton_qty AS vendor_carton_qty,
             flow_type_cd AS flow_type_cd,
             vendor_package_id AS vendor_package_id;


smith__idrp_eligible_loc_data_fltr = 
     FILTER smith__idrp_eligible_loc_data 
     BY TRIM(duns_type_cd)=='ORD';

smith__idrp_eligible_loc_data_fltr_gen = 
     FOREACH smith__idrp_eligible_loc_data_fltr 
     GENERATE 
             shc_vndr_nbr AS shc_vndr_nbr,
             loc AS loc;


join_smith_loc_vend_pack = 
     JOIN smith__idrp_eligible_loc_data_fltr_gen BY shc_vndr_nbr,
          smith__idrp_vend_pack_combined_data_fltr_gen BY order_duns_nbr;

join_smith_loc_vend_pack_gen = 
     FOREACH join_smith_loc_vend_pack 
     GENERATE 
             smith__idrp_vend_pack_combined_data_fltr_gen::ksn_id AS ksn_id,
             smith__idrp_eligible_loc_data_fltr_gen::loc AS duns_location_id,
             smith__idrp_vend_pack_combined_data_fltr_gen::vendor_carton_qty AS vendor_carton_qty,
             smith__idrp_vend_pack_combined_data_fltr_gen::flow_type_cd AS flow_type_cd,
             smith__idrp_vend_pack_combined_data_fltr_gen::vendor_package_id AS vendor_package_id,
             smith__idrp_vend_pack_combined_data_fltr_gen::purchase_status_cd AS purchase_status_cd,
             smith__idrp_vend_pack_combined_data_fltr_gen::purchase_status_dt AS purchase_status_dt;

group_join_data = 
     GROUP join_smith_loc_vend_pack_gen 
     BY (ksn_id,duns_location_id,vendor_carton_qty,flow_type_cd);


work__vendor_package_match1_gen = 
     FOREACH group_join_data{
                             ordered_data = ORDER join_smith_loc_vend_pack_gen BY purchase_status_cd ASC, purchase_status_dt DESC,vendor_package_id DESC;
                             first_record = LIMIT ordered_data 1;
                             GENERATE FLATTEN(first_record);
                            };


work__vendor_package_match1 = 
     FOREACH work__vendor_package_match1_gen 
     GENERATE 
             ksn_id AS ksn_id,
             duns_location_id AS duns_location_id,
             vendor_carton_qty AS vendor_carton_qty,
             flow_type_cd AS flow_type_cd,
             vendor_package_id AS vendor_package_id,
             purchase_status_cd AS purchase_status_cd,
             purchase_status_dt AS purchase_status_dt;


group_vendor_package_match1 = 
     GROUP work__vendor_package_match1 
     BY (ksn_id,duns_location_id,vendor_carton_qty);

group_vendor_package_match1_gen = 
     FOREACH group_vendor_package_match1{
                                         ordered_data = ORDER work__vendor_package_match1 BY purchase_status_cd ASC, purchase_status_dt DESC,vendor_package_id DESC;
                                         first_record = LIMIT ordered_data 1;
                                         GENERATE FLATTEN(first_record);
                                        };

work__vendor_package_match2 = 
     FOREACH group_vendor_package_match1_gen 
     GENERATE 
             ksn_id AS ksn_id,
             duns_location_id AS duns_location_id,
             vendor_carton_qty AS vendor_carton_qty,
             flow_type_cd AS flow_type_cd,
             vendor_package_id AS vendor_package_id,
             purchase_status_cd AS purchase_status_cd,
             purchase_status_dt AS purchase_status_dt;


group_vendor_package_match2 = 
     GROUP work__vendor_package_match1 
     BY (ksn_id,duns_location_id);

group_vendor_package_match2_gen = 
     FOREACH group_vendor_package_match2{
                                         ordered_data = ORDER work__vendor_package_match1 BY purchase_status_cd ASC, purchase_status_dt DESC,vendor_package_id DESC;
                                         first_record = LIMIT ordered_data 1;
                                         GENERATE FLATTEN(first_record);
                                        };

work__vendor_package_match3 = 
     FOREACH group_vendor_package_match2_gen 
     GENERATE 
             ksn_id AS ksn_id,
             duns_location_id AS duns_location_id,
             vendor_carton_qty AS vendor_carton_qty,
             flow_type_cd AS flow_type_cd,
             vendor_package_id AS vendor_package_id,
             purchase_status_cd AS purchase_status_cd,
             purchase_status_dt AS purchase_status_dt;


group_vendor_package_match3 = 
     GROUP work__vendor_package_match1
     BY ksn_id;

group_vendor_package_match3_gen = 
     FOREACH group_vendor_package_match3{
                                         ordered_data = ORDER work__vendor_package_match1 BY purchase_status_cd ASC, purchase_status_dt DESC,vendor_package_id DESC;
                                         first_record = LIMIT ordered_data 1;
                                         GENERATE FLATTEN(first_record);
                                        };

work__vendor_package_match4 = 
     FOREACH group_vendor_package_match3_gen
     GENERATE
             ksn_id AS ksn_id,
             duns_location_id AS duns_location_id,
             vendor_carton_qty AS vendor_carton_qty,
             flow_type_cd AS flow_type_cd,
             vendor_package_id AS vendor_package_id,
             purchase_status_cd AS purchase_status_cd,
             purchase_status_dt AS purchase_status_dt;


work__idrp_sears_vendor_package_location_step2 = 
     FOREACH work__idrp_sourced_sears_location_data
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
             active_ind AS active_ind,
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
             ( TRIM(purchase_order_vendor_location_id)!=''
                                ? purchase_order_vendor_location_id
                                :((original_source_nbr!='0' AND original_source_nbr!='' AND TRIM(original_source_nbr)!='')
                                        ? CONCAT_MULTIPLE('1',original_source_nbr,'_O')
                                        : purchase_order_vendor_location_id
                                 )
                         ) AS match_po_vend_loc_id, --CR4947
             ((TRIM(location_level_cd)=='WAREHOUSE') ? 'DC' : ((TRIM(location_level_cd)=='STORE' AND TRIM(source_location_level_cd)=='WAREHOUSE') ? 'DC' : ((TRIM(location_level_cd)=='STORE' AND TRIM(source_location_level_cd)=='VENDOR' AND TRIM(cross_merchandising_cd)=='EMP2JIT' AND TRIM(cross_merchandising_cd)!='' AND cross_merchandising_cd IS NOT NULL) ? 'JIT' : ((TRIM(location_level_cd)=='STORE' AND TRIM(source_location_level_cd)=='VENDOR' AND (TRIM(cross_merchandising_cd)!='EMP2JIT' OR TRIM(cross_merchandising_cd)=='' OR cross_merchandising_cd IS NULL)) ? 'DSD' : '')))) AS derived_flow_type_cd,
			 rim_last_record_creation_dt  AS rim_last_record_creation_dt;


outer_join_vendpack2_match1 = 
     JOIN work__idrp_sears_vendor_package_location_step2 BY (ksn_id,match_po_vend_loc_id,vendor_package_carton_qty,derived_flow_type_cd) LEFT OUTER,
          work__vendor_package_match1 BY (ksn_id,duns_location_id,vendor_carton_qty,flow_type_cd);


work__idrp_sears_vendor_package_location_step3 = 
     FOREACH outer_join_vendpack2_match1 
     GENERATE
             work__idrp_sears_vendor_package_location_step2::sears_division_nbr AS sears_division_nbr,
             work__idrp_sears_vendor_package_location_step2::sears_item_nbr AS sears_item_nbr,
             work__idrp_sears_vendor_package_location_step2::sears_sku_nbr AS sears_sku_nbr,
             work__idrp_sears_vendor_package_location_step2::sears_location_id AS sears_location_id,
             work__idrp_sears_vendor_package_location_step2::location_id AS location_id,
             work__idrp_sears_vendor_package_location_step2::location_level_cd AS location_level_cd,
             work__idrp_sears_vendor_package_location_step2::location_format_type_cd AS location_format_type_cd,
             work__idrp_sears_vendor_package_location_step2::location_owner_cd AS location_owner_cd,
             work__idrp_sears_vendor_package_location_step2::sears_source_location_nbr AS sears_source_location_nbr,
             work__idrp_sears_vendor_package_location_step2::source_location_id AS source_location_id,
             work__idrp_sears_vendor_package_location_step2::source_location_level_cd AS source_location_level_cd,
             work__idrp_sears_vendor_package_location_step2::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
             work__idrp_sears_vendor_package_location_step2::rim_status_cd AS rim_status_cd,
             work__idrp_sears_vendor_package_location_step2::active_ind AS active_ind,
             work__idrp_sears_vendor_package_location_step2::source_package_qty AS source_package_qty,
             work__idrp_sears_vendor_package_location_step2::shc_item_id AS shc_item_id,
             work__idrp_sears_vendor_package_location_step2::ksn_id AS ksn_id,
			((work__vendor_package_match1::ksn_id IS NOT NULL) AND (work__idrp_sears_vendor_package_location_step2::vendor_package_id IS NULL OR work__idrp_sears_vendor_package_location_step2::vendor_package_id=='') ? work__vendor_package_match1::vendor_package_id : work__idrp_sears_vendor_package_location_step2::vendor_package_id) AS vendor_package_id,
             work__idrp_sears_vendor_package_location_step2::vendor_package_carton_qty AS vendor_package_carton_qty,
             work__idrp_sears_vendor_package_location_step2::special_retail_order_system_ind AS special_retail_order_system_ind,
             work__idrp_sears_vendor_package_location_step2::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
             work__idrp_sears_vendor_package_location_step2::dot_com_allocation_ind AS dot_com_allocation_ind,
             work__idrp_sears_vendor_package_location_step2::distribution_type_cd AS distribution_type_cd,
             work__idrp_sears_vendor_package_location_step2::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
             work__idrp_sears_vendor_package_location_step2::special_order_candidate_ind AS special_order_candidate_ind,
             work__idrp_sears_vendor_package_location_step2::item_emp_ind AS item_emp_ind,
             work__idrp_sears_vendor_package_location_step2::easy_order_ind AS easy_order_ind,
             work__idrp_sears_vendor_package_location_step2::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
             work__idrp_sears_vendor_package_location_step2::rapid_item_ind AS rapid_item_ind,
             work__idrp_sears_vendor_package_location_step2::constrained_item_ind AS constrained_item_ind,
             work__idrp_sears_vendor_package_location_step2::sears_import_ind AS sears_import_ind,
             work__idrp_sears_vendor_package_location_step2::idrp_item_type_desc AS idrp_item_type_desc,
             work__idrp_sears_vendor_package_location_step2::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
             work__idrp_sears_vendor_package_location_step2::sams_migration_ind AS sams_migration_ind,
             work__idrp_sears_vendor_package_location_step2::emp_to_jit_ind AS emp_to_jit_ind,
             work__idrp_sears_vendor_package_location_step2::rim_flow_ind AS rim_flow_ind,
             work__idrp_sears_vendor_package_location_step2::cross_merchandising_cd AS cross_merchandising_cd,
             work__idrp_sears_vendor_package_location_step2::source_system_cd AS source_system_cd,
             work__idrp_sears_vendor_package_location_step2::original_source_nbr AS original_source_nbr,
             work__idrp_sears_vendor_package_location_step2::item_active_ind AS item_active_ind,
             work__idrp_sears_vendor_package_location_step2::stock_type_cd AS stock_type_cd,
             work__idrp_sears_vendor_package_location_step2::item_reserve_cd AS item_reserve_cd,
             work__idrp_sears_vendor_package_location_step2::non_stock_source_cd AS non_stock_source_cd,
             --work__idrp_sears_vendor_package_location_step2::product_condition_cd AS product_condition_cd,
             work__idrp_sears_vendor_package_location_step2::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             work__idrp_sears_vendor_package_location_step2::item_on_order_qty AS item_on_order_qty,
             work__idrp_sears_vendor_package_location_step2::item_reserve_qty AS item_reserve_qty,
             work__idrp_sears_vendor_package_location_step2::item_back_order_qty AS item_back_order_qty,
             work__idrp_sears_vendor_package_location_step2::item_next_period_future_order_qty AS item_next_period_future_order_qty,
             work__idrp_sears_vendor_package_location_step2::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             work__idrp_sears_vendor_package_location_step2::item_last_receive_dt AS item_last_receive_dt,
             work__idrp_sears_vendor_package_location_step2::item_last_ship_dt AS item_last_ship_dt,
             work__idrp_sears_vendor_package_location_step2::match_po_vend_loc_id AS match_po_vend_loc_id,
             work__idrp_sears_vendor_package_location_step2::derived_flow_type_cd AS derived_flow_type_cd,
	         work__idrp_sears_vendor_package_location_step2::rim_last_record_creation_dt AS rim_last_record_creation_dt;


outer_join_step3_match2 = 
      JOIN work__idrp_sears_vendor_package_location_step3 BY (ksn_id,match_po_vend_loc_id,vendor_package_carton_qty) LEFT OUTER,
           work__vendor_package_match2 BY (ksn_id,duns_location_id,vendor_carton_qty);


work__idrp_sears_vendor_package_location_step4 = 
     FOREACH outer_join_step3_match2 
     GENERATE
             work__idrp_sears_vendor_package_location_step3::sears_division_nbr AS sears_division_nbr,
             work__idrp_sears_vendor_package_location_step3::sears_item_nbr AS sears_item_nbr,
             work__idrp_sears_vendor_package_location_step3::sears_sku_nbr AS sears_sku_nbr,
             work__idrp_sears_vendor_package_location_step3::sears_location_id AS sears_location_id,
             work__idrp_sears_vendor_package_location_step3::location_id AS location_id,
             work__idrp_sears_vendor_package_location_step3::location_level_cd AS location_level_cd,
             work__idrp_sears_vendor_package_location_step3::location_format_type_cd AS location_format_type_cd,
             work__idrp_sears_vendor_package_location_step3::location_owner_cd AS location_owner_cd,
             work__idrp_sears_vendor_package_location_step3::sears_source_location_nbr AS sears_source_location_nbr,
             work__idrp_sears_vendor_package_location_step3::source_location_id AS source_location_id,
             work__idrp_sears_vendor_package_location_step3::source_location_level_cd AS source_location_level_cd,
             work__idrp_sears_vendor_package_location_step3::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
             work__idrp_sears_vendor_package_location_step3::rim_status_cd AS rim_status_cd,
             work__idrp_sears_vendor_package_location_step3::active_ind AS active_ind,
             work__idrp_sears_vendor_package_location_step3::source_package_qty AS source_package_qty,
             work__idrp_sears_vendor_package_location_step3::shc_item_id AS shc_item_id,
             work__idrp_sears_vendor_package_location_step3::ksn_id AS ksn_id,
             ((work__vendor_package_match2::ksn_id IS NOT NULL) AND (work__idrp_sears_vendor_package_location_step3::vendor_package_id IS NULL OR work__idrp_sears_vendor_package_location_step3::vendor_package_id=='') ? work__vendor_package_match2::vendor_package_id : work__idrp_sears_vendor_package_location_step3::vendor_package_id) AS vendor_package_id,
             work__idrp_sears_vendor_package_location_step3::vendor_package_carton_qty AS vendor_package_carton_qty,
             work__idrp_sears_vendor_package_location_step3::special_retail_order_system_ind AS special_retail_order_system_ind,
             work__idrp_sears_vendor_package_location_step3::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
             work__idrp_sears_vendor_package_location_step3::dot_com_allocation_ind AS dot_com_allocation_ind,
             work__idrp_sears_vendor_package_location_step3::distribution_type_cd AS distribution_type_cd,
             work__idrp_sears_vendor_package_location_step3::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
             work__idrp_sears_vendor_package_location_step3::special_order_candidate_ind AS special_order_candidate_ind,
             work__idrp_sears_vendor_package_location_step3::item_emp_ind AS item_emp_ind,
             work__idrp_sears_vendor_package_location_step3::easy_order_ind AS easy_order_ind,
             work__idrp_sears_vendor_package_location_step3::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
             work__idrp_sears_vendor_package_location_step3::rapid_item_ind AS rapid_item_ind,
             work__idrp_sears_vendor_package_location_step3::constrained_item_ind AS constrained_item_ind,
             work__idrp_sears_vendor_package_location_step3::sears_import_ind AS sears_import_ind,
             work__idrp_sears_vendor_package_location_step3::idrp_item_type_desc AS idrp_item_type_desc,
             work__idrp_sears_vendor_package_location_step3::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
             work__idrp_sears_vendor_package_location_step3::sams_migration_ind AS sams_migration_ind,
             work__idrp_sears_vendor_package_location_step3::emp_to_jit_ind AS emp_to_jit_ind,
             work__idrp_sears_vendor_package_location_step3::rim_flow_ind AS rim_flow_ind,
             work__idrp_sears_vendor_package_location_step3::cross_merchandising_cd AS cross_merchandising_cd,
             work__idrp_sears_vendor_package_location_step3::source_system_cd AS source_system_cd,
             work__idrp_sears_vendor_package_location_step3::original_source_nbr AS original_source_nbr,
             work__idrp_sears_vendor_package_location_step3::item_active_ind AS item_active_ind,
             work__idrp_sears_vendor_package_location_step3::stock_type_cd AS stock_type_cd,
             work__idrp_sears_vendor_package_location_step3::item_reserve_cd AS item_reserve_cd,
             work__idrp_sears_vendor_package_location_step3::non_stock_source_cd AS non_stock_source_cd,
             --work__idrp_sears_vendor_package_location_step3::product_condition_cd AS product_condition_cd,
             work__idrp_sears_vendor_package_location_step3::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             work__idrp_sears_vendor_package_location_step3::item_on_order_qty AS item_on_order_qty,
             work__idrp_sears_vendor_package_location_step3::item_reserve_qty AS item_reserve_qty,
             work__idrp_sears_vendor_package_location_step3::item_back_order_qty AS item_back_order_qty,
             work__idrp_sears_vendor_package_location_step3::item_next_period_future_order_qty AS item_next_period_future_order_qty,
             work__idrp_sears_vendor_package_location_step3::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             work__idrp_sears_vendor_package_location_step3::item_last_receive_dt AS item_last_receive_dt,
             work__idrp_sears_vendor_package_location_step3::item_last_ship_dt AS item_last_ship_dt,
             work__idrp_sears_vendor_package_location_step3::match_po_vend_loc_id AS match_po_vend_loc_id,
             work__idrp_sears_vendor_package_location_step3::derived_flow_type_cd AS derived_flow_type_cd,
 	         work__idrp_sears_vendor_package_location_step3::rim_last_record_creation_dt AS rim_last_record_creation_dt;


outer_join_step4_match3 = 
     JOIN work__idrp_sears_vendor_package_location_step4 BY (ksn_id,match_po_vend_loc_id) LEFT OUTER,
          work__vendor_package_match3 BY (ksn_id,duns_location_id);


work__idrp_sears_vendor_package_location_step5 = 
     FOREACH outer_join_step4_match3 
     GENERATE
             work__idrp_sears_vendor_package_location_step4::sears_division_nbr AS sears_division_nbr,
             work__idrp_sears_vendor_package_location_step4::sears_item_nbr AS sears_item_nbr,
             work__idrp_sears_vendor_package_location_step4::sears_sku_nbr AS sears_sku_nbr,
             work__idrp_sears_vendor_package_location_step4::sears_location_id AS sears_location_id,
             work__idrp_sears_vendor_package_location_step4::location_id AS location_id,
             work__idrp_sears_vendor_package_location_step4::location_level_cd AS location_level_cd,
             work__idrp_sears_vendor_package_location_step4::location_format_type_cd AS location_format_type_cd,
             work__idrp_sears_vendor_package_location_step4::location_owner_cd AS location_owner_cd,
             work__idrp_sears_vendor_package_location_step4::sears_source_location_nbr AS sears_source_location_nbr,
             work__idrp_sears_vendor_package_location_step4::source_location_id AS source_location_id,
             work__idrp_sears_vendor_package_location_step4::source_location_level_cd AS source_location_level_cd,
             work__idrp_sears_vendor_package_location_step4::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
             work__idrp_sears_vendor_package_location_step4::rim_status_cd AS rim_status_cd,
             work__idrp_sears_vendor_package_location_step4::active_ind AS active_ind,
             work__idrp_sears_vendor_package_location_step4::source_package_qty AS source_package_qty,
             work__idrp_sears_vendor_package_location_step4::shc_item_id AS shc_item_id,
             work__idrp_sears_vendor_package_location_step4::ksn_id AS ksn_id,
             ((work__vendor_package_match3::ksn_id IS NOT NULL) AND (work__idrp_sears_vendor_package_location_step4::vendor_package_id IS NULL OR work__idrp_sears_vendor_package_location_step4::vendor_package_id=='') ? work__vendor_package_match3::vendor_package_id: work__idrp_sears_vendor_package_location_step4::vendor_package_id) AS vendor_package_id,
             work__idrp_sears_vendor_package_location_step4::vendor_package_carton_qty AS vendor_package_carton_qty,
             work__idrp_sears_vendor_package_location_step4::special_retail_order_system_ind AS special_retail_order_system_ind,
             work__idrp_sears_vendor_package_location_step4::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
             work__idrp_sears_vendor_package_location_step4::dot_com_allocation_ind AS dot_com_allocation_ind,
             work__idrp_sears_vendor_package_location_step4::distribution_type_cd AS distribution_type_cd,
             work__idrp_sears_vendor_package_location_step4::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
             work__idrp_sears_vendor_package_location_step4::special_order_candidate_ind AS special_order_candidate_ind,
             work__idrp_sears_vendor_package_location_step4::item_emp_ind AS item_emp_ind,
             work__idrp_sears_vendor_package_location_step4::easy_order_ind AS easy_order_ind,
             work__idrp_sears_vendor_package_location_step4::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
             work__idrp_sears_vendor_package_location_step4::rapid_item_ind AS rapid_item_ind,
             work__idrp_sears_vendor_package_location_step4::constrained_item_ind AS constrained_item_ind,
             work__idrp_sears_vendor_package_location_step4::sears_import_ind AS sears_import_ind,
             work__idrp_sears_vendor_package_location_step4::idrp_item_type_desc AS idrp_item_type_desc,
             work__idrp_sears_vendor_package_location_step4::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
             work__idrp_sears_vendor_package_location_step4::sams_migration_ind AS sams_migration_ind,
             work__idrp_sears_vendor_package_location_step4::emp_to_jit_ind AS emp_to_jit_ind,
             work__idrp_sears_vendor_package_location_step4::rim_flow_ind AS rim_flow_ind,
             work__idrp_sears_vendor_package_location_step4::cross_merchandising_cd AS cross_merchandising_cd,
             work__idrp_sears_vendor_package_location_step4::source_system_cd AS source_system_cd,
             work__idrp_sears_vendor_package_location_step4::original_source_nbr AS original_source_nbr,
             work__idrp_sears_vendor_package_location_step4::item_active_ind AS item_active_ind,
             work__idrp_sears_vendor_package_location_step4::stock_type_cd AS stock_type_cd,
             work__idrp_sears_vendor_package_location_step4::item_reserve_cd AS item_reserve_cd,
             work__idrp_sears_vendor_package_location_step4::non_stock_source_cd AS non_stock_source_cd,
             --work__idrp_sears_vendor_package_location_step4::product_condition_cd AS product_condition_cd,
             work__idrp_sears_vendor_package_location_step4::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             work__idrp_sears_vendor_package_location_step4::item_on_order_qty AS item_on_order_qty,
             work__idrp_sears_vendor_package_location_step4::item_reserve_qty AS item_reserve_qty,
             work__idrp_sears_vendor_package_location_step4::item_back_order_qty AS item_back_order_qty,
             work__idrp_sears_vendor_package_location_step4::item_next_period_future_order_qty AS item_next_period_future_order_qty,
             work__idrp_sears_vendor_package_location_step4::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             work__idrp_sears_vendor_package_location_step4::item_last_receive_dt AS item_last_receive_dt,
             work__idrp_sears_vendor_package_location_step4::item_last_ship_dt AS item_last_ship_dt,
             work__idrp_sears_vendor_package_location_step4::match_po_vend_loc_id AS match_po_vend_loc_id,
             work__idrp_sears_vendor_package_location_step4::derived_flow_type_cd AS derived_flow_type_cd,
	         work__idrp_sears_vendor_package_location_step4::rim_last_record_creation_dt AS rim_last_record_creation_dt;


outer_join_step5_match4 = 
     JOIN work__idrp_sears_vendor_package_location_step5 BY (ksn_id) LEFT OUTER,
          work__vendor_package_match4 BY (ksn_id);


work__idrp_sears_vendor_package_location_step6 = 
     FOREACH outer_join_step5_match4 
     GENERATE
             work__idrp_sears_vendor_package_location_step5::sears_division_nbr AS sears_division_nbr,
             work__idrp_sears_vendor_package_location_step5::sears_item_nbr AS sears_item_nbr,
             work__idrp_sears_vendor_package_location_step5::sears_sku_nbr AS sears_sku_nbr,
             work__idrp_sears_vendor_package_location_step5::sears_location_id AS sears_location_id,
             work__idrp_sears_vendor_package_location_step5::location_id AS location_id,
             work__idrp_sears_vendor_package_location_step5::location_level_cd AS location_level_cd,
             work__idrp_sears_vendor_package_location_step5::location_format_type_cd AS location_format_type_cd,
             work__idrp_sears_vendor_package_location_step5::location_owner_cd AS location_owner_cd,
             work__idrp_sears_vendor_package_location_step5::sears_source_location_nbr AS sears_source_location_nbr,
             work__idrp_sears_vendor_package_location_step5::source_location_id AS source_location_id,
             work__idrp_sears_vendor_package_location_step5::source_location_level_cd AS source_location_level_cd,
             work__idrp_sears_vendor_package_location_step5::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
             work__idrp_sears_vendor_package_location_step5::rim_status_cd AS rim_status_cd,
             work__idrp_sears_vendor_package_location_step5::active_ind AS active_ind,
             work__idrp_sears_vendor_package_location_step5::source_package_qty AS source_package_qty,
             work__idrp_sears_vendor_package_location_step5::shc_item_id AS shc_item_id,
             work__idrp_sears_vendor_package_location_step5::ksn_id AS ksn_id,
             ((work__vendor_package_match4::ksn_id IS NOT NULL) AND (work__idrp_sears_vendor_package_location_step5::vendor_package_id IS NULL OR work__idrp_sears_vendor_package_location_step5::vendor_package_id=='') ? work__vendor_package_match4::vendor_package_id : work__idrp_sears_vendor_package_location_step5::vendor_package_id) AS vendor_package_id,
             work__idrp_sears_vendor_package_location_step5::vendor_package_carton_qty AS vendor_package_carton_qty,
             work__idrp_sears_vendor_package_location_step5::special_retail_order_system_ind AS special_retail_order_system_ind,
             work__idrp_sears_vendor_package_location_step5::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
             work__idrp_sears_vendor_package_location_step5::dot_com_allocation_ind AS dot_com_allocation_ind,
             work__idrp_sears_vendor_package_location_step5::distribution_type_cd AS distribution_type_cd,
             work__idrp_sears_vendor_package_location_step5::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
             work__idrp_sears_vendor_package_location_step5::special_order_candidate_ind AS special_order_candidate_ind,
             work__idrp_sears_vendor_package_location_step5::item_emp_ind AS item_emp_ind,
             work__idrp_sears_vendor_package_location_step5::easy_order_ind AS easy_order_ind,
             work__idrp_sears_vendor_package_location_step5::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
             work__idrp_sears_vendor_package_location_step5::rapid_item_ind AS rapid_item_ind,
             work__idrp_sears_vendor_package_location_step5::constrained_item_ind AS constrained_item_ind,
             work__idrp_sears_vendor_package_location_step5::sears_import_ind AS sears_import_ind,
             work__idrp_sears_vendor_package_location_step5::idrp_item_type_desc AS idrp_item_type_desc,
             work__idrp_sears_vendor_package_location_step5::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
             work__idrp_sears_vendor_package_location_step5::sams_migration_ind AS sams_migration_ind,
             work__idrp_sears_vendor_package_location_step5::emp_to_jit_ind AS emp_to_jit_ind,
             work__idrp_sears_vendor_package_location_step5::rim_flow_ind AS rim_flow_ind,
             work__idrp_sears_vendor_package_location_step5::cross_merchandising_cd AS cross_merchandising_cd,
             work__idrp_sears_vendor_package_location_step5::source_system_cd AS source_system_cd,
             work__idrp_sears_vendor_package_location_step5::original_source_nbr AS original_source_nbr,
             work__idrp_sears_vendor_package_location_step5::item_active_ind AS item_active_ind,
             work__idrp_sears_vendor_package_location_step5::stock_type_cd AS stock_type_cd,
             work__idrp_sears_vendor_package_location_step5::item_reserve_cd AS item_reserve_cd,
             work__idrp_sears_vendor_package_location_step5::non_stock_source_cd AS non_stock_source_cd,
             --work__idrp_sears_vendor_package_location_step5::product_condition_cd AS product_condition_cd,
             work__idrp_sears_vendor_package_location_step5::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             work__idrp_sears_vendor_package_location_step5::item_on_order_qty AS item_on_order_qty,
             work__idrp_sears_vendor_package_location_step5::item_reserve_qty AS item_reserve_qty,
             work__idrp_sears_vendor_package_location_step5::item_back_order_qty AS item_back_order_qty,
             work__idrp_sears_vendor_package_location_step5::item_next_period_future_order_qty AS item_next_period_future_order_qty,
             work__idrp_sears_vendor_package_location_step5::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             work__idrp_sears_vendor_package_location_step5::item_last_receive_dt AS item_last_receive_dt,
             work__idrp_sears_vendor_package_location_step5::item_last_ship_dt AS item_last_ship_dt,
             work__idrp_sears_vendor_package_location_step5::match_po_vend_loc_id AS match_po_vend_loc_id,
             work__idrp_sears_vendor_package_location_step5::derived_flow_type_cd AS derived_flow_type_cd,
	         work__idrp_sears_vendor_package_location_step5::rim_last_record_creation_dt AS rim_last_record_creation_dt;



outer_join_step6_smith_vend_pack = 
     JOIN work__idrp_sears_vendor_package_location_step6 BY vendor_package_id,
          smith__idrp_vend_pack_combined_data BY vendor_package_id;


outer_vend_pack_join_dc_combined = 
     JOIN outer_join_step6_smith_vend_pack BY (work__idrp_sears_vendor_package_location_step6::vendor_package_id,work__idrp_sears_vendor_package_location_step6::location_id) LEFT OUTER,
          smith__idrp_vend_pack_dc_combined_data BY (vendor_package_id,location_nbr);


outer_join_abovestep_shc_item = 
     JOIN outer_vend_pack_join_dc_combined BY outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::shc_item_id LEFT OUTER,
          smith__idrp_shc_item_combined_data BY shc_item_id;



outer_join_abovestep_ksn_attrinute = 
     JOIN outer_join_abovestep_shc_item BY outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::ksn_id LEFT OUTER,
          smith__idrp_ksn_attribute_current_data BY ksn_id;
		  
		  
----------------CR5050------------------------------------------------------------

flt_outer_join_abovestep_ksn_attrinute = filter outer_join_abovestep_ksn_attrinute by outer_join_abovestep_shc_item::smith__idrp_shc_item_combined_data::shc_item_type_cd != 'EXAS';		  


work__idrp_sears_vendor_package_location_step7 = 
     FOREACH flt_outer_join_abovestep_ksn_attrinute 
     GENERATE
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::sears_division_nbr AS sears_division_nbr,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::sears_item_nbr AS sears_item_nbr,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::sears_sku_nbr AS sears_sku_nbr,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::sears_location_id AS sears_location_id,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::location_id AS location_id,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::location_level_cd AS location_level_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::location_format_type_cd AS location_format_type_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::location_owner_cd AS location_owner_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::sears_source_location_nbr AS sears_source_location_nbr,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::source_location_id AS source_location_id,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::source_location_level_cd AS source_location_level_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::rim_status_cd AS rim_status_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::active_ind AS active_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::source_package_qty AS source_package_qty,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::shc_item_id AS shc_item_id,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::ksn_id AS ksn_id,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::vendor_package_id AS vendor_package_id,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::vendor_package_carton_qty AS vendor_package_carton_qty,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::special_retail_order_system_ind AS special_retail_order_system_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::dot_com_allocation_ind AS dot_com_allocation_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::distribution_type_cd AS distribution_type_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::special_order_candidate_ind AS special_order_candidate_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::item_emp_ind AS item_emp_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::easy_order_ind AS easy_order_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::rapid_item_ind AS rapid_item_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::constrained_item_ind AS constrained_item_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::sears_import_ind AS sears_import_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::idrp_item_type_desc AS idrp_item_type_desc,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::sams_migration_ind AS sams_migration_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::emp_to_jit_ind AS emp_to_jit_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::rim_flow_ind AS rim_flow_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::cross_merchandising_cd AS cross_merchandising_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::source_system_cd AS source_system_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::original_source_nbr AS original_source_nbr,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::item_active_ind AS item_active_ind,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::stock_type_cd AS stock_type_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::item_reserve_cd AS item_reserve_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::non_stock_source_cd AS non_stock_source_cd,
             --outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::product_condition_cd AS product_condition_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::item_on_order_qty AS item_on_order_qty,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::item_reserve_qty AS item_reserve_qty,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::item_back_order_qty AS item_back_order_qty,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::item_next_period_future_order_qty AS item_next_period_future_order_qty,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::item_last_receive_dt AS item_last_receive_dt,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::item_last_ship_dt AS item_last_ship_dt,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::match_po_vend_loc_id AS match_po_vend_loc_id,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::derived_flow_type_cd AS derived_flow_type_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::smith__idrp_vend_pack_combined_data::purchase_status_cd AS vendor_package_purchase_status_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::smith__idrp_vend_pack_combined_data::purchase_status_dt AS vendor_package_purchase_status_dt,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::smith__idrp_vend_pack_combined_data::flow_type_cd AS flow_type_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::smith__idrp_vend_pack_combined_data::owner_cd AS vendor_package_owner_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::smith__idrp_vend_pack_combined_data::vendor_stock_nbr AS vendor_stock_nbr,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::smith__idrp_vend_pack_combined_data::ksn_package_id AS ksn_package_id,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::smith__idrp_vend_pack_dc_combined_data::ksn_pack_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
             outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::smith__idrp_vend_pack_dc_combined_data::substition_eligibile_ind AS substitution_eligible_ind,
             outer_join_abovestep_shc_item::smith__idrp_shc_item_combined_data::purchase_status_cd AS item_purchase_status_cd,
             outer_join_abovestep_shc_item::smith__idrp_shc_item_combined_data::can_carry_model_id AS can_carry_model_id,
             outer_join_abovestep_shc_item::smith__idrp_shc_item_combined_data::idrp_order_method_cd AS allocation_replenishment_cd,
             smith__idrp_ksn_attribute_current_data::ksn_purchase_status_cd AS ksn_purchase_status_cd,
		     outer_join_abovestep_shc_item::outer_vend_pack_join_dc_combined::outer_join_step6_smith_vend_pack::work__idrp_sears_vendor_package_location_step6::rim_last_record_creation_dt AS rim_last_record_creation_dt;



outer_join_step7_scan_based_trading = 
     JOIN work__idrp_sears_vendor_package_location_step7 BY ((int)sears_division_nbr,(int)sears_item_nbr,sears_location_id) LEFT OUTER,
          smith__idrp_scan_based_trading_sears_location_current_data BY ((int)sears_division_nbr,(int)sears_item_nbr,sears_store_nbr);


work__idrp_sears_vendor_package_location_step8 = 
     FOREACH outer_join_step7_scan_based_trading 
     GENERATE
             work__idrp_sears_vendor_package_location_step7::sears_division_nbr AS sears_division_nbr,
             work__idrp_sears_vendor_package_location_step7::sears_item_nbr AS sears_item_nbr,
             work__idrp_sears_vendor_package_location_step7::sears_sku_nbr AS sears_sku_nbr,
             work__idrp_sears_vendor_package_location_step7::sears_location_id AS sears_location_id,
             work__idrp_sears_vendor_package_location_step7::location_id AS location_id,
             work__idrp_sears_vendor_package_location_step7::location_level_cd AS location_level_cd,
             work__idrp_sears_vendor_package_location_step7::location_format_type_cd AS location_format_type_cd,
             work__idrp_sears_vendor_package_location_step7::location_owner_cd AS location_owner_cd,
             work__idrp_sears_vendor_package_location_step7::sears_source_location_nbr AS sears_source_location_nbr,
             work__idrp_sears_vendor_package_location_step7::source_location_id AS source_location_id,
             work__idrp_sears_vendor_package_location_step7::source_location_level_cd AS source_location_level_cd,
             work__idrp_sears_vendor_package_location_step7::purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
             work__idrp_sears_vendor_package_location_step7::rim_status_cd AS rim_status_cd,
             work__idrp_sears_vendor_package_location_step7::active_ind AS active_ind,
             work__idrp_sears_vendor_package_location_step7::source_package_qty AS source_package_qty,
             work__idrp_sears_vendor_package_location_step7::shc_item_id AS shc_item_id,
             work__idrp_sears_vendor_package_location_step7::ksn_id AS ksn_id,
             work__idrp_sears_vendor_package_location_step7::vendor_package_id AS vendor_package_id,
             work__idrp_sears_vendor_package_location_step7::vendor_package_carton_qty AS vendor_package_carton_qty,
             work__idrp_sears_vendor_package_location_step7::special_retail_order_system_ind AS special_retail_order_system_ind,
             work__idrp_sears_vendor_package_location_step7::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
             work__idrp_sears_vendor_package_location_step7::dot_com_allocation_ind AS dot_com_allocation_ind,
             work__idrp_sears_vendor_package_location_step7::distribution_type_cd AS distribution_type_cd,
             work__idrp_sears_vendor_package_location_step7::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
             work__idrp_sears_vendor_package_location_step7::special_order_candidate_ind AS special_order_candidate_ind,
             work__idrp_sears_vendor_package_location_step7::item_emp_ind AS item_emp_ind,
             work__idrp_sears_vendor_package_location_step7::easy_order_ind AS easy_order_ind,
             work__idrp_sears_vendor_package_location_step7::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
             work__idrp_sears_vendor_package_location_step7::rapid_item_ind AS rapid_item_ind,
             work__idrp_sears_vendor_package_location_step7::constrained_item_ind AS constrained_item_ind,
             work__idrp_sears_vendor_package_location_step7::sears_import_ind AS sears_import_ind,
             work__idrp_sears_vendor_package_location_step7::idrp_item_type_desc AS idrp_item_type_desc,
             work__idrp_sears_vendor_package_location_step7::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
             work__idrp_sears_vendor_package_location_step7::sams_migration_ind AS sams_migration_ind,
             work__idrp_sears_vendor_package_location_step7::emp_to_jit_ind AS emp_to_jit_ind,
             work__idrp_sears_vendor_package_location_step7::rim_flow_ind AS rim_flow_ind,
             work__idrp_sears_vendor_package_location_step7::cross_merchandising_cd AS cross_merchandising_cd,
             work__idrp_sears_vendor_package_location_step7::source_system_cd AS source_system_cd,
             work__idrp_sears_vendor_package_location_step7::original_source_nbr AS original_source_nbr,
             work__idrp_sears_vendor_package_location_step7::item_active_ind AS item_active_ind,
             work__idrp_sears_vendor_package_location_step7::stock_type_cd AS stock_type_cd,
             work__idrp_sears_vendor_package_location_step7::item_reserve_cd AS item_reserve_cd,
             work__idrp_sears_vendor_package_location_step7::non_stock_source_cd AS non_stock_source_cd,
             --work__idrp_sears_vendor_package_location_step7::product_condition_cd AS product_condition_cd,
             work__idrp_sears_vendor_package_location_step7::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             work__idrp_sears_vendor_package_location_step7::item_on_order_qty AS item_on_order_qty,
             work__idrp_sears_vendor_package_location_step7::item_reserve_qty AS item_reserve_qty,
             work__idrp_sears_vendor_package_location_step7::item_back_order_qty AS item_back_order_qty,
             work__idrp_sears_vendor_package_location_step7::item_next_period_future_order_qty AS item_next_period_future_order_qty,
             work__idrp_sears_vendor_package_location_step7::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             work__idrp_sears_vendor_package_location_step7::item_last_receive_dt AS item_last_receive_dt,
             work__idrp_sears_vendor_package_location_step7::item_last_ship_dt AS item_last_ship_dt,
             work__idrp_sears_vendor_package_location_step7::match_po_vend_loc_id AS match_po_vend_loc_id,
             work__idrp_sears_vendor_package_location_step7::derived_flow_type_cd AS derived_flow_type_cd,
             work__idrp_sears_vendor_package_location_step7::vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
             work__idrp_sears_vendor_package_location_step7::vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
             work__idrp_sears_vendor_package_location_step7::flow_type_cd AS flow_type_cd,
             work__idrp_sears_vendor_package_location_step7::vendor_package_owner_cd AS vendor_package_owner_cd,
             work__idrp_sears_vendor_package_location_step7::vendor_stock_nbr AS vendor_stock_nbr,
             work__idrp_sears_vendor_package_location_step7::ksn_package_id AS ksn_package_id,
             work__idrp_sears_vendor_package_location_step7::ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
             work__idrp_sears_vendor_package_location_step7::substitution_eligible_ind AS substitution_eligible_ind,
             work__idrp_sears_vendor_package_location_step7::item_purchase_status_cd AS item_purchase_status_cd,
             work__idrp_sears_vendor_package_location_step7::can_carry_model_id AS can_carry_model_id,
             work__idrp_sears_vendor_package_location_step7::allocation_replenishment_cd AS allocation_replenishment_cd,
             work__idrp_sears_vendor_package_location_step7::ksn_purchase_status_cd AS ksn_purchase_status_cd,
             ((smith__idrp_scan_based_trading_sears_location_current_data::sears_scan_based_trading_duns_nbr IS NULL ) ? 'N' : (((smith__idrp_scan_based_trading_sears_location_current_data::sears_scan_based_trading_duns_nbr==work__idrp_sears_vendor_package_location_step7::sears_source_location_nbr) OR (source_location_level_cd=='WAREHOUSE' AND data_center_filled_ind=='Y')) ? 'Y' : 'N')) AS scan_based_trading_ind,
	         work__idrp_sears_vendor_package_location_step7::rim_last_record_creation_dt AS rim_last_record_creation_dt;


----------implemented CR #3240 on 28thOct2014--------------------------------------
			 
work__idrp_sears_vendor_package_location = 
     FOREACH work__idrp_sears_vendor_package_location_step8 
     GENERATE
             vendor_package_id AS vendor_package_id,
             location_id AS location_id,
             location_format_type_cd AS location_format_type_cd,
             location_level_cd AS location_level_cd,
             location_owner_cd AS location_owner_cd,
             'S' AS source_owner_cd,
             active_ind AS active_ind,
             '$CURRENT_DATE' AS active_ind_change_dt,
             allocation_replenishment_cd AS allocation_replenishment_cd,
             purchase_order_vendor_location_id AS purchase_order_vendor_location_id,
             ((location_level_cd=='STORE' AND active_ind=='Y') ? 'Y' : ((location_level_cd=='WAREHOUSE' AND active_ind=='Y' AND (TRIM(non_stock_source_cd)=='' or non_stock_source_cd is null or non_stock_source_cd!='STK')) ? 'Y' :'N')) AS replenishment_planning_ind,
             scan_based_trading_ind AS scan_based_trading_ind,
             source_location_id AS source_location_id,
             source_location_level_cd AS source_location_level_cd,
             source_package_qty AS source_package_qty,
             vendor_package_purchase_status_cd AS vendor_package_purchase_status_cd,
             vendor_package_purchase_status_dt AS vendor_package_purchase_status_dt,
             flow_type_cd AS flow_type_cd,
             sears_import_ind AS import_ind,
             '0' AS retail_carton_vendorpackage_id,
             vendor_package_owner_cd AS vendor_package_owner_cd,
             vendor_stock_nbr AS vendor_stock_nbr,
             shc_item_id AS shc_item_id,
             item_purchase_status_cd AS item_purchase_status_cd,
             can_carry_model_id AS can_carry_model_id,
             '$CURRENT_DATE' AS days_to_check_begin_day_dt,
             '9999-12-31' AS days_to_check_end_day_dt,
             ' ' AS reorder_method_cd,
             ksn_id AS ksn_id,
             ksn_purchase_status_cd AS ksn_purchase_status_cd,
             cross_merchandising_cd AS cross_merchandising_cd,
             dot_com_allocation_ind AS dotcom_orderable_cd,
             ' ' AS kmart_markdown_ind,
             ksn_package_id AS ksn_package_id,
             ksn_dc_package_purchase_status_cd AS ksn_dc_package_purchase_status_cd,
             ' ' AS dc_configuration_cd,
             ' ' AS substitution_eligible_ind,
             sears_division_nbr AS sears_division_nbr,
             sears_item_nbr AS sears_item_nbr,
             sears_sku_nbr AS sears_sku_nbr,
             sears_location_id AS sears_location_id,
             sears_source_location_nbr AS sears_source_location_id,
             rim_status_cd AS rim_status_cd,
             stock_type_cd AS stock_type_cd,
             non_stock_source_cd AS non_stock_source_cd,
             item_active_ind AS dos_item_active_ind,
             item_reserve_cd AS item_reserve_cd,
             '$CURRENT_DATE' AS create_dt,
             '$CURRENT_DATE' AS last_update_dt,
             vendor_package_carton_qty AS vendor_package_carton_qty,
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
             source_system_cd AS source_system_cd,
             original_source_nbr AS original_source_nbr,
             item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
             item_on_order_qty AS item_on_order_qty,
             item_reserve_qty AS item_reserve_qty,
             item_back_order_qty AS item_back_order_qty,
             item_next_period_future_order_qty AS item_next_period_future_order_qty,
             item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
             item_last_receive_dt AS item_last_receive_dt,
             item_last_ship_dt AS item_last_ship_dt,
	         rim_last_record_creation_dt AS rim_last_record_creation_dt,
             '$batchid' AS idrp_batch_id;

work__idrp_sears_vendor_package_location_dist = DISTINCT work__idrp_sears_vendor_package_location;

STORE work__idrp_sears_vendor_package_location_dist
INTO '$WORK__IDRP_SEARS_VENDOR_PACKAGE_LOCATION_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
