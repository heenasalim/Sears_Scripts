/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_location_current.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Wed Jun 18 10:00:48 EDT 2014
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
#
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/



/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

work__idrp_eligible_item_location_current_data = 
    LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOCATION_CURRENT_INCOMING_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_PIPE') 
    AS ($WORK__IDRP_ELIGIBLE_ITEM_LOCATION_CURRENT_SCHEMA);


smith__idrp_eligible_item_location_current_data = 
    FOREACH work__idrp_eligible_item_location_current_data
    GENERATE
           '$CURRENT_TIMESTAMP' AS load_ts,
           TRIM(shc_item_id) AS shc_item_id,
           TRIM(location_id) AS location_id,
           TRIM(source_owner_cd) AS source_owner_cd,
           TRIM(eligible_status_cd) AS eligible_status_cd,
           TRIM(source_package_qty) AS source_package_qty,
           TRIM(source_location_id) AS source_location_id,
           TRIM(purchase_order_vendor_location_id) AS purchase_order_vendor_location_id,
           TRIM(ksn_id) AS ksn_id,
           TRIM(item_purchase_status_cd) AS item_purchase_status_cd,
           TRIM(retail_carton_vendor_package_id) AS retail_carton_vendor_package_id,
           TRIM(days_to_check_begin_dt) AS days_to_check_begin_dt,
           TRIM(days_to_check_end_dt) AS days_to_check_end_dt,
           TRIM(dotcom_orderable_cd) AS dotcom_orderable_cd,
           TRIM(vendor_package_id) AS vendor_package_id,
           TRIM(vendor_package_purchase_status_cd) AS vendor_package_purchase_status_cd,
           TRIM(flow_type_cd) AS flow_type_cd,
           NULL AS deprecated_1,
           TRIM(reorder_method_cd) AS reorder_method_cd,
           TRIM(source_location_level_cd) AS source_location_level_cd,
           TRIM(vendor_stock_nbr) AS vendor_stock_nbr,
           TRIM(ksn_package_id) AS ksn_package_id,
           TRIM(ksn_dc_package_purchase_status_cd) AS ksn_dc_package_purchase_status_cd,
           NULL AS deprecated_2,
           NULL AS deprecated_3,
           NULL AS deprecated_4,
           NULL AS deprecated_5,
           NULL AS deprecated_6,
           TRIM(dc_configuration_cd) AS dc_configuration_cd,
           TRIM(import_ind) AS import_ind,
           TRIM(sears_division_nbr) AS sears_division_nbr,
           TRIM(sears_item_nbr) AS sears_item_nbr,
           TRIM(sears_sku_nbr) AS sears_sku_nbr,
           TRIM(sears_location_id) AS sears_location_id,
           TRIM(sears_source_location_id) AS sears_source_location_id,
           TRIM(rim_status_cd) AS rim_status_cd,
           TRIM(non_stock_source_cd) AS non_stock_source_cd,
           TRIM(dos_item_active_ind) AS dos_item_active_ind,
           TRIM(dos_item_reserve_cd) AS dos_item_reserve_cd,
           NULL AS deprecated_7,
           NULL AS deprecated_8,
           NULL AS deprecated_9,
           NULL AS deprecated_10,
           NULL AS deprecated_11,
           NULL AS deprecated_12,
           NULL AS deprecated_13,
           NULL AS deprecated_14,
           TRIM(stock_type_cd) AS stock_type_cd,
           TRIM(location_level_cd) AS location_level_cd,
           TRIM(can_carry_model_id) AS can_carry_model_id,
           TRIM(ksn_purchase_status_cd) AS ksn_purchase_status_cd,
           TRIM(eligible_status_begin_dt) AS eligible_status_begin_dt,
           TRIM(idrp_status_cd) AS idrp_status_cd,
           TRIM(idrp_status_begin_dt) AS idrp_status_begin_dt,
           TRIM(idrp_status_source_cd) AS idrp_status_source_cd,
           TRIM(previous_idrp_status_cd) AS previous_idrp_status_cd,
           TRIM(previous_idrp_status_begin_dt) AS previous_idrp_status_begin_dt,
           TRIM(previous_idrp_status_source_cd) AS previous_idrp_status_source_cd,
           TRIM(prev_elig_status_cd) AS previous_eligible_status_cd,
           TRIM(prev_elig_status_start_dt) AS previous_eligible_status_begin_dt,
           TRIM(create_dt) AS create_dt,
           TRIM(last_update_dt) AS last_update_dt,
           '$batchid' AS idrp_batch_id;

STORE smith__idrp_eligible_item_location_current_data 
INTO '$SMITH__IDRP_ELIGIBLE_ITEM_LOCATION_CURRENT_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
