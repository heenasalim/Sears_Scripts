/*
###############################################################################
#<>                           START HEADER DOCUMENT                         <>#
###############################################################################
# SCRIPT NAME:         perform_online_fulfillment_initialization_smith__idrp_online_drop_ship_items.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       27-11-2013 05:35
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
#
#
#
###############################################################################
#<<                 START COMMON HEADER CODE - DO NOT MANUALLY EDIT         >>#
###############################################################################
*/

-- Register the jar containing all PIG UDFs
--REGISTER $UDF_JAR;
SET default_parallel $NUM_PARALLEL;

/*
###############################################################################
#<<                           START CUSTOM HEADER CODE                      >>#
###############################################################################
*/

smith__item_combined_hierarchy_current_data = LOAD '$SMITH__ITEM_COMBINED_HIERARCHY_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__ITEM_COMBINED_HIERARCHY_CURRENT_SCHEMA);

gold__item_vendor_package_current_data = LOAD '$GOLD__ITEM_VENDOR_PACKAGE_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_VENDOR_PACKAGE_CURRENT_SCHEMA);

-----------------------------------------------------------Applying required filters-----------------------------------------------------------------
gold__item_vendor_package_current_data_fltr = FILTER gold__item_vendor_package_current_data BY 
                                                     TRIM(service_area_restriction_model_id) == '46162' OR 
                                                     TRIM(service_area_restriction_model_id) == '78459';

gold__item_vendor_package_current_data_fltr_1 = FILTER gold__item_vendor_package_current_data_fltr BY
                                                       TRIM(purchase_status_cd) == 'A';

gold__item_vendor_package_current_data_req = FOREACH gold__item_vendor_package_current_data_fltr_1 GENERATE 
                                                     ksn_id,
                                                     service_area_restriction_model_id;

smith__item_combined_hierarchy_current_data_fltr = FILTER smith__item_combined_hierarchy_current_data BY 
                                                          TRIM(shc_item_type_cd) == 'IIRC' OR 
                                                          TRIM(shc_item_type_cd) == 'TYP';

smith__item_combined_hierarchy_current_data_req = FOREACH smith__item_combined_hierarchy_current_data_fltr GENERATE 
                                                          ksn_id,
                                                          shc_item_id;

-----------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------join gold_item_vendor_pack to smith_item_combined--------------------------------------

smith_gold_item_data = JOIN smith__item_combined_hierarchy_current_data_req BY ksn_id, gold__item_vendor_package_current_data_req BY ksn_id;

smith_gold_item_data_gen = FOREACH smith_gold_item_data GENERATE                              
                                   shc_item_id AS item_id,
                                   service_area_restriction_model_id AS service_area_restriction_model_id;

smith__idrp_online_drop_ship_items_dist = DISTINCT smith_gold_item_data_gen;
smith__idrp_online_drop_ship_items_data = FOREACH smith__idrp_online_drop_ship_items_dist GENERATE 
                                          '$CURRENT_TIMESTAMP' AS load_ts,
                                          item_id,
                                          service_area_restriction_model_id;

STORE smith__idrp_online_drop_ship_items_data INTO '$SMITH__IDRP_ONLINE_DROP_SHIP_ITEMS_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');
----------------------------------------------------------------------------------------------------------------------------------------------------

/*
###############################################################################
#<<                START COMMON BODY CODE - DO NOT MANUALLY EDIT            >>#
###############################################################################
*/


/*
###############################################################################
#<<                          START CUSTOM BODY CODE                         >>#
###############################################################################
*/











/*
###############################################################################
#<<                START COMMON FOOTER CODE - DO NOT MANUALLY EDIT          >>#
###############################################################################
*/



/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
