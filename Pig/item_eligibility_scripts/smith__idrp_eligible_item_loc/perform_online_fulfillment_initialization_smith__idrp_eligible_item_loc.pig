/*
###############################################################################
#<>                           START HEADER DOCUMENT                         <>#
###############################################################################
# SCRIPT NAME:         perform_online_fulfillment_initialization_smith__idrp_eligible_item_loc.pig
# AUTHOR NAME:         Onkar Malewadikar
# CREATION DATE:       27-11-2013 02:48
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
REGISTER $UDF_JAR;


/*
###############################################################################
#<<                           START CUSTOM HEADER CODE                      >>#
###############################################################################
*/

SET default_parallel $NUM_PARALLEL;

/***** LOAD ALL REQUIRED TABLES *****/

smith__idrp_eligible_item_loc_data = 
    LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOC_VENDOR_LOCATION'
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
    AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);
	
smith__idrp_vend_pack_combined_data =
    LOAD '$SMITH__IDRP_VEND_PACK_COMBINED_LOCATION'
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
    AS ($SMITH__IDRP_VEND_PACK_COMBINED_SCHEMA);


smith__item_combined_hierarchy_current_data = 
    LOAD '$SMITH__ITEM_COMBINED_HIERARCHY_CURRENT_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($SMITH__ITEM_COMBINED_HIERARCHY_CURRENT_SCHEMA);

gold__item_package_current_data =
    LOAD '$GOLD__ITEM_PACKAGE_CURRENT_LOCATION'
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
    AS ($GOLD__ITEM_PACKAGE_CURRENT_SCHEMA);

--SCHEMA NOT PRESENT IN SCHEMA LOCATION ,WE NEED TO CREATE SCHEMA FILE FOR IT.	
smith__idrp_online_drop_ship_items_data = 
    LOAD '$SMITH__IDRP_ONLINE_DROP_SHIP_ITEMS_LOCATION'    
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
    AS ($SMITH__IDRP_ONLINE_DROP_SHIP_ITEMS_SCHEMA);
gold__item_aprk_current_data = 
    LOAD '$GOLD__ITEM_APRK_CURRENT_LOCATION' 
    USING  PigStorage('$FIELD_DELIMITER_CONTROL_A')
    AS ($GOLD__ITEM_APRK_CURRENT_SCHEMA);

smith__idrp_eligible_loc_data = 
    LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION'
    USING  PigStorage('$FIELD_DELIMITER_CONTROL_A')
    AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);

/*** GENERATE REQUIRED COLUMNS FROM TABLES ******/
smith__idrp_online_drop_ship_items_data_req = 
    FOREACH smith__idrp_online_drop_ship_items_data
    GENERATE
        item_id AS item_id,
        service_area_restriction_model_id AS service_area_restriction_model_id;

smith__idrp_online_drop_ship_items_data_req =
    DISTINCT smith__idrp_online_drop_ship_items_data_req;

smith__idrp_vend_pack_combined_data_req = 
    FOREACH smith__idrp_vend_pack_combined_data
    GENERATE 
	service_area_restriction_model_id AS service_area_restriction_model_id,
	purchase_status_cd AS purchase_status_cd,
	shc_item_id AS shc_item_id,
        ksn_id AS ksn_id,
        vendor_package_id AS vendor_package_id,
        flow_type_cd AS flow_type_cd,
        vendor_carton_qty AS vendor_carton_qty,
        vendor_stock_nbr AS vendor_stock_nbr,
        ksn_package_id AS ksn_package_id,
        import_cd AS import_cd,
	purchase_status_dt AS purchase_status_dt,
        aprk_id AS aprk_id,
        package_id AS package_id;		
		
smith__idrp_vend_pack_combined_data_req = 
    DISTINCT smith__idrp_vend_pack_combined_data_req;
	
smith__item_combined_hierarchy_current_data_required = 
    FOREACH smith__item_combined_hierarchy_current_data 
    GENERATE
        sears_division_nbr AS srs_division_nbr,
	sears_item_nbr AS srs_item_nbr,
	sears_sku_nbr AS srs_sku_cd,
        dotcom_allocation_ind AS dotcom_allocation_ind,
        purchase_status_cd AS purchase_status_cd,
        shc_item_id AS shc_item_id,
        ksn_id AS ksn_id;
smith__item_combined_hierarchy_current_data_required = 
    DISTINCT smith__item_combined_hierarchy_current_data_required;
	
smith__idrp_eligible_item_loc_data_req = 
    FOREACH smith__idrp_eligible_item_loc_data
    GENERATE
        loc AS loc,
	item AS item;
	
smith__idrp_eligible_item_loc_data_req = 
    DISTINCT smith__idrp_eligible_item_loc_data_req;

gold__item_package_current_data_req = 
    FOREACH gold__item_package_current_data 
    GENERATE
        package_id AS package_id;

gold__item_package_current_data_req = 
    DISTINCT gold__item_package_current_data_req;

gold__item_aprk_current_data_req = 
    FOREACH gold__item_aprk_current_data
    GENERATE
        aprk_id AS aprk_id,
        aprk_type_cd AS aprk_type_cd,
        order_duns_nbr AS order_duns_nbr;

gold__item_aprk_current_data_req = 
    DISTINCT gold__item_aprk_current_data_req;

smith__idrp_eligible_loc_data_req = 
    FOREACH smith__idrp_eligible_loc_data
    GENERATE
        loc AS loc,
        srs_loc AS srs_loc;

smith__idrp_eligible_loc_data_req = 
    DISTINCT smith__idrp_eligible_loc_data_req; 
/******FILTER smith__idrp_online_drop_ship_items_data_req BY service_area_restriction_model_id = 46162 ******/

smith__idrp_online_drop_ship_items_data_req_SARM_46162_filter =     
	FILTER smith__idrp_online_drop_ship_items_data_req 
	BY (int)service_area_restriction_model_id == 46162;

/*******FILTER smith__idrp_vend_pack_combined_data BY purchase_status_cd = 'A' AND service_area_restriction_model_id = 46162. ****/

smith__idrp_vend_pack_combined_data_st_cd_SARM_filter =
    FILTER  smith__idrp_vend_pack_combined_data_req
	BY  purchase_status_cd == 'A'  AND (int)service_area_restriction_model_id == 46162;
	
grp_vend_pack_combined_on_item = 
    GROUP smith__idrp_vend_pack_combined_data_st_cd_SARM_filter 
	BY shc_item_id;
	
flatten_vend_pack_combined_on_item = 
    FOREACH grp_vend_pack_combined_on_item
	        { sort_data_on_purch_st_dt = ORDER $1 BY purchase_status_dt DESC ;
			  take_purch_st_dt_max = LIMIT sort_data_on_purch_st_dt 1;
              GENERATE flatten(take_purch_st_dt_max);
            };


/******	 JOIN smith__idrp_online_drop_ship_items_data AND smith__idrp_vend_pack_combined_data ON item_id ****/

join_drop_ship_items_and_vend_pack_combined = 
    JOIN smith__idrp_online_drop_ship_items_data_req_SARM_46162_filter BY item_id,
         flatten_vend_pack_combined_on_item BY shc_item_id PARALLEL $NUM_PARALLEL;
gen_join_drop_ship_items_and_vend_pack_combined = 
    FOREACH join_drop_ship_items_and_vend_pack_combined 
    GENERATE
        shc_item_id AS shc_item_id,
        ksn_id AS ksn_id,
        vendor_package_id AS vend_pack_id,
        purchase_status_cd AS vend_pack_purch_stat_cd,
        flow_type_cd AS vendor_pack_flow_type,
        vendor_carton_qty AS vendor_pack_qty,
        vendor_stock_nbr AS vend_stk_nbr,
        ksn_package_id AS ksn_pack_id,
        (import_cd IS NULL ? '0' : (import_cd == 'I' ? '1' : '0')) AS imp_fl,
        'K' AS src_owner_cd ,
        '7800' AS loc,
        aprk_id AS aprk_id,
        package_id AS package_id;

gen_join_drop_ship_items_and_vend_pack_combined = 
    DISTINCT gen_join_drop_ship_items_and_vend_pack_combined;

/***** b.	Sears online location 9300 can be processed by INFOREM and RIM, so it may already be added to the item location table.  If it is not, we will add a record for the location ****/

/******FILTER smith__idrp_online_drop_ship_items_data_req BY service_area_restriction_model_id = 78459  ******/

smith__idrp_online_drop_ship_items_data_req_SARM_78459_filter =     
	FILTER smith__idrp_online_drop_ship_items_data_req 
	BY (int)service_area_restriction_model_id == 78459;

/**** i.	Select item_id from smith__idrp_online_drop_ship_items where service_area_restriction_mdl = 78459 and an smith__idrp_eligible_item_loc record is not found for the matching item_id, loc = 9300. For each item selected, use the data mapping below for adding the record.   ***/
smith__idrp_eligible_item_loc_data_req_filter_9300 = 
    FILTER smith__idrp_eligible_item_loc_data_req
    BY loc == '9300' ;
generate_smith__idrp_eligible_item_loc_filter_9300 = 
    FOREACH smith__idrp_eligible_item_loc_data_req_filter_9300
    GENERATE
        item AS filterd_item;
generate_smith__idrp_eligible_item_loc_filter_9300 = 
    DISTINCT generate_smith__idrp_eligible_item_loc_filter_9300;

join_with_item_loc_to_remove_items = 
    JOIN smith__idrp_eligible_item_loc_data_req BY item 
         LEFT OUTER , 
         generate_smith__idrp_eligible_item_loc_filter_9300 BY filterd_item;

smith__idrp_eligible_item_loc_data_req_filter =
    FILTER join_with_item_loc_to_remove_items
    BY filterd_item IS NULL;


/*** JOIN smith__idrp_eligible_item_loc AND smith__idrp_online_drop_ship_items on item_id****/

join_item_loc_drop_ship_items = 
    JOIN smith__idrp_online_drop_ship_items_data_req_SARM_78459_filter BY item_id ,
		 smith__idrp_eligible_item_loc_data_req_filter BY item PARALLEL $NUM_PARALLEL;
/*******FILTER smith__idrp_vend_pack_combined_data BY purchase_status_cd = 'A' AND service_area_restriction_model_id = 78459. ****/

smith__idrp_vend_pack_combined_data_st_cd_SARM_78459_filter =
    FILTER  smith__idrp_vend_pack_combined_data_req
	BY  purchase_status_cd == 'A'  AND (int)service_area_restriction_model_id == 78459;
	
grp_vend_pack_combined_78459_on_item = 
    GROUP smith__idrp_vend_pack_combined_data_st_cd_SARM_78459_filter 
	BY shc_item_id;
	
flatten_vend_pack_combined_78459_on_item = 
    FOREACH grp_vend_pack_combined_78459_on_item
	        { sort_data_on_purch_st_dt = ORDER $1 BY purchase_status_dt DESC ;
			  take_purch_st_dt_max = LIMIT sort_data_on_purch_st_dt 1;
              GENERATE flatten(take_purch_st_dt_max);
            };
			
			
/**** JOIN smith__idrp_online_drop_ship_items on item_id AND smith__idrp_vend_pack_combined_data ON item_id *****/

join_vend_pack_comb_78459_drop = 
    JOIN flatten_vend_pack_combined_78459_on_item BY shc_item_id,
         join_item_loc_drop_ship_items BY item_id PARALLEL $NUM_PARALLEL;

gen_join_vend_pack_comb_78459_drop = 
    FOREACH join_vend_pack_comb_78459_drop
    GENERATE
        shc_item_id AS shc_item_id,
        ksn_id AS ksn_id,
        vendor_package_id AS vend_pack_id,
        NULL AS vend_pack_purch_stat_cd,
        NULL AS vendor_pack_flow_type,
        vendor_carton_qty AS vendor_pack_qty,
        NULL AS vend_stk_nbr,
        NULL AS ksn_pack_id,
        (import_cd IS NULL ? '0' : (import_cd == 'I' ? '1' : '0')) AS imp_fl,
        'S' AS src_owner_cd ,
        '9300' AS loc,
        aprk_id AS aprk_id,
        package_id AS package_id;

gen_join_vend_pack_comb_78459_drop = 
    DISTINCT gen_join_vend_pack_comb_78459_drop;

/***** End of b.****/	 
/**** UNION a.(KMART DATA) AND b.(SEARS DATA) ****/

union_kmart_sears =
     UNION gen_join_drop_ship_items_and_vend_pack_combined,
           gen_join_vend_pack_comb_78459_drop;

union_kmart_sears = 
    DISTINCT union_kmart_sears;


/**** JOIN KMART AND SEARS DATA TO smith__item_combined_hierarchy_current TABLE ON item_id ******/

join_kmart_sears_data_to_combined_hierarchy_current = 
    JOIN union_kmart_sears BY (shc_item_id,ksn_id) ,
         smith__item_combined_hierarchy_current_data_required BY (shc_item_id,ksn_id) PARALLEL $NUM_PARALLEL;
/**** GENERATE REQUIRED COLUMNS FROM ABOVE JOIN *******/

gen_join_kmart_sears_data_to_combined_hierarchy_current = 
    FOREACH join_kmart_sears_data_to_combined_hierarchy_current
    GENERATE
        union_kmart_sears::shc_item_id AS shc_item_id,
        union_kmart_sears::ksn_id AS ksn_id,
        vend_pack_id AS vend_pack_id,
        vend_pack_purch_stat_cd AS vend_pack_purch_stat_cd,
        vendor_pack_flow_type AS vendor_pack_flow_type,
        vendor_pack_qty AS vendor_pack_qty,
        vend_stk_nbr AS vend_stk_nbr,
        ksn_pack_id AS ksn_pack_id,
        imp_fl AS imp_fl,
        src_owner_cd AS src_owner_cd,
        loc AS loc,
        (src_owner_cd == 'K' ? purchase_status_cd : NULL) AS purch_stat_cd,
	dotcom_allocation_ind  AS dotcom_order_indicator,
	srs_division_nbr AS srs_division_nbr,
        srs_item_nbr AS srs_item_nbr,
        srs_sku_cd AS srs_sku_cd,
        '0' AS retail_crtn_intrnl_pack_qty,
        aprk_id AS aprk_id,
        union_kmart_sears::package_id AS package_id;

gen_join_kmart_sears_data_to_combined_hierarchy_current = 
    DISTINCT gen_join_kmart_sears_data_to_combined_hierarchy_current;
/***** JOIN TO gold__item_package_current ON package_id ****/

join_vend_pack_combine_to_package_current = 
    JOIN gen_join_kmart_sears_data_to_combined_hierarchy_current BY package_id ,
         gold__item_package_current_data_req BY package_id PARALLEL $NUM_PARALLEL; 

gen_req_join_vend_pack_combine_to_package_current = 
    FOREACH join_vend_pack_combine_to_package_current
    GENERATE
        shc_item_id AS shc_item_id,
        ksn_id AS ksn_id,
        vend_pack_id AS vend_pack_id,
        vend_pack_purch_stat_cd AS vend_pack_purch_stat_cd,
        vendor_pack_flow_type AS vendor_pack_flow_type,
        vendor_pack_qty AS vendor_pack_qty,
        vend_stk_nbr AS vend_stk_nbr,
        ksn_pack_id AS ksn_pack_id,
        imp_fl AS imp_fl,
        src_owner_cd AS src_owner_cd,
        loc AS loc,
        purch_stat_cd AS purch_stat_cd,
        dotcom_order_indicator AS dotcom_order_indicator,
        srs_division_nbr AS srs_division_nbr,
        srs_item_nbr AS srs_item_nbr,
        srs_sku_cd AS srs_sku_cd,
        retail_crtn_intrnl_pack_qty AS retail_crtn_intrnl_pack_qty,
       (vendor_pack_qty IS NULL OR retail_crtn_intrnl_pack_qty IS NULL ? '0' : ((int)retail_crtn_intrnl_pack_qty == 0 ? vendor_pack_qty : (chararray)((int)retail_crtn_intrnl_pack_qty * (int)vendor_pack_qty))) AS src_pack_qty, 
        aprk_id AS aprk_id,
        gen_join_kmart_sears_data_to_combined_hierarchy_current::package_id AS package_id;

gen_req_join_vend_pack_combine_to_package_current = 
    DISTINCT gen_req_join_vend_pack_combine_to_package_current;

/***** FILTER GOLD__ITEM_APRK_CURRENT TABLE BY aprk_type_cd = ORD *******/

filter_gold__item_aprk_current_data_req_on_aprk_typ_cd =  
    FILTER gold__item_aprk_current_data_req
    BY aprk_type_cd == 'ORD';
/****** RETRIEVE ORDER DUNS NUMBER USING APRK_ID FROM SMITH__IDRP_VEND_PACK_COMBINED TABLE. 
JOIN TO GOLD__ITEM_APRK_CURRENT.  ******/	

join_vend_pack_combine_to_aprk_current = 
    JOIN gen_req_join_vend_pack_combine_to_package_current BY aprk_id ,
         filter_gold__item_aprk_current_data_req_on_aprk_typ_cd BY aprk_id PARALLEL $NUM_PARALLEL;

gen_req_join_vend_pack_combine_to_aprk_current = 
    FOREACH join_vend_pack_combine_to_aprk_current
    GENERATE
        shc_item_id AS shc_item_id,
        ksn_id AS ksn_id,
        vend_pack_id AS vend_pack_id,
        vend_pack_purch_stat_cd AS vend_pack_purch_stat_cd,
        vendor_pack_flow_type AS vendor_pack_flow_type,
        vendor_pack_qty AS vendor_pack_qty,
        vend_stk_nbr AS vend_stk_nbr,
        ksn_pack_id AS ksn_pack_id,
        imp_fl AS imp_fl,
        src_owner_cd AS src_owner_cd,
        loc AS loc,
        purch_stat_cd AS purch_stat_cd,
        dotcom_order_indicator AS dotcom_order_indicator,
        srs_division_nbr AS srs_division_nbr,
        srs_item_nbr AS srs_item_nbr,
        srs_sku_cd AS srs_sku_cd,
        retail_crtn_intrnl_pack_qty AS retail_crtn_intrnl_pack_qty,
        src_pack_qty AS src_pack_qty,
        CONCAT(order_duns_nbr,'_O') AS src_loc,
        CONCAT(order_duns_nbr,'_O') AS po_vnd_no;
	
gen_req_join_vend_pack_combine_to_aprk_current = 
    DISTINCT gen_req_join_vend_pack_combine_to_aprk_current;
/***** JOIN TO smith__idrp_eligible_loc TABLE ON loc TO CALCULATE srs_loc_nbr for loc  ****/

join_vend_pack_combine_to_eligible_loc = 
    JOIN gen_req_join_vend_pack_combine_to_aprk_current BY loc ,
         smith__idrp_eligible_loc_data_req BY loc PARALLEL $NUM_PARALLEL;

gen_req_join_vend_pack_combine_to_eligible_loc = 
    FOREACH join_vend_pack_combine_to_eligible_loc
    GENERATE
	    '$CURRENT_TIMESTAMP'	as	load_ts	,
        shc_item_id AS item,
        gen_req_join_vend_pack_combine_to_aprk_current::loc AS loc,
        src_owner_cd AS src_owner_cd,
        'A' AS elig_sts_cd,
        src_pack_qty AS src_pack_qty,
        src_loc AS src_loc,
        po_vnd_no AS po_vnd_no,
        ksn_id AS ksn_id,
        purch_stat_cd AS purch_stat_cd,
        retail_crtn_intrnl_pack_qty AS retail_crtn_intrnl_pack_qty,
        (src_owner_cd == 'K' ? '01/01/1970' : NULL ) AS days_to_check_begin_date,
        (src_owner_cd == 'K' ? '01/01/1970' : NULL ) AS days_to_check_end_date,
        dotcom_order_indicator AS dotcom_order_indicator,
        vend_pack_id AS vend_pack_id,
        vend_pack_purch_stat_cd AS vend_pack_purch_stat_cd,
        vendor_pack_flow_type AS vendor_pack_flow_type,
        vendor_pack_qty AS vendor_pack_qty,
        '' AS reorder_method_code,
        (src_owner_cd == 'K' ? 'V' : NULL ) AS str_supplier_cd,
        vend_stk_nbr AS vend_stk_nbr,
        ksn_pack_id AS ksn_pack_id,
        NULL AS ksn_dc_pack_purch_stat_cd,
        NULL AS inbnd_ord_uom_cd,
        NULL AS enable_jif_dc_ind,
        NULL AS stk_ind,
        NULL AS crtn_per_layer_qty,
        NULL AS layer_per_pall_qty,
        NULL AS dc_config_cd,
        imp_fl AS imp_fl,
        srs_division_nbr AS srs_division_nbr,
        srs_item_nbr AS srs_item_nbr,
        srs_sku_cd AS srs_sku_cd, 
        srs_loc AS srs_location_nbr,
        NULL AS srs_source_nbr,
        NULL AS rim_sts_cd,
        NULL AS non_stock_source_cd,
        NULL AS item_active_ind,
        NULL AS item_reserve_cd,
        NULL AS item_next_period_on_hand_qty,
        NULL AS item_reserve_qty,
        NULL AS item_back_order_qty,
        NULL AS item_next_period_future_order_qty,
        NULL AS item_on_order_qty,
        NULL AS item_next_period_in_transit_qty,
        NULL AS item_last_receive_dt,
        NULL AS item_last_ship_dt,
        NULL AS stk_typ_cd,
		'$batchid' AS batch_id  ;
gen_req_join_vend_pack_combine_to_eligible_loc = 
    DISTINCT gen_req_join_vend_pack_combine_to_eligible_loc;

union_with_item_loc = 
    UNION gen_req_join_vend_pack_combine_to_eligible_loc ,
          smith__idrp_eligible_item_loc_data;

union_with_item_loc = 
    DISTINCT union_with_item_loc;

STORE union_with_item_loc
INTO '$WORK__IDRP_ELIGIBLE_ITEM_LOC_NEW_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');
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
