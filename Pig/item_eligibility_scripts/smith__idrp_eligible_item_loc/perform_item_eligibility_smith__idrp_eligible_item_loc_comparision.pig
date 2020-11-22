/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_loc_comparision.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Thu Jan 02 01:38:15 EST 2014
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

SET default_parallel $NUM_PARALLEL;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

history = 
    LOAD '$SMITH__IDRP_ELIGIBLE_ITEM_LOC_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);

current = 
    LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOC_NEW_LOCATION'
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);


history_current_join = 
    JOIN current BY (item, loc) FULL OUTER, 
	     history BY (item, loc);


only_current = 
    FILTER history_current_join 
	BY (history::item IS NULL AND history::loc IS NULL);


only_history = 
    FILTER history_current_join 
	BY (current::item IS NULL AND current::loc IS NULL);


both = 
    FILTER history_current_join 
	BY ((current::item == history::item) and (current::loc == history::loc));


---- condition for comparing and updating data in history
--/*
--Compare remaining current store level records to the store level record on prior days 
--IE Item/Location table using Item_ID and LOC. 
--Add any new records to the IE Item/Locationt table, 
--replace matching records with current data and update missing records (on prior day only) by setting the ELIG_STS_CD to .D. uneligible.
--*/


-- prepare columns for only current rows, get data from current rows to be added to history record
rows_in_current = 
    FOREACH only_current 
	GENERATE 
		current::item AS item,
		current::loc AS loc,
		current::src_owner_cd AS src_owner_cd,
		current::elig_sts_cd AS elig_sts_cd,
		current::src_pack_qty AS src_pack_qty,
		current::src_loc AS src_loc,
		current::po_vnd_no AS po_vnd_no,
		current::ksn_id AS ksn_id,
		current::purch_stat_cd AS purch_stat_cd,
		current::retail_crtn_intrnl_pack_qty AS retail_crtn_intrnl_pack_qty,
		current::days_to_check_begin_date AS days_to_check_begin_date,
		current::days_to_check_end_date AS days_to_check_end_date,
		current::dotcom_order_indicator AS dotcom_order_indicator,
		current::vend_pack_id AS vend_pack_id,
		current::vend_pack_purch_stat_cd AS vend_pack_purch_stat_cd,
		current::vendor_pack_flow_type AS vendor_pack_flow_type,
		current::vendor_pack_qty AS vendor_pack_qty,
		current::reorder_method_code AS reorder_method_code,
		current::str_supplier_cd AS str_supplier_cd,
		current::vend_stk_nbr AS vend_stk_nbr,
		current::ksn_pack_id AS ksn_pack_id,
		current::ksn_dc_pack_purch_stat_cd AS ksn_dc_pack_purch_stat_cd,
		current::inbnd_ord_uom_cd AS inbnd_ord_uom_cd,
		current::enable_jif_dc_ind AS enable_jif_dc_ind,
		current::stk_ind AS stk_ind,
		current::crtn_per_layer_qty AS crtn_per_layer_qty,
		current::layer_per_pall_qty AS layer_per_pall_qty,
		current::dc_config_cd AS dc_config_cd,
		current::imp_fl AS imp_fl,
		current::srs_division_nbr AS srs_division_nbr,
		current::srs_item_nbr AS srs_item_nbr,
		current::srs_sku_cd AS srs_sku_cd,
		current::srs_location_nbr AS srs_location_nbr,
		current::srs_source_nbr AS srs_source_nbr,
		current::rim_sts_cd AS rim_sts_cd,
		current::non_stock_source_cd AS non_stock_source_cd,
		current::item_active_ind AS item_active_ind,
		current::item_reserve_cd AS item_reserve_cd,
		current::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
		current::item_reserve_qty AS item_reserve_qty,
		current::item_back_order_qty AS item_back_order_qty,
		current::item_next_period_future_order_qty AS item_next_period_future_order_qty,
		current::item_on_order_qty AS item_on_order_qty,
		current::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
		current::item_last_receive_dt AS item_last_receive_dt,
		current::item_last_ship_dt AS item_last_ship_dt,
		current::stk_typ_cd AS stk_typ_cd;


-- prepare columns for only history rows, get data from current rows into history record
rows_in_history = 
    FOREACH only_history 
	GENERATE 
		history::item AS item,
		history::loc AS loc,
		history::src_owner_cd AS src_owner_cd,
		'D' AS elig_sts_cd,
		history::src_pack_qty AS src_pack_qty,
		history::src_loc AS src_loc,
		history::po_vnd_no AS po_vnd_no,
		history::ksn_id AS ksn_id,
		history::purch_stat_cd AS purch_stat_cd,
		history::retail_crtn_intrnl_pack_qty AS retail_crtn_intrnl_pack_qty,
		history::days_to_check_begin_date AS days_to_check_begin_date,
		history::days_to_check_end_date AS days_to_check_end_date,
		history::dotcom_order_indicator AS dotcom_order_indicator,
		history::vend_pack_id AS vend_pack_id,
		history::vend_pack_purch_stat_cd AS vend_pack_purch_stat_cd,
		history::vendor_pack_flow_type AS vendor_pack_flow_type,
		history::vendor_pack_qty AS vendor_pack_qty,
		history::reorder_method_code AS reorder_method_code,
		history::str_supplier_cd AS str_supplier_cd,
		history::vend_stk_nbr AS vend_stk_nbr,
		history::ksn_pack_id AS ksn_pack_id,
		history::ksn_dc_pack_purch_stat_cd AS ksn_dc_pack_purch_stat_cd,
		history::inbnd_ord_uom_cd AS inbnd_ord_uom_cd,
		history::enable_jif_dc_ind AS enable_jif_dc_ind,
		history::stk_ind AS stk_ind,
		history::crtn_per_layer_qty AS crtn_per_layer_qty,
		history::layer_per_pall_qty AS layer_per_pall_qty,
		history::dc_config_cd AS dc_config_cd,
		history::imp_fl AS imp_fl,
		history::srs_division_nbr AS srs_division_nbr,
		history::srs_item_nbr AS srs_item_nbr,
		history::srs_sku_cd AS srs_sku_cd,
		history::srs_location_nbr AS srs_location_nbr,
		history::srs_source_nbr AS srs_source_nbr,
		history::rim_sts_cd AS rim_sts_cd,
		history::non_stock_source_cd AS non_stock_source_cd,
		history::item_active_ind AS item_active_ind,
		history::item_reserve_cd AS item_reserve_cd,
		history::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
		history::item_reserve_qty AS item_reserve_qty,
		history::item_back_order_qty AS item_back_order_qty,
		history::item_next_period_future_order_qty AS item_next_period_future_order_qty,
		history::item_on_order_qty AS item_on_order_qty,
		history::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
		history::item_last_receive_dt AS item_last_receive_dt,
		history::item_last_ship_dt AS item_last_ship_dt,
		history::stk_typ_cd AS stk_typ_cd;


-- prepare columns for  rows in both data source, get data from current rows into history record
rows_in_both = 
    FOREACH both 
	GENERATE 
		current::item AS item,
		current::loc AS loc,
		current::src_owner_cd AS src_owner_cd,
		current::elig_sts_cd AS elig_sts_cd,
		current::src_pack_qty AS src_pack_qty,
		current::src_loc AS src_loc,
		current::po_vnd_no AS po_vnd_no,
		current::ksn_id AS ksn_id,
		current::purch_stat_cd AS purch_stat_cd,
		current::retail_crtn_intrnl_pack_qty AS retail_crtn_intrnl_pack_qty,
		current::days_to_check_begin_date AS days_to_check_begin_date,
		current::days_to_check_end_date AS days_to_check_end_date,
		current::dotcom_order_indicator AS dotcom_order_indicator,
		current::vend_pack_id AS vend_pack_id,
		current::vend_pack_purch_stat_cd AS vend_pack_purch_stat_cd,
		current::vendor_pack_flow_type AS vendor_pack_flow_type,
		current::vendor_pack_qty AS vendor_pack_qty,
		current::reorder_method_code AS reorder_method_code,
		current::str_supplier_cd AS str_supplier_cd,
		current::vend_stk_nbr AS vend_stk_nbr,
		current::ksn_pack_id AS ksn_pack_id,
		current::ksn_dc_pack_purch_stat_cd AS ksn_dc_pack_purch_stat_cd,
		current::inbnd_ord_uom_cd AS inbnd_ord_uom_cd,
		current::enable_jif_dc_ind AS enable_jif_dc_ind,
		current::stk_ind AS stk_ind,
		current::crtn_per_layer_qty AS crtn_per_layer_qty,
		current::layer_per_pall_qty AS layer_per_pall_qty,
		current::dc_config_cd AS dc_config_cd,
		current::imp_fl AS imp_fl,
		current::srs_division_nbr AS srs_division_nbr,
		current::srs_item_nbr AS srs_item_nbr,
		current::srs_sku_cd AS srs_sku_cd,
		current::srs_location_nbr AS srs_location_nbr,
		current::srs_source_nbr AS srs_source_nbr,
		current::rim_sts_cd AS rim_sts_cd,
		current::non_stock_source_cd AS non_stock_source_cd,
		current::item_active_ind AS item_active_ind,
		current::item_reserve_cd AS item_reserve_cd,
		current::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
		current::item_reserve_qty AS item_reserve_qty,
		current::item_back_order_qty AS item_back_order_qty,
		current::item_next_period_future_order_qty AS item_next_period_future_order_qty,
		current::item_on_order_qty AS item_on_order_qty,
		current::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
		current::item_last_receive_dt AS item_last_receive_dt,
		current::item_last_ship_dt AS item_last_ship_dt,
		current::stk_typ_cd AS stk_typ_cd;


final_data = 
    UNION rows_in_history,
	      rows_in_current,
          rows_in_both;


distinct_data = DISTINCT final_data;
distinct_data = 
    FOREACH distinct_data
    GENERATE
        '$CURRENT_TIMESTAMP'	as	load_ts	,
        item AS item,
        loc AS loc,
        src_owner_cd AS src_owner_cd,
        elig_sts_cd AS elig_sts_cd,
        src_pack_qty AS src_pack_qty,
        src_loc AS src_loc,
        po_vnd_no AS po_vnd_no,
        ksn_id AS ksn_id,
        purch_stat_cd AS purch_stat_cd,
        retail_crtn_intrnl_pack_qty AS retail_crtn_intrnl_pack_qty,
        days_to_check_begin_date AS days_to_check_begin_date,
        days_to_check_end_date AS days_to_check_end_date,
        dotcom_order_indicator AS dotcom_order_indicator,
        vend_pack_id AS vend_pack_id,
        vend_pack_purch_stat_cd AS vend_pack_purch_stat_cd,
        vendor_pack_flow_type AS vendor_pack_flow_type,
        vendor_pack_qty AS vendor_pack_qty,
        reorder_method_code AS reorder_method_code,
        str_supplier_cd AS str_supplier_cd,
        vend_stk_nbr AS vend_stk_nbr,
        ksn_pack_id AS ksn_pack_id,
        ksn_dc_pack_purch_stat_cd AS ksn_dc_pack_purch_stat_cd,
        inbnd_ord_uom_cd AS inbnd_ord_uom_cd,
        enable_jif_dc_ind AS enable_jif_dc_ind,
        stk_ind AS stk_ind,
        crtn_per_layer_qty AS crtn_per_layer_qty,
        layer_per_pall_qty AS layer_per_pall_qty,
        dc_config_cd AS dc_config_cd,
        imp_fl AS imp_fl,
        srs_division_nbr AS srs_division_nbr,
        srs_item_nbr AS srs_item_nbr,
        srs_sku_cd AS srs_sku_cd,
        srs_location_nbr AS srs_location_nbr,
        srs_source_nbr AS srs_source_nbr,
        rim_sts_cd AS rim_sts_cd,
        non_stock_source_cd AS non_stock_source_cd,
        item_active_ind AS item_active_ind,
        item_reserve_cd AS item_reserve_cd,
        item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
        item_reserve_qty AS item_reserve_qty,
        item_back_order_qty AS item_back_order_qty,
        item_next_period_future_order_qty AS item_next_period_future_order_qty,
        item_on_order_qty AS item_on_order_qty,
        item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
        item_last_receive_dt AS item_last_receive_dt,
        item_last_ship_dt AS item_last_ship_dt,
        stk_typ_cd AS stk_typ_cd,
        '$batchid' AS batch_id  ;


STORE distinct_data 
INTO '$WORK__IDRP_ELIGIBLE_ITEM_LOC_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
