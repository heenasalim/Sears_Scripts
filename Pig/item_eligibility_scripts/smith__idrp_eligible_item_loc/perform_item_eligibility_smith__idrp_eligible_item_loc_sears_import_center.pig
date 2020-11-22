/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_loc_sears_import_center.pig
# AUTHOR NAME:         Mudit Mangal
# CREATION DATE:       Thu Jan 02 08:20:24 EST 2014
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
REGISTER $UDF_JAR;
DEFINE TrimLeadingZeros com.searshc.supplychain.idrp.udf.TrimLeadingZeros();
DEFINE AddDays com.searshc.supplychain.idrp.udf.AddOrRemoveDaysToDate();
SET default_parallel $NUM_PARALLEL;
/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/


LOAD_ITEM_LOC_SEARS_UNION = LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOC_SEARS_UNION_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);


LOAD_ELIGIBLE_LOC = LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);

join_combine_loc = JOIN LOAD_ITEM_LOC_SEARS_UNION BY TrimLeadingZeros(srs_source_nbr) LEFT OUTER,LOAD_ELIGIBLE_LOC BY TrimLeadingZeros(srs_vndr_nbr);




SPLIT  join_combine_loc into filt_join_combine_loc IF ( loc_lvl_cd == 'VENDOR' AND duns_type_cd == 'ORD' AND imp_fl == '1' AND  elig_sts_cd == 'A'),not_filt_join_combine_loc IF (loc_lvl_cd != 'VENDOR' OR duns_type_cd != 'ORD' OR imp_fl != '1' OR elig_sts_cd != 'A'),third_dataset IF (LOAD_ELIGIBLE_LOC::srs_vndr_nbr is NULL);

grp_join_combine_loc = GROUP filt_join_combine_loc BY  item;

gen_grp_join_combine_loc =  FOREACH grp_join_combine_loc
                                                 {
                                                  sorted = ORDER filt_join_combine_loc BY srs_source_nbr asc;
                                                  first_row = LIMIT sorted 1;
                                                  GENERATE FLATTEN (first_row);
                                                  };






gen_grp_join_combine_loc = FOREACH gen_grp_join_combine_loc GENERATE
                                        first_row::LOAD_ITEM_LOC_SEARS_UNION::item AS  item,
                                        'TF32-15' AS loc,
                                        'S' AS src_owner_cd,
                                        'A' AS elig_sts_cd,
                                        first_row::LOAD_ITEM_LOC_SEARS_UNION::src_pack_qty AS src_pack_qty,
                                        first_row::LOAD_ELIGIBLE_LOC::loc AS src_loc,
                                        first_row::LOAD_ELIGIBLE_LOC::loc AS po_vnd_no,
                                        first_row::LOAD_ITEM_LOC_SEARS_UNION::ksn_id AS ksn_id,
                                        '' AS purch_stat_cd,
                                        '' AS retail_crtn_intrnl_pack_qty,
'' AS days_to_check_begin_date,
                                        '' AS days_to_check_end_date,
                                        '' AS dotcom_order_indicator,
                                        '' AS vend_pack_id,
                                        '' AS vend_pack_purch_stat_cd,
                                        '' AS vendor_pack_flow_type,
                                        '' AS vendor_pack_qty,
                                        '' AS reorder_method_code,
                                        '' AS str_supplier_cd,
                                        '' AS vend_stk_nbr,
                                        '' AS ksn_pack_id,
                                        '' AS ksn_dc_pack_purch_stat_cd,
                                        '' AS inbnd_ord_uom_cd,
                                        '' AS enable_jif_dc_ind,
                                        '' AS stk_ind,
                                        '' AS crtn_per_layer_qty,
                                        '' AS layer_per_pall_qty,
                                        '' AS dc_config_cd,

                                        '1' AS imp_fl,
                                        first_row::LOAD_ITEM_LOC_SEARS_UNION::srs_division_nbr AS srs_division_nbr,
                                        first_row::LOAD_ITEM_LOC_SEARS_UNION::srs_item_nbr AS srs_item_nbr,
                                        first_row::LOAD_ITEM_LOC_SEARS_UNION::srs_sku_cd AS srs_sku_cd,
                                        'TF32-15' AS srs_location_nbr,
                                        first_row::LOAD_ITEM_LOC_SEARS_UNION::srs_source_nbr AS srs_source_nbr,
                                        '' AS rim_sts_cd,
                                        '' AS non_stock_source_cd,
                                        '' AS item_active_ind,
                                        '' AS item_reserve_cd,
                                        '' AS item_next_period_on_hand_qty,
                                        '' AS item_reserve_qty,
                                        '' AS item_back_order_qty,
                                        '' AS item_next_period_future_order_qty,
                                        '' AS item_on_order_qty,
                                        '' AS item_next_period_in_transit_qty,
                                        '' AS item_last_receive_dt,
                                        '' AS item_last_ship_dt,
                                        '' AS stk_typ_cd;


join2 = join filt_join_combine_loc BY item , gen_grp_join_combine_loc BY item;


gen_join2 = FOREACH join2 GENERATE
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::item,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::loc,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::src_owner_cd,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::elig_sts_cd,
gen_grp_join_combine_loc::src_pack_qty AS src_pack_qty,
'TF32-15' AS src_loc,
gen_grp_join_combine_loc::po_vnd_no AS po_vnd_no,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::ksn_id,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::purch_stat_cd,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::retail_crtn_intrnl_pack_qty,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::days_to_check_begin_date,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::days_to_check_end_date,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::dotcom_order_indicator,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::vend_pack_id,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::vend_pack_purch_stat_cd,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::vendor_pack_flow_type,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::vendor_pack_qty,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::reorder_method_code,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::str_supplier_cd,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::vend_stk_nbr,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::ksn_pack_id,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::ksn_dc_pack_purch_stat_cd,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::inbnd_ord_uom_cd,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::enable_jif_dc_ind,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::stk_ind,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::crtn_per_layer_qty,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::layer_per_pall_qty,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::dc_config_cd,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::imp_fl,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::srs_division_nbr,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::srs_item_nbr,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::srs_sku_cd,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::srs_location_nbr,
'TF32-15' AS srs_source_nbr,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::rim_sts_cd,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::non_stock_source_cd,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::item_active_ind,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::item_reserve_cd,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::item_next_period_on_hand_qty,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::item_reserve_qty,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::item_back_order_qty,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::item_next_period_future_order_qty,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::item_on_order_qty,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::item_next_period_in_transit_qty,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::item_last_receive_dt,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::item_last_ship_dt,
filt_join_combine_loc::LOAD_ITEM_LOC_SEARS_UNION::stk_typ_cd;


not_filt_join_combine_loc_gen = FOREACH not_filt_join_combine_loc GENERATE
LOAD_ITEM_LOC_SEARS_UNION::item,
LOAD_ITEM_LOC_SEARS_UNION::loc,
LOAD_ITEM_LOC_SEARS_UNION::src_owner_cd,
LOAD_ITEM_LOC_SEARS_UNION::elig_sts_cd,
LOAD_ITEM_LOC_SEARS_UNION::src_pack_qty AS src_pack_qty,
LOAD_ITEM_LOC_SEARS_UNION::src_loc AS src_loc,
LOAD_ITEM_LOC_SEARS_UNION::po_vnd_no AS po_vnd_no,
LOAD_ITEM_LOC_SEARS_UNION::ksn_id,
LOAD_ITEM_LOC_SEARS_UNION::purch_stat_cd,
LOAD_ITEM_LOC_SEARS_UNION::retail_crtn_intrnl_pack_qty,
LOAD_ITEM_LOC_SEARS_UNION::days_to_check_begin_date,
LOAD_ITEM_LOC_SEARS_UNION::days_to_check_end_date,
LOAD_ITEM_LOC_SEARS_UNION::dotcom_order_indicator,
LOAD_ITEM_LOC_SEARS_UNION::vend_pack_id,
LOAD_ITEM_LOC_SEARS_UNION::vend_pack_purch_stat_cd,
LOAD_ITEM_LOC_SEARS_UNION::vendor_pack_flow_type,
LOAD_ITEM_LOC_SEARS_UNION::vendor_pack_qty,
LOAD_ITEM_LOC_SEARS_UNION::reorder_method_code,
LOAD_ITEM_LOC_SEARS_UNION::str_supplier_cd,
LOAD_ITEM_LOC_SEARS_UNION::vend_stk_nbr,
LOAD_ITEM_LOC_SEARS_UNION::ksn_pack_id,
LOAD_ITEM_LOC_SEARS_UNION::ksn_dc_pack_purch_stat_cd,
LOAD_ITEM_LOC_SEARS_UNION::inbnd_ord_uom_cd,
LOAD_ITEM_LOC_SEARS_UNION::enable_jif_dc_ind,
LOAD_ITEM_LOC_SEARS_UNION::stk_ind,
LOAD_ITEM_LOC_SEARS_UNION::crtn_per_layer_qty,
LOAD_ITEM_LOC_SEARS_UNION::layer_per_pall_qty,
LOAD_ITEM_LOC_SEARS_UNION::dc_config_cd,
LOAD_ITEM_LOC_SEARS_UNION::imp_fl,
LOAD_ITEM_LOC_SEARS_UNION::srs_division_nbr,
LOAD_ITEM_LOC_SEARS_UNION::srs_item_nbr,
LOAD_ITEM_LOC_SEARS_UNION::srs_sku_cd,
LOAD_ITEM_LOC_SEARS_UNION::srs_location_nbr,
LOAD_ITEM_LOC_SEARS_UNION::srs_source_nbr AS srs_source_nbr,
LOAD_ITEM_LOC_SEARS_UNION::rim_sts_cd,
LOAD_ITEM_LOC_SEARS_UNION::non_stock_source_cd,
LOAD_ITEM_LOC_SEARS_UNION::item_active_ind,
LOAD_ITEM_LOC_SEARS_UNION::item_reserve_cd,
LOAD_ITEM_LOC_SEARS_UNION::item_next_period_on_hand_qty,
LOAD_ITEM_LOC_SEARS_UNION::item_reserve_qty,
LOAD_ITEM_LOC_SEARS_UNION::item_back_order_qty,
LOAD_ITEM_LOC_SEARS_UNION::item_next_period_future_order_qty,
LOAD_ITEM_LOC_SEARS_UNION::item_on_order_qty,
LOAD_ITEM_LOC_SEARS_UNION::item_next_period_in_transit_qty,
LOAD_ITEM_LOC_SEARS_UNION::item_last_receive_dt,
LOAD_ITEM_LOC_SEARS_UNION::item_last_ship_dt,
LOAD_ITEM_LOC_SEARS_UNION::stk_typ_cd;

third_dataset_gen = FOREACH third_dataset GENERATE
LOAD_ITEM_LOC_SEARS_UNION::item,
LOAD_ITEM_LOC_SEARS_UNION::loc,
LOAD_ITEM_LOC_SEARS_UNION::src_owner_cd,
LOAD_ITEM_LOC_SEARS_UNION::elig_sts_cd,
LOAD_ITEM_LOC_SEARS_UNION::src_pack_qty AS src_pack_qty,
LOAD_ITEM_LOC_SEARS_UNION::src_loc AS src_loc,
LOAD_ITEM_LOC_SEARS_UNION::po_vnd_no AS po_vnd_no,
LOAD_ITEM_LOC_SEARS_UNION::ksn_id,
LOAD_ITEM_LOC_SEARS_UNION::purch_stat_cd,
LOAD_ITEM_LOC_SEARS_UNION::retail_crtn_intrnl_pack_qty,
LOAD_ITEM_LOC_SEARS_UNION::days_to_check_begin_date,
LOAD_ITEM_LOC_SEARS_UNION::days_to_check_end_date,
LOAD_ITEM_LOC_SEARS_UNION::dotcom_order_indicator,
LOAD_ITEM_LOC_SEARS_UNION::vend_pack_id,
LOAD_ITEM_LOC_SEARS_UNION::vend_pack_purch_stat_cd,
LOAD_ITEM_LOC_SEARS_UNION::vendor_pack_flow_type,
LOAD_ITEM_LOC_SEARS_UNION::vendor_pack_qty,
LOAD_ITEM_LOC_SEARS_UNION::reorder_method_code,
LOAD_ITEM_LOC_SEARS_UNION::str_supplier_cd,
LOAD_ITEM_LOC_SEARS_UNION::vend_stk_nbr,
LOAD_ITEM_LOC_SEARS_UNION::ksn_pack_id,
LOAD_ITEM_LOC_SEARS_UNION::ksn_dc_pack_purch_stat_cd,
LOAD_ITEM_LOC_SEARS_UNION::inbnd_ord_uom_cd,
LOAD_ITEM_LOC_SEARS_UNION::enable_jif_dc_ind,
LOAD_ITEM_LOC_SEARS_UNION::stk_ind,
LOAD_ITEM_LOC_SEARS_UNION::crtn_per_layer_qty,
LOAD_ITEM_LOC_SEARS_UNION::layer_per_pall_qty,
LOAD_ITEM_LOC_SEARS_UNION::dc_config_cd,
LOAD_ITEM_LOC_SEARS_UNION::imp_fl,
LOAD_ITEM_LOC_SEARS_UNION::srs_division_nbr,
LOAD_ITEM_LOC_SEARS_UNION::srs_item_nbr,
LOAD_ITEM_LOC_SEARS_UNION::srs_sku_cd,
LOAD_ITEM_LOC_SEARS_UNION::srs_location_nbr,
LOAD_ITEM_LOC_SEARS_UNION::srs_source_nbr AS srs_source_nbr,
LOAD_ITEM_LOC_SEARS_UNION::rim_sts_cd,
LOAD_ITEM_LOC_SEARS_UNION::non_stock_source_cd,
LOAD_ITEM_LOC_SEARS_UNION::item_active_ind,
LOAD_ITEM_LOC_SEARS_UNION::item_reserve_cd,
LOAD_ITEM_LOC_SEARS_UNION::item_next_period_on_hand_qty,
LOAD_ITEM_LOC_SEARS_UNION::item_reserve_qty,
LOAD_ITEM_LOC_SEARS_UNION::item_back_order_qty,
LOAD_ITEM_LOC_SEARS_UNION::item_next_period_future_order_qty,
LOAD_ITEM_LOC_SEARS_UNION::item_on_order_qty,
LOAD_ITEM_LOC_SEARS_UNION::item_next_period_in_transit_qty,
LOAD_ITEM_LOC_SEARS_UNION::item_last_receive_dt,
LOAD_ITEM_LOC_SEARS_UNION::item_last_ship_dt,
LOAD_ITEM_LOC_SEARS_UNION::stk_typ_cd;

uni = UNION gen_grp_join_combine_loc,gen_join2,not_filt_join_combine_loc_gen,third_dataset_gen;

UNI_DIST = DISTINCT uni;
UNI_DIST = 
    FOREACH UNI_DIST
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


STORE UNI_DIST INTO '$WORK__IDRP_ELIGIBLE_ITEM_LOC_SEARS_IMPORT_CENTER_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
