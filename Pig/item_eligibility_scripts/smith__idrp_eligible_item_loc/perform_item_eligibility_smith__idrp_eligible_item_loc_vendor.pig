/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_loc_vendor.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Thu Jan 02 00:37:26 EST 2014
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
SET default_parallel $NUM_PARALLEL;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

vendor_input = 
    LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOC_KMART_SEARS_UNION_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);
	
vendor_input = FOREACH vendor_input GENERATE
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
stk_typ_cd AS stk_typ_cd;	


gen_vendor_input = 
        FOREACH vendor_input 
		GENERATE 
		        item,
		        loc,
		        src_loc,
		        ksn_id,
		        imp_fl,
		        srs_division_nbr,
		        srs_item_nbr,
		        srs_sku_cd,
		        srs_source_nbr,
		        elig_sts_cd;


filt_vendor_input = 
        FILTER gen_vendor_input 
		BY elig_sts_cd == 'A';


smith__idrp_eligible_loc = 
    LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);


smith__idrp_eligible_loc = 
        FOREACH smith__idrp_eligible_loc 
		GENERATE 
		        loc,
		        loc_lvl_cd;


filt1_smith__idrp_eligible_loc = 
        FILTER smith__idrp_eligible_loc 
		BY loc_lvl_cd == 'STORE' OR loc_lvl_cd == 'WAREHOUSE';


filt2_smith__idrp_eligible_loc = 
        FILTER smith__idrp_eligible_loc 
		BY loc_lvl_cd == 'VENDOR';


join_item_loc = 
        JOIN filt_vendor_input BY loc ,
		     filt1_smith__idrp_eligible_loc BY loc;


join_join_item_loc_eligible_loc = 
        JOIN join_item_loc BY filt_vendor_input::src_loc,
		     filt2_smith__idrp_eligible_loc BY loc;


gen_join_join_item_loc_eligible_loc = 
        FOREACH join_join_item_loc_eligible_loc 
		GENERATE
                join_item_loc::filt_vendor_input::item AS item,
                join_item_loc::filt_vendor_input::src_loc AS loc,
                '' AS src_owner_cd,
                'A' AS elig_sts_cd,
                '1' AS src_pack_qty,
                '' AS src_loc,
                '' AS po_vnd_no,
                '' AS ksn_id,
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
                join_item_loc::filt_vendor_input::imp_fl AS imp_fl,
                join_item_loc::filt_vendor_input::srs_division_nbr AS srs_division_nbr,
                join_item_loc::filt_vendor_input::srs_item_nbr AS srs_item_nbr,
                join_item_loc::filt_vendor_input::srs_sku_cd AS srs_sku_cd,
                join_item_loc::filt_vendor_input::srs_source_nbr AS srs_location_nbr,
                '' AS srs_source_nbr,
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


grp_gen_join_join_item_loc_eligible_loc = 
        GROUP gen_join_join_item_loc_eligible_loc 
		BY (item, loc);


limit_grp_gen_join_join_item_loc_eligible_loc = 
        FOREACH grp_gen_join_join_item_loc_eligible_loc
        {
            limit_item_loc = LIMIT gen_join_join_item_loc_eligible_loc 1;
            GENERATE FLATTEN(limit_item_loc);
        };


uni = 
        UNION vendor_input,
		      limit_grp_gen_join_join_item_loc_eligible_loc;


--dist_uni = DISTINCT uni;

TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111 = FOREACH uni GENERATE

item,
loc AS loc,
src_owner_cd,
elig_sts_cd,
src_pack_qty,
src_loc,
po_vnd_no,
ksn_id,
purch_stat_cd,
retail_crtn_intrnl_pack_qty,
days_to_check_begin_date,
days_to_check_end_date,
dotcom_order_indicator,
vend_pack_id,
vend_pack_purch_stat_cd,
vendor_pack_flow_type,
vendor_pack_qty,
reorder_method_code,
str_supplier_cd,
vend_stk_nbr,
ksn_pack_id,
ksn_dc_pack_purch_stat_cd,
inbnd_ord_uom_cd,
enable_jif_dc_ind,
stk_ind,
crtn_per_layer_qty,
layer_per_pall_qty,
dc_config_cd,
imp_fl,
srs_division_nbr,
srs_item_nbr,
srs_sku_cd,
srs_location_nbr AS srs_location_nbr,
srs_source_nbr AS srs_source_nbr,
rim_sts_cd,
non_stock_source_cd,
item_active_ind,
item_reserve_cd,
item_next_period_on_hand_qty,
item_reserve_qty,
item_back_order_qty,
item_next_period_future_order_qty,
item_on_order_qty,
item_next_period_in_transit_qty,
item_last_receive_dt,
item_last_ship_dt,
stk_typ_cd;

--LOAD_ELIGIBLE_ITEM_LOC_DISTINCT = DISTINCT TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111;

NOT_ELIGIBLE_ROWS= DISTINCT TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111;

NOT_ELIGIBLE_ROWS_JOIN = JOIN TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111 by (item,TrimLeadingZeros(src_loc)) LEFT OUTER, NOT_ELIGIBLE_ROWS BY (item, TrimLeadingZeros(loc));

TARGET_COLS_112233 = FOREACH NOT_ELIGIBLE_ROWS_JOIN GENERATE

TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::item as item,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::loc as loc,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::src_owner_cd as src_owner_cd,

(TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::loc MATCHES '.*?_[OS]'? TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::elig_sts_cd :(TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::elig_sts_cd =='D' AND NOT_ELIGIBLE_ROWS::elig_sts_cd is NOT NULL  AND  NOT_ELIGIBLE_ROWS::elig_sts_cd != '' ? TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::elig_sts_cd:(NOT_ELIGIBLE_ROWS::elig_sts_cd is NULL  OR  NOT_ELIGIBLE_ROWS::elig_sts_cd == '' ? 'D':NOT_ELIGIBLE_ROWS::elig_sts_cd ))) as elig_sts_cd ,

TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::src_pack_qty as src_pack_qty,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::src_loc as src_loc,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::po_vnd_no as po_vnd_no,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::ksn_id as ksn_id,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::purch_stat_cd as purch_stat_cd,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::retail_crtn_intrnl_pack_qty as retail_crtn_intrnl_pack_qty,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::days_to_check_begin_date as days_to_check_begin_date,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::days_to_check_end_date as days_to_check_end_date,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::dotcom_order_indicator as dotcom_order_indicator,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::vend_pack_id as vend_pack_id,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::vend_pack_purch_stat_cd as vend_pack_purch_stat_cd,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::vendor_pack_flow_type as vendor_pack_flow_type,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::vendor_pack_qty as vendor_pack_qty,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::reorder_method_code as reorder_method_code,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::str_supplier_cd as str_supplier_cd,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::vend_stk_nbr as vend_stk_nbr ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::ksn_pack_id as ksn_pack_id ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::ksn_dc_pack_purch_stat_cd as ksn_dc_pack_purch_stat_cd ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::inbnd_ord_uom_cd as inbnd_ord_uom_cd,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::enable_jif_dc_ind as enable_jif_dc_ind ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::stk_ind as stk_ind ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::crtn_per_layer_qty as crtn_per_layer_qty ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::layer_per_pall_qty as layer_per_pall_qty ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::dc_config_cd as dc_config_cd ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::imp_fl as imp_fl,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::srs_division_nbr as srs_division_nbr,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::srs_item_nbr as srs_item_nbr,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::srs_sku_cd as srs_sku_cd,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::srs_location_nbr as srs_location_nbr,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::srs_source_nbr as srs_source_nbr,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::rim_sts_cd as rim_sts_cd,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::non_stock_source_cd as non_stock_source_cd,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::item_active_ind as item_active_ind ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::item_reserve_cd as item_reserve_cd,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::item_next_period_on_hand_qty as item_next_period_on_hand_qty ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::item_reserve_qty as item_reserve_qty ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::item_back_order_qty as item_back_order_qty ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::item_next_period_future_order_qty as item_next_period_future_order_qty ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::item_on_order_qty as item_on_order_qty ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::item_next_period_in_transit_qty as item_next_period_in_transit_qty ,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::item_last_receive_dt as item_last_receive_dt,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::item_last_ship_dt as item_last_ship_dt,
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111::stk_typ_cd as stk_typ_cd;


NOT_ELIGIBLE_ROWS_1= DISTINCT TARGET_COLS_112233;

NOT_ELIGIBLE_ROWS_JOIN_1 = JOIN TARGET_COLS_112233 by (item,TrimLeadingZeros(src_loc)) LEFT OUTER, NOT_ELIGIBLE_ROWS_1 BY (item,TrimLeadingZeros(loc));

TARGET_COLS_1122333 = FOREACH NOT_ELIGIBLE_ROWS_JOIN_1 GENERATE

TARGET_COLS_112233::item as item,
TARGET_COLS_112233::loc as loc,
TARGET_COLS_112233::src_owner_cd as src_owner_cd,
(TARGET_COLS_112233::loc MATCHES '.*?_[OS]'? TARGET_COLS_112233::elig_sts_cd :(TARGET_COLS_112233::elig_sts_cd =='D' AND NOT_ELIGIBLE_ROWS_1::elig_sts_cd is NOT NULL  AND  NOT_ELIGIBLE_ROWS_1::elig_sts_cd != '' ? TARGET_COLS_112233::elig_sts_cd:(NOT_ELIGIBLE_ROWS_1::elig_sts_cd is NULL  OR  NOT_ELIGIBLE_ROWS_1::elig_sts_cd == '' ? 'D':NOT_ELIGIBLE_ROWS_1::elig_sts_cd ))) as elig_sts_cd,
TARGET_COLS_112233::src_pack_qty as src_pack_qty,
TARGET_COLS_112233::src_loc as src_loc,
TARGET_COLS_112233::po_vnd_no as po_vnd_no,
TARGET_COLS_112233::ksn_id as ksn_id,
TARGET_COLS_112233::purch_stat_cd as purch_stat_cd,
TARGET_COLS_112233::retail_crtn_intrnl_pack_qty as retail_crtn_intrnl_pack_qty,
TARGET_COLS_112233::days_to_check_begin_date as days_to_check_begin_date,
TARGET_COLS_112233::days_to_check_end_date as days_to_check_end_date,
TARGET_COLS_112233::dotcom_order_indicator as dotcom_order_indicator,
TARGET_COLS_112233::vend_pack_id as vend_pack_id,
TARGET_COLS_112233::vend_pack_purch_stat_cd as vend_pack_purch_stat_cd,
TARGET_COLS_112233::vendor_pack_flow_type as vendor_pack_flow_type,
TARGET_COLS_112233::vendor_pack_qty as vendor_pack_qty,
TARGET_COLS_112233::reorder_method_code as reorder_method_code,
TARGET_COLS_112233::str_supplier_cd as str_supplier_cd,
TARGET_COLS_112233::vend_stk_nbr as vend_stk_nbr ,
TARGET_COLS_112233::ksn_pack_id as ksn_pack_id ,
TARGET_COLS_112233::ksn_dc_pack_purch_stat_cd as ksn_dc_pack_purch_stat_cd ,
TARGET_COLS_112233::inbnd_ord_uom_cd as inbnd_ord_uom_cd,
TARGET_COLS_112233::enable_jif_dc_ind as enable_jif_dc_ind ,
TARGET_COLS_112233::stk_ind as stk_ind ,
TARGET_COLS_112233::crtn_per_layer_qty as crtn_per_layer_qty ,
TARGET_COLS_112233::layer_per_pall_qty as layer_per_pall_qty ,
TARGET_COLS_112233::dc_config_cd as dc_config_cd ,
TARGET_COLS_112233::imp_fl as imp_fl,
TARGET_COLS_112233::srs_division_nbr as srs_division_nbr,
TARGET_COLS_112233::srs_item_nbr as srs_item_nbr,
TARGET_COLS_112233::srs_sku_cd as srs_sku_cd,
TARGET_COLS_112233::srs_location_nbr as srs_location_nbr,
TARGET_COLS_112233::srs_source_nbr as srs_source_nbr,
TARGET_COLS_112233::rim_sts_cd as rim_sts_cd,
TARGET_COLS_112233::non_stock_source_cd as non_stock_source_cd,
TARGET_COLS_112233::item_active_ind as item_active_ind ,
TARGET_COLS_112233::item_reserve_cd as item_reserve_cd,
TARGET_COLS_112233::item_next_period_on_hand_qty as item_next_period_on_hand_qty ,
TARGET_COLS_112233::item_reserve_qty as item_reserve_qty ,
TARGET_COLS_112233::item_back_order_qty as item_back_order_qty ,
TARGET_COLS_112233::item_next_period_future_order_qty as item_next_period_future_order_qty ,
TARGET_COLS_112233::item_on_order_qty as item_on_order_qty ,
TARGET_COLS_112233::item_next_period_in_transit_qty as item_next_period_in_transit_qty ,
TARGET_COLS_112233::item_last_receive_dt as item_last_receive_dt,
TARGET_COLS_112233::item_last_ship_dt as item_last_ship_dt,
TARGET_COLS_112233::stk_typ_cd as stk_typ_cd;

--LOAD_ELIGIBLE_ITEM_LOC_DISTINCT = DISTINCT TARGET_COLS_1122333;

NOT_ELIGIBLE_ROWS_11= DISTINCT TARGET_COLS_1122333;

NOT_ELIGIBLE_ROWS_JOIN_11 = JOIN TARGET_COLS_1122333 BY (item,TrimLeadingZeros(src_loc)) LEFT OUTER, NOT_ELIGIBLE_ROWS_11 BY (item, TrimLeadingZeros(loc));

TARGET_COLS_11223334 = FOREACH NOT_ELIGIBLE_ROWS_JOIN_11 GENERATE

TARGET_COLS_1122333::item as item,
TARGET_COLS_1122333::loc as loc,
TARGET_COLS_1122333::src_owner_cd as src_owner_cd,

(TARGET_COLS_1122333::loc MATCHES '.*?_[OS]' ? TARGET_COLS_1122333::elig_sts_cd :(TARGET_COLS_1122333::elig_sts_cd =='D' AND NOT_ELIGIBLE_ROWS_11::elig_sts_cd is NOT NULL  AND  NOT_ELIGIBLE_ROWS_11::elig_sts_cd != '' ? TARGET_COLS_1122333::elig_sts_cd:(NOT_ELIGIBLE_ROWS_11::elig_sts_cd is NULL  OR  NOT_ELIGIBLE_ROWS_11::elig_sts_cd == '' ? 'D':NOT_ELIGIBLE_ROWS_11::elig_sts_cd ))) as elig_sts_cd,

TARGET_COLS_1122333::src_pack_qty as src_pack_qty,
TARGET_COLS_1122333::src_loc as src_loc,
TARGET_COLS_1122333::po_vnd_no as po_vnd_no,
TARGET_COLS_1122333::ksn_id as ksn_id,
TARGET_COLS_1122333::purch_stat_cd as purch_stat_cd,
TARGET_COLS_1122333::retail_crtn_intrnl_pack_qty as retail_crtn_intrnl_pack_qty,
TARGET_COLS_1122333::days_to_check_begin_date as days_to_check_begin_date,
TARGET_COLS_1122333::days_to_check_end_date as days_to_check_end_date,
TARGET_COLS_1122333::dotcom_order_indicator as dotcom_order_indicator,
TARGET_COLS_1122333::vend_pack_id as vend_pack_id,
TARGET_COLS_1122333::vend_pack_purch_stat_cd as vend_pack_purch_stat_cd,
TARGET_COLS_1122333::vendor_pack_flow_type as vendor_pack_flow_type,
TARGET_COLS_1122333::vendor_pack_qty as vendor_pack_qty,
TARGET_COLS_1122333::reorder_method_code as reorder_method_code,
TARGET_COLS_1122333::str_supplier_cd as str_supplier_cd,
TARGET_COLS_1122333::vend_stk_nbr as vend_stk_nbr ,
TARGET_COLS_1122333::ksn_pack_id as ksn_pack_id ,
TARGET_COLS_1122333::ksn_dc_pack_purch_stat_cd as ksn_dc_pack_purch_stat_cd ,
TARGET_COLS_1122333::inbnd_ord_uom_cd as inbnd_ord_uom_cd,
TARGET_COLS_1122333::enable_jif_dc_ind as enable_jif_dc_ind ,
TARGET_COLS_1122333::stk_ind as stk_ind ,
TARGET_COLS_1122333::crtn_per_layer_qty as crtn_per_layer_qty ,
TARGET_COLS_1122333::layer_per_pall_qty as layer_per_pall_qty ,
TARGET_COLS_1122333::dc_config_cd as dc_config_cd ,
TARGET_COLS_1122333::imp_fl as imp_fl,
TARGET_COLS_1122333::srs_division_nbr as srs_division_nbr,
TARGET_COLS_1122333::srs_item_nbr as srs_item_nbr,
TARGET_COLS_1122333::srs_sku_cd as srs_sku_cd,
TARGET_COLS_1122333::srs_location_nbr as srs_location_nbr,
TARGET_COLS_1122333::srs_source_nbr as srs_source_nbr,
TARGET_COLS_1122333::rim_sts_cd as rim_sts_cd,
TARGET_COLS_1122333::non_stock_source_cd as non_stock_source_cd,
TARGET_COLS_1122333::item_active_ind as item_active_ind ,
TARGET_COLS_1122333::item_reserve_cd as item_reserve_cd,
TARGET_COLS_1122333::item_next_period_on_hand_qty as item_next_period_on_hand_qty ,
TARGET_COLS_1122333::item_reserve_qty as item_reserve_qty ,
TARGET_COLS_1122333::item_back_order_qty as item_back_order_qty ,
TARGET_COLS_1122333::item_next_period_future_order_qty as item_next_period_future_order_qty ,
TARGET_COLS_1122333::item_on_order_qty as item_on_order_qty ,
TARGET_COLS_1122333::item_next_period_in_transit_qty as item_next_period_in_transit_qty ,
TARGET_COLS_1122333::item_last_receive_dt as item_last_receive_dt,
TARGET_COLS_1122333::item_last_ship_dt as item_last_ship_dt,
TARGET_COLS_1122333::stk_typ_cd as stk_typ_cd;

--LOAD_ELIGIBLE_ITEM_LOC_DISTINCT = DISTINCT TARGET_COLS_11223334;

NOT_ELIGIBLE_ROWS_111= DISTINCT TARGET_COLS_11223334;

NOT_ELIGIBLE_ROWS_JOIN_111 = JOIN TARGET_COLS_11223334 BY (item,TrimLeadingZeros(src_loc)) LEFT OUTER, NOT_ELIGIBLE_ROWS_111 BY (item, TrimLeadingZeros(loc));

TARGET_COLS_112233344 = FOREACH NOT_ELIGIBLE_ROWS_JOIN_111 GENERATE
'$CURRENT_TIMESTAMP'	as	load_ts	,
TARGET_COLS_11223334::item as item,
TARGET_COLS_11223334::loc as loc,
TARGET_COLS_11223334::src_owner_cd as src_owner_cd,
(TARGET_COLS_11223334::loc MATCHES '.*?_[OS]' ? TARGET_COLS_11223334::elig_sts_cd :(TARGET_COLS_11223334::elig_sts_cd =='D' AND NOT_ELIGIBLE_ROWS_111::elig_sts_cd is NOT NULL  AND  NOT_ELIGIBLE_ROWS_111::elig_sts_cd != '' ? TARGET_COLS_11223334::elig_sts_cd:(NOT_ELIGIBLE_ROWS_111::elig_sts_cd is NULL  OR  NOT_ELIGIBLE_ROWS_111::elig_sts_cd == '' ? 'D':NOT_ELIGIBLE_ROWS_111::elig_sts_cd ))) as elig_sts_cd,
TARGET_COLS_11223334::src_pack_qty as src_pack_qty,
TARGET_COLS_11223334::src_loc as src_loc,
TARGET_COLS_11223334::po_vnd_no as po_vnd_no,
TARGET_COLS_11223334::ksn_id as ksn_id,
TARGET_COLS_11223334::purch_stat_cd as purch_stat_cd,
TARGET_COLS_11223334::retail_crtn_intrnl_pack_qty as retail_crtn_intrnl_pack_qty,
TARGET_COLS_11223334::days_to_check_begin_date as days_to_check_begin_date,
TARGET_COLS_11223334::days_to_check_end_date as days_to_check_end_date,
TARGET_COLS_11223334::dotcom_order_indicator as dotcom_order_indicator,
TARGET_COLS_11223334::vend_pack_id as vend_pack_id,
TARGET_COLS_11223334::vend_pack_purch_stat_cd as vend_pack_purch_stat_cd,
TARGET_COLS_11223334::vendor_pack_flow_type as vendor_pack_flow_type,
TARGET_COLS_11223334::vendor_pack_qty as vendor_pack_qty,
TARGET_COLS_11223334::reorder_method_code as reorder_method_code,
TARGET_COLS_11223334::str_supplier_cd as str_supplier_cd,
TARGET_COLS_11223334::vend_stk_nbr as vend_stk_nbr ,
TARGET_COLS_11223334::ksn_pack_id as ksn_pack_id ,
TARGET_COLS_11223334::ksn_dc_pack_purch_stat_cd as ksn_dc_pack_purch_stat_cd ,
TARGET_COLS_11223334::inbnd_ord_uom_cd as inbnd_ord_uom_cd,
TARGET_COLS_11223334::enable_jif_dc_ind as enable_jif_dc_ind ,
TARGET_COLS_11223334::stk_ind as stk_ind ,
TARGET_COLS_11223334::crtn_per_layer_qty as crtn_per_layer_qty ,
TARGET_COLS_11223334::layer_per_pall_qty as layer_per_pall_qty ,
TARGET_COLS_11223334::dc_config_cd as dc_config_cd ,
TARGET_COLS_11223334::imp_fl as imp_fl,
TARGET_COLS_11223334::srs_division_nbr as srs_division_nbr,
TARGET_COLS_11223334::srs_item_nbr as srs_item_nbr,
TARGET_COLS_11223334::srs_sku_cd as srs_sku_cd,
TARGET_COLS_11223334::srs_location_nbr as srs_location_nbr,
TARGET_COLS_11223334::srs_source_nbr as srs_source_nbr,
TARGET_COLS_11223334::rim_sts_cd as rim_sts_cd,
TARGET_COLS_11223334::non_stock_source_cd as non_stock_source_cd,
TARGET_COLS_11223334::item_active_ind as item_active_ind ,
TARGET_COLS_11223334::item_reserve_cd as item_reserve_cd,
TARGET_COLS_11223334::item_next_period_on_hand_qty as item_next_period_on_hand_qty ,
TARGET_COLS_11223334::item_reserve_qty as item_reserve_qty ,
TARGET_COLS_11223334::item_back_order_qty as item_back_order_qty ,
TARGET_COLS_11223334::item_next_period_future_order_qty as item_next_period_future_order_qty ,
TARGET_COLS_11223334::item_on_order_qty as item_on_order_qty ,
TARGET_COLS_11223334::item_next_period_in_transit_qty as item_next_period_in_transit_qty ,
TARGET_COLS_11223334::item_last_receive_dt as item_last_receive_dt,
TARGET_COLS_11223334::item_last_ship_dt as item_last_ship_dt,
TARGET_COLS_11223334::stk_typ_cd as stk_typ_cd,
'$batchid' AS batch_id  ;

LOAD_ELIGIBLE_ITEM_LOC_DISTINCT = DISTINCT TARGET_COLS_112233344;




STORE LOAD_ELIGIBLE_ITEM_LOC_DISTINCT 
INTO '$WORK__IDRP_ELIGIBLE_ITEM_LOC_VENDOR_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
