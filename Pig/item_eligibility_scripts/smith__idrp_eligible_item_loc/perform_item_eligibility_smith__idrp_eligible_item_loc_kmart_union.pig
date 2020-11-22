/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_loc_kmart_union.pig
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Tue Dec 31 02:57:05 EST 2013
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

------------------------------------LOADING KMART STORE AND KMART DC DATA-----------------------------------------------

kmart_store_data = 
    LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOC_KMART_STORE_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);

kmart_dc_data = 
    LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOC_KMART_DC_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);

------------------------------------------UNION KMART DC+ STORE---------------------------------------------------------

all_kmart_union = 
        UNION kmart_store_data,
		      kmart_dc_data;


all_kmart_union_dist = DISTINCT all_kmart_union;

---------------------------SPLIT DATA ON PO_VEND_NO IS NULL AND NOT NULL-------------------------------------------------

SPLIT all_kmart_union_dist 
INTO null_po_vnd_no IF po_vnd_no IS NULL OR po_vnd_no == '' OR po_vnd_no == ' ',
     not_null_po_vnd_no IF po_vnd_no IS NOT NULL ;

---------------------------JOIN NULL AND NOT NULL DATA ON ITEM,SRCLOC----------------------------------------------------

join_null_not_null_vend_no = 
        JOIN null_po_vnd_no BY (item,src_loc),
	         not_null_po_vnd_no BY (item,loc);


generate_po_vnd_no_for_all_src_loc = 
        FOREACH join_null_not_null_vend_no
        GENERATE
                null_po_vnd_no::item AS item,
	            null_po_vnd_no::loc AS loc,
	            null_po_vnd_no::src_loc AS src_loc,
	            (null_po_vnd_no::po_vnd_no IS NULL OR null_po_vnd_no::po_vnd_no == '' OR null_po_vnd_no::po_vnd_no == ' ' ? not_null_po_vnd_no::po_vnd_no : null_po_vnd_no::po_vnd_no) AS po_vnd_no;


join_with_original_data = 
        JOIN all_kmart_union_dist BY (item,loc) LEFT OUTER ,
	         generate_po_vnd_no_for_all_src_loc BY (item,loc);


generate_kmart_data = 	
        FOREACH join_with_original_data
        GENERATE
		        '$CURRENT_TIMESTAMP' as load_ts ,
	            all_kmart_union_dist::item AS item,
                all_kmart_union_dist::loc AS loc,
                all_kmart_union_dist::src_owner_cd AS src_owner_cd,
                all_kmart_union_dist::elig_sts_cd AS elig_sts_cd,
                all_kmart_union_dist::src_pack_qty AS src_pack_qty,
                all_kmart_union_dist::src_loc AS src_loc,
                (all_kmart_union_dist::po_vnd_no IS NULL  OR all_kmart_union_dist::po_vnd_no == '' OR all_kmart_union_dist::po_vnd_no == ' ' ? generate_po_vnd_no_for_all_src_loc::po_vnd_no : all_kmart_union_dist::po_vnd_no) AS po_vnd_no,
                all_kmart_union_dist::ksn_id AS ksn_id,
                all_kmart_union_dist::purch_stat_cd AS purch_stat_cd,
                all_kmart_union_dist::retail_crtn_intrnl_pack_qty AS retail_crtn_intrnl_pack_qty,
                all_kmart_union_dist::days_to_check_begin_date AS days_to_check_begin_date,
                all_kmart_union_dist::days_to_check_end_date AS days_to_check_end_date,
                all_kmart_union_dist::dotcom_order_indicator AS dotcom_order_indicator,
                all_kmart_union_dist::vend_pack_id AS vend_pack_id,
                all_kmart_union_dist::vend_pack_purch_stat_cd AS vend_pack_purch_stat_cd,
                all_kmart_union_dist::vendor_pack_flow_type AS vendor_pack_flow_type,
                all_kmart_union_dist::vendor_pack_qty AS vendor_pack_qty,
                all_kmart_union_dist::reorder_method_code AS reorder_method_code,
                all_kmart_union_dist::str_supplier_cd AS str_supplier_cd,
                all_kmart_union_dist::vend_stk_nbr AS vend_stk_nbr,
                all_kmart_union_dist::ksn_pack_id AS ksn_pack_id,
                all_kmart_union_dist::ksn_dc_pack_purch_stat_cd AS ksn_dc_pack_purch_stat_cd,
                all_kmart_union_dist::inbnd_ord_uom_cd AS inbnd_ord_uom_cd,
                all_kmart_union_dist::enable_jif_dc_ind AS enable_jif_dc_ind,
                all_kmart_union_dist::stk_ind AS stk_ind,
                all_kmart_union_dist::crtn_per_layer_qty AS crtn_per_layer_qty,
                all_kmart_union_dist::layer_per_pall_qty AS layer_per_pall_qty,
                all_kmart_union_dist::dc_config_cd AS dc_config_cd,
                all_kmart_union_dist::imp_fl AS imp_fl,
                all_kmart_union_dist::srs_division_nbr AS srs_division_nbr,
                all_kmart_union_dist::srs_item_nbr AS srs_item_nbr,
                all_kmart_union_dist::srs_sku_cd AS srs_sku_cd,
                all_kmart_union_dist::srs_location_nbr AS srs_location_nbr,
                all_kmart_union_dist::srs_source_nbr AS srs_source_nbr,
                all_kmart_union_dist::rim_sts_cd AS rim_sts_cd,
                all_kmart_union_dist::non_stock_source_cd AS non_stock_source_cd,
                all_kmart_union_dist::item_active_ind AS item_active_ind,
                all_kmart_union_dist::item_reserve_cd AS item_reserve_cd,
                all_kmart_union_dist::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
                all_kmart_union_dist::item_reserve_qty AS item_reserve_qty,
                all_kmart_union_dist::item_back_order_qty AS item_back_order_qty,
                all_kmart_union_dist::item_next_period_future_order_qty AS item_next_period_future_order_qty,
                all_kmart_union_dist::item_on_order_qty AS item_on_order_qty,
                all_kmart_union_dist::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
                all_kmart_union_dist::item_last_receive_dt AS item_last_receive_dt,
                all_kmart_union_dist::item_last_ship_dt AS item_last_ship_dt,
                all_kmart_union_dist::stk_typ_cd AS stk_typ_cd,
				'$batchid' AS batch_id;


------------------------------STORING DATA------------------------------------------------

STORE generate_kmart_data 
INTO '$WORK__IDRP_ELIGIBLE_ITEM_LOC_KMART_UNION_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

------------------------------------------------------------------------------------------



/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
