/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_loc_sears_srim.pig
# AUTHOR NAME:         Mudit Mangal
# CREATION DATE:       Fri Dec 27 02:31:34 EST 2013
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


--LOAD SRIM file
LOAD_SRIM = LOAD '$GOLD__INVENTORY_SRIM_DAILY_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__INVENTORY_SRIM_DAILY_SCHEMA);

LOAD_SRIM = FOREACH LOAD_SRIM GENERATE division_nbr,item_nbr,sku_nbr,status_cd,store_pack_size_qty,warehouse_nbr,source_nbr;

--LOAD SMITH Item file
LOAD_SMITH_ITEM = LOAD '$SMITH__ITEM_COMBINED_HIERARCHY_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__ITEM_COMBINED_HIERARCHY_CURRENT_SCHEMA);

LOAD_SMITH_ITEM = FOREACH LOAD_SMITH_ITEM GENERATE shc_item_id_expiration_ts,sears_division_nbr,sears_item_nbr,sears_sku_nbr,ksn_id;

--LOAD VENDOR Package file
LOAD_VEND_PACK = LOAD '$GOLD__ITEM_VENDOR_PACKAGE_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_VENDOR_PACKAGE_CURRENT_SCHEMA);

LOAD_VEND_PACK = FOREACH LOAD_VEND_PACK GENERATE expiration_ts,owner_cd,purchase_status_cd,import_cd,ksn_id;

--LOAD ELIGIBLE Item file
LOAD_ELIGIBLE_ITEM = LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_ITEM_SCHEMA);

LOAD_ELIGIBLE_ITEM = FOREACH LOAD_ELIGIBLE_ITEM GENERATE elig_sts_cd,srs_div_no,srs_itm_no,srs_sku_no,item,easy_ord_fl,spc_ord_cdt_fl,itm_emp_fl;

--LOAD ELIGIBLE Loc file
LOAD_ELIGIBLE_LOC = LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);

----------------------------------------------------------------------------------------------------------------------------------

ELIGIBLE_ITEM_FILTER = FILTER LOAD_ELIGIBLE_ITEM BY elig_sts_cd == 'A' OR elig_sts_cd == 'D';

ITEM_COL_JOIN= JOIN LOAD_SRIM BY (division_nbr,TrimLeadingZeros(item_nbr),sku_nbr) , ELIGIBLE_ITEM_FILTER BY (srs_div_no,TrimLeadingZeros(srs_itm_no),srs_sku_no) ;
---------------------------------------------------------------------------------------------------------------------------------
LOAD_ELIGIBLE_LOC_1 = FOREACH LOAD_ELIGIBLE_LOC GENERATE

(loc is null ? '' : loc) as loc,
(srs_loc is null ? '': srs_loc)  as srs_loc,
(srs_vndr_nbr is null ? '': srs_vndr_nbr) as srs_vndr_nbr,
(loc_lvl_cd is null ? '': loc_lvl_cd )as loc_lvl_cd,
(loc_fmt_typ_cd is null ? '': loc_fmt_typ_cd )as loc_fmt_typ_cd,
(fmt_typ_cd is null ? '': fmt_typ_cd ) as fmt_typ_cd,
(duns_type_cd is NULL ? '': duns_type_cd) as duns_type_cd;

LOC_COL_FILTER = FILTER LOAD_ELIGIBLE_LOC_1 BY (loc_fmt_typ_cd != 'RRC' AND  loc_fmt_typ_cd != 'DDC' AND loc_fmt_typ_cd != 'MDO');


LOC_ITEM_COL_JOIN = JOIN ITEM_COL_JOIN BY TrimLeadingZeros(warehouse_nbr) LEFT OUTER, LOC_COL_FILTER BY TrimLeadingZeros(srs_loc);
----------------------------------------------------------------------------------------------------------------------------------

LOAD_SMITH_ITEM_EFFECTIVE = FILTER LOAD_SMITH_ITEM BY SUBSTRING(shc_item_id_expiration_ts,0,10) > '$CURRENT_DATE' ;
--------------------------------------------------------------------------------------------------------------------------------

LOC_ITEM_COL_JOIN_KSN = JOIN LOC_ITEM_COL_JOIN BY (ITEM_COL_JOIN::LOAD_SRIM::division_nbr,TrimLeadingZeros(ITEM_COL_JOIN::LOAD_SRIM::item_nbr),ITEM_COL_JOIN::LOAD_SRIM::sku_nbr) LEFT OUTER, LOAD_SMITH_ITEM_EFFECTIVE BY (sears_division_nbr, TrimLeadingZeros(sears_item_nbr), sears_sku_nbr);
--------------------------------------------------------------------------------------------------------------------------

LOAD_VEND_PACK_EFFECTIVE = FILTER LOAD_VEND_PACK BY SUBSTRING(expiration_ts,0,10) > '$CURRENT_DATE' ;

LOAD_VEND_PACK_EFFECTIVE_FILTER =  FILTER LOAD_VEND_PACK_EFFECTIVE BY (owner_cd == 'S'and (purchase_status_cd == 'A' or purchase_status_cd == 'W') and import_cd == 'I');
------------------------------------------------------------------------------------------------------------------------------

LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE  = FOREACH LOAD_VEND_PACK_EFFECTIVE_FILTER GENERATE ksn_id,import_cd;
---------------------------------------------------------------------------------------------------------------------------

LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE_DISTINCT = DISTINCT LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE;

LOC_ITEM_COL_JOIN_KSN_VEND = JOIN LOC_ITEM_COL_JOIN_KSN BY (LOAD_SMITH_ITEM_EFFECTIVE::ksn_id) LEFT OUTER, LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE_DISTINCT BY (ksn_id);
--------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------
--Columns Projection

TRANSFORMATION_FINAL_SRIM = FOREACH LOC_ITEM_COL_JOIN_KSN_VEND GENERATE

LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::ELIGIBLE_ITEM_FILTER::item as item,
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOC_COL_FILTER::loc as loc,
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOC_COL_FILTER::srs_loc as srs_loc, 
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOC_COL_FILTER::srs_vndr_nbr as srs_vndr_nbr,
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOC_COL_FILTER::loc_lvl_cd as loc_lvl_cd, 
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_SRIM::status_cd as status_cd,
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::ELIGIBLE_ITEM_FILTER::easy_ord_fl as easy_ord_fl,
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::ELIGIBLE_ITEM_FILTER::spc_ord_cdt_fl as spc_ord_cdt_fl,
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::ELIGIBLE_ITEM_FILTER::itm_emp_fl as itm_emp_fl,
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOC_COL_FILTER::loc_fmt_typ_cd as loc_fmt_typ_cd,
(LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_SRIM::store_pack_size_qty is NULL ? NULL: LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_SRIM::store_pack_size_qty) as src_pack_qty,
LOC_ITEM_COL_JOIN_KSN::LOAD_SMITH_ITEM_EFFECTIVE::ksn_id as ksn_id,
LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE_DISTINCT::import_cd as import_cd,
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_SRIM::division_nbr as srs_division_nbr,
TrimLeadingZeros(LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_SRIM::item_nbr) as srs_item_nbr,
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_SRIM::sku_nbr as srs_sku_cd,
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_SRIM::warehouse_nbr as srs_location_nbr,
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_SRIM::source_nbr as srs_source_nbr,
(LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_SRIM::status_cd is NULL ? '': LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_SRIM::status_cd) as rim_sts_cd,
LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOC_COL_FILTER::fmt_typ_cd as fmt_typ_cd,
'U' as upsert_field;
--------------------------------------------------------------------------------------------------------------------------------
TRANSFORMATION_FINAL_SRIM_DISTINCT = FILTER TRANSFORMATION_FINAL_SRIM BY (item is NOT NULL OR item !='') AND (loc is NOT NULL OR loc !='') AND (int)src_pack_qty >= 0 AND (rim_sts_cd == 'C' OR rim_sts_cd == 'L' OR rim_sts_cd == 'E' OR rim_sts_cd == 'R' OR rim_sts_cd == 'P' OR rim_sts_cd == 'S' OR rim_sts_cd == 'D' OR rim_sts_cd == 'F' OR rim_sts_cd == 'T' OR rim_sts_cd == 'Q' OR rim_sts_cd == 'X' OR rim_sts_cd == 'Z');

TRANSFORMATION_FINAL_SRIM_1 = FOREACH  TRANSFORMATION_FINAL_SRIM_DISTINCT GENERATE
item,
loc,
((status_cd is  null)? '': status_cd) as status_cd,
((spc_ord_cdt_fl is null)? '': spc_ord_cdt_fl) as spc_ord_cdt_fl,
((itm_emp_fl is null)? '': itm_emp_fl) as itm_emp_fl,
((loc_fmt_typ_cd is null)? '': loc_fmt_typ_cd) as loc_fmt_typ_cd,
((easy_ord_fl is null)? '': easy_ord_fl) as easy_ord_fl,
((srs_vndr_nbr is null)? '': srs_vndr_nbr) as srs_vndr_nbr,
((srs_loc is null)? '': srs_loc) as srs_loc,
loc_lvl_cd,
(chararray)src_pack_qty,
ksn_id,
((import_cd is null)? '': import_cd) as import_cd,
srs_division_nbr,
srs_item_nbr,
srs_sku_cd,
srs_location_nbr,
srs_source_nbr,
rim_sts_cd;

-------------------------------------------------------------------------------------------------------------------------

TARGET_COLS_SRIM_1 = FOREACH TRANSFORMATION_FINAL_SRIM_1 GENERATE
item,
loc,
'S' as src_owner_cd,
(( item is not null AND loc is not null AND (status_cd == 'R' or status_cd == 'S' or status_cd == 'P' or  status_cd == 'L' or status_cd == 'Z' or  status_cd == 'D' or  status_cd == 'F') AND easy_ord_fl == '0' AND  spc_ord_cdt_fl == '0') ? (itm_emp_fl =='0' ? 'A':(itm_emp_fl == '1' AND loc_fmt_typ_cd == 'CDFC'?'A':'D')):'D') as elig_sts_cd,
src_pack_qty,
'' as src_loc,
'' as po_vnd_no,
ksn_id,
'' as purch_stat_cd,
'0' as retail_crtn_intrnl_pack_qty,
'' as days_to_check_begin_date,
'' as days_to_check_end_date,
'' as dotcom_order_indicator,
'' as vend_pack_id,
'' as vend_pack_purch_stat_cd,
'' as vendor_pack_flow_type,
'' as vendor_pack_qty,
'' as reorder_method_code,
'' as str_supplier_cd,
'' as vend_stk_nbr,
'' as ksn_pack_id,
'' as ksn_dc_pack_purch_stat_cd,
'' as inbnd_ord_uom_cd,
'' as enable_jif_dc_ind,
'' as stk_ind,
'' as crtn_per_layer_qty,
'' as layer_per_pall_qty,
' ' as dc_config_cd,
(import_cd !='' ? '1':'0') as imp_fl,
srs_division_nbr,
srs_item_nbr,
srs_sku_cd,
srs_location_nbr,
srs_source_nbr,
rim_sts_cd,
'' as non_stock_source_cd,
'' as item_active_ind,
'' as item_reserve_cd,
'' as item_next_period_on_hand_qty,
'' as item_reserve_qty,
'' as item_back_order_qty,
'' as item_next_period_future_order_qty,
'' as item_on_order_qty,
'' as item_next_period_in_transit_qty,
'' as item_last_receive_dt,
'' as item_last_ship_dt,
' ' as stk_typ_cd;

--------------------------------------------------------------------------------------------------------------------------------------
SPLIT LOAD_ELIGIBLE_LOC INTO TARGET_COLS_SRIM_SRS_LOC IF loc_lvl_cd == 'WAREHOUSE', TARGET_COLS_SRIM_NOT_SRS_LOC IF ( loc_lvl_cd == 'VENDOR' AND duns_type_cd == 'ORD') ;


TARGET_COLS_SRIM_JOIN_2 = JOIN TARGET_COLS_SRIM_1  BY TrimLeadingZeros(srs_source_nbr) LEFT OUTER, TARGET_COLS_SRIM_NOT_SRS_LOC BY TrimLeadingZeros(srs_vndr_nbr) USING 'skewed';

TARGET_COLS_SRIM_JOIN_2_OP = FOREACH TARGET_COLS_SRIM_JOIN_2 GENERATE

TARGET_COLS_SRIM_1::item as item,
TARGET_COLS_SRIM_1::loc as loc,
TARGET_COLS_SRIM_1::src_owner_cd,
TARGET_COLS_SRIM_1::elig_sts_cd,
TARGET_COLS_SRIM_1::src_pack_qty,
TARGET_COLS_SRIM_NOT_SRS_LOC::loc as src_loc,
TARGET_COLS_SRIM_NOT_SRS_LOC::loc as po_vnd_no,
TARGET_COLS_SRIM_1::ksn_id,
TARGET_COLS_SRIM_1::purch_stat_cd,
TARGET_COLS_SRIM_1::retail_crtn_intrnl_pack_qty,
TARGET_COLS_SRIM_1::days_to_check_begin_date,
TARGET_COLS_SRIM_1::days_to_check_end_date,
TARGET_COLS_SRIM_1::dotcom_order_indicator,
TARGET_COLS_SRIM_1::vend_pack_id,
TARGET_COLS_SRIM_1::vend_pack_purch_stat_cd,
TARGET_COLS_SRIM_1::vendor_pack_flow_type,
TARGET_COLS_SRIM_1::vendor_pack_qty,
TARGET_COLS_SRIM_1::reorder_method_code,
TARGET_COLS_SRIM_1::str_supplier_cd,
TARGET_COLS_SRIM_1::vend_stk_nbr,
TARGET_COLS_SRIM_1::ksn_pack_id,
TARGET_COLS_SRIM_1::ksn_dc_pack_purch_stat_cd,
TARGET_COLS_SRIM_1::inbnd_ord_uom_cd,
TARGET_COLS_SRIM_1::enable_jif_dc_ind,
TARGET_COLS_SRIM_1::stk_ind,
TARGET_COLS_SRIM_1::crtn_per_layer_qty,
TARGET_COLS_SRIM_1::layer_per_pall_qty,
TARGET_COLS_SRIM_1::dc_config_cd,
TARGET_COLS_SRIM_1::imp_fl,
TARGET_COLS_SRIM_1::srs_division_nbr,
TARGET_COLS_SRIM_1::srs_item_nbr,
TARGET_COLS_SRIM_1::srs_sku_cd,
TARGET_COLS_SRIM_1::srs_location_nbr,
TARGET_COLS_SRIM_1::srs_source_nbr as srs_source_nbr,
TARGET_COLS_SRIM_1::rim_sts_cd,
TARGET_COLS_SRIM_1::non_stock_source_cd,
TARGET_COLS_SRIM_1::item_active_ind,
TARGET_COLS_SRIM_1::item_reserve_cd,
TARGET_COLS_SRIM_1::item_next_period_on_hand_qty,
TARGET_COLS_SRIM_1::item_reserve_qty,
TARGET_COLS_SRIM_1::item_back_order_qty,
TARGET_COLS_SRIM_1::item_next_period_future_order_qty,
TARGET_COLS_SRIM_1::item_on_order_qty,
TARGET_COLS_SRIM_1::item_next_period_in_transit_qty,
TARGET_COLS_SRIM_1::item_last_receive_dt,
TARGET_COLS_SRIM_1::item_last_ship_dt,
TARGET_COLS_SRIM_1::stk_typ_cd;

---------------------------------------------------------------------------------------------------------------------

TARGET_COLS_SRIM_JOIN_1 = JOIN TARGET_COLS_SRIM_JOIN_2_OP BY TrimLeadingZeros(srs_source_nbr) LEFT OUTER, TARGET_COLS_SRIM_SRS_LOC BY TrimLeadingZeros(srs_loc) USING 'skewed';

TARGET_COLS_SRIM_JOIN_1_OP = FOREACH TARGET_COLS_SRIM_JOIN_1 GENERATE


TARGET_COLS_SRIM_JOIN_2_OP::item AS item,
TARGET_COLS_SRIM_JOIN_2_OP::loc AS loc,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::src_owner_cd AS src_owner_cd,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::elig_sts_cd AS elig_sts_cd,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::src_pack_qty AS src_pack_qty,
((TARGET_COLS_SRIM_JOIN_2_OP::src_loc is NULL OR TARGET_COLS_SRIM_JOIN_2_OP::src_loc =='') ? (TARGET_COLS_SRIM_SRS_LOC::srs_loc is NULL OR TARGET_COLS_SRIM_SRS_LOC::srs_loc ==''?'':TARGET_COLS_SRIM_SRS_LOC::loc ):TARGET_COLS_SRIM_JOIN_2_OP::src_loc) AS src_loc,
TARGET_COLS_SRIM_JOIN_2_OP::po_vnd_no AS po_vnd_no,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::ksn_id AS ksn_id,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::purch_stat_cd AS purch_stat_cd,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::retail_crtn_intrnl_pack_qty AS retail_crtn_intrnl_pack_qty,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::days_to_check_begin_date AS days_to_check_begin_date,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::days_to_check_end_date AS days_to_check_end_date,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::dotcom_order_indicator AS dotcom_order_indicator,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::vend_pack_id AS vend_pack_id,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::vend_pack_purch_stat_cd AS vend_pack_purch_stat_cd,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::vendor_pack_flow_type AS vendor_pack_flow_type,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::vendor_pack_qty AS vendor_pack_qty,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::reorder_method_code AS reorder_method_code,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::str_supplier_cd AS str_supplier_cd,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::vend_stk_nbr AS vend_stk_nbr,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::ksn_pack_id AS ksn_pack_id,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::ksn_dc_pack_purch_stat_cd AS ksn_dc_pack_purch_stat_cd,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::inbnd_ord_uom_cd AS inbnd_ord_uom_cd,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::enable_jif_dc_ind AS enable_jif_dc_ind,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::stk_ind AS stk_ind,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::crtn_per_layer_qty AS crtn_per_layer_qty,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::layer_per_pall_qty AS layer_per_pall_qty,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::dc_config_cd AS dc_config_cd,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::imp_fl AS imp_fl,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::srs_division_nbr AS srs_division_nbr,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::srs_item_nbr AS srs_item_nbr,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::srs_sku_cd AS srs_sku_cd,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::srs_location_nbr AS srs_location_nbr,
TARGET_COLS_SRIM_JOIN_2_OP::srs_source_nbr AS srs_source_nbr,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::rim_sts_cd AS rim_sts_cd,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::non_stock_source_cd AS non_stock_source_cd,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::item_active_ind AS item_active_ind,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::item_reserve_cd AS item_reserve_cd,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::item_reserve_qty AS item_reserve_qty,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::item_back_order_qty AS item_back_order_qty,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::item_next_period_future_order_qty AS item_next_period_future_order_qty,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::item_on_order_qty AS item_on_order_qty,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::item_last_receive_dt AS item_last_receive_dt,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::item_last_ship_dt AS item_last_ship_dt,
TARGET_COLS_SRIM_JOIN_2_OP::TARGET_COLS_SRIM_1::stk_typ_cd AS stk_typ_cd;

-----------------------------------------------------------------------------------------------------------

TARGET_COLS_SRIM_JOIN_1_OP1 = FOREACH TARGET_COLS_SRIM_JOIN_1_OP GENERATE 
'$CURRENT_TIMESTAMP'	as	load_ts	,
item
,loc
,src_owner_cd
,(src_loc =='' OR src_loc is NULL ? 'D':elig_sts_cd) AS elig_sts_cd
,src_pack_qty
,src_loc
,po_vnd_no
,ksn_id
,purch_stat_cd
,retail_crtn_intrnl_pack_qty
,days_to_check_begin_date
,days_to_check_end_date
,dotcom_order_indicator
,vend_pack_id
,vend_pack_purch_stat_cd
,vendor_pack_flow_type
,vendor_pack_qty
,reorder_method_code
,str_supplier_cd
,vend_stk_nbr
,ksn_pack_id
,ksn_dc_pack_purch_stat_cd
,inbnd_ord_uom_cd
,enable_jif_dc_ind
,stk_ind
,crtn_per_layer_qty
,layer_per_pall_qty
,dc_config_cd
,imp_fl
,srs_division_nbr
,srs_item_nbr
,srs_sku_cd
,srs_location_nbr
,srs_source_nbr
,rim_sts_cd
,non_stock_source_cd
,item_active_ind
,item_reserve_cd
,item_next_period_on_hand_qty
,item_reserve_qty
,item_back_order_qty
,item_next_period_future_order_qty
,item_on_order_qty
,item_next_period_in_transit_qty
,item_last_receive_dt
,item_last_ship_dt
,stk_typ_cd 
,'$batchid' AS batch_id  ;

TARGET_COLS_SRIM_DISTINCT = DISTINCT TARGET_COLS_SRIM_JOIN_1_OP1 ;

----------------------------------------------------------------------------------------------------------------------------------


STORE TARGET_COLS_SRIM_DISTINCT INTO '$WORK__IDRP_ELIGIBLE_ITEM_LOC_SEARS_SRIM_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
