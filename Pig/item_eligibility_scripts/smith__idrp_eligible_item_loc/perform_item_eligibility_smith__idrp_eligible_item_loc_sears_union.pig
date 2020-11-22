/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_loc_sears_union.pig
# AUTHOR NAME:         Mudit Mangal
# CREATION DATE:       Thu Jan 02 07:47:28 EST 2014
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

--LOAD SEARS RIM file
LOAD_SEARS_RIM = LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOC_SEARS_RIM_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);

--LOAD SEARS DOS file
LOAD_SEARS_DOS = LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOC_SEARS_DOS_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);

--LOAD SEARS SRIM file
LOAD_SEARS_SRIM = LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOC_SEARS_SRIM_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);

----------------------------------------------------------------------------------------------------------------------------
SEARS_UNION = UNION LOAD_SEARS_RIM, LOAD_SEARS_DOS, LOAD_SEARS_SRIM;

LOAD_ELIGIBLE_ITEM_LOC = FOREACH SEARS_UNION GENERATE

item,
loc,
src_owner_cd,
elig_sts_cd,
src_pack_qty,
(src_loc is NULL ? '': src_loc )AS src_loc,
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
srs_location_nbr,
srs_source_nbr,
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
-------------------------------------------------------------------------------------------------------------------------------

/*
FINAL_JOIN_COLS_DISTINCT = DISTINCT LOAD_ELIGIBLE_ITEM_LOC ;

TARGET_COLS_DISTINCT_3_GRP_FINAL = GROUP FINAL_JOIN_COLS_DISTINCT BY (item,loc);

TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL = FOREACH TARGET_COLS_DISTINCT_3_GRP_FINAL
                                                               { ord_data_1 = ORDER FINAL_JOIN_COLS_DISTINCT BY elig_sts_cd ASC;
                                                                ord_data_lmt_1 = LIMIT ord_data_1 1;
                                                                GENERATE FLATTEN(ord_data_lmt_1);
                                                               };
-------------------------------------------------------------------------------------------------------------------------------
*/


SPLIT LOAD_ELIGIBLE_ITEM_LOC INTO PO_PRESENT IF (po_vnd_no is not null AND po_vnd_no !='') , PO_ABSENT IF (po_vnd_no is null OR po_vnd_no =='');


PO_VEND_VALUE_JOIN = JOIN PO_ABSENT by (item,TrimLeadingZeros(src_loc)) LEFT OUTER, PO_PRESENT BY (item, TrimLeadingZeros(loc));

TARGET_COLS = FOREACH PO_VEND_VALUE_JOIN GENERATE
PO_ABSENT::item as item,
PO_ABSENT::loc as loc,
PO_ABSENT::src_owner_cd as src_owner_cd,
PO_ABSENT::elig_sts_cd as elig_sts_cd,
PO_ABSENT::src_pack_qty as src_pack_qty,
PO_ABSENT::src_loc as src_loc,
((PO_ABSENT::po_vnd_no is NULL OR PO_ABSENT::po_vnd_no =='')? PO_PRESENT::po_vnd_no:PO_ABSENT::po_vnd_no)  as po_vnd_no,
PO_ABSENT::ksn_id as ksn_id,
PO_ABSENT::purch_stat_cd as purch_stat_cd,
PO_ABSENT::retail_crtn_intrnl_pack_qty as retail_crtn_intrnl_pack_qty,
PO_ABSENT::days_to_check_begin_date as days_to_check_begin_date,
PO_ABSENT::days_to_check_end_date as days_to_check_end_date,
PO_ABSENT::dotcom_order_indicator as dotcom_order_indicator,
PO_ABSENT::vend_pack_id as vend_pack_id,
PO_ABSENT::vend_pack_purch_stat_cd as vend_pack_purch_stat_cd,
PO_ABSENT::vendor_pack_flow_type as vendor_pack_flow_type,
PO_ABSENT::vendor_pack_qty as vendor_pack_qty,
PO_ABSENT::reorder_method_code as reorder_method_code,
PO_ABSENT::str_supplier_cd as str_supplier_cd,
PO_ABSENT::vend_stk_nbr as vend_stk_nbr ,
PO_ABSENT::ksn_pack_id as ksn_pack_id ,
PO_ABSENT::ksn_dc_pack_purch_stat_cd as ksn_dc_pack_purch_stat_cd ,
PO_ABSENT::inbnd_ord_uom_cd as inbnd_ord_uom_cd,
PO_ABSENT::enable_jif_dc_ind as enable_jif_dc_ind ,
PO_ABSENT::stk_ind as stk_ind ,
PO_ABSENT::crtn_per_layer_qty as crtn_per_layer_qty ,
PO_ABSENT::layer_per_pall_qty as layer_per_pall_qty ,
PO_ABSENT::dc_config_cd as dc_config_cd ,
PO_ABSENT::imp_fl as imp_fl,
PO_ABSENT::srs_division_nbr as srs_division_nbr,
PO_ABSENT::srs_item_nbr as srs_item_nbr,
PO_ABSENT::srs_sku_cd as srs_sku_cd,
PO_ABSENT::srs_location_nbr as srs_location_nbr,
PO_ABSENT::srs_source_nbr as srs_source_nbr,
PO_ABSENT::rim_sts_cd as rim_sts_cd,
PO_ABSENT::non_stock_source_cd as non_stock_source_cd,
PO_ABSENT::item_active_ind as item_active_ind ,
PO_ABSENT::item_reserve_cd as item_reserve_cd,
PO_ABSENT::item_next_period_on_hand_qty as item_next_period_on_hand_qty ,
PO_ABSENT::item_reserve_qty as item_reserve_qty ,
PO_ABSENT::item_back_order_qty as item_back_order_qty ,
PO_ABSENT::item_next_period_future_order_qty as item_next_period_future_order_qty ,
PO_ABSENT::item_on_order_qty as item_on_order_qty ,
PO_ABSENT::item_next_period_in_transit_qty as item_next_period_in_transit_qty ,
PO_ABSENT::item_last_receive_dt as item_last_receive_dt,
PO_ABSENT::item_last_ship_dt as item_last_ship_dt,
PO_ABSENT::stk_typ_cd as stk_typ_cd;

----------------------------------------------------------------------------------------------------------------------------
TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION = UNION PO_PRESENT, TARGET_COLS;

SPLIT TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION INTO PO_PRESENT_1 IF (po_vnd_no is not null AND po_vnd_no !='') , PO_ABSENT_1 IF (po_vnd_no is null OR po_vnd_no =='');


PO_VEND_VALUE_JOIN_1 = JOIN PO_ABSENT_1 by (item,TrimLeadingZeros(src_loc)) LEFT OUTER, PO_PRESENT_1 BY (item, TrimLeadingZeros(loc));

TARGET_COLS_1 = FOREACH PO_VEND_VALUE_JOIN_1 GENERATE
PO_ABSENT_1::item as item,
PO_ABSENT_1::loc as loc,
PO_ABSENT_1::src_owner_cd as src_owner_cd,
PO_ABSENT_1::elig_sts_cd as elig_sts_cd,
PO_ABSENT_1::src_pack_qty as src_pack_qty,
PO_ABSENT_1::src_loc as src_loc,
((PO_ABSENT_1::po_vnd_no is NULL OR PO_ABSENT_1::po_vnd_no =='')? PO_PRESENT_1::po_vnd_no:PO_ABSENT_1::po_vnd_no)  as po_vnd_no,
PO_ABSENT_1::ksn_id as ksn_id,
PO_ABSENT_1::purch_stat_cd as purch_stat_cd,
PO_ABSENT_1::retail_crtn_intrnl_pack_qty as retail_crtn_intrnl_pack_qty,
PO_ABSENT_1::days_to_check_begin_date as days_to_check_begin_date,
PO_ABSENT_1::days_to_check_end_date as days_to_check_end_date,
PO_ABSENT_1::dotcom_order_indicator as dotcom_order_indicator,
PO_ABSENT_1::vend_pack_id as vend_pack_id,
PO_ABSENT_1::vend_pack_purch_stat_cd as vend_pack_purch_stat_cd,
PO_ABSENT_1::vendor_pack_flow_type as vendor_pack_flow_type,
PO_ABSENT_1::vendor_pack_qty as vendor_pack_qty,
PO_ABSENT_1::reorder_method_code as reorder_method_code,
PO_ABSENT_1::str_supplier_cd as str_supplier_cd,
PO_ABSENT_1::vend_stk_nbr as vend_stk_nbr ,
PO_ABSENT_1::ksn_pack_id as ksn_pack_id ,
PO_ABSENT_1::ksn_dc_pack_purch_stat_cd as ksn_dc_pack_purch_stat_cd ,
PO_ABSENT_1::inbnd_ord_uom_cd as inbnd_ord_uom_cd,
PO_ABSENT_1::enable_jif_dc_ind as enable_jif_dc_ind ,
PO_ABSENT_1::stk_ind as stk_ind ,
PO_ABSENT_1::crtn_per_layer_qty as crtn_per_layer_qty ,
PO_ABSENT_1::layer_per_pall_qty as layer_per_pall_qty ,
PO_ABSENT_1::dc_config_cd as dc_config_cd ,
PO_ABSENT_1::imp_fl as imp_fl,
PO_ABSENT_1::srs_division_nbr as srs_division_nbr,
PO_ABSENT_1::srs_item_nbr as srs_item_nbr,
PO_ABSENT_1::srs_sku_cd as srs_sku_cd,
PO_ABSENT_1::srs_location_nbr as srs_location_nbr,
PO_ABSENT_1::srs_source_nbr as srs_source_nbr,
PO_ABSENT_1::rim_sts_cd as rim_sts_cd,
PO_ABSENT_1::non_stock_source_cd as non_stock_source_cd,
PO_ABSENT_1::item_active_ind as item_active_ind ,
PO_ABSENT_1::item_reserve_cd as item_reserve_cd,
PO_ABSENT_1::item_next_period_on_hand_qty as item_next_period_on_hand_qty ,
PO_ABSENT_1::item_reserve_qty as item_reserve_qty ,
PO_ABSENT_1::item_back_order_qty as item_back_order_qty ,
PO_ABSENT_1::item_next_period_future_order_qty as item_next_period_future_order_qty ,
PO_ABSENT_1::item_on_order_qty as item_on_order_qty ,
PO_ABSENT_1::item_next_period_in_transit_qty as item_next_period_in_transit_qty ,
PO_ABSENT_1::item_last_receive_dt as item_last_receive_dt,
PO_ABSENT_1::item_last_ship_dt as item_last_ship_dt,
PO_ABSENT_1::stk_typ_cd as stk_typ_cd;

TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1 = UNION PO_PRESENT_1, TARGET_COLS_1;

-----------------------------------------------------------------------------------------------------------------------------------


SPLIT TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1 INTO PO_PRESENT_11 IF (po_vnd_no is not null AND po_vnd_no !='') , PO_ABSENT_11 IF (po_vnd_no is null OR po_vnd_no =='');


PO_VEND_VALUE_JOIN_11 = JOIN PO_ABSENT_11 by (item,TrimLeadingZeros(src_loc)) LEFT OUTER, PO_PRESENT_11 BY (item, TrimLeadingZeros(loc));

TARGET_COLS_11 = FOREACH PO_VEND_VALUE_JOIN_11 GENERATE
PO_ABSENT_11::item as item,
PO_ABSENT_11::loc as loc,
PO_ABSENT_11::src_owner_cd as src_owner_cd,
PO_ABSENT_11::elig_sts_cd as elig_sts_cd,
PO_ABSENT_11::src_pack_qty as src_pack_qty,
PO_ABSENT_11::src_loc as src_loc,
((PO_ABSENT_11::po_vnd_no is NULL OR PO_ABSENT_11::po_vnd_no =='')? PO_PRESENT_11::po_vnd_no:PO_ABSENT_11::po_vnd_no)  as po_vnd_no,
PO_ABSENT_11::ksn_id as ksn_id,
PO_ABSENT_11::purch_stat_cd as purch_stat_cd,
PO_ABSENT_11::retail_crtn_intrnl_pack_qty as retail_crtn_intrnl_pack_qty,
PO_ABSENT_11::days_to_check_begin_date as days_to_check_begin_date,
PO_ABSENT_11::days_to_check_end_date as days_to_check_end_date,
PO_ABSENT_11::dotcom_order_indicator as dotcom_order_indicator,
PO_ABSENT_11::vend_pack_id as vend_pack_id,
PO_ABSENT_11::vend_pack_purch_stat_cd as vend_pack_purch_stat_cd,
PO_ABSENT_11::vendor_pack_flow_type as vendor_pack_flow_type,
PO_ABSENT_11::vendor_pack_qty as vendor_pack_qty,
PO_ABSENT_11::reorder_method_code as reorder_method_code,
PO_ABSENT_11::str_supplier_cd as str_supplier_cd,
PO_ABSENT_11::vend_stk_nbr as vend_stk_nbr ,
PO_ABSENT_11::ksn_pack_id as ksn_pack_id ,
PO_ABSENT_11::ksn_dc_pack_purch_stat_cd as ksn_dc_pack_purch_stat_cd ,
PO_ABSENT_11::inbnd_ord_uom_cd as inbnd_ord_uom_cd,
PO_ABSENT_11::enable_jif_dc_ind as enable_jif_dc_ind ,
PO_ABSENT_11::stk_ind as stk_ind ,
PO_ABSENT_11::crtn_per_layer_qty as crtn_per_layer_qty ,
PO_ABSENT_11::layer_per_pall_qty as layer_per_pall_qty ,
PO_ABSENT_11::dc_config_cd as dc_config_cd ,
PO_ABSENT_11::imp_fl as imp_fl,
PO_ABSENT_11::srs_division_nbr as srs_division_nbr,
PO_ABSENT_11::srs_item_nbr as srs_item_nbr,
PO_ABSENT_11::srs_sku_cd as srs_sku_cd,
PO_ABSENT_11::srs_location_nbr as srs_location_nbr,
PO_ABSENT_11::srs_source_nbr as srs_source_nbr,
PO_ABSENT_11::rim_sts_cd as rim_sts_cd,
PO_ABSENT_11::non_stock_source_cd as non_stock_source_cd,
PO_ABSENT_11::item_active_ind as item_active_ind ,
PO_ABSENT_11::item_reserve_cd as item_reserve_cd,
PO_ABSENT_11::item_next_period_on_hand_qty as item_next_period_on_hand_qty ,
PO_ABSENT_11::item_reserve_qty as item_reserve_qty ,
PO_ABSENT_11::item_back_order_qty as item_back_order_qty ,
PO_ABSENT_11::item_next_period_future_order_qty as item_next_period_future_order_qty ,
PO_ABSENT_11::item_on_order_qty as item_on_order_qty ,
PO_ABSENT_11::item_next_period_in_transit_qty as item_next_period_in_transit_qty ,
PO_ABSENT_11::item_last_receive_dt as item_last_receive_dt,
PO_ABSENT_11::item_last_ship_dt as item_last_ship_dt,
PO_ABSENT_11::stk_typ_cd as stk_typ_cd;


TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_11 = UNION PO_PRESENT_11, TARGET_COLS_11;


SPLIT TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_11 INTO PO_PRESENT_111 IF (po_vnd_no is not null AND po_vnd_no !='') , PO_ABSENT_111 IF (po_vnd_no is null OR po_vnd_no =='');


PO_VEND_VALUE_JOIN_111 = JOIN PO_ABSENT_111 by (item,TrimLeadingZeros(src_loc)) LEFT OUTER, PO_PRESENT_111 BY (item, TrimLeadingZeros(loc));

TARGET_COLS_111 = FOREACH PO_VEND_VALUE_JOIN_111 GENERATE
PO_ABSENT_111::item as item,
PO_ABSENT_111::loc as loc,
PO_ABSENT_111::src_owner_cd as src_owner_cd,
PO_ABSENT_111::elig_sts_cd as elig_sts_cd,
PO_ABSENT_111::src_pack_qty as src_pack_qty,
PO_ABSENT_111::src_loc as src_loc,
((PO_ABSENT_111::po_vnd_no is NULL OR PO_ABSENT_111::po_vnd_no =='')? PO_PRESENT_111::po_vnd_no:PO_ABSENT_111::po_vnd_no)  as po_vnd_no,
PO_ABSENT_111::ksn_id as ksn_id,
PO_ABSENT_111::purch_stat_cd as purch_stat_cd,
PO_ABSENT_111::retail_crtn_intrnl_pack_qty as retail_crtn_intrnl_pack_qty,
PO_ABSENT_111::days_to_check_begin_date as days_to_check_begin_date,
PO_ABSENT_111::days_to_check_end_date as days_to_check_end_date,
PO_ABSENT_111::dotcom_order_indicator as dotcom_order_indicator,
PO_ABSENT_111::vend_pack_id as vend_pack_id,
PO_ABSENT_111::vend_pack_purch_stat_cd as vend_pack_purch_stat_cd,
PO_ABSENT_111::vendor_pack_flow_type as vendor_pack_flow_type,
PO_ABSENT_111::vendor_pack_qty as vendor_pack_qty,
PO_ABSENT_111::reorder_method_code as reorder_method_code,
PO_ABSENT_111::str_supplier_cd as str_supplier_cd,
PO_ABSENT_111::vend_stk_nbr as vend_stk_nbr ,
PO_ABSENT_111::ksn_pack_id as ksn_pack_id ,
PO_ABSENT_111::ksn_dc_pack_purch_stat_cd as ksn_dc_pack_purch_stat_cd ,
PO_ABSENT_111::inbnd_ord_uom_cd as inbnd_ord_uom_cd,
PO_ABSENT_111::enable_jif_dc_ind as enable_jif_dc_ind ,
PO_ABSENT_111::stk_ind as stk_ind ,
PO_ABSENT_111::crtn_per_layer_qty as crtn_per_layer_qty ,
PO_ABSENT_111::layer_per_pall_qty as layer_per_pall_qty ,
PO_ABSENT_111::dc_config_cd as dc_config_cd ,
PO_ABSENT_111::imp_fl as imp_fl,
PO_ABSENT_111::srs_division_nbr as srs_division_nbr,
PO_ABSENT_111::srs_item_nbr as srs_item_nbr,
PO_ABSENT_111::srs_sku_cd as srs_sku_cd,
PO_ABSENT_111::srs_location_nbr as srs_location_nbr,
PO_ABSENT_111::srs_source_nbr as srs_source_nbr,
PO_ABSENT_111::rim_sts_cd as rim_sts_cd,
PO_ABSENT_111::non_stock_source_cd as non_stock_source_cd,
PO_ABSENT_111::item_active_ind as item_active_ind ,
PO_ABSENT_111::item_reserve_cd as item_reserve_cd,
PO_ABSENT_111::item_next_period_on_hand_qty as item_next_period_on_hand_qty ,
PO_ABSENT_111::item_reserve_qty as item_reserve_qty ,
PO_ABSENT_111::item_back_order_qty as item_back_order_qty ,
PO_ABSENT_111::item_next_period_future_order_qty as item_next_period_future_order_qty ,
PO_ABSENT_111::item_on_order_qty as item_on_order_qty ,
PO_ABSENT_111::item_next_period_in_transit_qty as item_next_period_in_transit_qty ,
PO_ABSENT_111::item_last_receive_dt as item_last_receive_dt,
PO_ABSENT_111::item_last_ship_dt as item_last_ship_dt,
PO_ABSENT_111::stk_typ_cd as stk_typ_cd;


TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_111 = UNION PO_PRESENT_111, TARGET_COLS_111;

TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111 = FOREACH TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_111 GENERATE

'$CURRENT_TIMESTAMP'	as	load_ts	,
item
,loc
,src_owner_cd
,elig_sts_cd
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
,'$batchid'	as	batch_id;


LOAD_ELIGIBLE_ITEM_LOC_DISTINCT = DISTINCT TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL_UNION_1111;

---------------------------------------------------------------------------------------------------------------------------------------

STORE LOAD_ELIGIBLE_ITEM_LOC_DISTINCT INTO '$WORK__IDRP_ELIGIBLE_ITEM_LOC_SEARS_UNION_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
