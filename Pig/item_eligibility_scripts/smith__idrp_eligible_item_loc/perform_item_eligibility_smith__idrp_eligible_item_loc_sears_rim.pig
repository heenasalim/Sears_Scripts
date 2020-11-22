/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_loc_sears_rim.pig
# AUTHOR NAME:         Mudit Mangal
# CREATION DATE:       Fri Dec 27 02:30:53 EST 2013
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
--LOAD RIM file
LOAD_RIM = LOAD '$GOLD__INVENTORY_RIM_DAILY_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__INVENTORY_RIM_DAILY_CURRENT_SCHEMA);

LOAD_RIM = FOREACH LOAD_RIM GENERATE division_nbr,item_nbr,sku_nbr,status_cd,store_pack_size_qty,source_nbr,store_nbr;

--LOAD DOS Facility file
LOAD_DOS_FACILITY = LOAD '$GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT_SCHEMA);

LOAD_DOS_FACILITY = FOREACH LOAD_DOS_FACILITY GENERATE dos_warehouse_nbr,sears_division_nbr,sears_item_nbr,sears_sku_nbr,non_stock_source_cd,vendor_nbr;

--LOAD SMITH Item file
LOAD_SMITH_ITEM = LOAD '$SMITH__ITEM_COMBINED_HIERARCHY_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__ITEM_COMBINED_HIERARCHY_CURRENT_SCHEMA);

LOAD_SMITH_ITEM = FOREACH LOAD_SMITH_ITEM GENERATE ksn_id,sears_sku_nbr,sears_item_nbr,sears_division_nbr,shc_item_id_expiration_ts;

--LOAD VENDOR Package file
LOAD_VEND_PACK = LOAD '$GOLD__ITEM_VENDOR_PACKAGE_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_VENDOR_PACKAGE_CURRENT_SCHEMA);

LOAD_VEND_PACK = FOREACH LOAD_VEND_PACK GENERATE expiration_ts,owner_cd,purchase_status_cd,import_cd,ksn_id;

--LOAD ELIGIBLE Item file
LOAD_ELIGIBLE_ITEM = LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_ITEM_SCHEMA);

LOAD_ELIGIBLE_ITEM = FOREACH LOAD_ELIGIBLE_ITEM GENERATE elig_sts_cd,srs_div_no,srs_itm_no,srs_sku_no,item,easy_ord_fl,whse_sizing,cross_mdse_attr_cd;

--LOAD ELIGIBLE Loc file
LOAD_ELIGIBLE_LOC = LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);

LOAD_ELIGIBLE_LOC = FOREACH LOAD_ELIGIBLE_LOC GENERATE srs_loc,loc,srs_vndr_nbr,loc_lvl_cd,loc_fmt_typ_cd,fmt_typ_cd,fmt_sub_typ_cd,duns_type_cd;

--LOAD DUMMY Vendor file
LOAD_DUMMY_VEND_WHSE = LOAD '$WORK__IDRP_DUMMY_VEND_WHSE_REF_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($WORK__IDRP_DUMMY_VEND_WHSE_REF_SCHEMA);
-------------------------------------------------------------------------------------------------------------------

ELIGIBLE_ITEM_FILTER = FILTER LOAD_ELIGIBLE_ITEM BY elig_sts_cd == 'A' OR elig_sts_cd == 'D';

ITEM_COL_JOIN= JOIN  LOAD_RIM BY (division_nbr,TrimLeadingZeros(item_nbr) ,sku_nbr), ELIGIBLE_ITEM_FILTER BY (srs_div_no,TrimLeadingZeros(srs_itm_no),srs_sku_no) USING 'skewed';
----------------------------------------------------------------------------------------------------------------

LOC_ITEM_COL_JOIN = JOIN ITEM_COL_JOIN BY TrimLeadingZeros(LOAD_RIM::store_nbr) , LOAD_ELIGIBLE_LOC BY TrimLeadingZeros(srs_loc) USING 'skewed';
---------------------------------------------------------------------------------------------------------------------------

LOAD_SMITH_ITEM_EFFECTIVE = FILTER LOAD_SMITH_ITEM BY  SUBSTRING(shc_item_id_expiration_ts,0,10) > '$CURRENT_DATE' ;
-----------------------------------------------------------------------------------------------------------------------------

LOC_ITEM_COL_JOIN_KSN = JOIN LOC_ITEM_COL_JOIN BY (ITEM_COL_JOIN::LOAD_RIM::division_nbr,TrimLeadingZeros(ITEM_COL_JOIN::LOAD_RIM::item_nbr),ITEM_COL_JOIN::LOAD_RIM::sku_nbr) LEFT OUTER ,LOAD_SMITH_ITEM_EFFECTIVE BY (sears_division_nbr, TrimLeadingZeros(sears_item_nbr), sears_sku_nbr);
-----------------------------------------------------------------------------------------------------------------------------

LOAD_VEND_PACK_EFFECTIVE = FILTER LOAD_VEND_PACK BY SUBSTRING(expiration_ts,0,10) > '$CURRENT_DATE' ;

LOAD_VEND_PACK_EFFECTIVE_FILTER =  FILTER LOAD_VEND_PACK_EFFECTIVE BY (owner_cd == 'S'and (purchase_status_cd == 'A' or purchase_status_cd == 'W') and import_cd == 'I');
------------------------------------------------------------------------------------------------------------------------------

LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE  = FOREACH LOAD_VEND_PACK_EFFECTIVE_FILTER GENERATE ksn_id, import_cd;
---------------------------------------------------------------------------------------------------------------------------

LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE_DISTINCT = DISTINCT LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE;
---------------------------------------------------------------------------------------------------------------------------------

LOC_ITEM_COL_JOIN_KSN_VEND = JOIN LOC_ITEM_COL_JOIN_KSN BY (LOAD_SMITH_ITEM_EFFECTIVE::ksn_id) LEFT OUTER, LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE_DISTINCT BY (ksn_id);
-----------------------------------------------------------------------------------------------------------------------------

LOAD_DOS_FACILITY_FILTER = FILTER LOAD_DOS_FACILITY BY non_stock_source_cd == 'STK';

LOC_ITEM_COL_JOIN_KSN_VEND_DOS = JOIN LOC_ITEM_COL_JOIN_KSN_VEND BY (LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::division_nbr,TrimLeadingZeros(LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::item_nbr),LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::sku_nbr, TrimLeadingZeros(LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::source_nbr)) LEFT OUTER , LOAD_DOS_FACILITY_FILTER BY (sears_division_nbr,TrimLeadingZeros(sears_item_nbr),sears_sku_nbr,TrimLeadingZeros(dos_warehouse_nbr)) USING 'replicated';
-------------------------------------------------------------------------------------------------------------------------

LOC_ITEM_COL_JOIN_KSN_VEND_DOS_DUMMY = JOIN LOC_ITEM_COL_JOIN_KSN_VEND_DOS BY TrimLeadingZeros(LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::source_nbr) LEFT OUTER, LOAD_DUMMY_VEND_WHSE BY TrimLeadingZeros(vendor_nbr) USING 'replicated';
-----------------------------------------------------------------------------------------------------------------------

--Columns Projection

TRANSFORMATION_FINAL_RIM = FOREACH LOC_ITEM_COL_JOIN_KSN_VEND_DOS_DUMMY GENERATE

LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::ELIGIBLE_ITEM_FILTER::item as item,
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::loc as loc,
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::srs_loc as srs_loc, 
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::srs_vndr_nbr as srs_vndr_nbr,
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::loc_lvl_cd as  loc_lvl_cd, 
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::loc_fmt_typ_cd as loc_fmt_typ_cd,
(LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::fmt_typ_cd IS NULL ? '': LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::fmt_typ_cd )as fmt_typ_cd,
(LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::fmt_sub_typ_cd IS NULL ? '': LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::fmt_sub_typ_cd ) as fmt_sub_typ_cd,
(LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::status_cd IS NULL ? '': LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::status_cd) as status_cd,
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::ELIGIBLE_ITEM_FILTER::easy_ord_fl as easy_ord_fl,
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::ELIGIBLE_ITEM_FILTER::whse_sizing as whse_sizing,
(LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::ELIGIBLE_ITEM_FILTER::cross_mdse_attr_cd IS NULL ? '': LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::ELIGIBLE_ITEM_FILTER::cross_mdse_attr_cd )as cross_mdse_attr_cd,
(LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::store_pack_size_qty IS NULL ? NULL: LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::store_pack_size_qty ) as store_pack_size_qty,
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOAD_SMITH_ITEM_EFFECTIVE::ksn_id as ksn_id,
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE_DISTINCT::import_cd as import_cd,
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::division_nbr as srs_division_nbr,
TrimLeadingZeros(LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::item_nbr) as srs_item_nbr,
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::sku_nbr as srs_sku_cd,
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::store_nbr as store_nbr,
LOAD_DUMMY_VEND_WHSE::warehouse_nbr as warehouse_nbr,
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOAD_DOS_FACILITY_FILTER::vendor_nbr as vendor_nbr,
LOC_ITEM_COL_JOIN_KSN_VEND_DOS::LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::LOAD_RIM::source_nbr as source_nbr,
'U' as upsert_field;
---------------------------------------------------------------------------------------------------------------------


TRANSFORMATION_FINAL_RIM_FILTER = FILTER TRANSFORMATION_FINAL_RIM BY 

((((cross_mdse_attr_cd IS NULL OR cross_mdse_attr_cd =='')  AND fmt_typ_cd == '001') OR (cross_mdse_attr_cd == 'SK1400' AND fmt_typ_cd == '001') OR (cross_mdse_attr_cd == 'SK3000' AND (fmt_typ_cd == '001' OR fmt_typ_cd == '002')) OR (cross_mdse_attr_cd == 'KM1000' AND fmt_typ_cd == '001' AND fmt_sub_typ_cd != 'G1' AND fmt_sub_typ_cd != 'G2') OR (cross_mdse_attr_cd == 'KM4001' AND fmt_typ_cd == '001' AND fmt_sub_typ_cd != 'G1' AND fmt_sub_typ_cd != 'G2') OR (cross_mdse_attr_cd == 'KM4005' AND fmt_typ_cd == '001' AND fmt_sub_typ_cd != 'G1' AND fmt_sub_typ_cd != 'G2') OR (cross_mdse_attr_cd == 'KM4009' AND fmt_typ_cd == '001' AND fmt_sub_typ_cd != 'G1' AND fmt_sub_typ_cd != 'G2') OR (cross_mdse_attr_cd == 'KM5000' AND fmt_typ_cd == '001' AND fmt_sub_typ_cd != 'G1' AND fmt_sub_typ_cd != 'G2') OR (cross_mdse_attr_cd == 'KM9000' AND fmt_typ_cd == '001' AND fmt_sub_typ_cd != 'G1' AND fmt_sub_typ_cd != 'G2'))  AND (item is NOT NULL OR item !='') AND (loc is NOT NULL OR loc !='') AND ((int)store_pack_size_qty >= 0) AND (status_cd == 'C' OR status_cd == 'L' OR status_cd == 'E' OR status_cd == 'R' OR status_cd == 'P' OR status_cd == 'S' OR status_cd == 'D' OR status_cd == 'F' OR status_cd == 'T' OR status_cd == 'Q' OR status_cd == 'X' OR status_cd == 'Z' ));

---------------------------------------------------------------------------------------------------------------------------------

TARGET_COLS_DISTINCT_3_GRP_FINAL = GROUP TRANSFORMATION_FINAL_RIM_FILTER BY (item,srs_loc);

TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL = FOREACH TARGET_COLS_DISTINCT_3_GRP_FINAL
                                           { ord_data_1 = ORDER TRANSFORMATION_FINAL_RIM_FILTER BY item,fmt_typ_cd,srs_loc ASC;
                                                         ord_data_lmt_1 = LIMIT ord_data_1 1;
                                                         GENERATE FLATTEN(ord_data_lmt_1);
                                           };


----------------------------------------------------------------------------------------------------------------------------------

TRANSFORMATION_FINAL_RIM_1 = FOREACH  TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL GENERATE
item,
loc,
((easy_ord_fl is  null)? '': easy_ord_fl) as easy_ord_fl,
((loc_fmt_typ_cd is null)? '': loc_fmt_typ_cd) as loc_fmt_typ_cd,
((fmt_typ_cd is null)? '': fmt_typ_cd) as fmt_typ_cd,
((fmt_sub_typ_cd is null)? '': fmt_sub_typ_cd) as fmt_sub_typ_cd,
((status_cd is null)? '': status_cd) as status_cd,
((whse_sizing is null)? '': whse_sizing) as whse_sizing,
((cross_mdse_attr_cd is null)? '': cross_mdse_attr_cd) as cross_mdse_attr_cd,
(((chararray)store_pack_size_qty is null)? '': (chararray)store_pack_size_qty) as store_pack_size_qty,
((srs_vndr_nbr is null)? '': srs_vndr_nbr) as srs_vndr_nbr,
((srs_loc is null)? '': srs_loc) as srs_loc,
loc_lvl_cd,
ksn_id,
((import_cd is null)? '': import_cd) as import_cd,
srs_division_nbr,
srs_item_nbr,
srs_sku_cd,
store_nbr,
((warehouse_nbr is null ) ? ((vendor_nbr is null)? source_nbr : vendor_nbr): warehouse_nbr) as srs_source_nbr,
vendor_nbr,
source_nbr;

-----------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
TARGET_COLS_RIM = FOREACH TRANSFORMATION_FINAL_RIM_1 GENERATE
item,
loc,
'S' as src_owner_cd,
((item is not null AND loc is not null AND loc_fmt_typ_cd == 'SINT' AND (status_cd == 'R' or  status_cd == 'S' or  status_cd == 'C' or status_cd == 'P' or status_cd == 'L' or status_cd == 'Z' or  status_cd == 'E' or status_cd == 'X') AND easy_ord_fl == '0' AND (whse_sizing == 'WG8800' or whse_sizing == 'WG8801' or whse_sizing == 'WG8804'))? 'A': (item is not null AND loc is not null AND loc_fmt_typ_cd != 'SINT' AND (status_cd == 'R' or  status_cd == 'S' or  status_cd == 'C' or status_cd == 'P' or status_cd == 'L' or status_cd == 'Z' or  status_cd == 'E') AND easy_ord_fl == '0'? 'A':'D')) as elig_sts_cd,

(((loc_fmt_typ_cd != 'SINT' AND loc_fmt_typ_cd != 'TCT') OR TrimLeadingZeros(store_nbr) == '9300')? store_pack_size_qty:'1') as src_pack_qty,
'' as src_loc,
'' as po_vnd_no,
ksn_id,
'' as purch_stat_cd ,
'0' as retail_crtn_intrnl_pack_qty ,
'' as days_to_check_begin_date ,
'' as days_to_check_end_date ,
'' as dotcom_order_indicator ,
'' as vend_pack_id,
'' as vend_pack_purch_stat_cd ,
'' as vendor_pack_flow_type ,
'' as vendor_pack_qty ,
'' as reorder_method_code ,
'' as str_supplier_cd ,
'' as vend_stk_nbr ,
'' as ksn_pack_id ,
'' as ksn_dc_pack_purch_stat_cd ,
'' as inbnd_ord_uom_cd,
'' as enable_jif_dc_ind ,
'' as stk_ind ,
'' as crtn_per_layer_qty ,
'' as layer_per_pall_qty ,
' ' as dc_config_cd ,
(import_cd !='' ? '1':'0') as imp_fl,
srs_division_nbr,
srs_item_nbr,
srs_sku_cd,
store_nbr as srs_location_nbr,
srs_source_nbr,
status_cd as rim_sts_cd,
'' as non_stock_source_cd,
'' as item_active_ind ,
'' as item_reserve_cd,
'' as item_next_period_on_hand_qty ,
'' as item_reserve_qty ,
'' as item_back_order_qty ,
'' as item_next_period_future_order_qty ,
'' as item_on_order_qty ,
'' as item_next_period_in_transit_qty ,
'' as item_last_receive_dt ,
'' as item_last_ship_dt ,
' ' as stk_typ_cd ;

----------------------------------------------------------------------------------------------------------------------------
SPLIT LOAD_ELIGIBLE_LOC INTO TARGET_COLS_RIM_SRS_LOC IF loc_lvl_cd == 'WAREHOUSE', TARGET_COLS_RIM_NOT_SRS_LOC IF ( loc_lvl_cd == 'VENDOR' AND duns_type_cd == 'ORD') ;

TARGET_COLS_RIM_JOIN_2 = JOIN TARGET_COLS_RIM  BY TrimLeadingZeros(srs_source_nbr) LEFT OUTER, TARGET_COLS_RIM_NOT_SRS_LOC BY TrimLeadingZeros(srs_vndr_nbr) USING 'skewed';

TARGET_COLS_RIM_JOIN_2_OP = FOREACH TARGET_COLS_RIM_JOIN_2 GENERATE
TARGET_COLS_RIM::ord_data_lmt_1::item as item,
TARGET_COLS_RIM::ord_data_lmt_1::loc as loc,
TARGET_COLS_RIM::src_owner_cd,
TARGET_COLS_RIM::elig_sts_cd,
TARGET_COLS_RIM::src_pack_qty,
TARGET_COLS_RIM_NOT_SRS_LOC::loc as src_loc,
TARGET_COLS_RIM_NOT_SRS_LOC::loc as po_vnd_no,
TARGET_COLS_RIM::ord_data_lmt_1::ksn_id,
TARGET_COLS_RIM::purch_stat_cd,
TARGET_COLS_RIM::retail_crtn_intrnl_pack_qty,
TARGET_COLS_RIM::days_to_check_begin_date,
TARGET_COLS_RIM::days_to_check_end_date,
TARGET_COLS_RIM::dotcom_order_indicator,
TARGET_COLS_RIM::vend_pack_id,
TARGET_COLS_RIM::vend_pack_purch_stat_cd,
TARGET_COLS_RIM::vendor_pack_flow_type,
TARGET_COLS_RIM::vendor_pack_qty,
TARGET_COLS_RIM::reorder_method_code,
TARGET_COLS_RIM::str_supplier_cd,
TARGET_COLS_RIM::vend_stk_nbr,
TARGET_COLS_RIM::ksn_pack_id,
TARGET_COLS_RIM::ksn_dc_pack_purch_stat_cd,
TARGET_COLS_RIM::inbnd_ord_uom_cd,
TARGET_COLS_RIM::enable_jif_dc_ind,
TARGET_COLS_RIM::stk_ind,
TARGET_COLS_RIM::crtn_per_layer_qty,
TARGET_COLS_RIM::layer_per_pall_qty,
TARGET_COLS_RIM::dc_config_cd,
TARGET_COLS_RIM::imp_fl,
TARGET_COLS_RIM::ord_data_lmt_1::srs_division_nbr,
TARGET_COLS_RIM::ord_data_lmt_1::srs_item_nbr,
TARGET_COLS_RIM::ord_data_lmt_1::srs_sku_cd,
TARGET_COLS_RIM::srs_location_nbr,
TARGET_COLS_RIM::srs_source_nbr as srs_source_nbr,
TARGET_COLS_RIM::rim_sts_cd,
TARGET_COLS_RIM::non_stock_source_cd,
TARGET_COLS_RIM::item_active_ind,
TARGET_COLS_RIM::item_reserve_cd,
TARGET_COLS_RIM::item_next_period_on_hand_qty,
TARGET_COLS_RIM::item_reserve_qty,
TARGET_COLS_RIM::item_back_order_qty,
TARGET_COLS_RIM::item_next_period_future_order_qty,
TARGET_COLS_RIM::item_on_order_qty,
TARGET_COLS_RIM::item_next_period_in_transit_qty,
TARGET_COLS_RIM::item_last_receive_dt,
TARGET_COLS_RIM::item_last_ship_dt,
TARGET_COLS_RIM::stk_typ_cd;

---------------------------------------------------------------------------------------------------------------------

TARGET_COLS_RIM_JOIN_1 = JOIN TARGET_COLS_RIM_JOIN_2_OP BY TrimLeadingZeros(srs_source_nbr) LEFT OUTER, TARGET_COLS_RIM_SRS_LOC BY TrimLeadingZeros(srs_loc) USING 'skewed';

TARGET_COLS_RIM_JOIN_1_OP = FOREACH TARGET_COLS_RIM_JOIN_1 GENERATE

TARGET_COLS_RIM_JOIN_2_OP::item AS item,
TARGET_COLS_RIM_JOIN_2_OP::loc AS loc,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::src_owner_cd AS src_owner_cd,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::elig_sts_cd AS elig_sts_cd,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::src_pack_qty AS src_pack_qty,
((TARGET_COLS_RIM_JOIN_2_OP::src_loc is NULL OR TARGET_COLS_RIM_JOIN_2_OP::src_loc =='') ? (TARGET_COLS_RIM_SRS_LOC::srs_loc is NULL OR TARGET_COLS_RIM_SRS_LOC::srs_loc ==''?'':TARGET_COLS_RIM_SRS_LOC::loc ):TARGET_COLS_RIM_JOIN_2_OP::src_loc) AS src_loc,
TARGET_COLS_RIM_JOIN_2_OP::po_vnd_no AS po_vnd_no,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::ord_data_lmt_1::ksn_id AS ksn_id,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::purch_stat_cd AS purch_stat_cd,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::retail_crtn_intrnl_pack_qty AS retail_crtn_intrnl_pack_qty,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::days_to_check_begin_date AS days_to_check_begin_date,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::days_to_check_end_date AS days_to_check_end_date,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::dotcom_order_indicator AS dotcom_order_indicator,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::vend_pack_id AS vend_pack_id,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::vend_pack_purch_stat_cd AS vend_pack_purch_stat_cd,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::vendor_pack_flow_type AS vendor_pack_flow_type,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::vendor_pack_qty AS vendor_pack_qty,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::reorder_method_code AS reorder_method_code,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::str_supplier_cd AS str_supplier_cd,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::vend_stk_nbr AS vend_stk_nbr,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::ksn_pack_id AS ksn_pack_id,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::ksn_dc_pack_purch_stat_cd AS ksn_dc_pack_purch_stat_cd,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::inbnd_ord_uom_cd AS inbnd_ord_uom_cd,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::enable_jif_dc_ind AS enable_jif_dc_ind,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::stk_ind AS stk_ind,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::crtn_per_layer_qty AS crtn_per_layer_qty,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::layer_per_pall_qty AS layer_per_pall_qty,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::dc_config_cd AS dc_config_cd,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::imp_fl AS imp_fl,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::ord_data_lmt_1::srs_division_nbr AS srs_division_nbr,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::ord_data_lmt_1::srs_item_nbr AS srs_item_nbr,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::ord_data_lmt_1::srs_sku_cd AS srs_sku_cd,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::srs_location_nbr AS srs_location_nbr,
TARGET_COLS_RIM_JOIN_2_OP::srs_source_nbr AS srs_source_nbr,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::rim_sts_cd AS rim_sts_cd,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::non_stock_source_cd AS non_stock_source_cd,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::item_active_ind AS item_active_ind,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::item_reserve_cd AS item_reserve_cd,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::item_reserve_qty AS item_reserve_qty,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::item_back_order_qty AS item_back_order_qty,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::item_next_period_future_order_qty AS item_next_period_future_order_qty,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::item_on_order_qty AS item_on_order_qty,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::item_last_receive_dt AS item_last_receive_dt,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::item_last_ship_dt AS item_last_ship_dt,
TARGET_COLS_RIM_JOIN_2_OP::TARGET_COLS_RIM::stk_typ_cd AS stk_typ_cd;
-----------------------------------------------------------------------------------------------------------

TARGET_COLS_RIM_JOIN_1_OP1 = FOREACH TARGET_COLS_RIM_JOIN_1_OP GENERATE 
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

TARGET_COLS_RIM_DISTINCT = DISTINCT TARGET_COLS_RIM_JOIN_1_OP1 ;
--------------------------------------------------------------------------------------------------------------------------------

STORE TARGET_COLS_RIM_DISTINCT INTO '$WORK__IDRP_ELIGIBLE_ITEM_LOC_SEARS_RIM_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
