/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_loc_sears_dos.pig
# AUTHOR NAME:         Mudit Mangal
# CREATION DATE:       Fri Dec 27 02:31:46 EST 2013
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



--LOAD DOS Facility file
LOAD_DOS_FACILITY_1 = LOAD '$GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT_SCHEMA);

--LOAD SRIM file
LOAD_SRIM = LOAD '$GOLD__INVENTORY_SRIM_DAILY_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__INVENTORY_SRIM_DAILY_SCHEMA);

LOAD_SRIM = FOREACH LOAD_SRIM GENERATE division_nbr,item_nbr,sku_nbr,warehouse_nbr,store_pack_size_qty,source_nbr;

--LOAD DOS Owner file
LOAD_DOS_OWNER_1 = LOAD '$GOLD__INVENTORY_SEARS_DC_ITEM_OWNER_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_TAB') AS ($GOLD__INVENTORY_SEARS_DC_ITEM_OWNER_CURRENT_SCHEMA);

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

LOAD_ELIGIBLE_LOC = FOREACH LOAD_ELIGIBLE_LOC GENERATE srs_loc,loc,srs_vndr_nbr,loc_lvl_cd,loc_fmt_typ_cd,fmt_typ_cd,duns_type_cd;
------------------------------------------------------------------------------------------------------------------------

LOAD_DOS_FACILITY = FOREACH LOAD_DOS_FACILITY_1 GENERATE
(dos_warehouse_nbr is null ? '': dos_warehouse_nbr) as dos_warehouse_nbr ,
(dos_division_nbr is null ? '': dos_division_nbr) as dos_division_nbr,
(sears_item_nbr is null ? '': sears_item_nbr) as sears_item_nbr ,
(dos_sku_cd is null ? '': dos_sku_cd) as dos_sku_cd ,
(corporate_cd is null ? '': corporate_cd) as corporate_cd  , 
(sears_sku_nbr is null ? '': sears_sku_nbr) as sears_sku_nbr , 
(sears_division_nbr is null ? '': sears_division_nbr) as sears_division_nbr , 
(non_stock_source_cd is null ? '': non_stock_source_cd ) as non_stock_source_cd ;



LOAD_DOS_OWNER = FOREACH LOAD_DOS_OWNER_1 GENERATE 
(product_condition_cd is null ? '': product_condition_cd )as product_condition_cd,
(location_nbr is null ? '': location_nbr ) as location_nbr,
(division_nbr is null ? '': division_nbr )as division_nbr,
(corporate_cd is null ? '': corporate_cd) as corporate_cd  , 
(item_nbr is null ? '': item_nbr) as item_nbr, 
(sku_cd is null ? '': sku_cd )as sku_cd, 
(item_active_ind is null ? '': item_active_ind )as item_active_ind, 
(item_next_period_on_hand_qty is null ? '': item_next_period_on_hand_qty )as item_next_period_on_hand_qty, 
(item_on_order_qty is null ? '': item_on_order_qty )as item_on_order_qty, 
(item_reserve_qty is null ? '': item_reserve_qty )as item_reserve_qty, 
(item_back_order_qty is null ? '': item_back_order_qty )as item_back_order_qty, 
(item_next_period_future_order_qty is null ? '': item_next_period_future_order_qty )as item_next_period_future_order_qty, 
(item_next_period_in_transit_qty is null ? '': item_next_period_in_transit_qty )as item_next_period_in_transit_qty, 
(item_last_receive_dt is null ? '': item_last_receive_dt )as item_last_receive_dt,
(item_last_ship_dt is null ? '': item_last_ship_dt )as item_last_ship_dt,
(item_reserve_cd is null ? '': item_reserve_cd )as item_reserve_cd ;


-----------------------------------------------------------------------------------------------------------------------

FILTER_DC_OWNER = FILTER LOAD_DOS_OWNER BY (product_condition_cd == '0490R');

JOIN_DC_OWNER_FACILITY = JOIN LOAD_DOS_FACILITY BY (dos_warehouse_nbr,dos_division_nbr,sears_item_nbr,dos_sku_cd,corporate_cd), FILTER_DC_OWNER BY (location_nbr,division_nbr,item_nbr,sku_cd,corporate_cd) ;
-------------------------------------------------------------------------------------------------------------------------

ELIGIBLE_ITEM_FILTER = FILTER LOAD_ELIGIBLE_ITEM BY elig_sts_cd == 'A' OR elig_sts_cd == 'D';

ITEM_COL_JOIN= JOIN JOIN_DC_OWNER_FACILITY BY (LOAD_DOS_FACILITY::sears_division_nbr,LOAD_DOS_FACILITY::sears_item_nbr,LOAD_DOS_FACILITY::sears_sku_nbr), ELIGIBLE_ITEM_FILTER BY (srs_div_no,srs_itm_no,srs_sku_no);
--------------------------------------------------------------------------------------------------------------------------

LOC_ITEM_COL_JOIN = JOIN ITEM_COL_JOIN BY TrimLeadingZeros(JOIN_DC_OWNER_FACILITY::LOAD_DOS_FACILITY::dos_warehouse_nbr) LEFT OUTER, LOAD_ELIGIBLE_LOC BY TrimLeadingZeros(srs_loc);
----------------------------------------------------------------------------------------------------------------------------

LOAD_SMITH_ITEM_EFFECTIVE = FILTER LOAD_SMITH_ITEM BY  SUBSTRING(shc_item_id_expiration_ts,0,10) > '$CURRENT_DATE' ;
---------------------------------------------------------------------------------------------------------------------------

LOC_ITEM_COL_JOIN_KSN = JOIN LOC_ITEM_COL_JOIN BY (ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::LOAD_DOS_FACILITY::sears_division_nbr,ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::LOAD_DOS_FACILITY::sears_item_nbr,ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::LOAD_DOS_FACILITY::sears_sku_nbr) LEFT OUTER, LOAD_SMITH_ITEM_EFFECTIVE BY (sears_division_nbr, sears_item_nbr, sears_sku_nbr) ;
---------------------------------------------------------------------------------------------------------------------------

LOAD_VEND_PACK_EFFECTIVE = FILTER LOAD_VEND_PACK BY SUBSTRING(expiration_ts,0,10) > '$CURRENT_DATE' ;

LOAD_VEND_PACK_EFFECTIVE_FILTER =  FILTER LOAD_VEND_PACK_EFFECTIVE BY (owner_cd == 'S'and (purchase_status_cd == 'A' or purchase_status_cd == 'W') and import_cd == 'I');
------------------------------------------------------------------------------------------------------------------------------

LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE  = FOREACH LOAD_VEND_PACK_EFFECTIVE_FILTER GENERATE ksn_id, import_cd;
---------------------------------------------------------------------------------------------------------------------------

LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE_DISTINCT = DISTINCT LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE;
LOC_ITEM_COL_JOIN_KSN_VEND = JOIN LOC_ITEM_COL_JOIN_KSN BY (LOAD_SMITH_ITEM_EFFECTIVE::ksn_id) LEFT OUTER, LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE_DISTINCT BY (ksn_id);
---------------------------------------------------------------------------------------------------------------------------

LOC_ITEM_COL_JOIN_KSN_VEND_SRIM = JOIN LOC_ITEM_COL_JOIN_KSN_VEND BY (LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::LOAD_DOS_FACILITY::sears_division_nbr,TrimLeadingZeros(LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::LOAD_DOS_FACILITY::sears_item_nbr) ,LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::LOAD_DOS_FACILITY::sears_sku_nbr,TrimLeadingZeros(LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::LOAD_DOS_FACILITY::dos_warehouse_nbr)), LOAD_SRIM BY (division_nbr,TrimLeadingZeros(item_nbr),sku_nbr,TrimLeadingZeros(warehouse_nbr));
----------------------------------------------------------------------------------------------------------------------------

--Columns Projection

TRANSFORMATION_FINAL_DOS = FOREACH LOC_ITEM_COL_JOIN_KSN_VEND_SRIM GENERATE

LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::ELIGIBLE_ITEM_FILTER::item as item,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::loc as loc,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::srs_loc as srs_loc, 
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::srs_vndr_nbr as srs_vndr_nbr,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::loc_lvl_cd as loc_lvl_cd, 
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::ELIGIBLE_ITEM_FILTER::easy_ord_fl as easy_ord_fl,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::ELIGIBLE_ITEM_FILTER::spc_ord_cdt_fl as spc_ord_cdt_fl,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::ELIGIBLE_ITEM_FILTER::itm_emp_fl as itm_emp_fl,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::loc_fmt_typ_cd as loc_fmt_typ_cd,
(LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::FILTER_DC_OWNER::item_active_ind IS NULL ? '': LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::FILTER_DC_OWNER::item_active_ind ) as item_active_ind,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::FILTER_DC_OWNER::item_next_period_on_hand_qty  as item_next_period_on_hand_qty,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::FILTER_DC_OWNER::item_on_order_qty as item_on_order_qty, LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::FILTER_DC_OWNER::item_reserve_qty as item_reserve_qty, LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::FILTER_DC_OWNER::item_back_order_qty as item_back_order_qty, LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::FILTER_DC_OWNER::item_next_period_future_order_qty as item_next_period_future_order_qty, LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::FILTER_DC_OWNER::item_next_period_in_transit_qty as item_next_period_in_transit_qty, LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::FILTER_DC_OWNER::item_last_receive_dt as item_last_receive_dt,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::FILTER_DC_OWNER::item_last_ship_dt as item_last_ship_dt, ((LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::LOAD_DOS_FACILITY::non_stock_source_cd is null )? ' ':non_stock_source_cd) as non_stock_source_cd,
(LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::FILTER_DC_OWNER::item_reserve_cd IS NULL ? '': LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::FILTER_DC_OWNER::item_reserve_cd) as item_reserve_cd,
LOAD_SRIM::store_pack_size_qty as src_pack_qty,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOAD_SMITH_ITEM_EFFECTIVE::ksn_id as ksn_id,
LOC_ITEM_COL_JOIN_KSN_VEND::LOAD_VEND_PACK_EFFECTIVE_FILTER_GENERATE_DISTINCT::import_cd as import_cd,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::LOAD_DOS_FACILITY::sears_division_nbr as srs_division_nbr,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::LOAD_DOS_FACILITY::sears_item_nbr as srs_item_nbr,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::LOAD_DOS_FACILITY::sears_sku_nbr as srs_sku_cd,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::ITEM_COL_JOIN::JOIN_DC_OWNER_FACILITY::LOAD_DOS_FACILITY::dos_warehouse_nbr as dos_warehouse_nbr,
LOAD_SRIM::source_nbr as srs_source_nbr,
LOC_ITEM_COL_JOIN_KSN_VEND::LOC_ITEM_COL_JOIN_KSN::LOC_ITEM_COL_JOIN::LOAD_ELIGIBLE_LOC::fmt_typ_cd as fmt_typ_cd,
'U' as upsert_field;
---------------------------------------------------------------------------------------------------------------------------------
/*
--TRANSFORMATION_FINAL_DOS_FILTER = FILTER TRANSFORMATION_FINAL_DOS BY loc_fmt_typ_cd != '';

----------------------------------------------------------------------------------------------------------------------------------

TARGET_COLS_DISTINCT_3_GRP_FINAL = GROUP TRANSFORMATION_FINAL_DOS BY (item,srs_loc);

TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL = FOREACH TARGET_COLS_DISTINCT_3_GRP_FINAL
                                           { ord_data_1 = ORDER TRANSFORMATION_FINAL_DOS BY item,fmt_typ_cd,srs_loc ASC;
                                                         ord_data_lmt_1 = LIMIT ord_data_1 1;
                                                         GENERATE FLATTEN(ord_data_lmt_1);
                                           };

TRANSFORMATION_FINAL_DOS_DISTINCT = DISTINCT TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL;
*/

/*
TRANSFORMATION_FINAL_DOS_DISTINCT = DISTINCT TRANSFORMATION_FINAL_DOS;
-----------------------------------------------------------------------------------------------------------------------------------

--describe TRANSFORMATION_FINAL_DOS_DISTINCT;

--Removing the output directory if exists
rmf $WORK_DIR$io_hdfs_path_dos_join;
STORE TRANSFORMATION_FINAL_DOS_DISTINCT INTO '$WORK_DIR$io_hdfs_path_dos_join' USING PigStorage($output_table_delimiter);
----------------------------------------------------------------------------------------------------------------------------------
*/

TRANSFORMATION_FINAL_DOS_DISTINCT = FILTER TRANSFORMATION_FINAL_DOS BY (item is NOT NULL OR item !='') AND (loc is NOT NULL OR loc !='') AND (item_active_ind == 'Y' OR item_active_ind == 'N') AND (item_reserve_cd == '1' OR item_reserve_cd =='2' OR item_reserve_cd =='4') ;

TRANSFORMATION_FINAL_DOS_1 = FOREACH  TRANSFORMATION_FINAL_DOS_DISTINCT GENERATE
item,
loc,
((easy_ord_fl is  null)? '': easy_ord_fl) as easy_ord_fl,
((spc_ord_cdt_fl is null)? '': spc_ord_cdt_fl) as spc_ord_cdt_fl,
((itm_emp_fl is null)? '': itm_emp_fl) as itm_emp_fl,
((loc_fmt_typ_cd is null)? '': loc_fmt_typ_cd) as loc_fmt_typ_cd,
((item_active_ind is null)? '': item_active_ind) as item_active_ind,
((non_stock_source_cd is null)? '': non_stock_source_cd) as non_stock_source_cd,
((item_on_order_qty is null)? '': item_on_order_qty) as item_on_order_qty,
((item_next_period_on_hand_qty is null)? '': item_next_period_on_hand_qty) as item_next_period_on_hand_qty,
((item_reserve_qty is null)? '': item_reserve_qty) as item_reserve_qty,
((item_back_order_qty is null)? '': item_back_order_qty) as item_back_order_qty,
((item_next_period_future_order_qty is null)? '': item_next_period_future_order_qty) as item_next_period_future_order_qty,
((item_next_period_in_transit_qty is null)? '': item_next_period_in_transit_qty) as item_next_period_in_transit_qty,
((item_last_receive_dt is null)? '': item_last_receive_dt) as item_last_receive_dt,
((item_last_ship_dt is null)? '': item_last_ship_dt) as item_last_ship_dt,
((srs_vndr_nbr is null)? '': srs_vndr_nbr) as srs_vndr_nbr,
((srs_loc is null)? '': srs_loc) as srs_loc,
loc_lvl_cd,
item_reserve_cd,
src_pack_qty,
ksn_id,
((import_cd is null)? '': import_cd) as import_cd,
srs_division_nbr,
srs_item_nbr,
srs_sku_cd,
dos_warehouse_nbr,
srs_source_nbr;
------------------------------------------------------------------------------------------------------------------------------------

TARGET_COLS_DOS = FOREACH TRANSFORMATION_FINAL_DOS_1 GENERATE

item,
loc,
'S' as src_owner_cd,
((item is not null AND loc is not NULL AND easy_ord_fl == '0' AND  spc_ord_cdt_fl == '0' AND  itm_emp_fl == '0')?((loc_fmt_typ_cd == 'RRC' AND  item_active_ind == 'Y' AND  non_stock_source_cd != 'STK')? 'A': ((loc_fmt_typ_cd == 'RRC' AND  item_active_ind == 'N' AND ((double)item_on_order_qty >0.00 or  (double)item_next_period_on_hand_qty > 0.00 or  (double)item_reserve_qty > 0.00 or  (double)item_back_order_qty > 0.00 or  (double)item_next_period_future_order_qty > 0.00 or  (double)item_next_period_in_transit_qty > 0.00 ) AND (AddDays(item_last_receive_dt,365)>'$CURRENT_DATE' or  AddDays(item_last_ship_dt,365)>'$CURRENT_DATE') AND  non_stock_source_cd != 'STK')?'A':((( loc_fmt_typ_cd == 'DDC' or  loc_fmt_typ_cd == 'MDO') AND  item_active_ind == 'Y')?'A':((( loc_fmt_typ_cd == 'DDC' or  loc_fmt_typ_cd == 'MDO') AND  item_active_ind == 'N' AND ( (double)item_on_order_qty > 0.00 or  (double)item_next_period_on_hand_qty > 0.00 or  (double)item_reserve_qty > 0.00 or (double)item_back_order_qty > 0.00 or  (double)item_next_period_future_order_qty > 0.00 or  (double)item_next_period_in_transit_qty > 0.00 ) AND (AddDays(item_last_receive_dt,365)>'$CURRENT_DATE' or  AddDays(item_last_ship_dt,365)>'$CURRENT_DATE'))?'A':'D')))):'D')  as elig_sts_cd,

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
(import_cd != '' ? '1':'0') as imp_fl,
srs_division_nbr,
srs_item_nbr,
srs_sku_cd,
TrimLeadingZeros(dos_warehouse_nbr) as srs_location_nbr,
srs_source_nbr,
'' as rim_sts_cd,
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
' ' as stk_typ_cd;
-----------------------------------------------------------------------------------------------------------------------------------
SPLIT LOAD_ELIGIBLE_LOC INTO TARGET_COLS_DOS_SRS_LOC IF loc_lvl_cd == 'WAREHOUSE', TARGET_COLS_DOS_NOT_SRS_LOC IF ( loc_lvl_cd == 'VENDOR' AND duns_type_cd == 'ORD') ;

TARGET_COLS_DOS_JOIN_2 = JOIN TARGET_COLS_DOS  BY TrimLeadingZeros(srs_source_nbr) LEFT OUTER, TARGET_COLS_DOS_NOT_SRS_LOC BY TrimLeadingZeros(srs_vndr_nbr) USING 'skewed';

TARGET_COLS_DOS_JOIN_2_OP = FOREACH TARGET_COLS_DOS_JOIN_2 GENERATE
TARGET_COLS_DOS::item as item,
TARGET_COLS_DOS::loc as loc,
TARGET_COLS_DOS::src_owner_cd,
TARGET_COLS_DOS::elig_sts_cd,
TARGET_COLS_DOS::src_pack_qty,
TARGET_COLS_DOS_NOT_SRS_LOC::loc as src_loc,
TARGET_COLS_DOS_NOT_SRS_LOC::loc as po_vnd_no,
TARGET_COLS_DOS::ksn_id,
TARGET_COLS_DOS::purch_stat_cd,
TARGET_COLS_DOS::retail_crtn_intrnl_pack_qty,
TARGET_COLS_DOS::days_to_check_begin_date,
TARGET_COLS_DOS::days_to_check_end_date,
TARGET_COLS_DOS::dotcom_order_indicator,
TARGET_COLS_DOS::vend_pack_id,
TARGET_COLS_DOS::vend_pack_purch_stat_cd,
TARGET_COLS_DOS::vendor_pack_flow_type,
TARGET_COLS_DOS::vendor_pack_qty,
TARGET_COLS_DOS::reorder_method_code,
TARGET_COLS_DOS::str_supplier_cd,
TARGET_COLS_DOS::vend_stk_nbr,
TARGET_COLS_DOS::ksn_pack_id,
TARGET_COLS_DOS::ksn_dc_pack_purch_stat_cd,
TARGET_COLS_DOS::inbnd_ord_uom_cd,
TARGET_COLS_DOS::enable_jif_dc_ind,
TARGET_COLS_DOS::stk_ind,
TARGET_COLS_DOS::crtn_per_layer_qty,
TARGET_COLS_DOS::layer_per_pall_qty,
TARGET_COLS_DOS::dc_config_cd,
TARGET_COLS_DOS::imp_fl,
TARGET_COLS_DOS::srs_division_nbr,
TARGET_COLS_DOS::srs_item_nbr,
TARGET_COLS_DOS::srs_sku_cd,
TARGET_COLS_DOS::srs_location_nbr,
TARGET_COLS_DOS::srs_source_nbr as srs_source_nbr,
TARGET_COLS_DOS::rim_sts_cd,
TARGET_COLS_DOS::non_stock_source_cd,
TARGET_COLS_DOS::item_active_ind,
TARGET_COLS_DOS::item_reserve_cd,
TARGET_COLS_DOS::item_next_period_on_hand_qty,
TARGET_COLS_DOS::item_reserve_qty,
TARGET_COLS_DOS::item_back_order_qty,
TARGET_COLS_DOS::item_next_period_future_order_qty,
TARGET_COLS_DOS::item_on_order_qty,
TARGET_COLS_DOS::item_next_period_in_transit_qty,
TARGET_COLS_DOS::item_last_receive_dt,
TARGET_COLS_DOS::item_last_ship_dt,
TARGET_COLS_DOS::stk_typ_cd;

---------------------------------------------------------------------------------------------------------------------
TARGET_COLS_DOS_JOIN_1 = JOIN TARGET_COLS_DOS_JOIN_2_OP BY TrimLeadingZeros(srs_source_nbr) LEFT OUTER , TARGET_COLS_DOS_SRS_LOC BY TrimLeadingZeros(srs_loc) USING 'skewed';

TARGET_COLS_DOS_JOIN_1_OP = FOREACH TARGET_COLS_DOS_JOIN_1 GENERATE

TARGET_COLS_DOS_JOIN_2_OP::item AS item,
TARGET_COLS_DOS_JOIN_2_OP::loc AS loc,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::src_owner_cd AS src_owner_cd,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::elig_sts_cd AS elig_sts_cd,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::src_pack_qty AS src_pack_qty,
((TARGET_COLS_DOS_JOIN_2_OP::src_loc is NULL OR TARGET_COLS_DOS_JOIN_2_OP::src_loc =='') ? (TARGET_COLS_DOS_SRS_LOC::srs_loc is NULL OR TARGET_COLS_DOS_SRS_LOC::srs_loc ==''?'':TARGET_COLS_DOS_SRS_LOC::loc ):TARGET_COLS_DOS_JOIN_2_OP::src_loc) AS src_loc,
TARGET_COLS_DOS_JOIN_2_OP::po_vnd_no AS po_vnd_no,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::ksn_id AS ksn_id,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::purch_stat_cd AS purch_stat_cd,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::retail_crtn_intrnl_pack_qty AS retail_crtn_intrnl_pack_qty,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::days_to_check_begin_date AS days_to_check_begin_date,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::days_to_check_end_date AS days_to_check_end_date,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::dotcom_order_indicator AS dotcom_order_indicator,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::vend_pack_id AS vend_pack_id,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::vend_pack_purch_stat_cd AS vend_pack_purch_stat_cd,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::vendor_pack_flow_type AS vendor_pack_flow_type,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::vendor_pack_qty AS vendor_pack_qty,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::reorder_method_code AS reorder_method_code,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::str_supplier_cd AS str_supplier_cd,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::vend_stk_nbr AS vend_stk_nbr,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::ksn_pack_id AS ksn_pack_id,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::ksn_dc_pack_purch_stat_cd AS ksn_dc_pack_purch_stat_cd,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::inbnd_ord_uom_cd AS inbnd_ord_uom_cd,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::enable_jif_dc_ind AS enable_jif_dc_ind,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::stk_ind AS stk_ind,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::crtn_per_layer_qty AS crtn_per_layer_qty,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::layer_per_pall_qty AS layer_per_pall_qty,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::dc_config_cd AS dc_config_cd,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::imp_fl AS imp_fl,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::srs_division_nbr AS srs_division_nbr,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::srs_item_nbr AS srs_item_nbr,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::srs_sku_cd AS srs_sku_cd,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::srs_location_nbr AS srs_location_nbr,
TARGET_COLS_DOS_JOIN_2_OP::srs_source_nbr AS srs_source_nbr,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::rim_sts_cd AS rim_sts_cd,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::non_stock_source_cd AS non_stock_source_cd,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::item_active_ind AS item_active_ind,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::item_reserve_cd AS item_reserve_cd,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::item_reserve_qty AS item_reserve_qty,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::item_back_order_qty AS item_back_order_qty,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::item_next_period_future_order_qty AS item_next_period_future_order_qty,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::item_on_order_qty AS item_on_order_qty,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::item_last_receive_dt AS item_last_receive_dt,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::item_last_ship_dt AS item_last_ship_dt,
TARGET_COLS_DOS_JOIN_2_OP::TARGET_COLS_DOS::stk_typ_cd AS stk_typ_cd;
-----------------------------------------------------------------------------------------------------------

TARGET_COLS_DOS_JOIN_1_OP1 = FOREACH TARGET_COLS_DOS_JOIN_1_OP GENERATE 
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
,'$batchid'	as	batch_id;

TARGET_COLS_DOS_DISTINCT = DISTINCT TARGET_COLS_DOS_JOIN_1_OP1 ;

-----------------------------------------------------------------------------------------------------------------------------

STORE TARGET_COLS_DOS_DISTINCT INTO '$WORK__IDRP_ELIGIBLE_ITEM_LOC_SEARS_DOS_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/

