/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_online_fulfillment_initialization_smith__idrp_eligible_item.pig
# AUTHOR NAME:         Mudit Mangal
# CREATION DATE:       Thu Jan 09 08:34:21 EST 2014
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
DEFINE GetDotComOrderableIndicator com.searshc.supplychain.idrp.udf.GetDotComOrderableIndicator();
SET default_parallel $NUM_PARALLEL;
set mapred.child.java.opts '-Xmx4096m'
set io.sort.mb 512
/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

--LOAD ELIGIBLE Item Part 2 file
NEW_COLS_NEXT_0 = LOAD '$WORK__IDRP_ELIGIBLE_ITEM_PART_2' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_ITEM_SCHEMA);

--LOAD GOLD ITEM HIERARCHY Package file
LOAD_GOLD_ITEM = LOAD '$GOLD__ITEM_SHC_HIERARCHY_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_SHC_HIERARCHY_CURRENT_SCHEMA);

--LOAD ELIGIBLE Item Loc file
LOAD_ELIGIBLE_ITEM_LOC = LOAD '$SMITH__IDRP_ELIGIBLE_ITEM_LOC_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);

--LOAD ONLINE FULFILLMENT file
LOAD_ONLINE_FULFILLMENT = LOAD '$WORK__IDRP_ONLINE_FULFILLMENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ONLINE_FULFILLMENT_SCHEMA);

--LOAD DROP SHIP ITEMS file
LOAD_DROP_SHIP = LOAD '$SMITH__IDRP_ONLINE_DROP_SHIP_ITEMS_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ONLINE_DROP_SHIP_ITEMS_SCHEMA);

--LOAD ONLINE BILLABLE WEIGHT
LOAD_ONLINE_BILL_WT = LOAD '$WORK__IDRP_ONLINE_BILLABLE_WEIGHT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ONLINE_BILLABLE_WEIGHT_SCHEMA);

--LOAD ITEM PACKAGE CURRENT
LOAD_PACKAGE_CURRENT = LOAD '$GOLD__ITEM_PACKAGE_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_PACKAGE_CURRENT_SCHEMA);

-------------------------------------------------------------------------------------------------------------------------------------

NEW_COLS_NEXT_0 = FOREACH NEW_COLS_NEXT_0 GENERATE

load_ts AS load_ts,
item AS item,
descr AS descr,
shc_dvsn_no AS shc_dvsn_no,
shc_dvsn_nm AS shc_dvsn_nm,
shc_dept_no AS shc_dept_no,
shc_dept_nm AS shc_dept_nm,
shc_cat_grp_no AS shc_cat_grp_no,
shc_cat_grp_nm AS shc_cat_grp_nm,
shc_cat_no AS shc_cat_no,
shc_cat_nm AS shc_cat_nm,
shc_subcat_no AS shc_subcat_no,
shc_subcat_nm AS shc_subcat_nm,
ref_ksn_id AS ref_ksn_id,
srs_bus_no AS srs_bus_no,
srs_bus_nm AS srs_bus_nm,
srs_div_no AS srs_div_no,
srs_div_nm AS srs_div_nm,
srs_ln_no AS srs_ln_no,
srs_ln_ds AS srs_ln_ds,
srs_sbl_no AS srs_sbl_no,
srs_sbl_ds AS srs_sbl_ds,
srs_cls_no AS srs_cls_no,
srs_cls_ds AS srs_cls_ds,
srs_itm_no AS srs_itm_no,
srs_sku_no AS srs_sku_no,
srs_div_itm AS srs_div_itm,
srs_div_itm_sku AS srs_div_itm_sku,
ima_smt_itm_no AS ima_smt_itm_no,
ima_smt_itm_ds AS ima_smt_itm_ds,
ima_smt_fac_qt AS ima_smt_fac_qt,
uom AS uom,
vol AS vol,
wgt AS wgt,
vnd_no AS vnd_no,
vnd_nm AS vnd_nm,
vnd_itm_no AS vnd_itm_no,
spc_ord_cdt_fl AS spc_ord_cdt_fl,
itm_emp_fl AS itm_emp_fl,
easy_ord_fl AS easy_ord_fl,
itm_rpd_fl AS itm_rpd_fl,
itm_cd_fl AS itm_cd_fl,
itm_imp_fl AS itm_imp_fl,
itm_cs_fl AS itm_cs_fl,
dd_ind AS dd_ind,
instl_ind AS instl_ind,
cheetah_elgbl_fl AS cheetah_elgbl_fl,
dot_com_cd AS dot_com_cd,
obn_830_fl AS obn_830_fl,
obn_830_dur AS obn_830_dur,
rpd_frz_dur AS rpd_frz_dur,
dist_typ_cd AS dist_typ_cd,
sls_pfm_seg_cd AS sls_pfm_seg_cd,
fmt_excl_cd AS fmt_excl_cd,
str_fcst_cd AS str_fcst_cd,
inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
ima_itm_typ_cd AS ima_itm_typ_cd,
itm_purch_sts_cd AS itm_purch_sts_cd,
ntwk_dist_cd AS ntwk_dist_cd,
fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
str_reord_auth_cd AS str_reord_auth_cd,
cross_mdse_attr_cd AS cross_mdse_attr_cd,
whse_sizing AS whse_sizing,
can_carr_mdl_id AS can_carr_mdl_id,
groc_crossover_ind AS groc_crossover_ind,
owner_cd AS owner_cd,
pln_id AS pln_id,
itm_pgm AS itm_pgm,
key_pgm AS key_pgm,
natl_un_cst_am AS natl_un_cst_am,
prd_sll_am AS prd_sll_am,
size AS size,
style AS style,
md_style_ref_cd AS md_style_ref_cd,
seas_cd AS seas_cd,
seas_yr AS seas_yr,
sub_seas_id AS sub_seas_id,
rpt_id AS rpt_id,
rpt_id_seq_no AS rpt_id_seq_no,
itm_fcst_grp_id AS itm_fcst_grp_id,
itm_fcst_grp_ds AS itm_fcst_grp_ds,
idrp_itm_typ_ds AS idrp_itm_typ_ds,
brand_ds AS brand_ds,
color_ds AS color_ds,
tire_size_ds AS tire_size_ds,
elig_sts_cd AS elig_sts_cd,
lst_sts_chg_dt AS lst_sts_chg_dt,
itm_del_fl AS itm_del_fl,
prd_prg_dt AS prd_prg_dt,
item_order_system_cd AS order_system_cd,
idrp_order_method_cd AS idrp_order_method_cd,
dotcom_assorted_cd AS dotcom_assorted_cd,
dotcom_orderable_ind AS dotcom_orderable_ind,
roadrunner_eligible_ind AS roadrunner_eligible_ind,
us_dot_ship_type_cd AS us_dot_ship_type_cd,
package_weight_pounds_qty AS package_weight_in_pounds,
package_depth_inch_qty AS package_depth_inch_qty,
package_height_inch_qty AS package_height_inch_qty,
package_width_inch_qty AS package_width_inch_qty,
mailable_ind AS mailable_ind,
temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
default_online_ts AS default_online_ts,
demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
temporary_ups_billable_weight_qty AS temporary_ups_billable_weight,
ups_billable_weight_qty AS ups_billable_weight,
ups_billable_weight_ts AS ups_billable_weight_ts,
demand_ups_billable_weight_qty AS demand_ups_billable_weight,
web_exclusive_ind AS web_exclusive_ind,
price_type_desc AS price_type_desc,
idrp_batch_id AS idrp_batch_id;



NEW_COLS_NEXT_JOIN = JOIN NEW_COLS_NEXT_0 BY item LEFT OUTER, LOAD_ONLINE_FULFILLMENT BY item_id ;

NEW_COLS_NEXT = FOREACH NEW_COLS_NEXT_JOIN GENERATE
NEW_COLS_NEXT_0::load_ts as load_ts
,NEW_COLS_NEXT_0::item   as  item
,NEW_COLS_NEXT_0::descr   as  descr 
,NEW_COLS_NEXT_0::shc_dvsn_no   as  shc_dvsn_no 
,NEW_COLS_NEXT_0::shc_dvsn_nm   as  shc_dvsn_nm 
,NEW_COLS_NEXT_0::shc_dept_no as shc_dept_no 
,NEW_COLS_NEXT_0::shc_dept_nm as shc_dept_nm 
,NEW_COLS_NEXT_0::shc_cat_grp_no as shc_cat_grp_no 
,NEW_COLS_NEXT_0::shc_cat_grp_nm as shc_cat_grp_nm 
,NEW_COLS_NEXT_0::shc_cat_no as shc_cat_no 
,NEW_COLS_NEXT_0::shc_cat_nm as shc_cat_nm 
,NEW_COLS_NEXT_0::shc_subcat_no as shc_subcat_no 
,NEW_COLS_NEXT_0::shc_subcat_nm as shc_subcat_nm 
,NEW_COLS_NEXT_0::ref_ksn_id as ref_ksn_id 
,NEW_COLS_NEXT_0::srs_bus_no as srs_bus_no 
,NEW_COLS_NEXT_0::srs_bus_nm as srs_bus_nm 
,NEW_COLS_NEXT_0::srs_div_no as srs_div_no 
,NEW_COLS_NEXT_0::srs_div_nm as srs_div_nm 
,NEW_COLS_NEXT_0::srs_ln_no as srs_ln_no 
,NEW_COLS_NEXT_0::srs_ln_ds as srs_ln_ds 
,NEW_COLS_NEXT_0::srs_sbl_no as srs_sbl_no 
,NEW_COLS_NEXT_0::srs_sbl_ds as srs_sbl_ds 
,NEW_COLS_NEXT_0::srs_cls_no as srs_cls_no 
,NEW_COLS_NEXT_0::srs_cls_ds as srs_cls_ds 
,NEW_COLS_NEXT_0::srs_itm_no as srs_itm_no 
,NEW_COLS_NEXT_0::srs_sku_no as srs_sku_no 
,NEW_COLS_NEXT_0::srs_div_itm as srs_div_itm 
,NEW_COLS_NEXT_0::srs_div_itm_sku as srs_div_itm_sku 
,NEW_COLS_NEXT_0::ima_smt_itm_no as ima_smt_itm_no 
,NEW_COLS_NEXT_0::ima_smt_itm_ds as ima_smt_itm_ds 
,NEW_COLS_NEXT_0::ima_smt_fac_qt as ima_smt_fac_qt 
,NEW_COLS_NEXT_0::uom as uom 
,NEW_COLS_NEXT_0::vol as vol 
,NEW_COLS_NEXT_0::wgt as wgt 
,NEW_COLS_NEXT_0::vnd_no as vnd_no 
,NEW_COLS_NEXT_0::vnd_nm as vnd_nm 
,NEW_COLS_NEXT_0::vnd_itm_no as vnd_itm_no 
,NEW_COLS_NEXT_0::spc_ord_cdt_fl as spc_ord_cdt_fl 
,NEW_COLS_NEXT_0::itm_emp_fl as itm_emp_fl 
,NEW_COLS_NEXT_0::easy_ord_fl as easy_ord_fl 
,NEW_COLS_NEXT_0::itm_rpd_fl as itm_rpd_fl 
,NEW_COLS_NEXT_0::itm_cd_fl as itm_cd_fl 
,NEW_COLS_NEXT_0::itm_imp_fl as itm_imp_fl 
,NEW_COLS_NEXT_0::itm_cs_fl as itm_cs_fl 
,NEW_COLS_NEXT_0::dd_ind as dd_ind
,NEW_COLS_NEXT_0::instl_ind as instl_ind
,NEW_COLS_NEXT_0::cheetah_elgbl_fl as cheetah_elgbl_fl
,NEW_COLS_NEXT_0::dot_com_cd as dot_com_cd 
,NEW_COLS_NEXT_0::obn_830_fl as obn_830_fl 
,NEW_COLS_NEXT_0::obn_830_dur as obn_830_dur 
,NEW_COLS_NEXT_0::rpd_frz_dur as rpd_frz_dur 
,NEW_COLS_NEXT_0::dist_typ_cd as dist_typ_cd 
,NEW_COLS_NEXT_0::sls_pfm_seg_cd as sls_pfm_seg_cd 
,NEW_COLS_NEXT_0::fmt_excl_cd as fmt_excl_cd
,NEW_COLS_NEXT_0::str_fcst_cd as str_fcst_cd 
,NEW_COLS_NEXT_0::inv_mgmt_srvc_cd as inv_mgmt_srvc_cd 
,NEW_COLS_NEXT_0::ima_itm_typ_cd as ima_itm_typ_cd 
,NEW_COLS_NEXT_0::itm_purch_sts_cd as itm_purch_sts_cd 
,NEW_COLS_NEXT_0::ntwk_dist_cd as ntwk_dist_cd 
,NEW_COLS_NEXT_0::fut_ntwk_dist_cd as fut_ntwk_dist_cd 
,NEW_COLS_NEXT_0::fut_ntwk_eff_dt as fut_ntwk_eff_dt 
,NEW_COLS_NEXT_0::jit_ntwk_dist_cd as jit_ntwk_dist_cd 
,NEW_COLS_NEXT_0::cust_dir_ntwk_cd AS cust_dir_ntwk_cd
,NEW_COLS_NEXT_0::str_reord_auth_cd as str_reord_auth_cd 
,NEW_COLS_NEXT_0::cross_mdse_attr_cd as cross_mdse_attr_cd 
,NEW_COLS_NEXT_0::whse_sizing as whse_sizing 
,NEW_COLS_NEXT_0::can_carr_mdl_id as can_carr_mdl_id 
,NEW_COLS_NEXT_0::groc_crossover_ind as groc_crossover_ind 
,NEW_COLS_NEXT_0::owner_cd as owner_cd 
,NEW_COLS_NEXT_0::pln_id as pln_id 
,NEW_COLS_NEXT_0::itm_pgm as itm_pgm 
,NEW_COLS_NEXT_0::key_pgm as key_pgm 
,NEW_COLS_NEXT_0::natl_un_cst_am as natl_un_cst_am 
,NEW_COLS_NEXT_0::prd_sll_am as prd_sll_am 
,NEW_COLS_NEXT_0::size as size 
,NEW_COLS_NEXT_0::style as style 
,NEW_COLS_NEXT_0::md_style_ref_cd as md_style_ref_cd 
,NEW_COLS_NEXT_0::seas_cd as seas_cd 
,NEW_COLS_NEXT_0::seas_yr as seas_yr 
,NEW_COLS_NEXT_0::sub_seas_id as sub_seas_id 
,NEW_COLS_NEXT_0::rpt_id as rpt_id 
,NEW_COLS_NEXT_0::rpt_id_seq_no as rpt_id_seq_no 
,NEW_COLS_NEXT_0::itm_fcst_grp_id as itm_fcst_grp_id 
,NEW_COLS_NEXT_0::itm_fcst_grp_ds as itm_fcst_grp_ds 
,NEW_COLS_NEXT_0::idrp_itm_typ_ds as idrp_itm_typ_ds 
,NEW_COLS_NEXT_0::brand_ds as brand_ds 
,NEW_COLS_NEXT_0::color_ds as color_ds 
,NEW_COLS_NEXT_0::tire_size_ds as tire_size_ds 
,NEW_COLS_NEXT_0::elig_sts_cd as elig_sts_cd 
,NEW_COLS_NEXT_0::lst_sts_chg_dt as lst_sts_chg_dt 
,NEW_COLS_NEXT_0::itm_del_fl as itm_del_fl 
,NEW_COLS_NEXT_0::prd_prg_dt as prd_prg_dt 
,NEW_COLS_NEXT_0::order_system_cd as order_system_cd 
,NEW_COLS_NEXT_0::idrp_order_method_cd as idrp_order_method_cd
,NEW_COLS_NEXT_0::dotcom_assorted_cd as dotcom_assorted_cd 
,NEW_COLS_NEXT_0::dotcom_orderable_ind as dotcom_orderable_ind 
,NEW_COLS_NEXT_0::roadrunner_eligible_ind as roadrunner_eligible_fl 
,NEW_COLS_NEXT_0::us_dot_ship_type_cd as us_dot_ship_type_cd 
,NEW_COLS_NEXT_0::package_weight_in_pounds as package_weight_in_pounds 
,NEW_COLS_NEXT_0::package_depth_inch_qty as package_depth_inch_qty 
,NEW_COLS_NEXT_0::package_height_inch_qty as package_height_inch_qty 
,NEW_COLS_NEXT_0::package_width_inch_qty as package_width_inch_qty 
,NEW_COLS_NEXT_0::mailable_ind as mailable_ind 
,NEW_COLS_NEXT_0::temporary_online_fulfillment_type_cd as temporary_online_fulfillment_type_cd 
,((LOAD_ONLINE_FULFILLMENT::item_id != '' AND LOAD_ONLINE_FULFILLMENT::item_id IS NOT NULL ) ? LOAD_ONLINE_FULFILLMENT::default_fulfillment_type_cd:'') as default_online_fulfillment_type_cd
,((LOAD_ONLINE_FULFILLMENT::item_id != '' AND LOAD_ONLINE_FULFILLMENT::item_id IS NOT NULL ) ? LOAD_ONLINE_FULFILLMENT::load_ts:'') as default_online_ts 
,NEW_COLS_NEXT_0::demand_online_fulfillment_cd as demand_online_fulfillment_cd 
,NEW_COLS_NEXT_0::temporary_ups_billable_weight as temporary_ups_billable_weight 
,NEW_COLS_NEXT_0::ups_billable_weight as ups_billable_weight 
,NEW_COLS_NEXT_0::ups_billable_weight_ts as ups_billable_weight_ts 
,NEW_COLS_NEXT_0::demand_ups_billable_weight as demand_ups_billable_weight 
,NEW_COLS_NEXT_0::web_exclusive_ind AS web_exclusive_ind,
NEW_COLS_NEXT_0::price_type_desc AS price_type_desc,
NEW_COLS_NEXT_0::idrp_batch_id AS idrp_batch_id;
----------------------------------------------------------------------------------------------------------------------------

-- Coding for US DOT SHIP TYPE CODE


smith__idrp_new_eligible_item_new = DISTINCT NEW_COLS_NEXT;


gold__item_package_current_data_reqd =     FOREACH LOAD_PACKAGE_CURRENT    GENERATE        ksn_id AS ksn_id,        us_dot_ship_type_cd AS us_dot_ship_type_cd;

gold__item_shc_hierarchy_current_data_reqd =     FOREACH LOAD_GOLD_ITEM    GENERATE         ksn_id AS ksn_id,         item_id AS item_id;

/*************************************/
filter_item_table_by_temporary_online_fulfillment_type_cd =     FILTER smith__idrp_new_eligible_item_new     BY (default_online_fulfillment_type_cd IS  NULL OR default_online_fulfillment_type_cd == '');

/****** i.	Join gold__item_shc_hierarchy_current to gold__item_package_current on ksn_id ******/
--LOAD shc_hierarchy and left join to item table 
join_item_to_shc_hierarchy_current = 
    JOIN filter_item_table_by_temporary_online_fulfillment_type_cd BY item LEFT OUTER,
         gold__item_shc_hierarchy_current_data_reqd BY item_id ;

join_shc_hierarchy_current_package_current =
    JOIN join_item_to_shc_hierarchy_current BY ksn_id,
         gold__item_package_current_data_reqd BY ksn_id;

generate_join_shc_hierarchy_current_package_current  = 
    FOREACH join_shc_hierarchy_current_package_current
	GENERATE 
	load_ts AS load_ts,    
        item AS item_id,
        descr AS descr,
        shc_dvsn_no AS shc_dvsn_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dept_no AS shc_dept_no,
        shc_dept_nm AS shc_dept_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_no AS shc_cat_no,
        shc_cat_nm AS shc_cat_nm,
        shc_subcat_no AS shc_subcat_no,
        shc_subcat_nm AS shc_subcat_nm,
        ref_ksn_id AS ref_ksn_id,
        srs_bus_no AS srs_bus_no,
        srs_bus_nm AS srs_bus_nm,
        srs_div_no AS srs_div_no,
        srs_div_nm AS srs_div_nm,
        srs_ln_no AS srs_ln_no,
        srs_ln_ds AS srs_ln_ds,
        srs_sbl_no AS srs_sbl_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_cls_no AS srs_cls_no,
        srs_cls_ds AS srs_cls_ds,
        srs_itm_no AS srs_itm_no,
        srs_sku_no AS srs_sku_no,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        vnd_no AS vnd_no,
        vnd_nm AS vnd_nm,
        vnd_itm_no AS vnd_itm_no,
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        easy_ord_fl AS easy_ord_fl,
        itm_rpd_fl AS itm_rpd_fl,
        itm_cd_fl AS itm_cd_fl,
        itm_imp_fl AS itm_imp_fl,
        itm_cs_fl AS itm_cs_fl,
        dd_ind AS dd_ind,
        instl_ind AS instl_ind,
		cheetah_elgbl_fl AS cheetah_elgbl_fl,
        dot_com_cd AS dot_com_cd,
        obn_830_fl AS obn_830_fl,
        obn_830_dur AS obn_830_dur,
        rpd_frz_dur AS rpd_frz_dur,
        dist_typ_cd AS dist_typ_cd,
        sls_pfm_seg_cd AS sls_pfm_seg_cd,
        fmt_excl_cd AS fmt_excl_cd,
        str_fcst_cd AS str_fcst_cd,
        inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
		cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        cross_mdse_attr_cd AS cross_mdse_attr_cd,
        whse_sizing AS whse_sizing,
        can_carr_mdl_id AS can_carr_mdl_id,
        groc_crossover_ind AS groc_crossover_ind,
        owner_cd AS owner_cd,
        pln_id AS pln_id,
        itm_pgm AS itm_pgm,
        key_pgm AS key_pgm,
        natl_un_cst_am AS natl_un_cst_am,
        prd_sll_am AS prd_sll_am,
        size AS size,
        style AS style,
        md_style_ref_cd AS md_style_ref_cd,
        seas_cd AS seas_cd,
        seas_yr AS seas_yr,
        sub_seas_id AS sub_seas_id,
        rpt_id AS rpt_id,
        rpt_id_seq_no AS rpt_id_seq_no,
        itm_fcst_grp_id AS itm_fcst_grp_id,
        itm_fcst_grp_ds AS itm_fcst_grp_ds,
        idrp_itm_typ_ds AS idrp_itm_typ_ds,
        brand_ds AS brand_ds,
        color_ds AS color_ds,
        tire_size_ds AS tire_size_ds,
        elig_sts_cd AS elig_sts_cd,
        lst_sts_chg_dt AS lst_sts_chg_dt,
        itm_del_fl AS itm_del_fl,
        prd_prg_dt AS prd_prg_dt,
        order_system_cd AS order_system_cd,
        idrp_order_method_cd AS idrp_order_method_cd,
        dotcom_assorted_cd AS dotcom_assorted_cd,
        dotcom_orderable_ind AS dotcom_orderable_ind,
        roadrunner_eligible_fl AS roadrunner_eligible_fl,
        gold__item_package_current_data_reqd::us_dot_ship_type_cd AS us_dot_ship_type_cd,
        package_weight_in_pounds AS package_weight_in_pounds,
        package_depth_inch_qty AS package_depth_inch_qty,
        package_height_inch_qty AS package_height_inch_qty,
        package_width_inch_qty AS package_width_inch_qty,
        mailable_ind AS mailable_ind,
        temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
        default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
        default_online_ts AS default_online_ts,
        demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
        temporary_ups_billable_weight AS temporary_ups_billable_weight,
        ups_billable_weight AS ups_billable_weight,
        ups_billable_weight_ts AS ups_billable_weight_ts,
        demand_ups_billable_weight AS demand_ups_billable_weight,
         web_exclusive_ind AS web_exclusive_ind,
price_type_desc AS price_type_desc,
idrp_batch_id AS idrp_batch_id ,   
        (gold__item_package_current_data_reqd::us_dot_ship_type_cd IS NULL ? '1' : (gold__item_package_current_data_reqd::us_dot_ship_type_cd == 'H' ? '3' : (gold__item_package_current_data_reqd::us_dot_ship_type_cd == 'S' ? '2' : '1'))) AS severity,
        gold__item_package_current_data_reqd::ksn_id AS ksn_id;


order_data_by_severity =     ORDER generate_join_shc_hierarchy_current_package_current 	      BY item_id ASC , severity DESC ;

distinct_order_data_by_severity =     DISTINCT order_data_by_severity;
	
grp_distinct_order_data_by_severity =     GROUP distinct_order_data_by_severity 	BY item_id;

generate_valid_and_invalid_items = 	    FOREACH grp_distinct_order_data_by_severity
    GENERATE         group AS item ,
		com.searshc.supplychain.idrp.udf.HasMultipleValues(distinct_order_data_by_severity.us_dot_ship_type_cd) AS error_value;		

join_with_actual_data =     JOIN generate_valid_and_invalid_items BY item,	     distinct_order_data_by_severity BY item_id;

SPLIT join_with_actual_data     INTO invalid_records IF error_value == 'MULTIPLE' ,	     valid_records IF error_value != 'MULTIPLE';
group_valid_records_by_item =     GROUP valid_records 	      BY item_id;

flatten_valid_records =     FOREACH group_valid_records_by_item
	{
	    order_data = ORDER $1 BY severity DESC ;
		limit_data = LIMIT order_data 1;
		GENERATE FLATTEN (limit_data);
	};   
        
flatten_valid_records = 
    FOREACH flatten_valid_records
    GENERATE 
	limit_data::distinct_order_data_by_severity::load_ts AS load_ts,
        limit_data::distinct_order_data_by_severity::item_id AS item,
        limit_data::distinct_order_data_by_severity::descr AS descr,
        limit_data::distinct_order_data_by_severity::shc_dvsn_no AS shc_dvsn_no,
        limit_data::distinct_order_data_by_severity::shc_dvsn_nm AS shc_dvsn_nm,
        limit_data::distinct_order_data_by_severity::shc_dept_no AS shc_dept_no,
        limit_data::distinct_order_data_by_severity::shc_dept_nm AS shc_dept_nm,
        limit_data::distinct_order_data_by_severity::shc_cat_grp_no AS shc_cat_grp_no,
        limit_data::distinct_order_data_by_severity::shc_cat_grp_nm AS shc_cat_grp_nm,
        limit_data::distinct_order_data_by_severity::shc_cat_no AS shc_cat_no,
        limit_data::distinct_order_data_by_severity::shc_cat_nm AS shc_cat_nm,
        limit_data::distinct_order_data_by_severity::shc_subcat_no AS shc_subcat_no,
        limit_data::distinct_order_data_by_severity::shc_subcat_nm AS shc_subcat_nm,
        limit_data::distinct_order_data_by_severity::ref_ksn_id AS ref_ksn_id,
        limit_data::distinct_order_data_by_severity::srs_bus_no AS srs_bus_no,
        limit_data::distinct_order_data_by_severity::srs_bus_nm AS srs_bus_nm,
        limit_data::distinct_order_data_by_severity::srs_div_no AS srs_div_no,
        limit_data::distinct_order_data_by_severity::srs_div_nm AS srs_div_nm,
        limit_data::distinct_order_data_by_severity::srs_ln_no AS srs_ln_no,
        limit_data::distinct_order_data_by_severity::srs_ln_ds AS srs_ln_ds,
        limit_data::distinct_order_data_by_severity::srs_sbl_no AS srs_sbl_no,
        limit_data::distinct_order_data_by_severity::srs_sbl_ds AS srs_sbl_ds,
        limit_data::distinct_order_data_by_severity::srs_cls_no AS srs_cls_no,
        limit_data::distinct_order_data_by_severity::srs_cls_ds AS srs_cls_ds,
        limit_data::distinct_order_data_by_severity::srs_itm_no AS srs_itm_no,
        limit_data::distinct_order_data_by_severity::srs_sku_no AS srs_sku_no,
        limit_data::distinct_order_data_by_severity::srs_div_itm AS srs_div_itm,
        limit_data::distinct_order_data_by_severity::srs_div_itm_sku AS srs_div_itm_sku,
        limit_data::distinct_order_data_by_severity::ima_smt_itm_no AS ima_smt_itm_no,
        limit_data::distinct_order_data_by_severity::ima_smt_itm_ds AS ima_smt_itm_ds,
        limit_data::distinct_order_data_by_severity::ima_smt_fac_qt AS ima_smt_fac_qt,
        limit_data::distinct_order_data_by_severity::uom AS uom,
        limit_data::distinct_order_data_by_severity::vol AS vol,
        limit_data::distinct_order_data_by_severity::wgt AS wgt,
        limit_data::distinct_order_data_by_severity::vnd_no AS vnd_no,
        limit_data::distinct_order_data_by_severity::vnd_nm AS vnd_nm,
        limit_data::distinct_order_data_by_severity::vnd_itm_no AS vnd_itm_no,
        limit_data::distinct_order_data_by_severity::spc_ord_cdt_fl AS spc_ord_cdt_fl,
        limit_data::distinct_order_data_by_severity::itm_emp_fl AS itm_emp_fl,
        limit_data::distinct_order_data_by_severity::easy_ord_fl AS easy_ord_fl,
        limit_data::distinct_order_data_by_severity::itm_rpd_fl AS itm_rpd_fl,
        limit_data::distinct_order_data_by_severity::itm_cd_fl AS itm_cd_fl,
        limit_data::distinct_order_data_by_severity::itm_imp_fl AS itm_imp_fl,
        limit_data::distinct_order_data_by_severity::itm_cs_fl AS itm_cs_fl,
        limit_data::distinct_order_data_by_severity::dd_ind AS dd_ind,
        limit_data::distinct_order_data_by_severity::instl_ind AS instl_ind,
		limit_data::distinct_order_data_by_severity::cheetah_elgbl_fl AS cheetah_elgbl_fl,
        limit_data::distinct_order_data_by_severity::dot_com_cd AS dot_com_cd,
        limit_data::distinct_order_data_by_severity::obn_830_fl AS obn_830_fl,
        limit_data::distinct_order_data_by_severity::obn_830_dur AS obn_830_dur,
        limit_data::distinct_order_data_by_severity::rpd_frz_dur AS rpd_frz_dur,
        limit_data::distinct_order_data_by_severity::dist_typ_cd AS dist_typ_cd,
        limit_data::distinct_order_data_by_severity::sls_pfm_seg_cd AS sls_pfm_seg_cd,
        limit_data::distinct_order_data_by_severity::fmt_excl_cd AS fmt_excl_cd,
        limit_data::distinct_order_data_by_severity::str_fcst_cd AS str_fcst_cd,
        limit_data::distinct_order_data_by_severity::inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
        limit_data::distinct_order_data_by_severity::ima_itm_typ_cd AS ima_itm_typ_cd,
        limit_data::distinct_order_data_by_severity::itm_purch_sts_cd AS itm_purch_sts_cd,
        limit_data::distinct_order_data_by_severity::ntwk_dist_cd AS ntwk_dist_cd,
        limit_data::distinct_order_data_by_severity::fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        limit_data::distinct_order_data_by_severity::fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        limit_data::distinct_order_data_by_severity::jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
		limit_data::distinct_order_data_by_severity::cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
        limit_data::distinct_order_data_by_severity::str_reord_auth_cd AS str_reord_auth_cd,
        limit_data::distinct_order_data_by_severity::cross_mdse_attr_cd AS cross_mdse_attr_cd,
        limit_data::distinct_order_data_by_severity::whse_sizing AS whse_sizing,
        limit_data::distinct_order_data_by_severity::can_carr_mdl_id AS can_carr_mdl_id,
        limit_data::distinct_order_data_by_severity::groc_crossover_ind AS groc_crossover_ind,
        limit_data::distinct_order_data_by_severity::owner_cd AS owner_cd,
        limit_data::distinct_order_data_by_severity::pln_id AS pln_id,
        limit_data::distinct_order_data_by_severity::itm_pgm AS itm_pgm,
        limit_data::distinct_order_data_by_severity::key_pgm AS key_pgm,
        limit_data::distinct_order_data_by_severity::natl_un_cst_am AS natl_un_cst_am,
        limit_data::distinct_order_data_by_severity::prd_sll_am AS prd_sll_am,
        limit_data::distinct_order_data_by_severity::size AS size,
        limit_data::distinct_order_data_by_severity::style AS style,
        limit_data::distinct_order_data_by_severity::md_style_ref_cd AS md_style_ref_cd,
        limit_data::distinct_order_data_by_severity::seas_cd AS seas_cd,
        limit_data::distinct_order_data_by_severity::seas_yr AS seas_yr,
        limit_data::distinct_order_data_by_severity::sub_seas_id AS sub_seas_id,
        limit_data::distinct_order_data_by_severity::rpt_id AS rpt_id,
        limit_data::distinct_order_data_by_severity::rpt_id_seq_no AS rpt_id_seq_no,
        limit_data::distinct_order_data_by_severity::itm_fcst_grp_id AS itm_fcst_grp_id,
        limit_data::distinct_order_data_by_severity::itm_fcst_grp_ds AS itm_fcst_grp_ds,
        limit_data::distinct_order_data_by_severity::idrp_itm_typ_ds AS idrp_itm_typ_ds,
        limit_data::distinct_order_data_by_severity::brand_ds AS brand_ds,
        limit_data::distinct_order_data_by_severity::color_ds AS color_ds,
        limit_data::distinct_order_data_by_severity::tire_size_ds AS tire_size_ds,
        limit_data::distinct_order_data_by_severity::elig_sts_cd AS elig_sts_cd,
        limit_data::distinct_order_data_by_severity::lst_sts_chg_dt AS lst_sts_chg_dt,
        limit_data::distinct_order_data_by_severity::itm_del_fl AS itm_del_fl,
        limit_data::distinct_order_data_by_severity::prd_prg_dt AS prd_prg_dt,
        limit_data::distinct_order_data_by_severity::order_system_cd AS order_system_cd,
        limit_data::distinct_order_data_by_severity::idrp_order_method_cd AS idrp_order_method_cd,
        limit_data::distinct_order_data_by_severity::dotcom_assorted_cd AS dotcom_assorted_cd,
        limit_data::distinct_order_data_by_severity::dotcom_orderable_ind AS dotcom_orderable_ind,
        limit_data::distinct_order_data_by_severity::roadrunner_eligible_fl AS roadrunner_eligible_fl,
        limit_data::distinct_order_data_by_severity::us_dot_ship_type_cd AS us_dot_ship_type_cd,
        limit_data::distinct_order_data_by_severity::package_weight_in_pounds AS package_weight_in_pounds,
        limit_data::distinct_order_data_by_severity::package_depth_inch_qty AS package_depth_inch_qty,
        limit_data::distinct_order_data_by_severity::package_height_inch_qty AS package_height_inch_qty,
        limit_data::distinct_order_data_by_severity::package_width_inch_qty AS package_width_inch_qty,
        limit_data::distinct_order_data_by_severity::mailable_ind AS mailable_ind,
        limit_data::distinct_order_data_by_severity::temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
        limit_data::distinct_order_data_by_severity::default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
        limit_data::distinct_order_data_by_severity::default_online_ts AS default_online_ts,
        limit_data::distinct_order_data_by_severity::demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
        limit_data::distinct_order_data_by_severity::temporary_ups_billable_weight AS temporary_ups_billable_weight,
        limit_data::distinct_order_data_by_severity::ups_billable_weight AS ups_billable_weight,
        limit_data::distinct_order_data_by_severity::ups_billable_weight_ts AS ups_billable_weight_ts,
        limit_data::distinct_order_data_by_severity::demand_ups_billable_weight AS demand_ups_billable_weight,
		limit_data::distinct_order_data_by_severity::web_exclusive_ind AS web_exclusive_ind,
limit_data::distinct_order_data_by_severity::price_type_desc AS price_type_desc,
limit_data::distinct_order_data_by_severity::idrp_batch_id AS idrp_batch_id;
        
		
join_again_with_item =     
    JOIN smith__idrp_new_eligible_item_new BY item 
         LEFT OUTER ,
         flatten_valid_records BY item ;
generate_us_dot_ship_typ = 
    FOREACH join_again_with_item
    GENERATE
	smith__idrp_new_eligible_item_new::load_ts AS load_ts,
        smith__idrp_new_eligible_item_new::item AS item,
        smith__idrp_new_eligible_item_new::descr AS descr,
        smith__idrp_new_eligible_item_new::shc_dvsn_no AS shc_dvsn_no,
        smith__idrp_new_eligible_item_new::shc_dvsn_nm AS shc_dvsn_nm,
        smith__idrp_new_eligible_item_new::shc_dept_no AS shc_dept_no,
        smith__idrp_new_eligible_item_new::shc_dept_nm AS shc_dept_nm,
        smith__idrp_new_eligible_item_new::shc_cat_grp_no AS shc_cat_grp_no,
        smith__idrp_new_eligible_item_new::shc_cat_grp_nm AS shc_cat_grp_nm,
        smith__idrp_new_eligible_item_new::shc_cat_no AS shc_cat_no,
        smith__idrp_new_eligible_item_new::shc_cat_nm AS shc_cat_nm,
        smith__idrp_new_eligible_item_new::shc_subcat_no AS shc_subcat_no,
        smith__idrp_new_eligible_item_new::shc_subcat_nm AS shc_subcat_nm,
        smith__idrp_new_eligible_item_new::ref_ksn_id AS ref_ksn_id,
        smith__idrp_new_eligible_item_new::srs_bus_no AS srs_bus_no,
        smith__idrp_new_eligible_item_new::srs_bus_nm AS srs_bus_nm,
        smith__idrp_new_eligible_item_new::srs_div_no AS srs_div_no,
        smith__idrp_new_eligible_item_new::srs_div_nm AS srs_div_nm,
        smith__idrp_new_eligible_item_new::srs_ln_no AS srs_ln_no,
        smith__idrp_new_eligible_item_new::srs_ln_ds AS srs_ln_ds,
        smith__idrp_new_eligible_item_new::srs_sbl_no AS srs_sbl_no,
        smith__idrp_new_eligible_item_new::srs_sbl_ds AS srs_sbl_ds,
        smith__idrp_new_eligible_item_new::srs_cls_no AS srs_cls_no,
        smith__idrp_new_eligible_item_new::srs_cls_ds AS srs_cls_ds,
        smith__idrp_new_eligible_item_new::srs_itm_no AS srs_itm_no,
        smith__idrp_new_eligible_item_new::srs_sku_no AS srs_sku_no,
        smith__idrp_new_eligible_item_new::srs_div_itm AS srs_div_itm,
        smith__idrp_new_eligible_item_new::srs_div_itm_sku AS srs_div_itm_sku,
        smith__idrp_new_eligible_item_new::ima_smt_itm_no AS ima_smt_itm_no,
        smith__idrp_new_eligible_item_new::ima_smt_itm_ds AS ima_smt_itm_ds,
        smith__idrp_new_eligible_item_new::ima_smt_fac_qt AS ima_smt_fac_qt,
        smith__idrp_new_eligible_item_new::uom AS uom,
        smith__idrp_new_eligible_item_new::vol AS vol,
        smith__idrp_new_eligible_item_new::wgt AS wgt,
        smith__idrp_new_eligible_item_new::vnd_no AS vnd_no,
        smith__idrp_new_eligible_item_new::vnd_nm AS vnd_nm,
        smith__idrp_new_eligible_item_new::vnd_itm_no AS vnd_itm_no,
        smith__idrp_new_eligible_item_new::spc_ord_cdt_fl AS spc_ord_cdt_fl,
        smith__idrp_new_eligible_item_new::itm_emp_fl AS itm_emp_fl,
        smith__idrp_new_eligible_item_new::easy_ord_fl AS easy_ord_fl,
        smith__idrp_new_eligible_item_new::itm_rpd_fl AS itm_rpd_fl,
        smith__idrp_new_eligible_item_new::itm_cd_fl AS itm_cd_fl,
        smith__idrp_new_eligible_item_new::itm_imp_fl AS itm_imp_fl,
        smith__idrp_new_eligible_item_new::itm_cs_fl AS itm_cs_fl,
        smith__idrp_new_eligible_item_new::dd_ind AS dd_ind,
        smith__idrp_new_eligible_item_new::instl_ind AS instl_ind,
		smith__idrp_new_eligible_item_new::cheetah_elgbl_fl AS cheetah_elgbl_fl,
        smith__idrp_new_eligible_item_new::dot_com_cd AS dot_com_cd,
        smith__idrp_new_eligible_item_new::obn_830_fl AS obn_830_fl,
        smith__idrp_new_eligible_item_new::obn_830_dur AS obn_830_dur,
        smith__idrp_new_eligible_item_new::rpd_frz_dur AS rpd_frz_dur,
        smith__idrp_new_eligible_item_new::dist_typ_cd AS dist_typ_cd,
        smith__idrp_new_eligible_item_new::sls_pfm_seg_cd AS sls_pfm_seg_cd,
        smith__idrp_new_eligible_item_new::fmt_excl_cd AS fmt_excl_cd,
        smith__idrp_new_eligible_item_new::str_fcst_cd AS str_fcst_cd,
        smith__idrp_new_eligible_item_new::inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
        smith__idrp_new_eligible_item_new::ima_itm_typ_cd AS ima_itm_typ_cd,
        smith__idrp_new_eligible_item_new::itm_purch_sts_cd AS itm_purch_sts_cd,
        smith__idrp_new_eligible_item_new::ntwk_dist_cd AS ntwk_dist_cd,
        smith__idrp_new_eligible_item_new::fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        smith__idrp_new_eligible_item_new::fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        smith__idrp_new_eligible_item_new::jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
		smith__idrp_new_eligible_item_new::cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
        smith__idrp_new_eligible_item_new::str_reord_auth_cd AS str_reord_auth_cd,
        smith__idrp_new_eligible_item_new::cross_mdse_attr_cd AS cross_mdse_attr_cd,
        smith__idrp_new_eligible_item_new::whse_sizing AS whse_sizing,
        smith__idrp_new_eligible_item_new::can_carr_mdl_id AS can_carr_mdl_id,
        smith__idrp_new_eligible_item_new::groc_crossover_ind AS groc_crossover_ind,
        smith__idrp_new_eligible_item_new::owner_cd AS owner_cd,
        smith__idrp_new_eligible_item_new::pln_id AS pln_id,
        smith__idrp_new_eligible_item_new::itm_pgm AS itm_pgm,
        smith__idrp_new_eligible_item_new::key_pgm AS key_pgm,
        smith__idrp_new_eligible_item_new::natl_un_cst_am AS natl_un_cst_am,
        smith__idrp_new_eligible_item_new::prd_sll_am AS prd_sll_am,
        smith__idrp_new_eligible_item_new::size AS size,
        smith__idrp_new_eligible_item_new::style AS style,
        smith__idrp_new_eligible_item_new::md_style_ref_cd AS md_style_ref_cd,
        smith__idrp_new_eligible_item_new::seas_cd AS seas_cd,
        smith__idrp_new_eligible_item_new::seas_yr AS seas_yr,
        smith__idrp_new_eligible_item_new::sub_seas_id AS sub_seas_id,
        smith__idrp_new_eligible_item_new::rpt_id AS rpt_id,
        smith__idrp_new_eligible_item_new::rpt_id_seq_no AS rpt_id_seq_no,
        smith__idrp_new_eligible_item_new::itm_fcst_grp_id AS itm_fcst_grp_id,
        smith__idrp_new_eligible_item_new::itm_fcst_grp_ds AS itm_fcst_grp_ds,
        smith__idrp_new_eligible_item_new::idrp_itm_typ_ds AS idrp_itm_typ_ds,
        smith__idrp_new_eligible_item_new::brand_ds AS brand_ds,
        smith__idrp_new_eligible_item_new::color_ds AS color_ds,
        smith__idrp_new_eligible_item_new::tire_size_ds AS tire_size_ds,
        smith__idrp_new_eligible_item_new::elig_sts_cd AS elig_sts_cd,
        smith__idrp_new_eligible_item_new::lst_sts_chg_dt AS lst_sts_chg_dt,
        smith__idrp_new_eligible_item_new::itm_del_fl AS itm_del_fl,
        smith__idrp_new_eligible_item_new::prd_prg_dt AS prd_prg_dt,
        smith__idrp_new_eligible_item_new::order_system_cd AS order_system_cd,
        smith__idrp_new_eligible_item_new::idrp_order_method_cd AS idrp_order_method_cd,
        smith__idrp_new_eligible_item_new::dotcom_assorted_cd AS dotcom_assorted_cd,
        smith__idrp_new_eligible_item_new::dotcom_orderable_ind AS dotcom_orderable_ind,
        smith__idrp_new_eligible_item_new::roadrunner_eligible_fl AS roadrunner_eligible_fl,
        flatten_valid_records::us_dot_ship_type_cd AS us_dot_ship_type_cd,
        smith__idrp_new_eligible_item_new::package_weight_in_pounds AS package_weight_in_pounds,
        smith__idrp_new_eligible_item_new::package_depth_inch_qty AS package_depth_inch_qty,
        smith__idrp_new_eligible_item_new::package_height_inch_qty AS package_height_inch_qty,
        smith__idrp_new_eligible_item_new::package_width_inch_qty AS package_width_inch_qty,
        smith__idrp_new_eligible_item_new::mailable_ind AS mailable_ind,
        smith__idrp_new_eligible_item_new::temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
        smith__idrp_new_eligible_item_new::default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
        smith__idrp_new_eligible_item_new::default_online_ts AS default_online_ts,
        smith__idrp_new_eligible_item_new::demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
        smith__idrp_new_eligible_item_new::temporary_ups_billable_weight AS temporary_ups_billable_weight,
        smith__idrp_new_eligible_item_new::ups_billable_weight AS ups_billable_weight,
        smith__idrp_new_eligible_item_new::ups_billable_weight_ts AS ups_billable_weight_ts,
        smith__idrp_new_eligible_item_new::demand_ups_billable_weight AS demand_ups_billable_weight,
        smith__idrp_new_eligible_item_new::web_exclusive_ind AS web_exclusive_ind,
smith__idrp_new_eligible_item_new::price_type_desc AS price_type_desc,
smith__idrp_new_eligible_item_new::idrp_batch_id AS idrp_batch_id;

/**** FOR INVALID OR ERROR DATA ***/

error_file_1 = 
    FOREACH invalid_records
    GENERATE
        '$CURRENT_TIMESTAMP' AS load_ts, 
        generate_valid_and_invalid_items::item AS item_id,
        distinct_order_data_by_severity::ksn_id AS ksn_id,
        NULL AS sears_division_nbr,
        NULL AS sears_item_nbr,
        NULL AS sears_sku_nbr,
        NULL AS websku,
        NULL AS package_id,
        distinct_order_data_by_severity::us_dot_ship_type_cd AS error_value,
        'Multiple US DOT Ship Type Code values were found for an Item' AS error_desc,
		'$batchid' AS idrp_batch_id;
/************ STORING VALID AND ERROR OUTPUT ****************************/
--------------------------------------------------------------------------------------------------------------------------------------

-- Logic for Mailable Indicator

SPLIT generate_us_dot_ship_typ INTO prev_data IF (default_online_fulfillment_type_cd IS NULL OR default_online_fulfillment_type_cd == ''), D11 IF 
(default_online_fulfillment_type_cd IS NOT NULL AND default_online_fulfillment_type_cd !='') ;

LOAD_GOLD_ITEM_NEW = FOREACH LOAD_GOLD_ITEM GENERATE
(item_id is NULL ? '':item_id) as item_id,
(ksn_purchase_status_cd is NULL ? '': ksn_purchase_status_cd) as ksn_purchase_status_cd,
(dotcom_eligibility_cd is NULL ? '': dotcom_eligibility_cd) as dotcom_eligibility_cd,
(ksn_id is NULL ? '': ksn_id) as ksn_id ;

GOLD_ITEM = FILTER LOAD_GOLD_ITEM_NEW BY ksn_purchase_status_cd  != 'U' AND dotcom_eligibility_cd  == '1' ;

join_gold_data_prev_data = JOIN prev_data BY item LEFT OUTER, GOLD_ITEM BY item_id ;

join_gold_data_prev_data_gen = FOREACH join_gold_data_prev_data GENERATE 
                                       prev_data::load_ts AS load_ts,
				       item AS item,
                                       prev_data::descr AS descr,
                                       prev_data::shc_dvsn_no AS shc_dvsn_no,
                                       prev_data::shc_dvsn_nm AS shc_dvsn_nm,
                                       prev_data::shc_dept_no AS shc_dept_no,
                                       prev_data::shc_dept_nm AS shc_dept_nm,
                                       prev_data::shc_cat_grp_no AS shc_cat_grp_no,
                                       prev_data::shc_cat_grp_nm AS shc_cat_grp_nm,
                                       prev_data::shc_cat_no AS shc_cat_no,
                                       prev_data::shc_cat_nm AS shc_cat_nm,
                                       prev_data::shc_subcat_no AS shc_subcat_no,
                                       prev_data::shc_subcat_nm AS shc_subcat_nm,
                                       prev_data::ref_ksn_id AS ref_ksn_id,
                                       prev_data::srs_bus_no AS srs_bus_no,
                                       prev_data::srs_bus_nm AS srs_bus_nm,
                                       prev_data::srs_div_no AS srs_div_no,
                                       prev_data::srs_div_nm AS srs_div_nm,
                                       prev_data::srs_ln_no AS srs_ln_no,
                                       prev_data::srs_ln_ds AS srs_ln_ds,
                                       prev_data::srs_sbl_no AS srs_sbl_no,
                                       prev_data::srs_sbl_ds AS srs_sbl_ds,
                                       prev_data::srs_cls_no AS srs_cls_no,
                                       prev_data::srs_cls_ds AS srs_cls_ds,
                                       prev_data::srs_itm_no AS srs_itm_no,
                                       prev_data::srs_sku_no AS srs_sku_no,
                                       prev_data::srs_div_itm AS srs_div_itm,
                                       prev_data::srs_div_itm_sku AS srs_div_itm_sku,
                                       prev_data::ima_smt_itm_no AS ima_smt_itm_no,
                                       prev_data::ima_smt_itm_ds AS ima_smt_itm_ds,
                                       prev_data::ima_smt_fac_qt AS ima_smt_fac_qt,
                                       prev_data::uom AS uom,
                                       prev_data::vol AS vol,
                                       prev_data::wgt AS wgt,
                                       prev_data::vnd_no AS vnd_no,
                                       prev_data::vnd_nm AS vnd_nm,
                                       prev_data::vnd_itm_no AS vnd_itm_no,
                                       prev_data::spc_ord_cdt_fl AS spc_ord_cdt_fl,
                                       prev_data::itm_emp_fl AS itm_emp_fl,
                                       prev_data::easy_ord_fl AS easy_ord_fl,
                                       prev_data::itm_rpd_fl AS itm_rpd_fl,
                                       prev_data::itm_cd_fl AS itm_cd_fl,
                                       prev_data::itm_imp_fl AS itm_imp_fl,
                                       prev_data::itm_cs_fl AS itm_cs_fl,
                                       prev_data::dd_ind AS dd_ind,
                                       prev_data::instl_ind AS instl_ind,
									   prev_data::cheetah_elgbl_fl AS cheetah_elgbl_fl,
                                       prev_data::dot_com_cd AS dot_com_cd,
                                       prev_data::obn_830_fl AS obn_830_fl,
                                       prev_data::obn_830_dur AS obn_830_dur,
                                       prev_data::rpd_frz_dur AS rpd_frz_dur,
                                       prev_data::dist_typ_cd AS dist_typ_cd,
                                       prev_data::sls_pfm_seg_cd AS sls_pfm_seg_cd,
                                       prev_data::fmt_excl_cd AS fmt_excl_cd,
                                       prev_data::str_fcst_cd AS str_fcst_cd,
                                       prev_data::inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
                                       prev_data::ima_itm_typ_cd AS ima_itm_typ_cd,
                                       prev_data::itm_purch_sts_cd AS itm_purch_sts_cd,
                                       prev_data::ntwk_dist_cd AS ntwk_dist_cd,
                                       prev_data::fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
                                       prev_data::fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
                                       prev_data::jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
									   prev_data::cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
                                       prev_data::str_reord_auth_cd AS str_reord_auth_cd,
                                       prev_data::cross_mdse_attr_cd AS cross_mdse_attr_cd,
                                       prev_data::whse_sizing AS whse_sizing,
                                       prev_data::can_carr_mdl_id AS can_carr_mdl_id,
                                       prev_data::groc_crossover_ind AS groc_crossover_ind,
                                       prev_data::owner_cd AS owner_cd,
                                       prev_data::pln_id AS pln_id,
                                       prev_data::itm_pgm AS itm_pgm,
                                       prev_data::key_pgm AS key_pgm,
                                       prev_data::natl_un_cst_am AS natl_un_cst_am,
                                       prev_data::prd_sll_am AS prd_sll_am,
                                       prev_data::size AS size,
                                       prev_data::style AS style,
                                       prev_data::md_style_ref_cd AS md_style_ref_cd,
                                       prev_data::seas_cd AS seas_cd,
                                       prev_data::seas_yr AS seas_yr,
                                       prev_data::sub_seas_id AS sub_seas_id,
                                       prev_data::rpt_id AS rpt_id,
                                       prev_data::rpt_id_seq_no AS rpt_id_seq_no,
                                       prev_data::itm_fcst_grp_id AS itm_fcst_grp_id,
                                       prev_data::itm_fcst_grp_ds AS itm_fcst_grp_ds,
                                       prev_data::idrp_itm_typ_ds AS idrp_itm_typ_ds,
                                       prev_data::brand_ds AS brand_ds,
                                       prev_data::color_ds AS color_ds,
                                       prev_data::tire_size_ds AS tire_size_ds,
                                       prev_data::elig_sts_cd AS elig_sts_cd,
                                       prev_data::lst_sts_chg_dt AS lst_sts_chg_dt,
                                       prev_data::itm_del_fl AS itm_del_fl,
                                       prev_data::prd_prg_dt AS prd_prg_dt,
                                       prev_data::order_system_cd AS order_system_cd,
                                       prev_data::idrp_order_method_cd AS idrp_order_method_cd,
                                       prev_data::dotcom_assorted_cd AS dotcom_assorted_cd,
                                       prev_data::dotcom_orderable_ind AS dotcom_orderable_ind,
                                       prev_data::roadrunner_eligible_fl AS roadrunner_eligible_fl,
                                       prev_data::us_dot_ship_type_cd AS us_dot_ship_type_cd,
                                       (prev_data::package_weight_in_pounds is NULL ? '':prev_data::package_weight_in_pounds) AS package_weight_in_pounds,
                                       (prev_data::package_depth_inch_qty is NULL ? '':prev_data::package_depth_inch_qty) AS package_depth_inch_qty,
                                       (prev_data::package_height_inch_qty is NULL ? '':prev_data::package_height_inch_qty) AS package_height_inch_qty,
                                       (prev_data::package_width_inch_qty is NULL ? '':prev_data::package_width_inch_qty )AS package_width_inch_qty,
                                       prev_data::mailable_ind AS mailable_ind,
                                       prev_data::temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
                                       prev_data::default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
                                       prev_data::default_online_ts AS default_online_ts,
                                       prev_data::demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
                                       prev_data::temporary_ups_billable_weight AS temporary_ups_billable_weight,
                                       prev_data::ups_billable_weight AS ups_billable_weight,
                                       prev_data::ups_billable_weight_ts AS ups_billable_weight_ts,
                                       prev_data::demand_ups_billable_weight AS demand_ups_billable_weight,
									   prev_data::web_exclusive_ind AS web_exclusive_ind,
										prev_data::price_type_desc AS price_type_desc,
										prev_data::idrp_batch_id AS idrp_batch_id,
                                       GOLD_ITEM::ksn_id AS ksn_id;
                            

join_gold_data_prev_data_gold_item_pack = JOIN  join_gold_data_prev_data_gen BY ksn_id LEFT OUTER, LOAD_PACKAGE_CURRENT BY ksn_id ;


join_gold_data_prev_data_gold_item_pack_gen = FOREACH join_gold_data_prev_data_gold_item_pack GENERATE 
													join_gold_data_prev_data_gen::load_ts AS load_ts,
                                                      join_gold_data_prev_data_gen::item AS item,
                                                      join_gold_data_prev_data_gen::descr AS descr,
                                                      join_gold_data_prev_data_gen::shc_dvsn_no AS shc_dvsn_no,
                                                      join_gold_data_prev_data_gen::shc_dvsn_nm AS shc_dvsn_nm,
                                                      join_gold_data_prev_data_gen::shc_dept_no AS shc_dept_no,
                                                      join_gold_data_prev_data_gen::shc_dept_nm AS shc_dept_nm,
                                                      join_gold_data_prev_data_gen::shc_cat_grp_no AS shc_cat_grp_no,
                                                      join_gold_data_prev_data_gen::shc_cat_grp_nm AS shc_cat_grp_nm,
                                                      join_gold_data_prev_data_gen::shc_cat_no AS shc_cat_no,
                                                      join_gold_data_prev_data_gen::shc_cat_nm AS shc_cat_nm,
                                                      join_gold_data_prev_data_gen::shc_subcat_no AS shc_subcat_no,
                                                      join_gold_data_prev_data_gen::shc_subcat_nm AS shc_subcat_nm,
                                                      join_gold_data_prev_data_gen::ref_ksn_id AS ref_ksn_id,
                                                      join_gold_data_prev_data_gen::srs_bus_no AS srs_bus_no,
                                                      join_gold_data_prev_data_gen::srs_bus_nm AS srs_bus_nm,
                                                      join_gold_data_prev_data_gen::srs_div_no AS srs_div_no,
                                                      join_gold_data_prev_data_gen::srs_div_nm AS srs_div_nm,
                                                      join_gold_data_prev_data_gen::srs_ln_no AS srs_ln_no,
                                                      join_gold_data_prev_data_gen::srs_ln_ds AS srs_ln_ds,
                                                      join_gold_data_prev_data_gen::srs_sbl_no AS srs_sbl_no,
                                                      join_gold_data_prev_data_gen::srs_sbl_ds AS srs_sbl_ds,
                                                      join_gold_data_prev_data_gen::srs_cls_no AS srs_cls_no,
                                                      join_gold_data_prev_data_gen::srs_cls_ds AS srs_cls_ds,
                                                      join_gold_data_prev_data_gen::srs_itm_no AS srs_itm_no,
                                                      join_gold_data_prev_data_gen::srs_sku_no AS srs_sku_no,
                                                      join_gold_data_prev_data_gen::srs_div_itm AS srs_div_itm,
                                                      join_gold_data_prev_data_gen::srs_div_itm_sku AS srs_div_itm_sku,
                                                      join_gold_data_prev_data_gen::ima_smt_itm_no AS ima_smt_itm_no,
                                                      join_gold_data_prev_data_gen::ima_smt_itm_ds AS ima_smt_itm_ds,
                                                      join_gold_data_prev_data_gen::ima_smt_fac_qt AS ima_smt_fac_qt,
                                                      join_gold_data_prev_data_gen::uom AS uom,
                                                      join_gold_data_prev_data_gen::vol AS vol,
                                                      join_gold_data_prev_data_gen::wgt AS wgt,
                                                      join_gold_data_prev_data_gen::vnd_no AS vnd_no,
                                                      join_gold_data_prev_data_gen::vnd_nm AS vnd_nm,
                                                      join_gold_data_prev_data_gen::vnd_itm_no AS vnd_itm_no,
                                                      join_gold_data_prev_data_gen::spc_ord_cdt_fl AS spc_ord_cdt_fl,
                                                      join_gold_data_prev_data_gen::itm_emp_fl AS itm_emp_fl,
                                                      join_gold_data_prev_data_gen::easy_ord_fl AS easy_ord_fl,
                                                      join_gold_data_prev_data_gen::itm_rpd_fl AS itm_rpd_fl,
                                                      join_gold_data_prev_data_gen::itm_cd_fl AS itm_cd_fl,
                                                      join_gold_data_prev_data_gen::itm_imp_fl AS itm_imp_fl,
                                                      join_gold_data_prev_data_gen::itm_cs_fl AS itm_cs_fl,
                                                      join_gold_data_prev_data_gen::dd_ind AS dd_ind,
                                                      join_gold_data_prev_data_gen::instl_ind AS instl_ind,
													  join_gold_data_prev_data_gen::cheetah_elgbl_fl AS cheetah_elgbl_fl,
                                                      join_gold_data_prev_data_gen::dot_com_cd AS dot_com_cd,
                                                      join_gold_data_prev_data_gen::obn_830_fl AS obn_830_fl,
                                                      join_gold_data_prev_data_gen::obn_830_dur AS obn_830_dur,
                                                      join_gold_data_prev_data_gen::rpd_frz_dur AS rpd_frz_dur,
                                                      join_gold_data_prev_data_gen::dist_typ_cd AS dist_typ_cd,
                                                      join_gold_data_prev_data_gen::sls_pfm_seg_cd AS sls_pfm_seg_cd,
                                                      join_gold_data_prev_data_gen::fmt_excl_cd AS fmt_excl_cd,
                                                      join_gold_data_prev_data_gen::str_fcst_cd AS str_fcst_cd,
                                                      join_gold_data_prev_data_gen::inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
                                                      join_gold_data_prev_data_gen::ima_itm_typ_cd AS ima_itm_typ_cd,
                                                      join_gold_data_prev_data_gen::itm_purch_sts_cd AS itm_purch_sts_cd,
                                                      join_gold_data_prev_data_gen::ntwk_dist_cd AS ntwk_dist_cd,
                                                      join_gold_data_prev_data_gen::fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
                                                      join_gold_data_prev_data_gen::fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
                                                      join_gold_data_prev_data_gen::jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
													  join_gold_data_prev_data_gen::cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
                                                      join_gold_data_prev_data_gen::str_reord_auth_cd AS str_reord_auth_cd,
                                                      join_gold_data_prev_data_gen::cross_mdse_attr_cd AS cross_mdse_attr_cd,
                                                      join_gold_data_prev_data_gen::whse_sizing AS whse_sizing,
                                                      join_gold_data_prev_data_gen::can_carr_mdl_id AS can_carr_mdl_id,
                                                      join_gold_data_prev_data_gen::groc_crossover_ind AS groc_crossover_ind,
                                                      join_gold_data_prev_data_gen::owner_cd AS owner_cd,
                                                      join_gold_data_prev_data_gen::pln_id AS pln_id,
                                                      join_gold_data_prev_data_gen::itm_pgm AS itm_pgm,
                                                      join_gold_data_prev_data_gen::key_pgm AS key_pgm,
                                                      join_gold_data_prev_data_gen::natl_un_cst_am AS natl_un_cst_am,
                                                      join_gold_data_prev_data_gen::prd_sll_am AS prd_sll_am,
                                                      join_gold_data_prev_data_gen::size AS size,
                                                      join_gold_data_prev_data_gen::style AS style,
                                                      join_gold_data_prev_data_gen::md_style_ref_cd AS md_style_ref_cd,
                                                      join_gold_data_prev_data_gen::seas_cd AS seas_cd,
                                                      join_gold_data_prev_data_gen::seas_yr AS seas_yr,
                                                      join_gold_data_prev_data_gen::sub_seas_id AS sub_seas_id,
                                                      join_gold_data_prev_data_gen::rpt_id AS rpt_id,
                                                      join_gold_data_prev_data_gen::rpt_id_seq_no AS rpt_id_seq_no,
                                                      join_gold_data_prev_data_gen::itm_fcst_grp_id AS itm_fcst_grp_id,
                                                      join_gold_data_prev_data_gen::itm_fcst_grp_ds AS itm_fcst_grp_ds,
                                                      join_gold_data_prev_data_gen::idrp_itm_typ_ds AS idrp_itm_typ_ds,
                                                      join_gold_data_prev_data_gen::brand_ds AS brand_ds,
                                                      join_gold_data_prev_data_gen::color_ds AS color_ds,
                                                      join_gold_data_prev_data_gen::tire_size_ds AS tire_size_ds,
                                                      join_gold_data_prev_data_gen::elig_sts_cd AS elig_sts_cd,
                                                      join_gold_data_prev_data_gen::lst_sts_chg_dt AS lst_sts_chg_dt,
                                                      join_gold_data_prev_data_gen::itm_del_fl AS itm_del_fl,
                                                      join_gold_data_prev_data_gen::prd_prg_dt AS prd_prg_dt,
                                                      join_gold_data_prev_data_gen::order_system_cd AS order_system_cd,
                                                      join_gold_data_prev_data_gen::idrp_order_method_cd AS idrp_order_method_cd,
                                                      join_gold_data_prev_data_gen::dotcom_assorted_cd AS dotcom_assorted_cd,
                                                      join_gold_data_prev_data_gen::dotcom_orderable_ind AS dotcom_orderable_ind,
                                                      join_gold_data_prev_data_gen::roadrunner_eligible_fl AS roadrunner_eligible_fl,
                                                      join_gold_data_prev_data_gen::us_dot_ship_type_cd AS us_dot_ship_type_cd,
                                                      join_gold_data_prev_data_gen::package_weight_in_pounds AS package_weight_in_pounds,
                                                      join_gold_data_prev_data_gen::package_depth_inch_qty AS package_depth_inch_qty,
                                                      join_gold_data_prev_data_gen::package_height_inch_qty AS package_height_inch_qty,
                                                      join_gold_data_prev_data_gen::package_width_inch_qty AS package_width_inch_qty, 
((LOAD_PACKAGE_CURRENT::package_height_inch_qty=='' OR LOAD_PACKAGE_CURRENT::package_height_inch_qty IS NULL OR LOAD_PACKAGE_CURRENT::package_depth_inch_qty=='' OR LOAD_PACKAGE_CURRENT::package_depth_inch_qty IS NULL OR LOAD_PACKAGE_CURRENT::package_width_inch_qty=='' OR LOAD_PACKAGE_CURRENT::package_width_inch_qty IS NULL OR LOAD_PACKAGE_CURRENT::package_weight_pounds_qty=='' OR LOAD_PACKAGE_CURRENT::package_weight_pounds_qty IS NULL) ? 'N' : ((((double)LOAD_PACKAGE_CURRENT::package_height_inch_qty > (double)LOAD_PACKAGE_CURRENT::package_depth_inch_qty) AND ((double)LOAD_PACKAGE_CURRENT::package_height_inch_qty > (double)LOAD_PACKAGE_CURRENT::package_width_inch_qty)) ? (((double)LOAD_PACKAGE_CURRENT::package_height_inch_qty + 2*(double)((double)LOAD_PACKAGE_CURRENT::package_depth_inch_qty + (double)LOAD_PACKAGE_CURRENT::package_width_inch_qty)) < 130 ? ((double)LOAD_PACKAGE_CURRENT::package_height_inch_qty < 108 ? ((double)LOAD_PACKAGE_CURRENT::package_weight_pounds_qty < 150 ? 'Y' : 'N') : 'N') : 'N') : ((((double)LOAD_PACKAGE_CURRENT::package_depth_inch_qty > (double)LOAD_PACKAGE_CURRENT::package_height_inch_qty) AND ((double)LOAD_PACKAGE_CURRENT::package_depth_inch_qty > (double)LOAD_PACKAGE_CURRENT::package_width_inch_qty)) ? (((double)LOAD_PACKAGE_CURRENT::package_depth_inch_qty + 2*(double)((double)LOAD_PACKAGE_CURRENT::package_height_inch_qty + (double)LOAD_PACKAGE_CURRENT::package_width_inch_qty)) < 130 ? ((double)LOAD_PACKAGE_CURRENT::package_depth_inch_qty < 108 ? ((double)LOAD_PACKAGE_CURRENT::package_weight_pounds_qty < 150 ? 'Y' : 'N') : 'N') : 'N') : ((((double)LOAD_PACKAGE_CURRENT::package_width_inch_qty > (double)LOAD_PACKAGE_CURRENT::package_height_inch_qty) AND ((double)LOAD_PACKAGE_CURRENT::package_width_inch_qty > (double)LOAD_PACKAGE_CURRENT::package_depth_inch_qty)) ? (((double)LOAD_PACKAGE_CURRENT::package_width_inch_qty + 2*(double)((double)LOAD_PACKAGE_CURRENT::package_height_inch_qty + (double)LOAD_PACKAGE_CURRENT::package_depth_inch_qty)) < 130 ? ((double)LOAD_PACKAGE_CURRENT::package_width_inch_qty < 108 ? ((double)LOAD_PACKAGE_CURRENT::package_weight_pounds_qty < 150 ? 'Y' : 'N') : 'N') : 'N') : 'N')))) AS mailable_ind,                                                    

													join_gold_data_prev_data_gen::temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
                                                      join_gold_data_prev_data_gen::default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
                                                      join_gold_data_prev_data_gen::default_online_ts AS default_online_ts,
                                                      join_gold_data_prev_data_gen::demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
                                                      join_gold_data_prev_data_gen::temporary_ups_billable_weight AS temporary_ups_billable_weight,
                                                      join_gold_data_prev_data_gen::ups_billable_weight AS ups_billable_weight,
                                                      join_gold_data_prev_data_gen::ups_billable_weight_ts AS ups_billable_weight_ts,
                                                      join_gold_data_prev_data_gen::demand_ups_billable_weight AS demand_ups_billable_weight,
                                                      join_gold_data_prev_data_gen::web_exclusive_ind AS web_exclusive_ind,
										join_gold_data_prev_data_gen::price_type_desc AS price_type_desc,
										join_gold_data_prev_data_gen::idrp_batch_id AS idrp_batch_id,
                                                      LOAD_PACKAGE_CURRENT::package_id AS package_id;

-------------------------------------------------------checking for multiple mailable_ind for an item----------------------------------------------

gruoped_data = GROUP join_gold_data_prev_data_gold_item_pack_gen BY item;

gruoped_data_gen = FOREACH gruoped_data GENERATE 
                           group AS item_id,
                           com.searshc.supplychain.idrp.udf.HasMultipleValues(join_gold_data_prev_data_gold_item_pack_gen.mailable_ind) AS check_error;
						   
join_grouped_data_prev_data = JOIN join_gold_data_prev_data_gold_item_pack_gen BY item, gruoped_data_gen BY item_id;


join_grouped_data_prev_data_gen = FOREACH join_grouped_data_prev_data GENERATE 
											join_gold_data_prev_data_gold_item_pack_gen::load_ts AS load_ts,
                                          join_gold_data_prev_data_gold_item_pack_gen::item AS item,
                                          join_gold_data_prev_data_gold_item_pack_gen::descr AS descr,
                                          join_gold_data_prev_data_gold_item_pack_gen::shc_dvsn_no AS shc_dvsn_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::shc_dvsn_nm AS shc_dvsn_nm,
                                          join_gold_data_prev_data_gold_item_pack_gen::shc_dept_no AS shc_dept_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::shc_dept_nm AS shc_dept_nm,
                                          join_gold_data_prev_data_gold_item_pack_gen::shc_cat_grp_no AS shc_cat_grp_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::shc_cat_grp_nm AS shc_cat_grp_nm,
                                          join_gold_data_prev_data_gold_item_pack_gen::shc_cat_no AS shc_cat_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::shc_cat_nm AS shc_cat_nm,
                                          join_gold_data_prev_data_gold_item_pack_gen::shc_subcat_no AS shc_subcat_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::shc_subcat_nm AS shc_subcat_nm,
                                          join_gold_data_prev_data_gold_item_pack_gen::ref_ksn_id AS ref_ksn_id,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_bus_no AS srs_bus_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_bus_nm AS srs_bus_nm,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_div_no AS srs_div_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_div_nm AS srs_div_nm,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_ln_no AS srs_ln_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_ln_ds AS srs_ln_ds,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_sbl_no AS srs_sbl_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_sbl_ds AS srs_sbl_ds,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_cls_no AS srs_cls_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_cls_ds AS srs_cls_ds,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_itm_no AS srs_itm_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_sku_no AS srs_sku_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_div_itm AS srs_div_itm,
                                          join_gold_data_prev_data_gold_item_pack_gen::srs_div_itm_sku AS srs_div_itm_sku,
                                          join_gold_data_prev_data_gold_item_pack_gen::ima_smt_itm_no AS ima_smt_itm_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::ima_smt_itm_ds AS ima_smt_itm_ds,
                                          join_gold_data_prev_data_gold_item_pack_gen::ima_smt_fac_qt AS ima_smt_fac_qt,
                                          join_gold_data_prev_data_gold_item_pack_gen::uom AS uom,
                                          join_gold_data_prev_data_gold_item_pack_gen::vol AS vol,
                                          join_gold_data_prev_data_gold_item_pack_gen::wgt AS wgt,
                                          join_gold_data_prev_data_gold_item_pack_gen::vnd_no AS vnd_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::vnd_nm AS vnd_nm,
                                          join_gold_data_prev_data_gold_item_pack_gen::vnd_itm_no AS vnd_itm_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::spc_ord_cdt_fl AS spc_ord_cdt_fl,
                                          join_gold_data_prev_data_gold_item_pack_gen::itm_emp_fl AS itm_emp_fl,
                                          join_gold_data_prev_data_gold_item_pack_gen::easy_ord_fl AS easy_ord_fl,
                                          join_gold_data_prev_data_gold_item_pack_gen::itm_rpd_fl AS itm_rpd_fl,
                                          join_gold_data_prev_data_gold_item_pack_gen::itm_cd_fl AS itm_cd_fl,
                                          join_gold_data_prev_data_gold_item_pack_gen::itm_imp_fl AS itm_imp_fl,
                                          join_gold_data_prev_data_gold_item_pack_gen::itm_cs_fl AS itm_cs_fl,
                                          join_gold_data_prev_data_gold_item_pack_gen::dd_ind AS dd_ind,
                                          join_gold_data_prev_data_gold_item_pack_gen::instl_ind AS instl_ind,
										  join_gold_data_prev_data_gold_item_pack_gen::cheetah_elgbl_fl AS cheetah_elgbl_fl,
                                          join_gold_data_prev_data_gold_item_pack_gen::dot_com_cd AS dot_com_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::obn_830_fl AS obn_830_fl,
                                          join_gold_data_prev_data_gold_item_pack_gen::obn_830_dur AS obn_830_dur,
                                          join_gold_data_prev_data_gold_item_pack_gen::rpd_frz_dur AS rpd_frz_dur,
                                          join_gold_data_prev_data_gold_item_pack_gen::dist_typ_cd AS dist_typ_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::sls_pfm_seg_cd AS sls_pfm_seg_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::fmt_excl_cd AS fmt_excl_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::str_fcst_cd AS str_fcst_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::ima_itm_typ_cd AS ima_itm_typ_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::itm_purch_sts_cd AS itm_purch_sts_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::ntwk_dist_cd AS ntwk_dist_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
                                          join_gold_data_prev_data_gold_item_pack_gen::jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
										  join_gold_data_prev_data_gold_item_pack_gen::cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::str_reord_auth_cd AS str_reord_auth_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::cross_mdse_attr_cd AS cross_mdse_attr_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::whse_sizing AS whse_sizing,
                                          join_gold_data_prev_data_gold_item_pack_gen::can_carr_mdl_id AS can_carr_mdl_id,
                                          join_gold_data_prev_data_gold_item_pack_gen::groc_crossover_ind AS groc_crossover_ind,
                                          join_gold_data_prev_data_gold_item_pack_gen::owner_cd AS owner_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::pln_id AS pln_id,
                                          join_gold_data_prev_data_gold_item_pack_gen::itm_pgm AS itm_pgm,
                                          join_gold_data_prev_data_gold_item_pack_gen::key_pgm AS key_pgm,
                                          join_gold_data_prev_data_gold_item_pack_gen::natl_un_cst_am AS natl_un_cst_am,
                                          join_gold_data_prev_data_gold_item_pack_gen::prd_sll_am AS prd_sll_am,
                                          join_gold_data_prev_data_gold_item_pack_gen::size AS size,
                                          join_gold_data_prev_data_gold_item_pack_gen::style AS style,
                                          join_gold_data_prev_data_gold_item_pack_gen::md_style_ref_cd AS md_style_ref_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::seas_cd AS seas_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::seas_yr AS seas_yr,
                                          join_gold_data_prev_data_gold_item_pack_gen::sub_seas_id AS sub_seas_id,
                                          join_gold_data_prev_data_gold_item_pack_gen::rpt_id AS rpt_id,
                                          join_gold_data_prev_data_gold_item_pack_gen::rpt_id_seq_no AS rpt_id_seq_no,
                                          join_gold_data_prev_data_gold_item_pack_gen::itm_fcst_grp_id AS itm_fcst_grp_id,
                                          join_gold_data_prev_data_gold_item_pack_gen::itm_fcst_grp_ds AS itm_fcst_grp_ds,
                                          join_gold_data_prev_data_gold_item_pack_gen::idrp_itm_typ_ds AS idrp_itm_typ_ds,
                                          join_gold_data_prev_data_gold_item_pack_gen::brand_ds AS brand_ds,
                                          join_gold_data_prev_data_gold_item_pack_gen::color_ds AS color_ds,
                                          join_gold_data_prev_data_gold_item_pack_gen::tire_size_ds AS tire_size_ds,
                                          join_gold_data_prev_data_gold_item_pack_gen::elig_sts_cd AS elig_sts_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::lst_sts_chg_dt AS lst_sts_chg_dt,
                                          join_gold_data_prev_data_gold_item_pack_gen::itm_del_fl AS itm_del_fl,
                                          join_gold_data_prev_data_gold_item_pack_gen::prd_prg_dt AS prd_prg_dt,
                                          join_gold_data_prev_data_gold_item_pack_gen::order_system_cd AS order_system_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::idrp_order_method_cd AS idrp_order_method_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::dotcom_assorted_cd AS dotcom_assorted_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::dotcom_orderable_ind AS dotcom_orderable_ind,
                                          join_gold_data_prev_data_gold_item_pack_gen::roadrunner_eligible_fl AS roadrunner_eligible_fl,
                                          join_gold_data_prev_data_gold_item_pack_gen::us_dot_ship_type_cd AS us_dot_ship_type_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::package_weight_in_pounds AS package_weight_in_pounds,
                                          join_gold_data_prev_data_gold_item_pack_gen::package_depth_inch_qty AS package_depth_inch_qty,
                                          join_gold_data_prev_data_gold_item_pack_gen::package_height_inch_qty AS package_height_inch_qty,
                                          join_gold_data_prev_data_gold_item_pack_gen::package_width_inch_qty AS package_width_inch_qty, 
                                          join_gold_data_prev_data_gold_item_pack_gen::mailable_ind AS mailable_ind,                         
										  join_gold_data_prev_data_gold_item_pack_gen::temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::default_online_ts AS default_online_ts,
                                          join_gold_data_prev_data_gold_item_pack_gen::demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
                                          join_gold_data_prev_data_gold_item_pack_gen::temporary_ups_billable_weight AS temporary_ups_billable_weight,
                                          join_gold_data_prev_data_gold_item_pack_gen::ups_billable_weight AS ups_billable_weight,
                                          join_gold_data_prev_data_gold_item_pack_gen::ups_billable_weight_ts AS ups_billable_weight_ts,
                                          join_gold_data_prev_data_gold_item_pack_gen::demand_ups_billable_weight AS demand_ups_billable_weight,
                                        join_gold_data_prev_data_gold_item_pack_gen::web_exclusive_ind AS web_exclusive_ind,
										join_gold_data_prev_data_gold_item_pack_gen::price_type_desc AS price_type_desc,
										join_gold_data_prev_data_gold_item_pack_gen::idrp_batch_id AS idrp_batch_id,
										  join_gold_data_prev_data_gold_item_pack_gen::package_id AS package_id,
										  (gruoped_data_gen::check_error == 'MULTIPLE' ? 'ERROR' : 'NO ERROR') AS check_error;

										  
SPLIT join_grouped_data_prev_data_gen INTO error_data IF (check_error == 'ERROR'),
                                           no_error_data IF (check_error == 'NO ERROR');
										   
										  
final_data_dist_join_item = JOIN prev_data BY item, gruoped_data_gen BY item_id;

final_data_dist_join_item_gen = FOREACH final_data_dist_join_item GENERATE 
                                        load_ts,
										item,
                     descr,
                     shc_dvsn_no,
                     shc_dvsn_nm,
                     shc_dept_no,
                     shc_dept_nm,
                     shc_cat_grp_no,
                     shc_cat_grp_nm,
                     shc_cat_no,
                     shc_cat_nm,
                     shc_subcat_no,
                     shc_subcat_nm,
                     ref_ksn_id,
                     srs_bus_no,
                     srs_bus_nm,
                     srs_div_no,
                     srs_div_nm,
                     srs_ln_no,
                     srs_ln_ds,
                     srs_sbl_no,
                     srs_sbl_ds,
                     srs_cls_no,
                     srs_cls_ds,
                     srs_itm_no,
                     srs_sku_no,
                     srs_div_itm,
                     srs_div_itm_sku,
                     ima_smt_itm_no,
                     ima_smt_itm_ds,
                     ima_smt_fac_qt,
                     uom,
                     vol,
                     wgt,
                     vnd_no,
                     vnd_nm,
                     vnd_itm_no,
                     spc_ord_cdt_fl,
                     itm_emp_fl,
                     easy_ord_fl,
                     itm_rpd_fl,
                     itm_cd_fl,
                     itm_imp_fl,
                     itm_cs_fl,
                     dd_ind,
                     instl_ind,
					 cheetah_elgbl_fl,
                     dot_com_cd,
                     obn_830_fl,
                     obn_830_dur,
                     rpd_frz_dur,
                     (dist_typ_cd is NULL ? '':dist_typ_cd )as dist_typ_cd,
                     sls_pfm_seg_cd,
                     fmt_excl_cd,
                     str_fcst_cd,
                     inv_mgmt_srvc_cd,
                     ima_itm_typ_cd,
                     itm_purch_sts_cd,
                     ntwk_dist_cd,
                     fut_ntwk_dist_cd,
                     fut_ntwk_eff_dt,
                     jit_ntwk_dist_cd,
					 cust_dir_ntwk_cd,
                     str_reord_auth_cd,
                     cross_mdse_attr_cd,
                     (whse_sizing is NULL ? '':whse_sizing) as whse_sizing,
                     can_carr_mdl_id,
                     groc_crossover_ind,
                     (owner_cd is NULL ? '':owner_cd )as owner_cd,
                     pln_id,
                     itm_pgm,
                     key_pgm,
                     natl_un_cst_am,
                     prd_sll_am,
                     size,
                     style,
                     md_style_ref_cd,
                     seas_cd,
                     seas_yr,
                     sub_seas_id,
                     rpt_id,
                     rpt_id_seq_no,
                     itm_fcst_grp_id,
                     itm_fcst_grp_ds,
                     idrp_itm_typ_ds,
                     brand_ds,
                     color_ds,
                     tire_size_ds,
                     elig_sts_cd,
                     lst_sts_chg_dt,
                     itm_del_fl,
                     prd_prg_dt,
                     (order_system_cd is NULL ? '':order_system_cd) as order_system_cd,
                     idrp_order_method_cd,
                     (dotcom_assorted_cd is NULL ? '':dotcom_assorted_cd ) as dotcom_assorted_cd,
                     dotcom_orderable_ind,
                     roadrunner_eligible_fl,
                     (us_dot_ship_type_cd is NULL ? '':us_dot_ship_type_cd ) as us_dot_ship_type_cd,
                     package_weight_in_pounds,
                     package_depth_inch_qty,
                     package_height_inch_qty,
                     package_width_inch_qty,
                     (gruoped_data_gen::check_error == 'MULTIPLE' ? 'N' : gruoped_data_gen::check_error) AS mailable_ind,
                     temporary_online_fulfillment_type_cd,
                     default_online_fulfillment_type_cd,
                     default_online_ts,
                     demand_online_fulfillment_cd,
                     temporary_ups_billable_weight,
                     ups_billable_weight,
                     ups_billable_weight_ts,
                     demand_ups_billable_weight,
                     web_exclusive_ind,
price_type_desc,
idrp_batch_id;           
					 

error_data_gen = FOREACH error_data GENERATE 
                         '$CURRENT_TIMESTAMP' AS load_ts,
                         item AS item_id,
                         '' AS ksn_id,
                         '' AS sears_division_nbr,
                         '' AS sears_item_nbr,
                         '' AS sears_sku_nbr,
                         '' AS websku,
                         package_id AS package_id,
                         mailable_ind AS error_value,
                         'Multiple Mailable Indicators found for an Item' AS error_desc,
						 '$batchid' AS idrp_batch_id;

final_data_dist = UNION final_data_dist_join_item_gen, D11;

NEW_COLS_NEXT = DISTINCT final_data_dist ;

NEW_COLS_NEXT = FOREACH NEW_COLS_NEXT GENERATE
                         load_ts AS load_ts,
                         item AS item,
                         descr AS descr,
                         shc_dvsn_no AS shc_dvsn_no,
                         shc_dvsn_nm AS shc_dvsn_nm,
                         shc_dept_no AS shc_dept_no,
                         shc_dept_nm AS shc_dept_nm,
                         shc_cat_grp_no AS shc_cat_grp_no,
                         shc_cat_grp_nm AS shc_cat_grp_nm,
                         shc_cat_no AS shc_cat_no,
                         shc_cat_nm AS shc_cat_nm,
                         shc_subcat_no AS shc_subcat_no,
                         shc_subcat_nm AS shc_subcat_nm,
                         ref_ksn_id AS ref_ksn_id,
                         srs_bus_no AS srs_bus_no,
                         srs_bus_nm AS srs_bus_nm,
                         srs_div_no AS srs_div_no,
                         srs_div_nm AS srs_div_nm,
                         srs_ln_no AS srs_ln_no,
                         srs_ln_ds AS srs_ln_ds,
                         srs_sbl_no AS srs_sbl_no,
                         srs_sbl_ds AS srs_sbl_ds,
                         srs_cls_no AS srs_cls_no,
                         srs_cls_ds AS srs_cls_ds,
                         srs_itm_no AS srs_itm_no,
                         srs_sku_no AS srs_sku_no,
                         srs_div_itm AS srs_div_itm,
                         srs_div_itm_sku AS srs_div_itm_sku,
                         ima_smt_itm_no AS ima_smt_itm_no,
                         ima_smt_itm_ds AS ima_smt_itm_ds,
                         ima_smt_fac_qt AS ima_smt_fac_qt,
                         uom AS uom,
                         vol AS vol,
                         wgt AS wgt,
                         vnd_no AS vnd_no,
                         vnd_nm AS vnd_nm,
                         vnd_itm_no AS vnd_itm_no,
                         spc_ord_cdt_fl AS spc_ord_cdt_fl,
                         itm_emp_fl AS itm_emp_fl,
                         easy_ord_fl AS easy_ord_fl,
                         itm_rpd_fl AS itm_rpd_fl,
                         itm_cd_fl AS itm_cd_fl,
                         itm_imp_fl AS itm_imp_fl,
                         itm_cs_fl AS itm_cs_fl,
                         dd_ind AS dd_ind,
                         instl_ind AS instl_ind,
                         cheetah_elgbl_fl AS cheetah_elgbl_fl,
                         dot_com_cd AS dot_com_cd,
                         obn_830_fl AS obn_830_fl,
                         obn_830_dur AS obn_830_dur,
                         rpd_frz_dur AS rpd_frz_dur,
                         dist_typ_cd AS dist_typ_cd,
                         sls_pfm_seg_cd AS sls_pfm_seg_cd,
                         fmt_excl_cd AS fmt_excl_cd,
                         str_fcst_cd AS str_fcst_cd,
                         inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
                         ima_itm_typ_cd AS ima_itm_typ_cd,
                         itm_purch_sts_cd AS itm_purch_sts_cd,
                         ntwk_dist_cd AS ntwk_dist_cd,
                         fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
                         fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
                         jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
                         cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
                         str_reord_auth_cd AS str_reord_auth_cd,
                         cross_mdse_attr_cd AS cross_mdse_attr_cd,
                         whse_sizing AS whse_sizing,
                         can_carr_mdl_id AS can_carr_mdl_id,
                         groc_crossover_ind AS groc_crossover_ind,
                         owner_cd AS owner_cd,
                         pln_id AS pln_id,
                         itm_pgm AS itm_pgm,
                         key_pgm AS key_pgm,
                         natl_un_cst_am AS natl_un_cst_am,
                         prd_sll_am AS prd_sll_am,
                         size AS size,
                         style AS style,
                         md_style_ref_cd AS md_style_ref_cd,
                         seas_cd AS seas_cd,
                         seas_yr AS seas_yr,
                         sub_seas_id AS sub_seas_id,
                         rpt_id AS rpt_id,
                         rpt_id_seq_no AS rpt_id_seq_no,
                         itm_fcst_grp_id AS itm_fcst_grp_id,
                         itm_fcst_grp_ds AS itm_fcst_grp_ds,
                         idrp_itm_typ_ds AS idrp_itm_typ_ds,
                         brand_ds AS brand_ds,
                         color_ds AS color_ds,
                         tire_size_ds AS tire_size_ds,
                         elig_sts_cd AS elig_sts_cd,
                         lst_sts_chg_dt AS lst_sts_chg_dt,
                         itm_del_fl AS itm_del_fl,
                         prd_prg_dt AS prd_prg_dt,
                         order_system_cd AS order_system_cd,
                         idrp_order_method_cd AS idrp_order_method_cd,
                         dotcom_assorted_cd AS dotcom_assorted_cd,
                         dotcom_orderable_ind AS dotcom_orderable_ind,
                         roadrunner_eligible_fl AS roadrunner_eligible_fl,
                         us_dot_ship_type_cd AS us_dot_ship_type_cd,
                         package_weight_in_pounds AS package_weight_in_pounds,
                         package_depth_inch_qty AS package_depth_inch_qty,
                         package_height_inch_qty AS package_height_inch_qty,
                         package_width_inch_qty AS package_width_inch_qty,
                         mailable_ind AS mailable_ind,
                         temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
                         default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
                         default_online_ts AS default_online_ts,
                         demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
                         temporary_ups_billable_weight AS temporary_ups_billable_weight,
                         ups_billable_weight AS ups_billable_weight,
                         ups_billable_weight_ts AS ups_billable_weight_ts,
                         demand_ups_billable_weight AS demand_ups_billable_weight,
                         web_exclusive_ind AS web_exclusive_ind,
                         price_type_desc AS price_type_desc,
                         idrp_batch_id AS idrp_batch_id;
--------------------------------------------------------------------------------------------------------------------------------------
DROP_ITEMS = FOREACH LOAD_DROP_SHIP GENERATE (item_id is NULL ? '':item_id) as item_id  ;

DROP_ITEMS_DIST = DISTINCT DROP_ITEMS ;

LOAD_ELIGIBLE_ITEM_LOC_1 = FOREACH LOAD_ELIGIBLE_ITEM_LOC GENERATE
(item is NULL ? '':item) as item ,
(loc is NULL ? '':loc) as loc,
(elig_sts_cd is NULL ? '':elig_sts_cd) as elig_sts_cd,
(rim_sts_cd is NULL ? '':rim_sts_cd) as rim_sts_cd ;


JOIN_23 = JOIN NEW_COLS_NEXT BY (item) LEFT OUTER, LOAD_ELIGIBLE_ITEM_LOC_1 BY item ;

JOIN_NEW_COLS_NEXT  = JOIN JOIN_23 BY NEW_COLS_NEXT::item LEFT OUTER, DROP_ITEMS_DIST BY item_id ;

JOIN_NEW_COLS_NEXT_2 = JOIN JOIN_NEW_COLS_NEXT BY (JOIN_23::NEW_COLS_NEXT::item) LEFT OUTER, LOAD_ONLINE_FULFILLMENT BY item_id ;
--------------------------------------------------------------------
JOIN_NEW_COLS_NEXT_GEN = FOREACH JOIN_NEW_COLS_NEXT_2 GENERATE

JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::load_ts as  load_ts,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::item AS item,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::descr AS descr,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::shc_dvsn_no AS shc_dvsn_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::shc_dvsn_nm AS shc_dvsn_nm,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::shc_dept_no AS shc_dept_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::shc_dept_nm AS shc_dept_nm,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::shc_cat_grp_no AS shc_cat_grp_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::shc_cat_grp_nm AS shc_cat_grp_nm,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::shc_cat_no AS shc_cat_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::shc_cat_nm AS shc_cat_nm,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::shc_subcat_no AS shc_subcat_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::shc_subcat_nm AS shc_subcat_nm,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::ref_ksn_id AS ref_ksn_id,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_bus_no AS srs_bus_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_bus_nm AS srs_bus_nm,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_div_no AS srs_div_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_div_nm AS srs_div_nm,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_ln_no AS srs_ln_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_ln_ds AS srs_ln_ds,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_sbl_no AS srs_sbl_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_sbl_ds AS srs_sbl_ds,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_cls_no AS srs_cls_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_cls_ds AS srs_cls_ds,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_itm_no AS srs_itm_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_sku_no AS srs_sku_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_div_itm AS srs_div_itm,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::srs_div_itm_sku AS srs_div_itm_sku,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::ima_smt_itm_no AS ima_smt_itm_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::ima_smt_itm_ds AS ima_smt_itm_ds,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::ima_smt_fac_qt AS ima_smt_fac_qt,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::uom AS uom,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::vol AS vol,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::wgt AS wgt,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::vnd_no AS vnd_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::vnd_nm AS vnd_nm,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::vnd_itm_no AS vnd_itm_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::spc_ord_cdt_fl AS spc_ord_cdt_fl,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::itm_emp_fl AS itm_emp_fl,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::easy_ord_fl AS easy_ord_fl,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::itm_rpd_fl AS itm_rpd_fl,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::itm_cd_fl AS itm_cd_fl,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::itm_imp_fl AS itm_imp_fl,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::itm_cs_fl AS itm_cs_fl,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::dd_ind AS dd_ind ,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::instl_ind AS instl_ind ,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::cheetah_elgbl_fl AS cheetah_elgbl_fl,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::dot_com_cd AS dot_com_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::obn_830_fl AS obn_830_fl,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::obn_830_dur AS obn_830_dur,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::rpd_frz_dur AS rpd_frz_dur,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::dist_typ_cd AS dist_typ_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::sls_pfm_seg_cd AS sls_pfm_seg_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::fmt_excl_cd AS fmt_excl_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::str_fcst_cd AS str_fcst_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::ima_itm_typ_cd AS ima_itm_typ_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::itm_purch_sts_cd AS itm_purch_sts_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::ntwk_dist_cd AS ntwk_dist_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::str_reord_auth_cd AS str_reord_auth_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::cross_mdse_attr_cd AS cross_mdse_attr_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::whse_sizing AS whse_sizing,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::can_carr_mdl_id AS can_carr_mdl_id,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::groc_crossover_ind AS groc_crossover_ind,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::owner_cd AS owner_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::pln_id AS pln_id,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::itm_pgm AS itm_pgm,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::key_pgm AS key_pgm,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::natl_un_cst_am AS natl_un_cst_am,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::prd_sll_am AS prd_sll_am,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::size AS size,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::style AS style,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::md_style_ref_cd AS md_style_ref_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::seas_cd AS seas_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::seas_yr AS seas_yr,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::sub_seas_id AS sub_seas_id,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::rpt_id AS rpt_id,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::rpt_id_seq_no AS rpt_id_seq_no,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::itm_fcst_grp_id AS itm_fcst_grp_id,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::itm_fcst_grp_ds AS itm_fcst_grp_ds,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::idrp_itm_typ_ds AS idrp_itm_typ_ds,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::brand_ds AS brand_ds,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::color_ds AS color_ds,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::tire_size_ds AS tire_size_ds,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::elig_sts_cd AS elig_sts_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::lst_sts_chg_dt AS lst_sts_chg_dt,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::itm_del_fl AS itm_del_fl,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::prd_prg_dt AS prd_prg_dt,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::order_system_cd AS order_system_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::idrp_order_method_cd AS idrp_order_method_cd ,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::dotcom_assorted_cd AS dotcom_assorted_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::dotcom_orderable_ind AS dotcom_orderable_ind,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::roadrunner_eligible_fl AS roadrunner_eligible_fl,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::us_dot_ship_type_cd AS us_dot_ship_type_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::package_weight_in_pounds AS package_weight_in_pounds,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::package_depth_inch_qty AS package_depth_inch_qty,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::package_height_inch_qty AS package_height_inch_qty,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::package_width_inch_qty AS package_width_inch_qty,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::mailable_ind AS mailable_ind,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd, 
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::default_online_ts AS default_online_ts,   
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::demand_online_fulfillment_cd AS demand_online_fulfillment_cd,   
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::temporary_ups_billable_weight AS temporary_ups_billable_weight,  
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::ups_billable_weight AS ups_billable_weight, 
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::ups_billable_weight_ts AS ups_billable_weight_ts,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::demand_ups_billable_weight AS demand_ups_billable_weight,   
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::web_exclusive_ind AS web_exclusive_ind,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::price_type_desc AS price_type_desc,
JOIN_NEW_COLS_NEXT::JOIN_23::NEW_COLS_NEXT::idrp_batch_id AS idrp_batch_id,
(LOAD_ONLINE_FULFILLMENT::item_id is NULL ? '':LOAD_ONLINE_FULFILLMENT::item_id) as item_id_of,
(LOAD_ONLINE_FULFILLMENT::temporary_fulfillment_type_cd is NULL ? '':LOAD_ONLINE_FULFILLMENT::temporary_fulfillment_type_cd) as temp_of,
(DROP_ITEMS_DIST::item_id is NULL ? '':DROP_ITEMS_DIST::item_id) as item_id_ds,
(JOIN_NEW_COLS_NEXT::JOIN_23::LOAD_ELIGIBLE_ITEM_LOC_1::elig_sts_cd is NULL ? '':JOIN_NEW_COLS_NEXT::JOIN_23::LOAD_ELIGIBLE_ITEM_LOC_1::elig_sts_cd) as ie_elig_sts_cd ,
(JOIN_NEW_COLS_NEXT::JOIN_23::LOAD_ELIGIBLE_ITEM_LOC_1::loc is NULL ? '':JOIN_NEW_COLS_NEXT::JOIN_23::LOAD_ELIGIBLE_ITEM_LOC_1::loc) as ie_loc ,
(JOIN_NEW_COLS_NEXT::JOIN_23::LOAD_ELIGIBLE_ITEM_LOC_1::rim_sts_cd is NULL ? '':JOIN_NEW_COLS_NEXT::JOIN_23::LOAD_ELIGIBLE_ITEM_LOC_1::rim_sts_cd) as ie_rim_sts_cd;

----------------------------------------------------------------------------------------------------------------------------------

JOIN_NEW_COLS_NEXT_GEN_22 = FOREACH JOIN_NEW_COLS_NEXT_GEN GENERATE

load_ts AS load_ts,
item AS item ,
descr AS descr ,
shc_dvsn_no AS shc_dvsn_no ,
shc_dvsn_nm AS shc_dvsn_nm ,
shc_dept_no AS shc_dept_no ,
shc_dept_nm AS shc_dept_nm ,
shc_cat_grp_no AS shc_cat_grp_no ,
shc_cat_grp_nm AS shc_cat_grp_nm ,
shc_cat_no AS shc_cat_no ,
shc_cat_nm AS shc_cat_nm ,
shc_subcat_no AS shc_subcat_no ,
shc_subcat_nm AS shc_subcat_nm ,
ref_ksn_id AS ref_ksn_id ,
srs_bus_no AS srs_bus_no ,
srs_bus_nm AS srs_bus_nm ,
srs_div_no AS srs_div_no ,
srs_div_nm AS srs_div_nm ,
srs_ln_no AS srs_ln_no ,
srs_ln_ds AS srs_ln_ds ,
srs_sbl_no AS srs_sbl_no ,
srs_sbl_ds AS srs_sbl_ds ,
srs_cls_no AS srs_cls_no ,
srs_cls_ds AS srs_cls_ds ,
srs_itm_no AS srs_itm_no ,
srs_sku_no AS srs_sku_no ,
srs_div_itm AS srs_div_itm ,
srs_div_itm_sku AS srs_div_itm_sku ,
ima_smt_itm_no AS ima_smt_itm_no ,
ima_smt_itm_ds AS ima_smt_itm_ds ,
ima_smt_fac_qt AS ima_smt_fac_qt ,
uom AS uom ,
vol AS vol ,
wgt AS wgt ,
vnd_no AS vnd_no ,
vnd_nm AS vnd_nm ,
vnd_itm_no AS vnd_itm_no ,
spc_ord_cdt_fl AS spc_ord_cdt_fl ,
itm_emp_fl AS itm_emp_fl ,
easy_ord_fl AS easy_ord_fl ,
itm_rpd_fl AS itm_rpd_fl ,
itm_cd_fl AS itm_cd_fl ,
itm_imp_fl AS itm_imp_fl ,
itm_cs_fl AS itm_cs_fl ,
dd_ind  AS dd_ind  ,
instl_ind  AS instl_ind  ,
cheetah_elgbl_fl AS cheetah_elgbl_fl,
dot_com_cd AS dot_com_cd ,
obn_830_fl AS obn_830_fl ,
obn_830_dur AS obn_830_dur ,
rpd_frz_dur AS rpd_frz_dur ,
dist_typ_cd AS dist_typ_cd ,
sls_pfm_seg_cd AS sls_pfm_seg_cd ,
fmt_excl_cd AS fmt_excl_cd ,
str_fcst_cd AS str_fcst_cd ,
inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd ,
ima_itm_typ_cd AS ima_itm_typ_cd ,
itm_purch_sts_cd AS itm_purch_sts_cd ,
ntwk_dist_cd AS ntwk_dist_cd ,
fut_ntwk_dist_cd AS fut_ntwk_dist_cd ,
fut_ntwk_eff_dt AS fut_ntwk_eff_dt ,
jit_ntwk_dist_cd AS jit_ntwk_dist_cd ,
cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
str_reord_auth_cd AS str_reord_auth_cd ,
cross_mdse_attr_cd AS cross_mdse_attr_cd ,
whse_sizing AS whse_sizing ,
can_carr_mdl_id AS can_carr_mdl_id ,
groc_crossover_ind AS groc_crossover_ind ,
owner_cd AS owner_cd ,
pln_id AS pln_id ,
itm_pgm AS itm_pgm ,
key_pgm AS key_pgm ,
natl_un_cst_am AS natl_un_cst_am ,
prd_sll_am AS prd_sll_am ,
size AS size ,
style AS style ,
md_style_ref_cd AS md_style_ref_cd ,
seas_cd AS seas_cd ,
seas_yr AS seas_yr ,
sub_seas_id AS sub_seas_id ,
rpt_id AS rpt_id ,
rpt_id_seq_no AS rpt_id_seq_no ,
itm_fcst_grp_id AS itm_fcst_grp_id ,
itm_fcst_grp_ds AS itm_fcst_grp_ds ,
idrp_itm_typ_ds AS idrp_itm_typ_ds ,
brand_ds AS brand_ds ,
color_ds AS color_ds ,
tire_size_ds AS tire_size_ds ,
elig_sts_cd AS elig_sts_cd ,
lst_sts_chg_dt AS lst_sts_chg_dt ,
itm_del_fl AS itm_del_fl ,
prd_prg_dt AS prd_prg_dt ,
order_system_cd AS order_system_cd ,
idrp_order_method_cd  AS idrp_order_method_cd  ,
dotcom_assorted_cd AS dotcom_assorted_cd ,
dotcom_orderable_ind AS dotcom_orderable_ind ,
roadrunner_eligible_fl AS roadrunner_eligible_fl ,
us_dot_ship_type_cd AS us_dot_ship_type_cd ,
package_weight_in_pounds AS package_weight_in_pounds ,
package_depth_inch_qty AS package_depth_inch_qty ,
package_height_inch_qty AS package_height_inch_qty ,
package_width_inch_qty AS package_width_inch_qty ,
mailable_ind AS mailable_ind ,
(item_id_of != ''? temp_of:(item_id_ds !='' ? 'NONE': ((dotcom_assorted_cd == '1' AND us_dot_ship_type_cd !='H' AND mailable_ind =='Y' AND (owner_cd == 'K' OR owner_cd == 'B') AND order_system_cd !='RIM' AND ie_elig_sts_cd =='A' AND (ie_loc == '7840' OR ie_loc == '9300')) OR (dotcom_assorted_cd == '1' AND us_dot_ship_type_cd !='H' AND mailable_ind =='Y' AND (owner_cd == 'S' OR owner_cd == 'B')AND order_system_cd =='RIM' AND (whse_sizing == 'WG8800' OR whse_sizing == 'WG8801') AND ie_loc == '9300' AND ie_elig_sts_cd =='A' AND (ie_rim_sts_cd == 'R' OR ie_rim_sts_cd == 'S' OR ie_rim_sts_cd == 'C' OR ie_rim_sts_cd == 'P' OR ie_rim_sts_cd == 'E') ) ? 'TW':(dotcom_assorted_cd =='1' AND us_dot_ship_type_cd =='H' ?'SPU':(dotcom_assorted_cd=='1' AND dist_typ_cd == 'DD' AND (owner_cd == 'S' OR owner_cd == 'B')AND order_system_cd =='RIM' AND ie_loc == '9300' AND ie_rim_sts_cd == 'L' ? 'DDC':'NONE'))))) as temporary_online_fulfillment_type_cd ,
default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd ,
default_online_ts AS default_online_ts ,
demand_online_fulfillment_cd  AS demand_online_fulfillment_cd  ,
temporary_ups_billable_weight AS temporary_ups_billable_weight ,
 ups_billable_weight AS  ups_billable_weight ,
ups_billable_weight_ts AS ups_billable_weight_ts ,
demand_ups_billable_weight AS demand_ups_billable_weight ,
web_exclusive_ind AS web_exclusive_ind,
price_type_desc AS price_type_desc,
idrp_batch_id AS idrp_batch_id;

---------------------------------------------------------------------------------------------------------------------------------- 
JOIN_NEW_COLS_NEXT_GEN_2 = FOREACH JOIN_NEW_COLS_NEXT_GEN_22 GENERATE 

load_ts,item,descr,shc_dvsn_no,shc_dvsn_nm,shc_dept_no,shc_dept_nm,shc_cat_grp_no,shc_cat_grp_nm,shc_cat_no,shc_cat_nm,shc_subcat_no,shc_subcat_nm,ref_ksn_id,srs_bus_no,srs_bus_nm,srs_div_no,srs_div_nm,srs_ln_no,srs_ln_ds,srs_sbl_no,srs_sbl_ds,srs_cls_no,srs_cls_ds,srs_itm_no,srs_sku_no,srs_div_itm,srs_div_itm_sku,ima_smt_itm_no,ima_smt_itm_ds,ima_smt_fac_qt,uom,vol,wgt,vnd_no,vnd_nm,vnd_itm_no,spc_ord_cdt_fl,itm_emp_fl,easy_ord_fl,itm_rpd_fl,itm_cd_fl,itm_imp_fl,itm_cs_fl,dd_ind,instl_ind,cheetah_elgbl_fl,dot_com_cd,obn_830_fl,obn_830_dur,rpd_frz_dur,dist_typ_cd,sls_pfm_seg_cd,fmt_excl_cd,str_fcst_cd,inv_mgmt_srvc_cd,ima_itm_typ_cd,itm_purch_sts_cd,ntwk_dist_cd,fut_ntwk_dist_cd,fut_ntwk_eff_dt,jit_ntwk_dist_cd,cust_dir_ntwk_cd,str_reord_auth_cd,cross_mdse_attr_cd,whse_sizing,can_carr_mdl_id,groc_crossover_ind,owner_cd,pln_id,itm_pgm,key_pgm,natl_un_cst_am,prd_sll_am,size,style,md_style_ref_cd,seas_cd,seas_yr,sub_seas_id,rpt_id,rpt_id_seq_no,itm_fcst_grp_id,itm_fcst_grp_ds,idrp_itm_typ_ds,brand_ds,color_ds,tire_size_ds,elig_sts_cd,lst_sts_chg_dt,itm_del_fl,prd_prg_dt,order_system_cd,idrp_order_method_cd ,dotcom_assorted_cd,dotcom_orderable_ind,roadrunner_eligible_fl,us_dot_ship_type_cd,package_weight_in_pounds,package_depth_inch_qty,package_height_inch_qty,package_width_inch_qty,mailable_ind,temporary_online_fulfillment_type_cd,default_online_fulfillment_type_cd,default_online_ts,(default_online_fulfillment_type_cd !='' ? default_online_fulfillment_type_cd:temporary_online_fulfillment_type_cd) as demand_online_fulfillment_cd ,temporary_ups_billable_weight, ups_billable_weight,ups_billable_weight_ts,demand_ups_billable_weight, web_exclusive_ind,
price_type_desc,
idrp_batch_id; 

--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
JOIN_BILL_TBL = JOIN JOIN_NEW_COLS_NEXT_GEN_2 BY item LEFT OUTER, LOAD_ONLINE_BILL_WT  BY item_id ;


JOIN_BILL_TBL_OPT = FOREACH JOIN_BILL_TBL GENERATE 
JOIN_NEW_COLS_NEXT_GEN_2::load_ts AS load_ts,
JOIN_NEW_COLS_NEXT_GEN_2::item AS item,
JOIN_NEW_COLS_NEXT_GEN_2::descr AS descr,
JOIN_NEW_COLS_NEXT_GEN_2::shc_dvsn_no AS shc_dvsn_no,
JOIN_NEW_COLS_NEXT_GEN_2::shc_dvsn_nm AS shc_dvsn_nm,
JOIN_NEW_COLS_NEXT_GEN_2::shc_dept_no AS shc_dept_no,
JOIN_NEW_COLS_NEXT_GEN_2::shc_dept_nm AS shc_dept_nm,
JOIN_NEW_COLS_NEXT_GEN_2::shc_cat_grp_no AS shc_cat_grp_no,
JOIN_NEW_COLS_NEXT_GEN_2::shc_cat_grp_nm AS shc_cat_grp_nm,
JOIN_NEW_COLS_NEXT_GEN_2::shc_cat_no AS shc_cat_no,
JOIN_NEW_COLS_NEXT_GEN_2::shc_cat_nm AS shc_cat_nm,
JOIN_NEW_COLS_NEXT_GEN_2::shc_subcat_no AS shc_subcat_no,
JOIN_NEW_COLS_NEXT_GEN_2::shc_subcat_nm AS shc_subcat_nm,
JOIN_NEW_COLS_NEXT_GEN_2::ref_ksn_id AS ref_ksn_id,
JOIN_NEW_COLS_NEXT_GEN_2::srs_bus_no AS srs_bus_no,
JOIN_NEW_COLS_NEXT_GEN_2::srs_bus_nm AS srs_bus_nm,
JOIN_NEW_COLS_NEXT_GEN_2::srs_div_no AS srs_div_no,
JOIN_NEW_COLS_NEXT_GEN_2::srs_div_nm AS srs_div_nm,
JOIN_NEW_COLS_NEXT_GEN_2::srs_ln_no AS srs_ln_no,
JOIN_NEW_COLS_NEXT_GEN_2::srs_ln_ds AS srs_ln_ds,
JOIN_NEW_COLS_NEXT_GEN_2::srs_sbl_no AS srs_sbl_no,
JOIN_NEW_COLS_NEXT_GEN_2::srs_sbl_ds AS srs_sbl_ds,
JOIN_NEW_COLS_NEXT_GEN_2::srs_cls_no AS srs_cls_no,
JOIN_NEW_COLS_NEXT_GEN_2::srs_cls_ds AS srs_cls_ds,
JOIN_NEW_COLS_NEXT_GEN_2::srs_itm_no AS srs_itm_no,
JOIN_NEW_COLS_NEXT_GEN_2::srs_sku_no AS srs_sku_no,
JOIN_NEW_COLS_NEXT_GEN_2::srs_div_itm AS srs_div_itm,
JOIN_NEW_COLS_NEXT_GEN_2::srs_div_itm_sku AS srs_div_itm_sku,
JOIN_NEW_COLS_NEXT_GEN_2::ima_smt_itm_no AS ima_smt_itm_no,
JOIN_NEW_COLS_NEXT_GEN_2::ima_smt_itm_ds AS ima_smt_itm_ds,
JOIN_NEW_COLS_NEXT_GEN_2::ima_smt_fac_qt AS ima_smt_fac_qt,
JOIN_NEW_COLS_NEXT_GEN_2::uom AS uom,
JOIN_NEW_COLS_NEXT_GEN_2::vol AS vol,
JOIN_NEW_COLS_NEXT_GEN_2::wgt AS wgt,
JOIN_NEW_COLS_NEXT_GEN_2::vnd_no AS vnd_no,
JOIN_NEW_COLS_NEXT_GEN_2::vnd_nm AS vnd_nm,
JOIN_NEW_COLS_NEXT_GEN_2::vnd_itm_no AS vnd_itm_no,
JOIN_NEW_COLS_NEXT_GEN_2::spc_ord_cdt_fl AS spc_ord_cdt_fl,
JOIN_NEW_COLS_NEXT_GEN_2::itm_emp_fl AS itm_emp_fl,
JOIN_NEW_COLS_NEXT_GEN_2::easy_ord_fl AS easy_ord_fl,
JOIN_NEW_COLS_NEXT_GEN_2::itm_rpd_fl AS itm_rpd_fl,
JOIN_NEW_COLS_NEXT_GEN_2::itm_cd_fl AS itm_cd_fl,
JOIN_NEW_COLS_NEXT_GEN_2::itm_imp_fl AS itm_imp_fl,
JOIN_NEW_COLS_NEXT_GEN_2::itm_cs_fl AS itm_cs_fl,
JOIN_NEW_COLS_NEXT_GEN_2::dd_ind AS dd_ind ,
JOIN_NEW_COLS_NEXT_GEN_2::instl_ind AS instl_ind ,
JOIN_NEW_COLS_NEXT_GEN_2::cheetah_elgbl_fl AS cheetah_elgbl_fl,
JOIN_NEW_COLS_NEXT_GEN_2::dot_com_cd AS dot_com_cd,
JOIN_NEW_COLS_NEXT_GEN_2::obn_830_fl AS obn_830_fl,
JOIN_NEW_COLS_NEXT_GEN_2::obn_830_dur AS obn_830_dur,
JOIN_NEW_COLS_NEXT_GEN_2::rpd_frz_dur AS rpd_frz_dur,
JOIN_NEW_COLS_NEXT_GEN_2::dist_typ_cd AS dist_typ_cd,
JOIN_NEW_COLS_NEXT_GEN_2::sls_pfm_seg_cd AS sls_pfm_seg_cd,
JOIN_NEW_COLS_NEXT_GEN_2::fmt_excl_cd AS fmt_excl_cd,
JOIN_NEW_COLS_NEXT_GEN_2::str_fcst_cd AS str_fcst_cd,
JOIN_NEW_COLS_NEXT_GEN_2::inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
JOIN_NEW_COLS_NEXT_GEN_2::ima_itm_typ_cd AS ima_itm_typ_cd,
JOIN_NEW_COLS_NEXT_GEN_2::itm_purch_sts_cd AS itm_purch_sts_cd,
JOIN_NEW_COLS_NEXT_GEN_2::ntwk_dist_cd AS ntwk_dist_cd,
JOIN_NEW_COLS_NEXT_GEN_2::fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
JOIN_NEW_COLS_NEXT_GEN_2::fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
JOIN_NEW_COLS_NEXT_GEN_2::jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
JOIN_NEW_COLS_NEXT_GEN_2::cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
JOIN_NEW_COLS_NEXT_GEN_2::str_reord_auth_cd AS str_reord_auth_cd,
JOIN_NEW_COLS_NEXT_GEN_2::cross_mdse_attr_cd AS cross_mdse_attr_cd,
JOIN_NEW_COLS_NEXT_GEN_2::whse_sizing AS whse_sizing,
JOIN_NEW_COLS_NEXT_GEN_2::can_carr_mdl_id AS can_carr_mdl_id,
JOIN_NEW_COLS_NEXT_GEN_2::groc_crossover_ind AS groc_crossover_ind,
JOIN_NEW_COLS_NEXT_GEN_2::owner_cd AS owner_cd,
JOIN_NEW_COLS_NEXT_GEN_2::pln_id AS pln_id,
JOIN_NEW_COLS_NEXT_GEN_2::itm_pgm AS itm_pgm,
JOIN_NEW_COLS_NEXT_GEN_2::key_pgm AS key_pgm,
JOIN_NEW_COLS_NEXT_GEN_2::natl_un_cst_am AS natl_un_cst_am,
JOIN_NEW_COLS_NEXT_GEN_2::prd_sll_am AS prd_sll_am,
JOIN_NEW_COLS_NEXT_GEN_2::size AS size,
JOIN_NEW_COLS_NEXT_GEN_2::style AS style,
JOIN_NEW_COLS_NEXT_GEN_2::md_style_ref_cd AS md_style_ref_cd,
JOIN_NEW_COLS_NEXT_GEN_2::seas_cd AS seas_cd,
JOIN_NEW_COLS_NEXT_GEN_2::seas_yr AS seas_yr,
JOIN_NEW_COLS_NEXT_GEN_2::sub_seas_id AS sub_seas_id,
JOIN_NEW_COLS_NEXT_GEN_2::rpt_id AS rpt_id,
JOIN_NEW_COLS_NEXT_GEN_2::rpt_id_seq_no AS rpt_id_seq_no,
JOIN_NEW_COLS_NEXT_GEN_2::itm_fcst_grp_id AS itm_fcst_grp_id,
JOIN_NEW_COLS_NEXT_GEN_2::itm_fcst_grp_ds AS itm_fcst_grp_ds,
JOIN_NEW_COLS_NEXT_GEN_2::idrp_itm_typ_ds AS idrp_itm_typ_ds,
JOIN_NEW_COLS_NEXT_GEN_2::brand_ds AS brand_ds,
JOIN_NEW_COLS_NEXT_GEN_2::color_ds AS color_ds,
JOIN_NEW_COLS_NEXT_GEN_2::tire_size_ds AS tire_size_ds,
JOIN_NEW_COLS_NEXT_GEN_2::elig_sts_cd AS elig_sts_cd,
JOIN_NEW_COLS_NEXT_GEN_2::lst_sts_chg_dt AS lst_sts_chg_dt,
JOIN_NEW_COLS_NEXT_GEN_2::itm_del_fl AS itm_del_fl,
JOIN_NEW_COLS_NEXT_GEN_2::prd_prg_dt AS prd_prg_dt,
JOIN_NEW_COLS_NEXT_GEN_2::order_system_cd AS order_system_cd,
JOIN_NEW_COLS_NEXT_GEN_2::idrp_order_method_cd AS idrp_order_method_cd ,
JOIN_NEW_COLS_NEXT_GEN_2::dotcom_assorted_cd AS dotcom_assorted_cd,
JOIN_NEW_COLS_NEXT_GEN_2::dotcom_orderable_ind AS dotcom_orderable_ind,
JOIN_NEW_COLS_NEXT_GEN_2::roadrunner_eligible_fl AS roadrunner_eligible_fl,
JOIN_NEW_COLS_NEXT_GEN_2::us_dot_ship_type_cd AS us_dot_ship_type_cd,
JOIN_NEW_COLS_NEXT_GEN_2::package_weight_in_pounds AS package_weight_in_pounds,
JOIN_NEW_COLS_NEXT_GEN_2::package_depth_inch_qty AS package_depth_inch_qty,
JOIN_NEW_COLS_NEXT_GEN_2::package_height_inch_qty AS package_height_inch_qty,
JOIN_NEW_COLS_NEXT_GEN_2::package_width_inch_qty AS package_width_inch_qty,
JOIN_NEW_COLS_NEXT_GEN_2::mailable_ind AS mailable_ind,
JOIN_NEW_COLS_NEXT_GEN_2::temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
JOIN_NEW_COLS_NEXT_GEN_2::default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
JOIN_NEW_COLS_NEXT_GEN_2::default_online_ts AS default_online_ts,
JOIN_NEW_COLS_NEXT_GEN_2::demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
JOIN_NEW_COLS_NEXT_GEN_2::temporary_ups_billable_weight AS temporary_ups_billable_weight,
(LOAD_ONLINE_BILL_WT::item_id !='' AND LOAD_ONLINE_BILL_WT::item_id IS NOT NULL ? LOAD_ONLINE_BILL_WT::ups_billable_weight:'') as  ups_billable_weight,
(LOAD_ONLINE_BILL_WT::item_id !='' AND LOAD_ONLINE_BILL_WT::item_id IS NOT NULL ? '$CURRENT_TIMESTAMP':'') AS ups_billable_weight_ts,
JOIN_NEW_COLS_NEXT_GEN_2::demand_ups_billable_weight AS demand_ups_billable_weight,
JOIN_NEW_COLS_NEXT_GEN_2::web_exclusive_ind AS web_exclusive_ind,
JOIN_NEW_COLS_NEXT_GEN_2::price_type_desc AS price_type_desc,
JOIN_NEW_COLS_NEXT_GEN_2::idrp_batch_id AS idrp_batch_id;  

-------------------------------------------------------------------------------------------------------------------

PACK = FOREACH LOAD_PACKAGE_CURRENT GENERATE

(ksn_id is NULL ? '' : ksn_id )as ksn_id,
(package_id is NULL ? '': package_id )as package_id,
(package_weight_pounds_qty is NULL ? 0.0:ROUND((double)package_weight_pounds_qty)) as package_weight_pounds_qty,
(package_depth_inch_qty is NULL ? 0.0:  ROUND((double)package_depth_inch_qty))as package_depth_inch_qty,
(package_width_inch_qty is NULL ? 0.0:  ROUND((double)package_width_inch_qty)) as package_width_inch_qty,
(package_height_inch_qty is NULL ? 0.0: ROUND((double)package_height_inch_qty)) as package_height_inch_qty,
((double)package_height_inch_qty*(double)package_width_inch_qty*(double)package_depth_inch_qty) as cubic_size_in_inches ;

PACK_2 = FOREACH PACK GENERATE
ksn_id,
package_id,
package_weight_pounds_qty,
package_depth_inch_qty,
package_width_inch_qty,
package_height_inch_qty,
cubic_size_in_inches,
((double)cubic_size_in_inches >= 5184.0 ? ((double)cubic_size_in_inches/218.0): (double)package_weight_pounds_qty ) as dimension_weight_in_pounds ;


PACK_3 = FOREACH PACK_2 GENERATE
ksn_id,
package_id,
package_weight_pounds_qty,
package_depth_inch_qty,
package_width_inch_qty,
package_height_inch_qty,
cubic_size_in_inches,
dimension_weight_in_pounds,
((double)dimension_weight_in_pounds > (double)package_weight_pounds_qty ? (double)dimension_weight_in_pounds:(double)package_weight_pounds_qty) as pkg_temporary_ups_billable_weight ;
 


JOIN_PACK = JOIN  GOLD_ITEM BY ksn_id LEFT OUTER, PACK_3 BY ksn_id;

JOIN_BILL_TBL_OPT_2 = JOIN JOIN_BILL_TBL_OPT BY item LEFT OUTER, JOIN_PACK BY (GOLD_ITEM::item_id)  USING 'skewed';

JOIN_BILL_TBL_OPT_2_GRP = GROUP JOIN_BILL_TBL_OPT_2 BY (JOIN_BILL_TBL_OPT::item);

TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL = FOREACH JOIN_BILL_TBL_OPT_2_GRP
                                           { ord_data_1 = ORDER JOIN_BILL_TBL_OPT_2 BY JOIN_BILL_TBL_OPT::item, JOIN_PACK::PACK_3::pkg_temporary_ups_billable_weight DESC;
                                                         ord_data_lmt_1 = LIMIT ord_data_1 1;
                                                         GENERATE FLATTEN(ord_data_lmt_1);
                                           };

JOIN_BILL_TBL_OPT_22 = FOREACH TARGET_COLS_DISTINCT_3_GRP_GEN_FINAL GENERATE
ord_data_lmt_1::JOIN_BILL_TBL_OPT::load_ts  AS load_ts ,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::item AS item,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::descr AS descr,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::shc_dvsn_no AS shc_dvsn_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::shc_dvsn_nm AS shc_dvsn_nm,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::shc_dept_no AS shc_dept_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::shc_dept_nm AS shc_dept_nm,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::shc_cat_grp_no AS shc_cat_grp_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::shc_cat_grp_nm AS shc_cat_grp_nm,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::shc_cat_no AS shc_cat_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::shc_cat_nm AS shc_cat_nm,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::shc_subcat_no AS shc_subcat_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::shc_subcat_nm AS shc_subcat_nm,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::ref_ksn_id AS ref_ksn_id,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_bus_no AS srs_bus_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_bus_nm AS srs_bus_nm,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_div_no AS srs_div_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_div_nm AS srs_div_nm,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_ln_no AS srs_ln_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_ln_ds AS srs_ln_ds,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_sbl_no AS srs_sbl_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_sbl_ds AS srs_sbl_ds,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_cls_no AS srs_cls_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_cls_ds AS srs_cls_ds,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_itm_no AS srs_itm_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_sku_no AS srs_sku_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_div_itm AS srs_div_itm,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::srs_div_itm_sku AS srs_div_itm_sku,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::ima_smt_itm_no AS ima_smt_itm_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::ima_smt_itm_ds AS ima_smt_itm_ds,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::ima_smt_fac_qt AS ima_smt_fac_qt,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::uom AS uom,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::vol AS vol,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::wgt AS wgt,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::vnd_no AS vnd_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::vnd_nm AS vnd_nm,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::vnd_itm_no AS vnd_itm_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::spc_ord_cdt_fl AS spc_ord_cdt_fl,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::itm_emp_fl AS itm_emp_fl,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::easy_ord_fl AS easy_ord_fl,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::itm_rpd_fl AS itm_rpd_fl,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::itm_cd_fl AS itm_cd_fl,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::itm_imp_fl AS itm_imp_fl,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::itm_cs_fl AS itm_cs_fl,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::dd_ind AS dd_ind ,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::instl_ind AS instl_ind ,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::cheetah_elgbl_fl AS cheetah_elgbl_fl,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::dot_com_cd AS dot_com_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::obn_830_fl AS obn_830_fl,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::obn_830_dur AS obn_830_dur,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::rpd_frz_dur AS rpd_frz_dur,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::dist_typ_cd AS dist_typ_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::sls_pfm_seg_cd AS sls_pfm_seg_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::fmt_excl_cd AS fmt_excl_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::str_fcst_cd AS str_fcst_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::ima_itm_typ_cd AS ima_itm_typ_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::itm_purch_sts_cd AS itm_purch_sts_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::ntwk_dist_cd AS ntwk_dist_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::str_reord_auth_cd AS str_reord_auth_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::cross_mdse_attr_cd AS cross_mdse_attr_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::whse_sizing AS whse_sizing,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::can_carr_mdl_id AS can_carr_mdl_id,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::groc_crossover_ind AS groc_crossover_ind,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::owner_cd AS owner_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::pln_id AS pln_id,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::itm_pgm AS itm_pgm,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::key_pgm AS key_pgm,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::natl_un_cst_am AS natl_un_cst_am,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::prd_sll_am AS prd_sll_am,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::size AS size,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::style AS style,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::md_style_ref_cd AS md_style_ref_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::seas_cd AS seas_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::seas_yr AS seas_yr,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::sub_seas_id AS sub_seas_id,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::rpt_id AS rpt_id,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::rpt_id_seq_no AS rpt_id_seq_no,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::itm_fcst_grp_id AS itm_fcst_grp_id,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::itm_fcst_grp_ds AS itm_fcst_grp_ds,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::idrp_itm_typ_ds AS idrp_itm_typ_ds,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::brand_ds AS brand_ds,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::color_ds AS color_ds,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::tire_size_ds AS tire_size_ds,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::elig_sts_cd AS elig_sts_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::lst_sts_chg_dt AS lst_sts_chg_dt,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::itm_del_fl AS itm_del_fl,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::prd_prg_dt AS prd_prg_dt,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::order_system_cd AS order_system_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::idrp_order_method_cd AS idrp_order_method_cd ,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::dotcom_assorted_cd AS dotcom_assorted_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::dotcom_orderable_ind AS dotcom_orderable_ind,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::roadrunner_eligible_fl AS roadrunner_eligible_ind,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::us_dot_ship_type_cd AS us_dot_ship_type_cd,
((chararray)ord_data_lmt_1::JOIN_PACK::PACK_3::package_weight_pounds_qty) as package_weight_in_pounds ,
((chararray)ord_data_lmt_1::JOIN_PACK::PACK_3::package_depth_inch_qty) as package_depth_inch_qty,
((chararray)ord_data_lmt_1::JOIN_PACK::PACK_3::package_height_inch_qty) as package_height_inch_qty,
((chararray)ord_data_lmt_1::JOIN_PACK::PACK_3::package_width_inch_qty) as package_width_inch_qty ,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::mailable_ind AS mailable_ind,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::default_online_ts AS default_online_ts,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
(demand_online_fulfillment_cd =='TW' AND (ups_billable_weight == '0' OR ups_billable_weight IS NULL OR ups_billable_weight =='') ? (chararray)ord_data_lmt_1::JOIN_PACK::PACK_3::pkg_temporary_ups_billable_weight:(chararray)ord_data_lmt_1::JOIN_BILL_TBL_OPT::temporary_ups_billable_weight) as temporary_ups_billable_weight ,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::ups_billable_weight AS  ups_billable_weight,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::ups_billable_weight_ts AS ups_billable_weight_ts,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::demand_ups_billable_weight AS demand_ups_billable_weight,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::web_exclusive_ind AS web_exclusive_ind,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::price_type_desc AS price_type_desc,
ord_data_lmt_1::JOIN_BILL_TBL_OPT::idrp_batch_id AS idrp_batch_id;
------------------------------------------------------------------------------------------------------------------
JOIN_BILL_TBL_OPT_22_FINAL = FOREACH JOIN_BILL_TBL_OPT_22 GENERATE
load_ts,item,descr,shc_dvsn_no,shc_dvsn_nm,shc_dept_no,shc_dept_nm,shc_cat_grp_no,shc_cat_grp_nm,shc_cat_no,shc_cat_nm,shc_subcat_no,shc_subcat_nm,ref_ksn_id,srs_bus_no,srs_bus_nm,srs_div_no,srs_div_nm,srs_ln_no,srs_ln_ds,srs_sbl_no,srs_sbl_ds,srs_cls_no,srs_cls_ds,srs_itm_no,srs_sku_no,srs_div_itm,srs_div_itm_sku,ima_smt_itm_no,ima_smt_itm_ds,ima_smt_fac_qt,uom,vol,wgt,vnd_no,vnd_nm,vnd_itm_no,spc_ord_cdt_fl,itm_emp_fl,easy_ord_fl,itm_rpd_fl,itm_cd_fl,itm_imp_fl,itm_cs_fl,dd_ind,instl_ind,cheetah_elgbl_fl,dot_com_cd,obn_830_fl,obn_830_dur,rpd_frz_dur,dist_typ_cd,sls_pfm_seg_cd,fmt_excl_cd,str_fcst_cd,inv_mgmt_srvc_cd,ima_itm_typ_cd,itm_purch_sts_cd,ntwk_dist_cd,fut_ntwk_dist_cd,fut_ntwk_eff_dt,jit_ntwk_dist_cd,cust_dir_ntwk_cd,str_reord_auth_cd,cross_mdse_attr_cd,whse_sizing,can_carr_mdl_id,groc_crossover_ind,owner_cd,pln_id,itm_pgm,key_pgm,natl_un_cst_am,prd_sll_am,size,style,md_style_ref_cd,seas_cd,seas_yr,sub_seas_id,rpt_id,rpt_id_seq_no,itm_fcst_grp_id,itm_fcst_grp_ds,idrp_itm_typ_ds,brand_ds,color_ds,tire_size_ds,elig_sts_cd,lst_sts_chg_dt,itm_del_fl,prd_prg_dt,order_system_cd,idrp_order_method_cd ,dotcom_assorted_cd,dotcom_orderable_ind,roadrunner_eligible_ind,us_dot_ship_type_cd,package_weight_in_pounds,package_depth_inch_qty,package_height_inch_qty,package_width_inch_qty,mailable_ind,temporary_online_fulfillment_type_cd,default_online_fulfillment_type_cd,default_online_ts, demand_online_fulfillment_cd ,temporary_ups_billable_weight, ups_billable_weight,ups_billable_weight_ts,(ups_billable_weight is NOT NULL AND ups_billable_weight !='' ? ups_billable_weight:temporary_ups_billable_weight) as demand_ups_billable_weight,web_exclusive_ind,price_type_desc,idrp_batch_id;
-------------------------------------------------------------------------------------------------------------------

ERROR_UNION = UNION error_file_1, error_data_gen ;

-----------------------------------------------------------------------------------------------------------------

STORE ERROR_UNION INTO '$WORK__IDRP_ELIGIBLE_ITEM_ERROR' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');  

STORE JOIN_BILL_TBL_OPT_22_FINAL INTO '$WORK__IDRP_ELIGIBLE_ITEM_PART_3' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');  
  

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
