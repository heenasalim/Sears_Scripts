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
/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

--LOAD CORE Bridge Item file

LOAD_CORE_BRIDGE_ITEM = LOAD '$GOLD__ITEM_CORE_BRIDGE_ITEM_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_CORE_BRIDGE_ITEM_SCHEMA);

--LOAD ELIGIBLE Item Part 2 file
LOAD_ELIGIBLE_ITEM_2 = LOAD '$WORK__IDRP_ELIGIBLE_ITEM_PART_1' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_ITEM_SCHEMA);

--LOAD GOLD ITEM HIERARCHY Package file
LOAD_GOLD_ITEM = LOAD '$GOLD__ITEM_SHC_HIERARCHY_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_SHC_HIERARCHY_CURRENT_SCHEMA);

--LOAD ELIGIBLE Item Loc file
LOAD_ELIGIBLE_ITEM_LOC = LOAD '$SMITH__IDRP_ELIGIBLE_ITEM_LOC_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);

------------------------------------------------------------------------------------------------------------------

-- Calculating order_system_cd ;
---------------------------------------------------------------------------------------------------------------------

LOAD_ELIGIBLE_ITEM_2 = FOREACH LOAD_ELIGIBLE_ITEM_2 GENERATE

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
item_order_system_cd AS item_order_system_cd,
idrp_order_method_cd AS idrp_order_method_cd,
dotcom_assorted_cd AS dotcom_assorted_cd,
dotcom_orderable_ind AS dotcom_orderable_ind,
roadrunner_eligible_ind AS roadrunner_eligible_ind,
us_dot_ship_type_cd AS us_dot_ship_type_cd,
package_weight_pounds_qty AS package_weight_pounds_qty,
package_depth_inch_qty AS package_depth_inch_qty,
package_height_inch_qty AS package_height_inch_qty,
package_width_inch_qty AS package_width_inch_qty,
mailable_ind AS mailable_ind,
temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
default_online_ts AS default_online_ts,
demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
temporary_ups_billable_weight_qty AS temporary_ups_billable_weight_qty,
ups_billable_weight_qty AS ups_billable_weight_qty,
ups_billable_weight_ts AS ups_billable_weight_ts,
demand_ups_billable_weight_qty AS demand_ups_billable_weight_qty,
web_exclusive_ind AS web_exclusive_ind,
price_type_desc AS price_type_desc,
idrp_batch_id AS idrp_batch_id;



NEW_JOIN = JOIN LOAD_ELIGIBLE_ITEM_2 BY (srs_div_no,srs_itm_no) LEFT OUTER, LOAD_CORE_BRIDGE_ITEM BY (sears_division_nbr,sears_item_nbr) ;


NEW_JOIN_OUTPUT = GROUP NEW_JOIN BY LOAD_ELIGIBLE_ITEM_2::item;

NEW_JOIN_OUTPUT_FINAL = FOREACH NEW_JOIN_OUTPUT
                                           { ord_data_1 = ORDER NEW_JOIN BY LOAD_CORE_BRIDGE_ITEM::item_order_system_cd ASC;
                                                         ord_data_lmt_1 = LIMIT ord_data_1 1;
                                                         GENERATE FLATTEN(ord_data_lmt_1);
														 
											};			 
ITEM_2 = FOREACH NEW_JOIN_OUTPUT_FINAL GENERATE

LOAD_ELIGIBLE_ITEM_2::load_ts as  load_ts,item,descr,shc_dvsn_no,shc_dvsn_nm,shc_dept_no,shc_dept_nm,shc_cat_grp_no,shc_cat_grp_nm,shc_cat_no,shc_cat_nm,shc_subcat_no,shc_subcat_nm,ref_ksn_id,srs_bus_no,srs_bus_nm,srs_div_no,srs_div_nm,srs_ln_no,srs_ln_ds,srs_sbl_no,srs_sbl_ds,srs_cls_no,srs_cls_ds,srs_itm_no,srs_sku_no,srs_div_itm,srs_div_itm_sku,ima_smt_itm_no,ima_smt_itm_ds,ima_smt_fac_qt,uom,vol,wgt,vnd_no,vnd_nm,vnd_itm_no,spc_ord_cdt_fl,itm_emp_fl,easy_ord_fl,itm_rpd_fl,itm_cd_fl,itm_imp_fl,itm_cs_fl,dd_ind,instl_ind,cheetah_elgbl_fl,dot_com_cd,obn_830_fl,obn_830_dur,rpd_frz_dur,dist_typ_cd,sls_pfm_seg_cd,fmt_excl_cd,str_fcst_cd,inv_mgmt_srvc_cd,ima_itm_typ_cd,itm_purch_sts_cd,ntwk_dist_cd,fut_ntwk_dist_cd,fut_ntwk_eff_dt,jit_ntwk_dist_cd,cust_dir_ntwk_cd,str_reord_auth_cd,cross_mdse_attr_cd,whse_sizing,can_carr_mdl_id,groc_crossover_ind,owner_cd,pln_id,itm_pgm,key_pgm,natl_un_cst_am,prd_sll_am,size,style,md_style_ref_cd,seas_cd,seas_yr,sub_seas_id,rpt_id,rpt_id_seq_no,itm_fcst_grp_id,itm_fcst_grp_ds,idrp_itm_typ_ds,brand_ds,color_ds,tire_size_ds,elig_sts_cd,lst_sts_chg_dt,itm_del_fl,prd_prg_dt,LOAD_CORE_BRIDGE_ITEM::item_order_system_cd as order_system_cd,idrp_order_method_cd ,dotcom_assorted_cd,dotcom_orderable_ind,roadrunner_eligible_ind,us_dot_ship_type_cd,package_weight_pounds_qty,package_depth_inch_qty,package_height_inch_qty,package_width_inch_qty,mailable_ind,temporary_online_fulfillment_type_cd,default_online_fulfillment_type_cd,default_online_ts,demand_online_fulfillment_cd,temporary_ups_billable_weight_qty,ups_billable_weight_qty,ups_billable_weight_ts,demand_ups_billable_weight_qty,web_exclusive_ind,price_type_desc,idrp_batch_id;


---------------------------------------------------------------------------------------------------------------
ITEM_2_DISTINCT = DISTINCT ITEM_2 ;

--------------------------------------------------------------------------------------------------------------

-- Calculating dotcom_assorted_cd ;

--------------------------------------------------------------------------------------------------------------
LOAD_GOLD_ITEM_NEW = FOREACH LOAD_GOLD_ITEM GENERATE

(item_id is NULL ? '':item_id) as item_id,
(ksn_purchase_status_cd is NULL ? '': ksn_purchase_status_cd) as ksn_purchase_status_cd,
(dotcom_eligibility_cd is NULL ? '': dotcom_eligibility_cd) as dotcom_eligibility_cd,
(ksn_id is NULL ? '': ksn_id) as ksn_id ;

GOLD_ITEM = FILTER LOAD_GOLD_ITEM_NEW BY ksn_purchase_status_cd  != 'U' AND dotcom_eligibility_cd  == '1' ;

GOLD_ITEMS = FOREACH GOLD_ITEM GENERATE item_id;

DISTINCT_GOLD_ITEMS = DISTINCT GOLD_ITEMS;

JOIN_SMITH_GOLD = JOIN ITEM_2_DISTINCT BY item LEFT OUTER, DISTINCT_GOLD_ITEMS BY item_id ;

NEW_COLS = FOREACH JOIN_SMITH_GOLD GENERATE

load_ts,item,descr,shc_dvsn_no,shc_dvsn_nm,shc_dept_no,shc_dept_nm,shc_cat_grp_no,shc_cat_grp_nm,shc_cat_no,shc_cat_nm,shc_subcat_no,shc_subcat_nm,ref_ksn_id,srs_bus_no,srs_bus_nm,srs_div_no,srs_div_nm,srs_ln_no,srs_ln_ds,srs_sbl_no,srs_sbl_ds,srs_cls_no,srs_cls_ds,srs_itm_no,srs_sku_no,srs_div_itm,srs_div_itm_sku,ima_smt_itm_no,ima_smt_itm_ds,ima_smt_fac_qt,uom,vol,wgt,vnd_no,vnd_nm,vnd_itm_no,spc_ord_cdt_fl,itm_emp_fl,easy_ord_fl,itm_rpd_fl,itm_cd_fl,itm_imp_fl,itm_cs_fl,dd_ind,instl_ind,cheetah_elgbl_fl,dot_com_cd,obn_830_fl,obn_830_dur,rpd_frz_dur,dist_typ_cd,sls_pfm_seg_cd,fmt_excl_cd,str_fcst_cd,inv_mgmt_srvc_cd,ima_itm_typ_cd,itm_purch_sts_cd,ntwk_dist_cd,fut_ntwk_dist_cd,fut_ntwk_eff_dt,jit_ntwk_dist_cd,cust_dir_ntwk_cd,str_reord_auth_cd,cross_mdse_attr_cd,whse_sizing,can_carr_mdl_id,groc_crossover_ind,owner_cd,pln_id,itm_pgm,key_pgm,natl_un_cst_am,prd_sll_am,size,style,md_style_ref_cd,seas_cd,seas_yr,sub_seas_id,rpt_id,rpt_id_seq_no,itm_fcst_grp_id,itm_fcst_grp_ds,idrp_itm_typ_ds,brand_ds,color_ds,tire_size_ds,elig_sts_cd,lst_sts_chg_dt,itm_del_fl,prd_prg_dt,order_system_cd,idrp_order_method_cd ,

((DISTINCT_GOLD_ITEMS::item_id != '' AND DISTINCT_GOLD_ITEMS::item_id IS NOT NULL ) ? '1':'0') as dotcom_assorted_cd,

dotcom_orderable_ind,roadrunner_eligible_ind,us_dot_ship_type_cd,package_weight_pounds_qty,package_depth_inch_qty,package_height_inch_qty,package_width_inch_qty,mailable_ind,temporary_online_fulfillment_type_cd,default_online_fulfillment_type_cd,default_online_ts,demand_online_fulfillment_cd,temporary_ups_billable_weight_qty,ups_billable_weight_qty,ups_billable_weight_ts,demand_ups_billable_weight_qty,web_exclusive_ind,price_type_desc,idrp_batch_id;

---------------------------------------------------------------------------------------------------------------------------------

-- Calculating dotcom_orderable_ind ;
------------------------------------------------------------------------------------------------------------------------
LOAD_ELIGIBLE_ITEM_LOC_FILTER = FILTER LOAD_ELIGIBLE_ITEM_LOC BY (elig_sts_cd == 'A');

D1= FOREACH LOAD_ELIGIBLE_ITEM_LOC_FILTER GENERATE
item as item,
(dotcom_order_indicator is NULL ? '':dotcom_order_indicator) as dotcom_order_indicator;

D2 = GROUP D1 BY  item;
D3 = FOREACH D2 GENERATE group as item, GetDotComOrderableIndicator(D1) as dotcom_order_indicator;

JOIN_1 = JOIN NEW_COLS BY item LEFT OUTER, D3 BY item ;

NEW_COLS_NEXT = FOREACH JOIN_1 GENERATE
NEW_COLS::ITEM_2_DISTINCT::load_ts as load_ts,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::item as item,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::descr as descr,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::shc_dvsn_no as shc_dvsn_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::shc_dvsn_nm as shc_dvsn_nm,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::shc_dept_no as shc_dept_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::shc_dept_nm as shc_dept_nm,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::shc_cat_grp_no as shc_cat_grp_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::shc_cat_grp_nm as shc_cat_grp_nm,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::shc_cat_no as shc_cat_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::shc_cat_nm as shc_cat_nm,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::shc_subcat_no as shc_subcat_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::shc_subcat_nm as shc_subcat_nm,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::ref_ksn_id as ref_ksn_id,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_bus_no as srs_bus_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_bus_nm as srs_bus_nm ,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_div_no as srs_div_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_div_nm as srs_div_nm ,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_ln_no as srs_ln_no ,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_ln_ds as srs_ln_ds ,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_sbl_no as srs_sbl_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_sbl_ds as srs_sbl_ds,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_cls_no as srs_cls_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_cls_ds as srs_cls_ds ,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_itm_no as srs_itm_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_sku_no as srs_sku_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_div_itm  as srs_div_itm,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::srs_div_itm_sku as srs_div_itm_sku,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::ima_smt_itm_no  as ima_smt_itm_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::ima_smt_itm_ds as ima_smt_itm_ds,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::ima_smt_fac_qt as ima_smt_fac_qt,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::uom as uom,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::vol as vol,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::wgt as wgt,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::vnd_no as vnd_no ,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::vnd_nm as vnd_nm ,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::vnd_itm_no as vnd_itm_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::spc_ord_cdt_fl as spc_ord_cdt_fl,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::itm_emp_fl as itm_emp_fl,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::easy_ord_fl as easy_ord_fl,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::itm_rpd_fl as itm_rpd_fl,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::itm_cd_fl as itm_cd_fl,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::itm_imp_fl as itm_imp_fl,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::itm_cs_fl as itm_cs_fl,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::dd_ind as dd_ind,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::instl_ind as instl_ind,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::cheetah_elgbl_fl as cheetah_elgbl_fl,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::dot_com_cd as dot_com_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::obn_830_fl as obn_830_fl,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::obn_830_dur as obn_830_dur,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::rpd_frz_dur as rpd_frz_dur,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::dist_typ_cd as dist_typ_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::sls_pfm_seg_cd as sls_pfm_seg_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::fmt_excl_cd as fmt_excl_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::str_fcst_cd as str_fcst_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::inv_mgmt_srvc_cd as inv_mgmt_srvc_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::ima_itm_typ_cd as ima_itm_typ_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::itm_purch_sts_cd as itm_purch_sts_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::ntwk_dist_cd as ntwk_dist_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::fut_ntwk_dist_cd as fut_ntwk_dist_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::fut_ntwk_eff_dt as fut_ntwk_eff_dt,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::jit_ntwk_dist_cd as jit_ntwk_dist_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::cust_dir_ntwk_cd as cust_dir_ntwk_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::str_reord_auth_cd as str_reord_auth_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::cross_mdse_attr_cd  as cross_mdse_attr_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::whse_sizing as whse_sizing,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::can_carr_mdl_id as can_carr_mdl_id,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::groc_crossover_ind as groc_crossover_ind,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::owner_cd as owner_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::pln_id as pln_id,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::itm_pgm as itm_pgm,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::key_pgm as key_pgm,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::natl_un_cst_am as natl_un_cst_am,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::prd_sll_am as prd_sll_am,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::size as size,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::style as style,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::md_style_ref_cd  as md_style_ref_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::seas_cd as seas_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::seas_yr as seas_yr,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::sub_seas_id as sub_seas_id,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::rpt_id as rpt_id,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::rpt_id_seq_no as rpt_id_seq_no,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::itm_fcst_grp_id as itm_fcst_grp_id,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::itm_fcst_grp_ds as itm_fcst_grp_ds,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::idrp_itm_typ_ds as idrp_itm_typ_ds,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::brand_ds as brand_ds,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::color_ds as color_ds,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::tire_size_ds as tire_size_ds,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::elig_sts_cd as elig_sts_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::lst_sts_chg_dt as lst_sts_chg_dt,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::itm_del_fl as itm_del_fl,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::prd_prg_dt as prd_prg_dt,
NEW_COLS::ITEM_2_DISTINCT::order_system_cd as order_system_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::idrp_order_method_cd as idrp_order_method_cd,
(NEW_COLS::dotcom_assorted_cd is NULL ? '' : NEW_COLS::dotcom_assorted_cd) as dotcom_assorted_cd,
(D3::dotcom_order_indicator is NULL ? '': D3::dotcom_order_indicator) as dotcom_orderable_ind,
'' as roadrunner_eligible_ind ,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::us_dot_ship_type_cd as us_dot_ship_type_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::package_weight_pounds_qty as package_weight_pounds_qty,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::package_depth_inch_qty as package_depth_inch_qty,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::package_height_inch_qty as package_height_inch_qty ,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::package_width_inch_qty as package_width_inch_qty,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::mailable_ind as mailable_ind,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::temporary_online_fulfillment_type_cd as temporary_online_fulfillment_type_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::default_online_fulfillment_type_cd as default_online_fulfillment_type_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::default_online_ts as default_online_ts,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::demand_online_fulfillment_cd as demand_online_fulfillment_cd,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::temporary_ups_billable_weight_qty as temporary_ups_billable_weight_qty,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::ups_billable_weight_qty as ups_billable_weight_qty,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::ups_billable_weight_ts as ups_billable_weight_ts,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::demand_ups_billable_weight_qty as demand_ups_billable_weight_qty,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::web_exclusive_ind AS web_exclusive_ind,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::price_type_desc AS price_type_desc,
NEW_COLS::ITEM_2_DISTINCT::ord_data_lmt_1::LOAD_ELIGIBLE_ITEM_2::idrp_batch_id AS idrp_batch_id;
-------------------------------------------------------------------------------------------------------------------

STORE NEW_COLS_NEXT INTO '$WORK__IDRP_ELIGIBLE_ITEM_PART_2' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
