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
SET default_parallel $NUM_PARALLEL;
/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

--LOAD ELIGIBLE loc file
LOAD_ELIGIBLE_LOC = LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);

--LOAD ELIGIBLE Item file
LOAD_ELIGIBLE_ITEM = LOAD '$WORK__IDRP_ELIGIBLE_ITEM_2_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_ITEM_SCHEMA);

--LOAD ELIGIBLE Item Loc file
LOAD_ELIGIBLE_ITEM_LOC = LOAD '$SMITH__IDRP_ELIGIBLE_ITEM_LOC_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);

------------------------------------------------------------------------------------------------------------------

SPLIT LOAD_ELIGIBLE_ITEM_LOC INTO D1 IF ( rim_sts_cd == 'R' OR  rim_sts_cd == 'S' OR  rim_sts_cd == 'C' OR  rim_sts_cd == 'P' OR  rim_sts_cd == 'L' OR  rim_sts_cd == 'Z' OR  rim_sts_cd == 'E'), D2 IF (reorder_method_code == 'A' OR ( rim_sts_cd == 'R' OR  rim_sts_cd == 'S' OR  rim_sts_cd == 'C' OR  rim_sts_cd == 'P')) ;

D1_ITEM = FOREACH D1 GENERATE item ;
D1_DIST_ITEM = DISTINCT D1_ITEM ;

LOAD_ELIGIBLE_LOC_FILTER = FILTER LOAD_ELIGIBLE_LOC BY loc_lvl_cd == 'STORE' ;

ITEM_LOC_LOC_JOIN = JOIN D2 BY loc LEFT OUTER, LOAD_ELIGIBLE_LOC_FILTER BY loc ;

D2_ITEM = FOREACH ITEM_LOC_LOC_JOIN GENERATE D2::item ;
D2_DIST_ITEM = DISTINCT D2_ITEM;

LOAD_ELIGIBLE_ITEM = FOREACH LOAD_ELIGIBLE_ITEM GENERATE

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


LOAD_ELIGIBLE_ITEM_JOIN = JOIN LOAD_ELIGIBLE_ITEM BY item LEFT OUTER, D1_DIST_ITEM BY item using 'skewed';


NEW_ELIGIBLE_ITEM = FOREACH LOAD_ELIGIBLE_ITEM_JOIN GENERATE

'$CURRENT_TIMESTAMP'	as	load_ts	,
(LOAD_ELIGIBLE_ITEM::item is NULL ? '': LOAD_ELIGIBLE_ITEM::item ) as item,
LOAD_ELIGIBLE_ITEM::descr	as	descr	,
(LOAD_ELIGIBLE_ITEM::shc_dvsn_no is NULL ? '': LOAD_ELIGIBLE_ITEM::shc_dvsn_no )	as	shc_dvsn_no	,
LOAD_ELIGIBLE_ITEM::shc_dvsn_nm	as	shc_dvsn_nm	,
LOAD_ELIGIBLE_ITEM::shc_dept_no	as	shc_dept_no	,
LOAD_ELIGIBLE_ITEM::shc_dept_nm	as	shc_dept_nm	,
LOAD_ELIGIBLE_ITEM::shc_cat_grp_no	as	shc_cat_grp_no	,
LOAD_ELIGIBLE_ITEM::shc_cat_grp_nm	as	shc_cat_grp_nm	,
LOAD_ELIGIBLE_ITEM::shc_cat_no	as	shc_cat_no	,
LOAD_ELIGIBLE_ITEM::shc_cat_nm	as	shc_cat_nm	,
LOAD_ELIGIBLE_ITEM::shc_subcat_no	as	shc_subcat_no	,
LOAD_ELIGIBLE_ITEM::shc_subcat_nm	as	shc_subcat_nm	,
LOAD_ELIGIBLE_ITEM::ref_ksn_id	as	ref_ksn_id	,
LOAD_ELIGIBLE_ITEM::srs_bus_no	as	srs_bus_no	,
LOAD_ELIGIBLE_ITEM::srs_bus_nm	as	srs_bus_nm	,
(LOAD_ELIGIBLE_ITEM::srs_div_no is NULL ? '':LOAD_ELIGIBLE_ITEM::srs_div_no)	as	srs_div_no	,
LOAD_ELIGIBLE_ITEM::srs_div_nm	as	srs_div_nm	,
LOAD_ELIGIBLE_ITEM::srs_ln_no	as	srs_ln_no	,
LOAD_ELIGIBLE_ITEM::srs_ln_ds	as	srs_ln_ds	,
(LOAD_ELIGIBLE_ITEM::srs_sbl_no is NULL ? '': LOAD_ELIGIBLE_ITEM::srs_sbl_no )	as	srs_sbl_no	,
LOAD_ELIGIBLE_ITEM::srs_sbl_ds	as	srs_sbl_ds	,
LOAD_ELIGIBLE_ITEM::srs_cls_no	as	srs_cls_no	,
LOAD_ELIGIBLE_ITEM::srs_cls_ds	as	srs_cls_ds	,
LOAD_ELIGIBLE_ITEM::srs_itm_no	as	srs_itm_no	,
LOAD_ELIGIBLE_ITEM::srs_sku_no	as	srs_sku_no	,
LOAD_ELIGIBLE_ITEM::srs_div_itm	as	srs_div_itm	,
LOAD_ELIGIBLE_ITEM::srs_div_itm_sku	as	srs_div_itm_sku	,
LOAD_ELIGIBLE_ITEM::ima_smt_itm_no	as	ima_smt_itm_no	,
LOAD_ELIGIBLE_ITEM::ima_smt_itm_ds	as	ima_smt_itm_ds	,
LOAD_ELIGIBLE_ITEM::ima_smt_fac_qt	as	ima_smt_fac_qt	,
LOAD_ELIGIBLE_ITEM::uom	as	uom	,
LOAD_ELIGIBLE_ITEM::vol	as	vol	,
LOAD_ELIGIBLE_ITEM::wgt	as	wgt	,
LOAD_ELIGIBLE_ITEM::vnd_no	as	vnd_no	,
LOAD_ELIGIBLE_ITEM::vnd_nm	as	vnd_nm	,
LOAD_ELIGIBLE_ITEM::vnd_itm_no	as	 vnd_itm_no	,
LOAD_ELIGIBLE_ITEM::spc_ord_cdt_fl	as	spc_ord_cdt_fl	,
LOAD_ELIGIBLE_ITEM::itm_emp_fl	as	itm_emp_fl	,
LOAD_ELIGIBLE_ITEM::easy_ord_fl	as	easy_ord_fl	,
LOAD_ELIGIBLE_ITEM::itm_rpd_fl	as	itm_rpd_fl	,
LOAD_ELIGIBLE_ITEM::itm_cd_fl	as	itm_cd_fl	,
LOAD_ELIGIBLE_ITEM::itm_imp_fl	as	itm_imp_fl	,
LOAD_ELIGIBLE_ITEM::itm_cs_fl	as	itm_cs_fl	,
LOAD_ELIGIBLE_ITEM::dd_ind 	as	dd_ind 	,
LOAD_ELIGIBLE_ITEM::instl_ind 	as	instl_ind 	,
LOAD_ELIGIBLE_ITEM::cheetah_elgbl_fl as cheetah_elgbl_fl,
LOAD_ELIGIBLE_ITEM::dot_com_cd	as	dot_com_cd	,
LOAD_ELIGIBLE_ITEM::obn_830_fl	as	obn_830_fl	,
LOAD_ELIGIBLE_ITEM::obn_830_dur	as	obn_830_dur	,
LOAD_ELIGIBLE_ITEM::rpd_frz_dur	as	rpd_frz_dur	,
LOAD_ELIGIBLE_ITEM::dist_typ_cd	as	dist_typ_cd	,
LOAD_ELIGIBLE_ITEM::sls_pfm_seg_cd	as	sls_pfm_seg_cd	,
LOAD_ELIGIBLE_ITEM::fmt_excl_cd	as	fmt_excl_cd	,
(LOAD_ELIGIBLE_ITEM::str_fcst_cd is NULL  ? '':LOAD_ELIGIBLE_ITEM::str_fcst_cd) as str_fcst_cd	,
(LOAD_ELIGIBLE_ITEM::ima_itm_typ_cd == 'EXAS' ? 'ALLOC':'NONE') as inv_mgmt_srvc_cd,
(LOAD_ELIGIBLE_ITEM::ima_itm_typ_cd	is NULL ? '': LOAD_ELIGIBLE_ITEM::ima_itm_typ_cd) as	ima_itm_typ_cd	,
LOAD_ELIGIBLE_ITEM::itm_purch_sts_cd	as	itm_purch_sts_cd	,
LOAD_ELIGIBLE_ITEM::ntwk_dist_cd	as	ntwk_dist_cd	,
LOAD_ELIGIBLE_ITEM::fut_ntwk_dist_cd	as	fut_ntwk_dist_cd	,
LOAD_ELIGIBLE_ITEM::fut_ntwk_eff_dt	as	fut_ntwk_eff_dt	,
LOAD_ELIGIBLE_ITEM::jit_ntwk_dist_cd	as	jit_ntwk_dist_cd	,
LOAD_ELIGIBLE_ITEM::cust_dir_ntwk_cd as cust_dir_ntwk_cd,
LOAD_ELIGIBLE_ITEM::str_reord_auth_cd	as	str_reord_auth_cd	,
LOAD_ELIGIBLE_ITEM::cross_mdse_attr_cd	as	cross_mdse_attr_cd	,
LOAD_ELIGIBLE_ITEM::whse_sizing	as	whse_sizing	,
LOAD_ELIGIBLE_ITEM::can_carr_mdl_id	as	can_carr_mdl_id	,
LOAD_ELIGIBLE_ITEM::groc_crossover_ind	as	groc_crossover_ind	,
LOAD_ELIGIBLE_ITEM::owner_cd	as	owner_cd	,
LOAD_ELIGIBLE_ITEM::pln_id	as	pln_id	,
LOAD_ELIGIBLE_ITEM::itm_pgm	as	itm_pgm	,
LOAD_ELIGIBLE_ITEM::key_pgm	as	key_pgm	,
LOAD_ELIGIBLE_ITEM::natl_un_cst_am	as	natl_un_cst_am	,
LOAD_ELIGIBLE_ITEM::prd_sll_am	as	prd_sll_am	,
LOAD_ELIGIBLE_ITEM::size	as	size	,
LOAD_ELIGIBLE_ITEM::style	as	style	,
LOAD_ELIGIBLE_ITEM::md_style_ref_cd	as	md_style_ref_cd	,
LOAD_ELIGIBLE_ITEM::seas_cd	as	seas_cd	,
LOAD_ELIGIBLE_ITEM::seas_yr	as	seas_yr	,
LOAD_ELIGIBLE_ITEM::sub_seas_id	as	sub_seas_id	,
(LOAD_ELIGIBLE_ITEM::rpt_id is NULL ? '': LOAD_ELIGIBLE_ITEM::rpt_id)	as	rpt_id	,
LOAD_ELIGIBLE_ITEM::rpt_id_seq_no	as	rpt_id_seq_no	,
LOAD_ELIGIBLE_ITEM::itm_fcst_grp_id	as itm_fcst_grp_id,  
LOAD_ELIGIBLE_ITEM::itm_fcst_grp_ds	as	itm_fcst_grp_ds	, 
LOAD_ELIGIBLE_ITEM::idrp_itm_typ_ds	as	idrp_itm_typ_ds	,
LOAD_ELIGIBLE_ITEM::brand_ds	as	brand_ds	,
LOAD_ELIGIBLE_ITEM::color_ds	as	color_ds	,
LOAD_ELIGIBLE_ITEM::tire_size_ds	as	tire_size_ds	,
LOAD_ELIGIBLE_ITEM::elig_sts_cd	as	elig_sts_cd	,
LOAD_ELIGIBLE_ITEM::lst_sts_chg_dt	as	lst_sts_chg_dt	,
LOAD_ELIGIBLE_ITEM::itm_del_fl	as	itm_del_fl	,
LOAD_ELIGIBLE_ITEM::prd_prg_dt	as	prd_prg_dt	,
''	as	order_system_cd	,
(LOAD_ELIGIBLE_ITEM::ima_itm_typ_cd == 'EXAS'? 'A':'N')	as	idrp_order_method_cd ,
''	as	dotcom_assorted_cd	,
''	as	dotcom_orderable_ind	,
''	as	roadrunner_eligible_fl	,
''	as	us_dot_ship_type_cd	,
''	as	package_weight_in_pounds	,
''	as	package_depth_inch_qty	,
''	as	package_height_inch_qty	,
''	as	package_width_inch_qty	,
''	as	mailable_ind	,
''	as	temporary_online_fulfillment_type_cd	,
''	as	default_online_fulfillment_type_cd	,
''	as	default_online_ts	,
''	as	demand_online_fulfillment_cd	,
''	as	temporary_ups_billable_weight	,
''	as	ups_billable_weight	,
''	as	ups_billable_weight_ts	,
''	as	demand_ups_billable_weight	,
LOAD_ELIGIBLE_ITEM::web_exclusive_ind AS web_exclusive_ind,
LOAD_ELIGIBLE_ITEM::price_type_desc AS price_type_desc,
LOAD_ELIGIBLE_ITEM::idrp_batch_id AS idrp_batch_id,
(D1_DIST_ITEM::item is NULL ? '':D1_DIST_ITEM::item) as item_2;
----------------------------------------------------------------------------------------------------------------------------------

-- Calculating str_fcst_cd, itm_fcst_grp_id
----------------------------------------------------------------------------------------------------------------------------------

NEW_ELIGIBLE_ITEM_2 = FOREACH NEW_ELIGIBLE_ITEM GENERATE
load_ts,item,descr,shc_dvsn_no,shc_dvsn_nm,shc_dept_no,shc_dept_nm,shc_cat_grp_no,shc_cat_grp_nm,shc_cat_no,shc_cat_nm,
shc_subcat_no,shc_subcat_nm,ref_ksn_id,srs_bus_no,srs_bus_nm,srs_div_no,srs_div_nm,srs_ln_no,
srs_ln_ds,srs_sbl_no,srs_sbl_ds,srs_cls_no,srs_cls_ds,srs_itm_no,srs_sku_no,srs_div_itm,
srs_div_itm_sku,ima_smt_itm_no,ima_smt_itm_ds,ima_smt_fac_qt,uom,vol,wgt,vnd_no,vnd_nm,vnd_itm_no,
spc_ord_cdt_fl,itm_emp_fl,easy_ord_fl,itm_rpd_fl,itm_cd_fl,itm_imp_fl,itm_cs_fl,dd_ind ,
instl_ind,cheetah_elgbl_fl ,dot_com_cd,obn_830_fl,obn_830_dur,rpd_frz_dur,dist_typ_cd,sls_pfm_seg_cd,fmt_excl_cd,

(str_fcst_cd == '' AND ( ima_itm_typ_cd == 'TYP' OR  ima_itm_typ_cd == 'IIRC') AND item_2 !='' ? 'S':str_fcst_cd ) as	str_fcst_cd	,
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
whse_sizing,
can_carr_mdl_id,
groc_crossover_ind,
owner_cd,
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

(shc_dvsn_no == '35' ? CONCAT(rpt_id,'-G'): ( shc_dvsn_no == '72' AND srs_sbl_no !='' ? CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(srs_div_no,'-'),srs_ln_no),'-'),srs_sbl_no),'-G') : ((shc_dvsn_no == '72' AND srs_sbl_no =='')? CONCAT((chararray)item,'-G'):((shc_dvsn_no == '7' OR shc_dvsn_no == '23' OR shc_dvsn_no == '26' OR shc_dvsn_no == '34' OR shc_dvsn_no == '41' OR shc_dvsn_no == '85' OR shc_dvsn_no == '86' OR shc_dvsn_no == '87' OR shc_dvsn_no == '88') AND srs_div_no  !='' ? CONCAT(srs_div_no,'-G'):((shc_dvsn_no == '7' OR shc_dvsn_no == '23' OR shc_dvsn_no == '26' OR shc_dvsn_no == '34' OR shc_dvsn_no == '41' OR shc_dvsn_no == '85' OR shc_dvsn_no == '86' OR shc_dvsn_no == '87' OR shc_dvsn_no == '88') AND srs_div_no  =='' ? CONCAT((chararray)item,'-G'):CONCAT((chararray)item,'-G'))))))  as   itm_fcst_grp_id ,

itm_fcst_grp_ds,
idrp_itm_typ_ds,
brand_ds,
color_ds,
tire_size_ds,
elig_sts_cd,
lst_sts_chg_dt,
itm_del_fl,
prd_prg_dt,
order_system_cd,
idrp_order_method_cd,
dotcom_assorted_cd,
dotcom_orderable_ind,
roadrunner_eligible_fl,
us_dot_ship_type_cd,
package_weight_in_pounds,
package_depth_inch_qty,
package_height_inch_qty,
package_width_inch_qty,
mailable_ind,
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
idrp_batch_id,
item_2;
--------------------------------------------------------------------------------------------------------------------------------------

--Calculating inv_mgmt_srvc_cd, itm_fcst_grp_ds, idrp_order_method_cd
----------------------------------------------------------------------------------------------------------------------------------

LOAD_ELIGIBLE_ITEM_JOIN_2 = JOIN NEW_ELIGIBLE_ITEM_2 BY item LEFT OUTER, D2_DIST_ITEM BY item using 'skewed';

NEW_ELIGIBLE_ITEM_3 = FOREACH LOAD_ELIGIBLE_ITEM_JOIN_2 GENERATE
load_ts,
NEW_ELIGIBLE_ITEM_2::item as item,descr,shc_dvsn_no,shc_dvsn_nm,shc_dept_no,shc_dept_nm,shc_cat_grp_no,shc_cat_grp_nm,shc_cat_no,shc_cat_nm,
shc_subcat_no,shc_subcat_nm,ref_ksn_id,srs_bus_no,srs_bus_nm,srs_div_no,srs_div_nm,srs_ln_no,
srs_ln_ds,srs_sbl_no,srs_sbl_ds,srs_cls_no,srs_cls_ds,srs_itm_no,srs_sku_no,srs_div_itm,
srs_div_itm_sku,ima_smt_itm_no,ima_smt_itm_ds,ima_smt_fac_qt,uom,vol,wgt,vnd_no,vnd_nm,vnd_itm_no,
spc_ord_cdt_fl,itm_emp_fl,easy_ord_fl,itm_rpd_fl,itm_cd_fl,itm_imp_fl,itm_cs_fl,dd_ind ,
instl_ind,cheetah_elgbl_fl ,dot_com_cd,obn_830_fl,obn_830_dur,rpd_frz_dur,dist_typ_cd,sls_pfm_seg_cd,fmt_excl_cd,
str_fcst_cd	,
(( D2_DIST_ITEM::D2::item != '' AND D2_DIST_ITEM::D2::item is NOT NULL  AND str_fcst_cd == 'S' AND ( ima_itm_typ_cd == 'TYP' OR  ima_itm_typ_cd == 'IIRC')) ? 'REPL':'ALLOC') as inv_mgmt_srvc_cd,
ima_itm_typ_cd,
itm_purch_sts_cd,
ntwk_dist_cd,
fut_ntwk_dist_cd,
fut_ntwk_eff_dt,
jit_ntwk_dist_cd,
cust_dir_ntwk_cd,
str_reord_auth_cd,
cross_mdse_attr_cd,
whse_sizing,
can_carr_mdl_id,
groc_crossover_ind,
owner_cd,
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
CONCAT((chararray)itm_fcst_grp_id,' Forecast Group') as itm_fcst_grp_ds,
idrp_itm_typ_ds,
brand_ds,
color_ds,
tire_size_ds,
elig_sts_cd,
lst_sts_chg_dt,
itm_del_fl,
prd_prg_dt,
order_system_cd,
(( D2_DIST_ITEM::D2::item != ''  AND D2_DIST_ITEM::D2::item is NOT NULL  AND str_fcst_cd == 'S' AND ( ima_itm_typ_cd == 'TYP' OR  ima_itm_typ_cd == 'IIRC'))? 'R':'A') as idrp_order_method_cd ,
dotcom_assorted_cd,
dotcom_orderable_ind,
roadrunner_eligible_fl,
us_dot_ship_type_cd,
package_weight_in_pounds,
package_depth_inch_qty,
package_height_inch_qty,
package_width_inch_qty,
mailable_ind,
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

------------------------------------------------------------------------------------------------------------

NEW_ELIGIBLE_ITEM_3_DISTINCT = DISTINCT NEW_ELIGIBLE_ITEM_3 ;

STORE NEW_ELIGIBLE_ITEM_3_DISTINCT INTO '$WORK__IDRP_ELIGIBLE_ITEM_PART_1' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
