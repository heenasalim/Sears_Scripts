/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_2.pig
# AUTHOR NAME:         Onkar Malewadikar
# CREATION DATE:       Mon Oct 14 11:17:34 EDT 2013
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

gen_join_data_to_rpt_grp = 
    LOAD '$WORK__IDRP_ELIGIBLE_ITEM_1_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($SMITH__IDRP_ELIGIBLE_ITEM_SCHEMA);
	
smith__idrp_eligible_item_loc_data = 
    LOAD '$SMITH__IDRP_ELIGIBLE_ITEM_LOC_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($SMITH__IDRP_ELIGIBLE_ITEM_LOC_SCHEMA);
	
smith__idrp_eligible_loc_data =  
    LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);

/************** GENERATING REQUIRED CLOUMNS FROM ITEM_LOC TABLE AND 
                 TO AVOID NAME CLASHING WE MARK ITEM_ID FROM LOC TABLE AS LOC_ITEM  *****************************************/
gen_smith__idrp_eligible_item_loc_data = 
    FOREACH smith__idrp_eligible_item_loc_data 
	GENERATE
        item AS loc_item, 
	    po_vnd_no,
	    vend_stk_nbr;

/******** GROUP SMITH__IDRP_ELIGIBLE_ITEM_LOC BY ITEM_ID *******************/

grp_dist_idrp_eligible_item_loc = 
    GROUP gen_smith__idrp_eligible_item_loc_data 
	BY (loc_item);

gen_grp_idrp_eligible_item_loc = 
    FOREACH grp_dist_idrp_eligible_item_loc 
	GENERATE 
	    group AS item_id,
		com.searshc.supplychain.idrp.udf.HasMultipleValues(gen_smith__idrp_eligible_item_loc_data.po_vnd_no)   AS po_vnd_no, 
		com.searshc.supplychain.idrp.udf.HasMultipleValues(gen_smith__idrp_eligible_item_loc_data.vend_stk_nbr) AS vend_stk_nbr;

SPLIT gen_grp_idrp_eligible_item_loc 
INTO 
	single_vend_no IF po_vnd_no != 'MULTIPLE',
	multi_data IF po_vnd_no == 'MULTIPLE';

gen_multi_data = 
    FOREACH multi_data 
	GENERATE 
	    item_id AS item_id,
		po_vnd_no AS po_vnd_no,
		'MULTIPLE' AS vnd_nm,
		vend_stk_nbr AS vend_stk_nbr;


		/************ LOADING ELIGIBLE_LOC TABLE  ****************/		
		
smith__idrp_eligible_loc_data = 
    FOREACH smith__idrp_eligible_loc_data 
	GENERATE 
	    loc AS loc,
		descr AS descr;

smith__idrp_eligible_loc_data = 
    DISTINCT smith__idrp_eligible_loc_data;
	
join_single = 
    JOIN single_vend_no BY po_vnd_no,
	     smith__idrp_eligible_loc_data BY loc PARALLEL $NUM_PARALLEL;

vnd_nm_join_single = 
    FOREACH join_single 
	GENERATE 
	    item_id AS item_id,
		po_vnd_no AS po_vnd_no,
		descr AS vnd_nm,
		vend_stk_nbr AS vend_stk_nbr;

union_vnd_nm_no = 
    UNION gen_multi_data,
	      vnd_nm_join_single;
		  
union_vnd_nm_no = 
    DISTINCT union_vnd_nm_no;
	
/*******************JOIN DATA TO PREVIOUS OP *********************************************/

LAST_JOIN = 
    JOIN gen_join_data_to_rpt_grp BY (int)item 
	LEFT OUTER ,
	union_vnd_nm_no BY (int)item_id PARALLEL $NUM_PARALLEL;

/********* GENERATING REQUIRED COLUMNS AND PERFORMING TRANSFORMATION LOGIC **************/

GEN_LAST_JOIN = 
    FOREACH LAST_JOIN 
	GENERATE
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
union_vnd_nm_no::po_vnd_no  AS vnd_no,
union_vnd_nm_no::vnd_nm  AS vnd_nm,
union_vnd_nm_no::vend_stk_nbr  AS vnd_itm_no,
spc_ord_cdt_fl,
itm_emp_fl,
easy_ord_fl,
itm_rpd_fl,
itm_cd_fl,
itm_imp_fl,
itm_cs_fl,
dd_ind ,
instl_ind ,
cheetah_elgbl_fl,
dot_com_cd,
obn_830_fl,
obn_830_dur,
rpd_frz_dur,
dist_typ_cd,
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
rpt_id ,
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
item_order_system_cd,
idrp_order_method_cd,
dotcom_assorted_cd,
dotcom_orderable_ind ,
roadrunner_eligible_ind,
us_dot_ship_type_cd,
package_weight_pounds_qty,
package_depth_inch_qty,
package_height_inch_qty,
package_width_inch_qty,
mailable_ind,
temporary_online_fulfillment_type_cd,
default_online_fulfillment_type_cd,
default_online_ts,
demand_online_fulfillment_cd,
temporary_ups_billable_weight_qty,
ups_billable_weight_qty,
ups_billable_weight_ts,
demand_ups_billable_weight_qty,
web_exclusive_ind,
price_type_desc,
idrp_batch_id ;

/*****************BELOW STEP IS FOR CALCULATING ITM_IMP_FL *************/
        /********GROUP ABOVE DATA ON item_id *************/

gen_imp_fl_data =
    FOREACH smith__idrp_eligible_item_loc_data
    GENERATE
            item AS loc_item,
                imp_fl AS imp_fl;

gen_imp_fl_data =
    DISTINCT gen_imp_fl_data;
-----GROUPING ON item_id
grp_gen_union_vnd_itm_no = 
    GROUP gen_imp_fl_data
	    BY loc_item; 
-- PERFORMING MAX PER item 
max_grp_gen_union_vnd_itm_no = 
    FOREACH grp_gen_union_vnd_itm_no 
    GENERATE *,MAX(gen_imp_fl_data.imp_fl) AS itm_imp_fl; 
	
gen_grp_gen_union_vnd_itm_no = 
    FOREACH max_grp_gen_union_vnd_itm_no 
	GENERATE 
	    FLATTEN(gen_imp_fl_data),
		itm_imp_fl;

gen_grp_gen_union_vnd_itm_no = 
    FOREACH gen_grp_gen_union_vnd_itm_no 
	GENERATE 
		loc_item AS loc_item,
		(itm_imp_fl IS NULL ? '0' : itm_imp_fl) AS itm_imp_fl;

/**********************JOINING WITH PREVIOUS OUTPUT *****************************/

JOIN_LAST =
    JOIN GEN_LAST_JOIN BY (int)item
         LEFT OUTER,
         gen_grp_gen_union_vnd_itm_no BY (int)loc_item PARALLEL $NUM_PARALLEL;
GEN_REQ_COLUMNS =
    FOREACH JOIN_LAST
    GENERATE 
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
        itm_cs_fl AS itm_cs_fl,
        dd_ind  AS dd_ind ,
        instl_ind  AS instl_ind ,
        dot_com_cd AS dot_com_cd,
        obn_830_fl AS obn_830_fl,
        obn_830_dur AS obn_830_dur,
        rpd_frz_dur AS rpd_frz_dur,
        dist_typ_cd AS dist_typ_cd,
        sls_pfm_seg_cd AS sls_pfm_seg_cd,
        fmt_excl_cd AS fmt_excl_cd,
        str_fcst_cd AS str_fcst_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
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
        brand_ds AS brand_ds,
        color_ds AS color_ds,
        tire_size_ds AS tire_size_ds,
        elig_sts_cd AS elig_sts_cd,
        cheetah_elgbl_fl AS cheetah_elgbl_fl,
        inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
        cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
        itm_fcst_grp_id AS itm_fcst_grp_id,
        itm_fcst_grp_ds AS itm_fcst_grp_ds,
        idrp_itm_typ_ds AS idrp_itm_typ_ds,
        lst_sts_chg_dt AS lst_sts_chg_dt,
        itm_del_fl AS itm_del_fl,
        prd_prg_dt AS prd_prg_dt,
        (gen_grp_gen_union_vnd_itm_no::itm_imp_fl IS NULL ? '0' : gen_grp_gen_union_vnd_itm_no::itm_imp_fl) AS itm_imp_fl,
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        easy_ord_fl AS easy_ord_fl,
        itm_emp_fl AS itm_emp_fl,
        itm_cd_fl AS itm_cd_fl,
        itm_rpd_fl AS itm_rpd_fl,
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
  
        
GEN_JOIN_LAST =      
    FOREACH GEN_REQ_COLUMNS
    GENERATE
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
        --TEMP
        itm_cs_fl,
        dd_ind ,
        instl_ind ,
        dot_com_cd,
        obn_830_fl,
        obn_830_dur,
        rpd_frz_dur,
        dist_typ_cd,
        sls_pfm_seg_cd,
        fmt_excl_cd,
        str_fcst_cd,
        ima_itm_typ_cd,
        itm_purch_sts_cd,
        ntwk_dist_cd,
        fut_ntwk_dist_cd,
        fut_ntwk_eff_dt,
        jit_ntwk_dist_cd,
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
        brand_ds,
        color_ds,
        tire_size_ds,
        elig_sts_cd,
        cheetah_elgbl_fl,
        inv_mgmt_srvc_cd,
        cust_dir_ntwk_cd,
        itm_fcst_grp_id,
        itm_fcst_grp_ds,
        idrp_itm_typ_ds,
        lst_sts_chg_dt,
        itm_del_fl,
        prd_prg_dt,
		(owner_cd == 'S' OR owner_cd == 'B' ? (spc_ord_cdt_fl == '1' ? '1'  : '0') : '0' ) AS spc_ord_cdt_fl,
(owner_cd == 'S' OR owner_cd == 'B' ? (spc_ord_cdt_fl == '1' ? '0' : (spc_ord_cdt_fl == '0' AND easy_ord_fl == '1' ? '1' : '0')) :  '0') AS easy_ord_fl,
(owner_cd == 'S' OR owner_cd == 'B'  ? (spc_ord_cdt_fl == '1' ? '0' : (spc_ord_cdt_fl == '0' AND easy_ord_fl == '0' AND itm_imp_fl == '1' AND (itm_emp_fl == '1' OR itm_emp_fl == '0') ? '1' : '0')) : (owner_cd == 'K' ? (itm_imp_fl == '1' ? '1' : '0') : '0') ) AS itm_imp_fl,
(owner_cd == 'S' OR owner_cd == 'B' ? (spc_ord_cdt_fl == '1' ? '0' : (spc_ord_cdt_fl == '0' AND easy_ord_fl == '0' AND itm_emp_fl == '1' ? '1' : '0')) : '0' ) AS itm_emp_fl,
(owner_cd == 'S' OR owner_cd == 'B' ? (spc_ord_cdt_fl == '1' OR easy_ord_fl == '1' OR itm_imp_fl == '1' OR itm_emp_fl == '1' ? '0' : itm_cd_fl) : '0') AS itm_cd_fl,
(owner_cd == 'S' OR owner_cd == 'B' ? (spc_ord_cdt_fl == '1' OR easy_ord_fl == '1' OR itm_imp_fl == '1' OR itm_emp_fl == '1' ? '0' : itm_rpd_fl) : '0') AS itm_rpd_fl,
		item_order_system_cd,
idrp_order_method_cd,
dotcom_assorted_cd,
dotcom_orderable_ind,
roadrunner_eligible_ind,
us_dot_ship_type_cd,
package_weight_pounds_qty,
package_depth_inch_qty,
package_height_inch_qty,
package_width_inch_qty,
mailable_ind,
temporary_online_fulfillment_type_cd,
default_online_fulfillment_type_cd,
default_online_ts,
demand_online_fulfillment_cd,
temporary_ups_billable_weight_qty,
ups_billable_weight_qty,
ups_billable_weight_ts,
demand_ups_billable_weight_qty,
web_exclusive_ind,
price_type_desc,
idrp_batch_id;

/********* GENERATING REQUIRED COLUMNS AND PERFORMING TRANSFORMATION LOGIC **************/
    
GEN_FINAL_RESULT = 
    FOREACH GEN_JOIN_LAST 
	GENERATE
	'$CURRENT_TIMESTAMP' AS load_ts,
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
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        easy_ord_fl AS easy_ord_fl,
        itm_rpd_fl AS itm_rpd_fl,
        itm_cd_fl AS itm_cd_fl,
        itm_imp_fl AS itm_imp_fl,
        itm_cs_fl,
        dd_ind ,
        instl_ind ,
        cheetah_elgbl_fl,
        dot_com_cd,
        obn_830_fl AS obn_830_fl ,
        (obn_830_dur IS NULL ? '0' : obn_830_dur) AS obn_830_dur,
        (rpd_frz_dur IS NULL ? '0' : rpd_frz_dur) AS rpd_frz_dur,
        (dist_typ_cd IS NULL ? 'TW' : dist_typ_cd) AS dist_typ_cd,
        sls_pfm_seg_cd,
        (fmt_excl_cd IS NULL ? 'SHARED' : fmt_excl_cd) AS fmt_excl_cd,
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
        itm_fcst_grp_ds,
        (owner_cd == 'S' OR owner_cd == 'B' ? (spc_ord_cdt_fl == '1' ? 'RSOS' : (easy_ord_fl == '1' ? 'EASY ORDER' : (itm_imp_fl == '1' ? 'IMPORT' : (itm_emp_fl == '1' AND itm_imp_fl == '0' ? 'EMP' : (itm_cd_fl == '1' AND itm_rpd_fl == '1' ? 'CONSTRAINED' : (itm_rpd_fl == '1' AND itm_cd_fl == '0' ? 'RAPID' : 'DOMESTIC')))))) : (owner_cd == 'K' ? (spc_ord_cdt_fl == '1' ? 'RSOS' : (itm_imp_fl == '1' ? 'IMPORT' : 'DOMESTIC')) : 'DOMESTIC' )) AS idrp_itm_typ_ds,
        brand_ds,
        color_ds,
        tire_size_ds,
        elig_sts_cd,
        lst_sts_chg_dt,
        '0' AS itm_del_fl,
        prd_prg_dt,
		item_order_system_cd,
idrp_order_method_cd,
dotcom_assorted_cd,
dotcom_orderable_ind,
roadrunner_eligible_ind,
us_dot_ship_type_cd,
package_weight_pounds_qty,
package_depth_inch_qty,
package_height_inch_qty,
package_width_inch_qty,
mailable_ind,
temporary_online_fulfillment_type_cd,
default_online_fulfillment_type_cd,
default_online_ts,
demand_online_fulfillment_cd,
temporary_ups_billable_weight_qty,
ups_billable_weight_qty,
ups_billable_weight_ts,
demand_ups_billable_weight_qty,
web_exclusive_ind,
price_type_desc,
'$batchid' AS idrp_batch_id;

        
GEN_FINAL_RESULT = 
    DISTINCT GEN_FINAL_RESULT;

/**************** STORE COMPLETE ITEM DATA**************************************/

STORE GEN_FINAL_RESULT 
INTO '$WORK__IDRP_ELIGIBLE_ITEM_2_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/**************** END OF SCRIPT ************************************************/

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
