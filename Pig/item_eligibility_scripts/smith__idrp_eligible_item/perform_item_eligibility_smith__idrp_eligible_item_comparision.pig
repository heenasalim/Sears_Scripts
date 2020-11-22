/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_comparision.pig
# AUTHOR NAME:         Onkar Malewadikar
# CREATION DATE:       Mon Oct 14 11:22:16 EDT 2013
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


/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/
SET default_parallel $NUM_PARALLEL;
--load history data
smith__idrp_eligible_item_history = 
    LOAD '$SMITH__IDRP_ELIGIBLE_ITEM_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($SMITH__IDRP_ELIGIBLE_ITEM_SCHEMA);

----load current data
smith__idrp_eligible_item_current = 
    LOAD '$WORK__IDRP_ELIGIBLE_ITEM_PART_3' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($SMITH__IDRP_ELIGIBLE_ITEM_SCHEMA);
	
-- join the two data sources
smith__idrp_eligible_item_current_join_history = 
    JOIN smith__idrp_eligible_item_current by (item) 
	     FULL OUTER ,
		 smith__idrp_eligible_item_history by (item) PARALLEL $NUM_PARALLEL;

---- if item is absent from current
smith__idrp_eligible_item_history_only = 
    FILTER smith__idrp_eligible_item_current_join_history 
	BY ( smith__idrp_eligible_item_current::item IS NULL );
		 
-- if item is absent from history
smith__idrp_eligible_item_current_only = 
    FILTER smith__idrp_eligible_item_current_join_history 
	BY ( smith__idrp_eligible_item_history::item IS NULL );

-- if item is present in both i.e. history and current
smith__idrp_eligible_item_both_current_and_history = 
    FILTER smith__idrp_eligible_item_current_join_history 
	BY (smith__idrp_eligible_item_current::item == smith__idrp_eligible_item_history::item) ;

-- condition for comparing and updating data in history
/*
Compare remaining current store level records to the store level record on prior days 
IE Item/Location table using Item_ID and LOC. 
Add any new records to the IE Item/Locationt table, 
replace matching records with current data and update missing records (on prior day only) by setting the ELIG_STS_CD to .D. uneligible.
*/

-- prepare columns for only current rows, get data from current rows to be added to history record
smith__idrp_eligible_item_current_only_gen = 
    FOREACH smith__idrp_eligible_item_current_only 
	GENERATE  
        smith__idrp_eligible_item_current::load_ts AS load_ts,
		smith__idrp_eligible_item_current::item AS item,
        smith__idrp_eligible_item_current::descr AS descr,
        smith__idrp_eligible_item_current::shc_dvsn_no AS shc_dvsn_no,
        smith__idrp_eligible_item_current::shc_dvsn_nm AS shc_dvsn_nm,
        smith__idrp_eligible_item_current::shc_dept_no AS shc_dept_no,
        smith__idrp_eligible_item_current::shc_dept_nm AS shc_dept_nm,
        smith__idrp_eligible_item_current::shc_cat_grp_no AS shc_cat_grp_no,
        smith__idrp_eligible_item_current::shc_cat_grp_nm AS shc_cat_grp_nm,
        smith__idrp_eligible_item_current::shc_cat_no AS shc_cat_no,
        smith__idrp_eligible_item_current::shc_cat_nm AS shc_cat_nm,
        smith__idrp_eligible_item_current::shc_subcat_no AS shc_subcat_no,
        smith__idrp_eligible_item_current::shc_subcat_nm AS shc_subcat_nm,
        smith__idrp_eligible_item_current::ref_ksn_id AS ref_ksn_id,
        smith__idrp_eligible_item_current::srs_bus_no AS srs_bus_no,
        smith__idrp_eligible_item_current::srs_bus_nm AS srs_bus_nm,
        smith__idrp_eligible_item_current::srs_div_no AS srs_div_no,
        smith__idrp_eligible_item_current::srs_div_nm AS srs_div_nm,
        smith__idrp_eligible_item_current::srs_ln_no AS srs_ln_no,
        smith__idrp_eligible_item_current::srs_ln_ds AS srs_ln_ds,
        smith__idrp_eligible_item_current::srs_sbl_no AS srs_sbl_no,
        smith__idrp_eligible_item_current::srs_sbl_ds AS srs_sbl_ds,
        smith__idrp_eligible_item_current::srs_cls_no AS srs_cls_no,
        smith__idrp_eligible_item_current::srs_cls_ds AS srs_cls_ds,
        smith__idrp_eligible_item_current::srs_itm_no AS srs_itm_no,
        smith__idrp_eligible_item_current::srs_sku_no AS srs_sku_no,
        smith__idrp_eligible_item_current::srs_div_itm AS srs_div_itm,
        smith__idrp_eligible_item_current::srs_div_itm_sku AS srs_div_itm_sku,
        smith__idrp_eligible_item_current::ima_smt_itm_no AS ima_smt_itm_no,
        smith__idrp_eligible_item_current::ima_smt_itm_ds AS ima_smt_itm_ds,
        smith__idrp_eligible_item_current::ima_smt_fac_qt AS ima_smt_fac_qt,
        smith__idrp_eligible_item_current::uom AS uom,
        smith__idrp_eligible_item_current::vol AS vol,
        smith__idrp_eligible_item_current::wgt AS wgt,
        smith__idrp_eligible_item_current::vnd_no AS vnd_no,
        smith__idrp_eligible_item_current::vnd_nm AS vnd_nm,
        smith__idrp_eligible_item_current::vnd_itm_no AS vnd_itm_no,
        smith__idrp_eligible_item_current::spc_ord_cdt_fl AS spc_ord_cdt_fl,
        smith__idrp_eligible_item_current::itm_emp_fl AS itm_emp_fl,
        smith__idrp_eligible_item_current::easy_ord_fl AS easy_ord_fl,
        smith__idrp_eligible_item_current::itm_rpd_fl AS itm_rpd_fl,
        smith__idrp_eligible_item_current::itm_cd_fl AS itm_cd_fl,
        smith__idrp_eligible_item_current::itm_imp_fl AS itm_imp_fl,
        smith__idrp_eligible_item_current::itm_cs_fl AS itm_cs_fl,
        smith__idrp_eligible_item_current::dd_ind AS dd_ind,
        smith__idrp_eligible_item_current::instl_ind AS instl_ind,
        smith__idrp_eligible_item_current::cheetah_elgbl_fl AS cheetah_elgbl_fl,
        smith__idrp_eligible_item_current::dot_com_cd AS dot_com_cd,
        smith__idrp_eligible_item_current::obn_830_fl AS obn_830_fl,
        smith__idrp_eligible_item_current::obn_830_dur AS obn_830_dur,
        smith__idrp_eligible_item_current::rpd_frz_dur AS rpd_frz_dur,
        smith__idrp_eligible_item_current::dist_typ_cd AS dist_typ_cd,
        smith__idrp_eligible_item_current::sls_pfm_seg_cd AS sls_pfm_seg_cd,
        smith__idrp_eligible_item_current::fmt_excl_cd AS fmt_excl_cd,
        smith__idrp_eligible_item_current::str_fcst_cd AS str_fcst_cd,
        smith__idrp_eligible_item_current::inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
        smith__idrp_eligible_item_current::ima_itm_typ_cd AS ima_itm_typ_cd,
        smith__idrp_eligible_item_current::itm_purch_sts_cd AS itm_purch_sts_cd,
        smith__idrp_eligible_item_current::ntwk_dist_cd AS ntwk_dist_cd,
        smith__idrp_eligible_item_current::fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        smith__idrp_eligible_item_current::fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        smith__idrp_eligible_item_current::jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        smith__idrp_eligible_item_current::cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
        smith__idrp_eligible_item_current::str_reord_auth_cd AS str_reord_auth_cd,
        smith__idrp_eligible_item_current::cross_mdse_attr_cd AS cross_mdse_attr_cd,
        smith__idrp_eligible_item_current::whse_sizing AS whse_sizing,
        smith__idrp_eligible_item_current::can_carr_mdl_id AS can_carr_mdl_id,
        smith__idrp_eligible_item_current::groc_crossover_ind AS groc_crossover_ind,
        smith__idrp_eligible_item_current::owner_cd AS owner_cd,
        smith__idrp_eligible_item_current::pln_id AS pln_id,
        smith__idrp_eligible_item_current::itm_pgm AS itm_pgm,
        smith__idrp_eligible_item_current::key_pgm AS key_pgm,
        smith__idrp_eligible_item_current::natl_un_cst_am AS natl_un_cst_am,
        smith__idrp_eligible_item_current::prd_sll_am AS prd_sll_am,
        smith__idrp_eligible_item_current::size AS size,
        smith__idrp_eligible_item_current::style AS style,
        smith__idrp_eligible_item_current::md_style_ref_cd AS md_style_ref_cd,
        smith__idrp_eligible_item_current::seas_cd AS seas_cd,
        smith__idrp_eligible_item_current::seas_yr AS seas_yr,
        smith__idrp_eligible_item_current::sub_seas_id AS sub_seas_id,
        smith__idrp_eligible_item_current::rpt_id AS rpt_id,
        smith__idrp_eligible_item_current::rpt_id_seq_no AS rpt_id_seq_no,
        smith__idrp_eligible_item_current::itm_fcst_grp_id AS itm_fcst_grp_id,
        smith__idrp_eligible_item_current::itm_fcst_grp_ds AS itm_fcst_grp_ds,
        smith__idrp_eligible_item_current::idrp_itm_typ_ds AS idrp_itm_typ_ds,
        smith__idrp_eligible_item_current::brand_ds AS brand_ds,
        smith__idrp_eligible_item_current::color_ds AS color_ds,
        smith__idrp_eligible_item_current::tire_size_ds AS tire_size_ds,
        'A' AS elig_sts_cd,
        '$CURRENT_DATE' AS lst_sts_chg_dt,
        smith__idrp_eligible_item_current::itm_del_fl AS itm_del_fl,
        smith__idrp_eligible_item_current::prd_prg_dt AS prd_prg_dt,
		smith__idrp_eligible_item_current::item_order_system_cd AS item_order_system_cd,
smith__idrp_eligible_item_current::idrp_order_method_cd AS idrp_order_method_cd,
smith__idrp_eligible_item_current::dotcom_assorted_cd AS dotcom_assorted_cd,
smith__idrp_eligible_item_current::dotcom_orderable_ind AS dotcom_orderable_ind,
smith__idrp_eligible_item_current::roadrunner_eligible_ind AS roadrunner_eligible_ind,
smith__idrp_eligible_item_current::us_dot_ship_type_cd AS us_dot_ship_type_cd,
smith__idrp_eligible_item_current::package_weight_pounds_qty AS package_weight_pounds_qty,
smith__idrp_eligible_item_current::package_depth_inch_qty AS package_depth_inch_qty,
smith__idrp_eligible_item_current::package_height_inch_qty AS package_height_inch_qty,
smith__idrp_eligible_item_current::package_width_inch_qty AS package_width_inch_qty,
smith__idrp_eligible_item_current::mailable_ind AS mailable_ind,
smith__idrp_eligible_item_current::temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
smith__idrp_eligible_item_current::default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
smith__idrp_eligible_item_current::default_online_ts AS default_online_ts,
smith__idrp_eligible_item_current::demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
smith__idrp_eligible_item_current::temporary_ups_billable_weight_qty AS temporary_ups_billable_weight_qty,
smith__idrp_eligible_item_current::ups_billable_weight_qty AS ups_billable_weight_qty,
smith__idrp_eligible_item_current::ups_billable_weight_ts AS ups_billable_weight_ts,
smith__idrp_eligible_item_current::demand_ups_billable_weight_qty AS demand_ups_billable_weight_qty,
smith__idrp_eligible_item_current::web_exclusive_ind AS web_exclusive_ind,
smith__idrp_eligible_item_current::price_type_desc AS price_type_desc,
smith__idrp_eligible_item_current::idrp_batch_id AS idrp_batch_id;
				
				
-- prepare columns for only history rows, get data from current rows into history record
smith__idrp_eligible_item_history_only_gen = 
    FOREACH smith__idrp_eligible_item_history_only 
	GENERATE  
        smith__idrp_eligible_item_history::load_ts AS load_ts,
		smith__idrp_eligible_item_history::item AS item,
        smith__idrp_eligible_item_history::descr AS descr,
        smith__idrp_eligible_item_history::shc_dvsn_no AS shc_dvsn_no,
        smith__idrp_eligible_item_history::shc_dvsn_nm AS shc_dvsn_nm,
        smith__idrp_eligible_item_history::shc_dept_no AS shc_dept_no,
        smith__idrp_eligible_item_history::shc_dept_nm AS shc_dept_nm,
        smith__idrp_eligible_item_history::shc_cat_grp_no AS shc_cat_grp_no,
        smith__idrp_eligible_item_history::shc_cat_grp_nm AS shc_cat_grp_nm,
        smith__idrp_eligible_item_history::shc_cat_no AS shc_cat_no,
        smith__idrp_eligible_item_history::shc_cat_nm AS shc_cat_nm,
        smith__idrp_eligible_item_history::shc_subcat_no AS shc_subcat_no,
        smith__idrp_eligible_item_history::shc_subcat_nm AS shc_subcat_nm,
        smith__idrp_eligible_item_history::ref_ksn_id AS ref_ksn_id,
        smith__idrp_eligible_item_history::srs_bus_no AS srs_bus_no,
        smith__idrp_eligible_item_history::srs_bus_nm AS srs_bus_nm,
        smith__idrp_eligible_item_history::srs_div_no AS srs_div_no,
        smith__idrp_eligible_item_history::srs_div_nm AS srs_div_nm,
        smith__idrp_eligible_item_history::srs_ln_no AS srs_ln_no,
        smith__idrp_eligible_item_history::srs_ln_ds AS srs_ln_ds,
        smith__idrp_eligible_item_history::srs_sbl_no AS srs_sbl_no,
        smith__idrp_eligible_item_history::srs_sbl_ds AS srs_sbl_ds,
        smith__idrp_eligible_item_history::srs_cls_no AS srs_cls_no,
        smith__idrp_eligible_item_history::srs_cls_ds AS srs_cls_ds,
        smith__idrp_eligible_item_history::srs_itm_no AS srs_itm_no,
        smith__idrp_eligible_item_history::srs_sku_no AS srs_sku_no,
        smith__idrp_eligible_item_history::srs_div_itm AS srs_div_itm,
        smith__idrp_eligible_item_history::srs_div_itm_sku AS srs_div_itm_sku,
        smith__idrp_eligible_item_history::ima_smt_itm_no AS ima_smt_itm_no,
        smith__idrp_eligible_item_history::ima_smt_itm_ds AS ima_smt_itm_ds,
        smith__idrp_eligible_item_history::ima_smt_fac_qt AS ima_smt_fac_qt,
        smith__idrp_eligible_item_history::uom AS uom,
        smith__idrp_eligible_item_history::vol AS vol,
        smith__idrp_eligible_item_history::wgt AS wgt,
        smith__idrp_eligible_item_history::vnd_no AS vnd_no,
        smith__idrp_eligible_item_history::vnd_nm AS vnd_nm,
        smith__idrp_eligible_item_history::vnd_itm_no AS vnd_itm_no,
        smith__idrp_eligible_item_history::spc_ord_cdt_fl AS spc_ord_cdt_fl,
        smith__idrp_eligible_item_history::itm_emp_fl AS itm_emp_fl,
        smith__idrp_eligible_item_history::easy_ord_fl AS easy_ord_fl,
        smith__idrp_eligible_item_history::itm_rpd_fl AS itm_rpd_fl,
        smith__idrp_eligible_item_history::itm_cd_fl AS itm_cd_fl,
        smith__idrp_eligible_item_history::itm_imp_fl AS itm_imp_fl,
        smith__idrp_eligible_item_history::itm_cs_fl AS itm_cs_fl,
        smith__idrp_eligible_item_history::dd_ind AS dd_ind,
        smith__idrp_eligible_item_history::instl_ind AS instl_ind,
        smith__idrp_eligible_item_history::cheetah_elgbl_fl AS cheetah_elgbl_fl,
        smith__idrp_eligible_item_history::dot_com_cd AS dot_com_cd,
        smith__idrp_eligible_item_history::obn_830_fl AS obn_830_fl,
        smith__idrp_eligible_item_history::obn_830_dur AS obn_830_dur,
        smith__idrp_eligible_item_history::rpd_frz_dur AS rpd_frz_dur,
        smith__idrp_eligible_item_history::dist_typ_cd AS dist_typ_cd,
        smith__idrp_eligible_item_history::sls_pfm_seg_cd AS sls_pfm_seg_cd,
        smith__idrp_eligible_item_history::fmt_excl_cd AS fmt_excl_cd,
        smith__idrp_eligible_item_history::str_fcst_cd AS str_fcst_cd,
        smith__idrp_eligible_item_history::inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
        smith__idrp_eligible_item_history::ima_itm_typ_cd AS ima_itm_typ_cd,
        smith__idrp_eligible_item_history::itm_purch_sts_cd AS itm_purch_sts_cd,
        smith__idrp_eligible_item_history::ntwk_dist_cd AS ntwk_dist_cd,
        smith__idrp_eligible_item_history::fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        smith__idrp_eligible_item_history::fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        smith__idrp_eligible_item_history::jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        smith__idrp_eligible_item_history::cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
        smith__idrp_eligible_item_history::str_reord_auth_cd AS str_reord_auth_cd,
        smith__idrp_eligible_item_history::cross_mdse_attr_cd AS cross_mdse_attr_cd,
        smith__idrp_eligible_item_history::whse_sizing AS whse_sizing,
        smith__idrp_eligible_item_history::can_carr_mdl_id AS can_carr_mdl_id,
        smith__idrp_eligible_item_history::groc_crossover_ind AS groc_crossover_ind,
        smith__idrp_eligible_item_history::owner_cd AS owner_cd,
        smith__idrp_eligible_item_history::pln_id AS pln_id,
        smith__idrp_eligible_item_history::itm_pgm AS itm_pgm,
        smith__idrp_eligible_item_history::key_pgm AS key_pgm,
        smith__idrp_eligible_item_history::natl_un_cst_am AS natl_un_cst_am,
        smith__idrp_eligible_item_history::prd_sll_am AS prd_sll_am,
        smith__idrp_eligible_item_history::size AS size,
        smith__idrp_eligible_item_history::style AS style,
        smith__idrp_eligible_item_history::md_style_ref_cd AS md_style_ref_cd,
        smith__idrp_eligible_item_history::seas_cd AS seas_cd,
        smith__idrp_eligible_item_history::seas_yr AS seas_yr,
        smith__idrp_eligible_item_history::sub_seas_id AS sub_seas_id,
        smith__idrp_eligible_item_history::rpt_id AS rpt_id,
        smith__idrp_eligible_item_history::rpt_id_seq_no AS rpt_id_seq_no,
        smith__idrp_eligible_item_history::itm_fcst_grp_id AS itm_fcst_grp_id,
        smith__idrp_eligible_item_history::itm_fcst_grp_ds AS itm_fcst_grp_ds,
        smith__idrp_eligible_item_history::idrp_itm_typ_ds AS idrp_itm_typ_ds,
        smith__idrp_eligible_item_history::brand_ds AS brand_ds,
        smith__idrp_eligible_item_history::color_ds AS color_ds,
        smith__idrp_eligible_item_history::tire_size_ds AS tire_size_ds,
        'D' AS elig_sts_cd,                                     --mark the row AS D for eligibility code
        (smith__idrp_eligible_item_history::elig_sts_cd != 'D' ? '$CURRENT_DATE' : smith__idrp_eligible_item_history::lst_sts_chg_dt) AS lst_sts_chg_dt,
        smith__idrp_eligible_item_history::itm_del_fl AS itm_del_fl,
        smith__idrp_eligible_item_history::prd_prg_dt AS prd_prg_dt,
		smith__idrp_eligible_item_history::item_order_system_cd AS item_order_system_cd,
smith__idrp_eligible_item_history::idrp_order_method_cd AS idrp_order_method_cd,
smith__idrp_eligible_item_history::dotcom_assorted_cd AS dotcom_assorted_cd,
smith__idrp_eligible_item_history::dotcom_orderable_ind AS dotcom_orderable_ind,
smith__idrp_eligible_item_history::roadrunner_eligible_ind AS roadrunner_eligible_ind,
smith__idrp_eligible_item_history::us_dot_ship_type_cd AS us_dot_ship_type_cd,
smith__idrp_eligible_item_history::package_weight_pounds_qty AS package_weight_pounds_qty,
smith__idrp_eligible_item_history::package_depth_inch_qty AS package_depth_inch_qty,
smith__idrp_eligible_item_history::package_height_inch_qty AS package_height_inch_qty,
smith__idrp_eligible_item_history::package_width_inch_qty AS package_width_inch_qty,
smith__idrp_eligible_item_history::mailable_ind AS mailable_ind,
smith__idrp_eligible_item_history::temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
smith__idrp_eligible_item_history::default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
smith__idrp_eligible_item_history::default_online_ts AS default_online_ts,
smith__idrp_eligible_item_history::demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
smith__idrp_eligible_item_history::temporary_ups_billable_weight_qty AS temporary_ups_billable_weight_qty,
smith__idrp_eligible_item_history::ups_billable_weight_qty AS ups_billable_weight_qty,
smith__idrp_eligible_item_history::ups_billable_weight_ts AS ups_billable_weight_ts,
smith__idrp_eligible_item_history::demand_ups_billable_weight_qty AS demand_ups_billable_weight_qty,
smith__idrp_eligible_item_history::web_exclusive_ind AS web_exclusive_ind,
smith__idrp_eligible_item_history::price_type_desc AS price_type_desc,
smith__idrp_eligible_item_history::idrp_batch_id AS idrp_batch_id;
				
-- for rows in both the files
-- prepare columns for  rows in both data source, get data from current rows into history record
smith__idrp_eligible_item_both_current_and_history_gen = 
    FOREACH smith__idrp_eligible_item_both_current_and_history 
	GENERATE 
	smith__idrp_eligible_item_current::load_ts AS load_ts,
        smith__idrp_eligible_item_current::item AS item,
        smith__idrp_eligible_item_current::descr AS descr,
        smith__idrp_eligible_item_current::shc_dvsn_no AS shc_dvsn_no,
        smith__idrp_eligible_item_current::shc_dvsn_nm AS shc_dvsn_nm,
        smith__idrp_eligible_item_current::shc_dept_no AS shc_dept_no,
        smith__idrp_eligible_item_current::shc_dept_nm AS shc_dept_nm,
        smith__idrp_eligible_item_current::shc_cat_grp_no AS shc_cat_grp_no,
        smith__idrp_eligible_item_current::shc_cat_grp_nm AS shc_cat_grp_nm,
        smith__idrp_eligible_item_current::shc_cat_no AS shc_cat_no,
        smith__idrp_eligible_item_current::shc_cat_nm AS shc_cat_nm,
        smith__idrp_eligible_item_current::shc_subcat_no AS shc_subcat_no,
        smith__idrp_eligible_item_current::shc_subcat_nm AS shc_subcat_nm,
        smith__idrp_eligible_item_current::ref_ksn_id AS ref_ksn_id,
        smith__idrp_eligible_item_current::srs_bus_no AS srs_bus_no,
        smith__idrp_eligible_item_current::srs_bus_nm AS srs_bus_nm,
        smith__idrp_eligible_item_current::srs_div_no AS srs_div_no,
        smith__idrp_eligible_item_current::srs_div_nm AS srs_div_nm,
        smith__idrp_eligible_item_current::srs_ln_no AS srs_ln_no,
        smith__idrp_eligible_item_current::srs_ln_ds AS srs_ln_ds,
        smith__idrp_eligible_item_current::srs_sbl_no AS srs_sbl_no,
        smith__idrp_eligible_item_current::srs_sbl_ds AS srs_sbl_ds,
        smith__idrp_eligible_item_current::srs_cls_no AS srs_cls_no,
        smith__idrp_eligible_item_current::srs_cls_ds AS srs_cls_ds,
        smith__idrp_eligible_item_current::srs_itm_no AS srs_itm_no,
        smith__idrp_eligible_item_current::srs_sku_no AS srs_sku_no,
        smith__idrp_eligible_item_current::srs_div_itm AS srs_div_itm,
        smith__idrp_eligible_item_current::srs_div_itm_sku AS srs_div_itm_sku,
        smith__idrp_eligible_item_current::ima_smt_itm_no AS ima_smt_itm_no,
        smith__idrp_eligible_item_current::ima_smt_itm_ds AS ima_smt_itm_ds,
        smith__idrp_eligible_item_current::ima_smt_fac_qt AS ima_smt_fac_qt,
        smith__idrp_eligible_item_current::uom AS uom,
        smith__idrp_eligible_item_current::vol AS vol,
        smith__idrp_eligible_item_current::wgt AS wgt,
        smith__idrp_eligible_item_current::vnd_no AS vnd_no,
        smith__idrp_eligible_item_current::vnd_nm AS vnd_nm,
        smith__idrp_eligible_item_current::vnd_itm_no AS vnd_itm_no,
        smith__idrp_eligible_item_current::spc_ord_cdt_fl AS spc_ord_cdt_fl,
        smith__idrp_eligible_item_current::itm_emp_fl AS itm_emp_fl,
        smith__idrp_eligible_item_current::easy_ord_fl AS easy_ord_fl,
        smith__idrp_eligible_item_current::itm_rpd_fl AS itm_rpd_fl,
        smith__idrp_eligible_item_current::itm_cd_fl AS itm_cd_fl,
        smith__idrp_eligible_item_current::itm_imp_fl AS itm_imp_fl,
        smith__idrp_eligible_item_current::itm_cs_fl AS itm_cs_fl,
        smith__idrp_eligible_item_current::dd_ind AS dd_ind,
        smith__idrp_eligible_item_current::instl_ind AS instl_ind,
        smith__idrp_eligible_item_current::cheetah_elgbl_fl AS cheetah_elgbl_fl,
        smith__idrp_eligible_item_current::dot_com_cd AS dot_com_cd,
        smith__idrp_eligible_item_current::obn_830_fl AS obn_830_fl,
        smith__idrp_eligible_item_current::obn_830_dur AS obn_830_dur,
        smith__idrp_eligible_item_current::rpd_frz_dur AS rpd_frz_dur,
        smith__idrp_eligible_item_current::dist_typ_cd AS dist_typ_cd,
        smith__idrp_eligible_item_current::sls_pfm_seg_cd AS sls_pfm_seg_cd,
        smith__idrp_eligible_item_current::fmt_excl_cd AS fmt_excl_cd,
        smith__idrp_eligible_item_current::str_fcst_cd AS str_fcst_cd,
        smith__idrp_eligible_item_current::inv_mgmt_srvc_cd AS inv_mgmt_srvc_cd,
        smith__idrp_eligible_item_current::ima_itm_typ_cd AS ima_itm_typ_cd,
        smith__idrp_eligible_item_current::itm_purch_sts_cd AS itm_purch_sts_cd,
        smith__idrp_eligible_item_current::ntwk_dist_cd AS ntwk_dist_cd,
        smith__idrp_eligible_item_current::fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        smith__idrp_eligible_item_current::fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        smith__idrp_eligible_item_current::jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        smith__idrp_eligible_item_current::cust_dir_ntwk_cd AS cust_dir_ntwk_cd,
        smith__idrp_eligible_item_current::str_reord_auth_cd AS str_reord_auth_cd,
        smith__idrp_eligible_item_current::cross_mdse_attr_cd AS cross_mdse_attr_cd,
        smith__idrp_eligible_item_current::whse_sizing AS whse_sizing,
        smith__idrp_eligible_item_current::can_carr_mdl_id AS can_carr_mdl_id,
        smith__idrp_eligible_item_current::groc_crossover_ind AS groc_crossover_ind,
        smith__idrp_eligible_item_current::owner_cd AS owner_cd,
        smith__idrp_eligible_item_current::pln_id AS pln_id,
        smith__idrp_eligible_item_current::itm_pgm AS itm_pgm,
        smith__idrp_eligible_item_current::key_pgm AS key_pgm,
        smith__idrp_eligible_item_current::natl_un_cst_am AS natl_un_cst_am,
        smith__idrp_eligible_item_current::prd_sll_am AS prd_sll_am,
        smith__idrp_eligible_item_current::size AS size,
        smith__idrp_eligible_item_current::style AS style,
        smith__idrp_eligible_item_current::md_style_ref_cd AS md_style_ref_cd,
        smith__idrp_eligible_item_current::seas_cd AS seas_cd,
        smith__idrp_eligible_item_current::seas_yr AS seas_yr,
        smith__idrp_eligible_item_current::sub_seas_id AS sub_seas_id,
        smith__idrp_eligible_item_current::rpt_id AS rpt_id,
        smith__idrp_eligible_item_current::rpt_id_seq_no AS rpt_id_seq_no,
        smith__idrp_eligible_item_current::itm_fcst_grp_id AS itm_fcst_grp_id,
        smith__idrp_eligible_item_current::itm_fcst_grp_ds AS itm_fcst_grp_ds,
        smith__idrp_eligible_item_current::idrp_itm_typ_ds AS idrp_itm_typ_ds,
        smith__idrp_eligible_item_current::brand_ds AS brand_ds,
        smith__idrp_eligible_item_current::color_ds AS color_ds,
        smith__idrp_eligible_item_current::tire_size_ds AS tire_size_ds,
        'A' AS elig_sts_cd,
        (smith__idrp_eligible_item_history::elig_sts_cd == 'D' ? '$CURRENT_DATE' : smith__idrp_eligible_item_history::lst_sts_chg_dt) AS lst_sts_chg_dt,
        smith__idrp_eligible_item_current::itm_del_fl AS itm_del_fl,
        smith__idrp_eligible_item_current::prd_prg_dt AS prd_prg_dt,
		smith__idrp_eligible_item_current::item_order_system_cd AS item_order_system_cd,
smith__idrp_eligible_item_current::idrp_order_method_cd AS idrp_order_method_cd,
smith__idrp_eligible_item_current::dotcom_assorted_cd AS dotcom_assorted_cd,
smith__idrp_eligible_item_current::dotcom_orderable_ind AS dotcom_orderable_ind,
smith__idrp_eligible_item_current::roadrunner_eligible_ind AS roadrunner_eligible_ind,
smith__idrp_eligible_item_current::us_dot_ship_type_cd AS us_dot_ship_type_cd,
smith__idrp_eligible_item_current::package_weight_pounds_qty AS package_weight_pounds_qty,
smith__idrp_eligible_item_current::package_depth_inch_qty AS package_depth_inch_qty,
smith__idrp_eligible_item_current::package_height_inch_qty AS package_height_inch_qty,
smith__idrp_eligible_item_current::package_width_inch_qty AS package_width_inch_qty,
smith__idrp_eligible_item_current::mailable_ind AS mailable_ind,
smith__idrp_eligible_item_current::temporary_online_fulfillment_type_cd AS temporary_online_fulfillment_type_cd,
smith__idrp_eligible_item_current::default_online_fulfillment_type_cd AS default_online_fulfillment_type_cd,
smith__idrp_eligible_item_current::default_online_ts AS default_online_ts,
smith__idrp_eligible_item_current::demand_online_fulfillment_cd AS demand_online_fulfillment_cd,
smith__idrp_eligible_item_current::temporary_ups_billable_weight_qty AS temporary_ups_billable_weight_qty,
smith__idrp_eligible_item_current::ups_billable_weight_qty AS ups_billable_weight_qty,
smith__idrp_eligible_item_current::ups_billable_weight_ts AS ups_billable_weight_ts,
smith__idrp_eligible_item_current::demand_ups_billable_weight_qty AS demand_ups_billable_weight_qty,
smith__idrp_eligible_item_current::web_exclusive_ind AS web_exclusive_ind,
smith__idrp_eligible_item_current::price_type_desc AS price_type_desc,
smith__idrp_eligible_item_current::idrp_batch_id AS idrp_batch_id;
				
smith__idrp_eligible_item_final_union = 
    UNION smith__idrp_eligible_item_history_only_gen, 
	      smith__idrp_eligible_item_current_only_gen,
		  smith__idrp_eligible_item_both_current_and_history_gen;
				
--This will be previous data for tomorrow's run
/************* STORING DATA TO HDFS *******************/
STORE smith__idrp_eligible_item_final_union 
INTO '$WORK__IDRP_ELIGIBLE_ITEM_COMPARISION_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');
/************ END OF SCRIPT ***************************/

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
