/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_1.pig
# AUTHOR NAME:         Onkar Malewadikar
# CREATION DATE:       Mon Oct 14 11:17:13 EDT 2013
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

--TODO revist this approach
SET default_parallel $NUM_PARALLEL;

--register udf jar
REGISTER $UDF_JAR;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

/******************************* LOAD FOR ALL TABLES AND FILES REQUIRED ***********************************/
smith__item_combined_hierarchy_current_data = 
    LOAD '$SMITH__ITEM_COMBINED_HIERARCHY_CURRENT_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($SMITH__ITEM_COMBINED_HIERARCHY_CURRENT_SCHEMA);

gold__item_sears_channel_distribution_current_data = 
    LOAD '$GOLD__ITEM_SEARS_CHANNEL_DISTRIBUTION_CURRENT_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($GOLD__ITEM_SEARS_CHANNEL_DISTRIBUTION_CURRENT_SCHEMA);

gold__item_vendor_package_current_data = 
    LOAD '$GOLD__ITEM_VENDOR_PACKAGE_CURRENT_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($GOLD__ITEM_VENDOR_PACKAGE_CURRENT_SCHEMA);

gold__item_package_current_data = 
    LOAD '$GOLD__ITEM_PACKAGE_CURRENT_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($GOLD__ITEM_PACKAGE_CURRENT_SCHEMA);

gold__item_attribute_relate_current_data = 
    LOAD '$GOLD__ITEM_ATTRIBUTE_RELATE_CURRENT_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($GOLD__ITEM_ATTRIBUTE_RELATE_CURRENT_SCHEMA);

gold__inventory_sears_dc_item_facility_current_data = 
    LOAD '$GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT_SCHEMA);

gold__item_core_bridge_item_data = 
    LOAD '$GOLD__ITEM_CORE_BRIDGE_ITEM_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($GOLD__ITEM_CORE_BRIDGE_ITEM_SCHEMA);

item_rpt_cost_data = 
    LOAD '$WORK__IDRP_ITEM_RPT_COST_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($WORK__IDRP_ITEM_RPT_COST_SCHEMA);

item_rpt_grp_data = 
    LOAD '$WORK__IDRP_ITEM_RPT_GRP_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
    AS ($WORK__IDRP_ITEM_RPT_GRP_SCHEMA);
/******** GENERATING REQUIRED COLUMNS FROM SMITH__ITEM_COMBINED_HIERARCHY_CURRENT TABLE ***********/
         /********** DIRECT MAPPING *********/
smith__item_combined_hierarchy_current_data_filter = 
    FILTER smith__item_combined_hierarchy_current_data 
	    BY (shc_item_type_cd == 'TYP' OR shc_item_type_cd == 'EXAS' OR shc_item_type_cd == 'IIRC' OR shc_item_type_cd == 'INVC') 
		    AND (purchase_status_cd == 'A' OR purchase_status_cd == 'W');

smith__item_combined_hierarchy_current_table = 
    FOREACH smith__item_combined_hierarchy_current_data_filter 
	GENERATE 
        shc_item_id AS item,
        shc_item_desc AS descr,
        shc_division_nbr  AS shc_dvsn_no,
        shc_division_desc AS shc_dvsn_nm,
        shc_department_nbr AS shc_dept_no,
        shc_department_desc AS shc_dept_nm,
        shc_category_group_level_nbr AS shc_cat_grp_no,
        shc_category_group_desc AS shc_cat_grp_nm,
        shc_category_nbr AS shc_cat_no,
        shc_category_desc AS shc_cat_nm,
        shc_sub_category_nbr AS shc_subcat_no,
        shc_sub_category_desc AS shc_subcat_nm,
        sears_business_nbr AS srs_bus_no,
        sears_business_desc AS srs_bus_nm,
        sears_division_nbr AS srs_div_no,
        sears_division_desc AS srs_div_nm,
        sears_line_nbr AS srs_ln_no,
        sears_line_desc AS srs_ln_ds,
        sears_sub_line_nbr AS srs_sbl_no,
        sears_sub_line_desc AS srs_sbl_ds,
        sears_class_nbr AS srs_cls_no,
        sears_class_desc AS srs_cls_ds,
        sears_item_nbr AS srs_itm_no,
        sears_sku_nbr AS srs_sku_no,
        delivered_direct_ind AS dd_ind, 
        installation_ind AS instl_ind ,
        store_forecast_cd AS str_fcst_cd,
        shc_item_type_cd AS ima_itm_typ_cd,
        purchase_status_cd AS itm_purch_sts_cd,
        network_distribution_cd AS ntwk_dist_cd,
        future_network_distribution_cd AS fut_ntwk_dist_cd,
        future_network_distribution_effective_dt AS fut_ntwk_eff_dt,
        jit_network_distribution_cd AS jit_ntwk_dist_cd,
        reorder_authentication_cd AS str_reord_auth_cd,
        can_carry_model_id AS can_carr_mdl_id,
        grocery_item_ind AS groc_crossover_ind,
        shc_item_corporate_owner_cd AS owner_cd,
        iplan_id AS pln_id,
        markdown_style_reference_cd AS md_style_ref_cd,
        sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        similar_ksn_id,
        shc_item_desc AS item_desc ,
        similar_ksn_pct,
        special_retail_order_system_ind,
        dotcom_eligibility_cd,
        season_cd,
        season_year_nbr,
        sub_season_id,
        referred_package_id,
        ksn_id AS smith_ksn_id;

/********************** JOIN SMITH__ITEM_COMBINED_HIERARCHY_CURRENT WITH GOLD__ITEM_PACKAGE_CURRENT TABLE ON PACKAGE ID **********/

gen_gold__item_package_current_data = 
    FOREACH gold__item_package_current_data
	GENERATE
	    package_id,
		ksn_id,
		uom_cd,
		package_cube_volume_inch_qty,
		package_weight_pounds_qty;

join_pkg_curr_smith_hier_curr = 
    JOIN smith__item_combined_hierarchy_current_table BY ((long)referred_package_id,smith_ksn_id),
	     gen_gold__item_package_current_data BY ((long)package_id,ksn_id) ;

gen_join_pkg_curr_smith_hier_curr = 
    FOREACH join_pkg_curr_smith_hier_curr 
	GENERATE 
        can_carr_mdl_id,
        dd_ind,
        fut_ntwk_dist_cd,
        fut_ntwk_eff_dt,
        groc_crossover_ind,
        instl_ind,
        pln_id,
        owner_cd,
        ima_itm_typ_cd,
        jit_ntwk_dist_cd,
        md_style_ref_cd,
        ntwk_dist_cd,
        itm_purch_sts_cd,
        str_reord_auth_cd,
        srs_bus_nm,
        srs_bus_no,
        srs_cls_ds,
        srs_cls_no,
        srs_div_nm,
        srs_div_no,
        srs_itm_no,
        srs_ln_ds,
        srs_ln_no,
        srs_sku_no,
        srs_sbl_ds,
        srs_sbl_no,
        shc_cat_nm,
        shc_cat_grp_nm,
        shc_cat_grp_no,
        shc_cat_no,
        shc_dept_nm,
        shc_dept_no,
        shc_dvsn_nm,
        shc_dvsn_no ,
        descr,
        item,
        shc_subcat_nm,
        shc_subcat_no,
        str_fcst_cd,
        similar_ksn_pct,
        special_retail_order_system_ind,
        similar_ksn_id,
        sub_season_id AS sub_seas_id,
        dotcom_eligibility_cd,
        item_desc,
        season_year_nbr AS seas_yr,
--        com.searshc.supplychain.idrp.udf.CONCAT_MULTIPLE(srs_div_no,'-',srs_itm_no) AS srs_div_itm,
        CONCAT(srs_div_no,CONCAT('-',srs_itm_no)) AS srs_div_itm,
--        com.searshc.supplychain.idrp.udf.CONCAT_MULTIPLE(srs_div_no,'-',srs_itm_no,'-',srs_sku_no) AS srs_div_itm_sku,
        CONCAT(srs_div_no,CONCAT('-',CONCAT(srs_itm_no,CONCAT('-',srs_sku_no)))) AS srs_div_itm_sku,
        similar_ksn_id AS ima_smt_itm_no,
        ((long)similar_ksn_pct/100) AS ima_smt_fac_qt,
        uom_cd AS uom,
        package_cube_volume_inch_qty AS vol,
        package_weight_pounds_qty AS wgt,
        season_cd AS seas_cd,
        --extra
        referred_package_id,
        gen_gold__item_package_current_data::ksn_id AS ksn_id;

		/****** CALCULATING ITEM_DESC FOR EACH KSN  ****/        
smith__item_combined_hierarchy_current_data_ksn_id_shc_item_desc = 
    FOREACH smith__item_combined_hierarchy_current_data
	GENERATE
	    ksn_id,
		shc_item_desc;
		
join_pkg_curr_smith_hier_again = 
     JOIN gen_join_pkg_curr_smith_hier_curr BY (long)ima_smt_itm_no 
	      LEFT OUTER, 
		  smith__item_combined_hierarchy_current_data_ksn_id_shc_item_desc BY (long)ksn_id;

gen_join_pkg_curr_smith_hier_again = 
    FOREACH join_pkg_curr_smith_hier_again 
	GENERATE
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::can_carr_mdl_id AS can_carr_mdl_id,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::dd_ind AS dd_ind,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::groc_crossover_ind AS groc_crossover_ind,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::instl_ind AS instl_ind,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::pln_id AS pln_id,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::owner_cd AS owner_cd,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::ima_itm_typ_cd AS ima_itm_typ_cd,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::md_style_ref_cd AS md_style_ref_cd,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::ntwk_dist_cd AS ntwk_dist_cd,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::itm_purch_sts_cd AS itm_purch_sts_cd,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::str_reord_auth_cd AS str_reord_auth_cd,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::srs_bus_nm AS srs_bus_nm,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::srs_bus_no AS srs_bus_no,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::srs_cls_ds AS srs_cls_ds,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::srs_cls_no AS srs_cls_no,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::srs_div_nm AS srs_div_nm,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::srs_div_no AS srs_div_no,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::srs_itm_no AS srs_itm_no,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::srs_ln_ds AS srs_ln_ds,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::srs_ln_no AS srs_ln_no,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::srs_sku_no AS srs_sku_no,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::srs_sbl_ds AS srs_sbl_ds,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::srs_sbl_no AS srs_sbl_no,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::shc_cat_nm AS shc_cat_nm,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::shc_cat_grp_nm AS shc_cat_grp_nm,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::shc_cat_grp_no AS shc_cat_grp_no,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::shc_cat_no AS shc_cat_no,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::shc_dept_nm AS shc_dept_nm,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::shc_dept_no AS shc_dept_no,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::shc_dvsn_nm AS shc_dvsn_nm,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::shc_dvsn_no AS shc_dvsn_no,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::descr AS descr,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::item AS item,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::shc_subcat_nm AS shc_subcat_nm,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::shc_subcat_no AS shc_subcat_no,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::str_fcst_cd AS str_fcst_cd,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::similar_ksn_pct AS similar_ksn_pct,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::special_retail_order_system_ind AS special_retail_order_system_ind,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::similar_ksn_id AS similar_ksn_id,
        gen_join_pkg_curr_smith_hier_curr::sub_seas_id AS sub_seas_id,
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        gen_join_pkg_curr_smith_hier_curr::seas_yr AS seas_yr,
        gen_join_pkg_curr_smith_hier_curr::srs_div_itm AS srs_div_itm,
        gen_join_pkg_curr_smith_hier_curr::srs_div_itm_sku AS srs_div_itm_sku,
        gen_join_pkg_curr_smith_hier_curr::ima_smt_itm_no AS ima_smt_itm_no,
        smith__item_combined_hierarchy_current_data_ksn_id_shc_item_desc::shc_item_desc AS ima_smt_itm_ds,
        gen_join_pkg_curr_smith_hier_curr::ima_smt_fac_qt AS ima_smt_fac_qt,
        gen_join_pkg_curr_smith_hier_curr::uom AS uom,
        gen_join_pkg_curr_smith_hier_curr::vol AS vol,
        gen_join_pkg_curr_smith_hier_curr::wgt AS wgt,
        gen_join_pkg_curr_smith_hier_curr::seas_cd AS seas_cd,
        --extra
        gen_join_pkg_curr_smith_hier_curr::smith__item_combined_hierarchy_current_table::referred_package_id AS referred_package_id,
        gen_join_pkg_curr_smith_hier_curr::ksn_id AS ksn_id;


/****** 38. FOR ITEMS WHERE SHC_OWNER_CD IN ('S', 'B')
             JOIN SMITH__ITEM_COMBINED_HIERARCHY_CURRENT TO GOLD__ITEM_SEARS_CHANNEL_DISTRIBUTION_CURRENT ON SRS_DIV_NO AND SRS_ITM_NO
              AND EXTRACT ORGANIZATIONAL_GROUP_TYPE_CD. IF SPECIAL_RETAIL_ORDER_SYSTEM_IND = 'Y' AND THE ONLY CHANNEL OF DISTRIBUTION 
               PRESENT FOR THE DIVISION/ITEM = 'RSU', SET SPC_ORD_CDT_FL = 1; OTHERWISE, SET TO 0.FOR ITEMS WHERE SHC_OWNER_CD = 'K', 
                 DEFAULT TO 0. ************/


gold__item_sears_channel_distribution_current_data_grouped_by_divnbr_itm_nbr = 
    GROUP gold__item_sears_channel_distribution_current_data 
	BY (sears_division_nbr,sears_item_nbr);

gold__item_sears_channel_distribution_current_data_with_distribution_type =
     FOREACH gold__item_sears_channel_distribution_current_data_grouped_by_divnbr_itm_nbr
        GENERATE
            FLATTEN(com.searshc.supplychain.idrp.udf.GetDistributionType(gold__item_sears_channel_distribution_current_data)) AS ($GOLD__ITEM_SEARS_CHANNEL_DISTRIBUTION_WITH_DISTRIBUTION_TYPE);

join_distribution_current_gen_filter_smith_combined = 
    JOIN gen_join_pkg_curr_smith_hier_again BY (srs_div_no,(long)srs_itm_no) 
	     LEFT OUTER , 
		 gold__item_sears_channel_distribution_current_data_with_distribution_type BY (sears_division_nbr,(long)sears_item_nbr);

gen_distribution_current_smith_combined_spc_ord_cdt_fl_Y = 
    FOREACH join_distribution_current_gen_filter_smith_combined 
	GENERATE 
        (special_retail_order_system_ind == 'Y' ? ((owner_cd == 'S' OR owner_cd == 'B') ? ( only_rsu_distribution_channel == 'true' ? '1' : '0') : '0' ) : '0' ) AS spc_ord_cdt_fl,
        (special_retail_order_system_ind == 'N' ? ((owner_cd == 'S' OR owner_cd == 'B') ? ( only_rsu_distribution_channel == 'true' ? '1' : '0') : '0' ) : '0' ) AS itm_emp_fl, ( ( owner_cd == 'S' OR owner_cd == 'B' ) ? distribution_type_code  : 'TW'  ) as dist_typ_cd,
        can_carr_mdl_id,
        dd_ind,
        fut_ntwk_dist_cd,
        fut_ntwk_eff_dt,
        groc_crossover_ind,
        instl_ind,
        pln_id,
        owner_cd,
        ima_itm_typ_cd,
        jit_ntwk_dist_cd,
        md_style_ref_cd,
        ntwk_dist_cd,
        itm_purch_sts_cd,
        str_reord_auth_cd,
        srs_bus_nm,
        srs_bus_no,
        srs_cls_ds,
        srs_cls_no,
        srs_div_nm,
        srs_div_no,
        srs_itm_no,
        srs_ln_ds,
        srs_ln_no,
        srs_sku_no,
        srs_sbl_ds,
        srs_sbl_no,
        shc_cat_nm,
        shc_cat_grp_nm,
        shc_cat_grp_no,
        shc_cat_no,
        shc_dept_nm,
        shc_dept_no,
        shc_dvsn_nm,
        shc_dvsn_no ,
        descr,
        item,
        shc_subcat_nm,
        shc_subcat_no,
        str_fcst_cd,
        similar_ksn_pct,
        special_retail_order_system_ind,
        similar_ksn_id,
        sub_seas_id,
        dotcom_eligibility_cd,
        item_desc,
        seas_yr,
        srs_div_itm,
        srs_div_itm_sku,
        ima_smt_itm_no,
        ima_smt_itm_ds,
        ima_smt_fac_qt,
        uom,
        vol,
        wgt,
        seas_cd,
        ksn_id,
        --extra
        referred_package_id;
/****** END OF 38 ***********/
/***** FILTERING GOLD__ITEM_ATTRIBUTE_RELATE_CURRENT 
              ON ATTRIBUTE_IDS AND PERFORMING CR-798 ****/
gold__item_attribute_relate_current_data_filter =
    FILTER gold__item_attribute_relate_current_data
        BY  (
            ksn_id IS NOT NULL
            AND
            (
                attribute_id == '30'
                OR
                attribute_id == '50'
                OR
                attribute_id == '90'
                OR
                (
                   attribute_id == '220'
                   AND
                   sub_attribute_id == '1535'
                )
                OR
                attribute_id == '360'
                OR
                attribute_id == '400'
                OR
                attribute_id == '420'
                OR
                (
                    attribute_id == '430'
                    AND
                    sub_attribute_id == '1566'
                )
                OR
                attribute_id == '710'
                OR
                attribute_id == '720'
                OR
                attribute_id == '730'
                OR
                attribute_id == '1610'
             )
           AND
           (
             '$CURRENT_TIMESTAMP' >= effective_ts 
              AND 
              '$CURRENT_TIMESTAMP' <= expiration_ts 
           )
           );

gen_attribute_relate_curr_tbl_ksn_valu = 
    FOREACH gold__item_attribute_relate_current_data_filter
	GENERATE 
        ksn_id AS attr_ksn_id,
        value_definition_tx,
        sub_attribute_id,
        item_id AS attr_item_id,
        attribute_relate_id AS attr_relate_id,
        attribute_id,
        value_nm,
        package_id AS attr_package_id,
        attribute_relate_alternate_id AS attribute_relate_alternate_id;
		
SPLIT gen_attribute_relate_curr_tbl_ksn_valu 
INTO 
   gen_attribute_relate_curr_tbl_ksn_valu_30   IF attribute_id == '30',
   gen_attribute_relate_curr_tbl_ksn_valu_50   IF attribute_id == '50',
   gen_attribute_relate_curr_tbl_ksn_valu_90   IF attribute_id == '90',
   gen_attribute_relate_curr_tbl_ksn_valu_220  IF (attribute_id == '220' AND sub_attribute_id == '1535'),
   gen_attribute_relate_curr_tbl_ksn_valu_360  IF attribute_id == '360',
   gen_attribute_relate_curr_tbl_ksn_valu_400  IF attribute_id == '400',
   gen_attribute_relate_curr_tbl_ksn_valu_420  IF attribute_id == '420',
   gen_attribute_relate_curr_tbl_ksn_valu_430  IF (attribute_id == '430' AND sub_attribute_id == '1566'),
   gen_attribute_relate_curr_tbl_ksn_valu_710  IF attribute_id == '710' ,
   gen_attribute_relate_curr_tbl_ksn_valu_720  IF attribute_id == '720',
   gen_attribute_relate_curr_tbl_ksn_valu_730  IF attribute_id == '730' ,
   gen_attribute_relate_curr_tbl_ksn_valu_1610 IF attribute_id == '1610' ;		

/******* IN ABOVE FILTER WE NEED TO FIND KSN_ID'S WHICH ARE NOT NULL FOR FURTHER PROCESSING .
          BUT FOR CALCULATING BRAND_DS IS ON PACKAGE LEVEL (CR-798) NOT ON KSN LEVEL . SO FOR THIS STEP NO KSN FILTERATION REQUIRED.
            THATS WHY WE ARE FILTERING attribute_id 10 SEPERATELY *******/
gold__item_attribute_relate_current_data_attr_10_filter = 
     FILTER gold__item_attribute_relate_current_data
     BY attribute_id == '10' 
        AND 
	   ( '$CURRENT_TIMESTAMP' >= effective_ts AND '$CURRENT_TIMESTAMP' <= expiration_ts );

gen_attribute_relate_curr_tbl_ksn_valu_10 = 
    FOREACH gold__item_attribute_relate_current_data_attr_10_filter
    GENERATE 
        value_nm AS value_nm,
        package_id AS attr_package_id_10,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_10;

join_with_attr_id_10 = 
    JOIN gen_distribution_current_smith_combined_spc_ord_cdt_fl_Y BY referred_package_id
	 LEFT OUTER ,
	 gen_attribute_relate_curr_tbl_ksn_valu_10 BY attr_package_id_10 PARALLEL $NUM_PARALLEL;

group_data_by_package_id_to_find_brand_ds  =
    GROUP join_with_attr_id_10
    BY referred_package_id;

flatten_data_to_find_brand_ds =
        FOREACH group_data_by_package_id_to_find_brand_ds
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER $1 BY attribute_relate_alternate_id_10 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };

gen_join_with_attr_id_10 = 
    FOREACH flatten_data_to_find_brand_ds 
    GENERATE
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        dist_typ_cd AS dist_typ_cd,
        can_carr_mdl_id AS can_carr_mdl_id,
        dd_ind AS dd_ind,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        groc_crossover_ind AS groc_crossover_ind,
        instl_ind AS instl_ind,
        pln_id AS pln_id,
        owner_cd AS owner_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        md_style_ref_cd AS md_style_ref_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        srs_bus_nm AS srs_bus_nm,
        srs_bus_no AS srs_bus_no,
        srs_cls_ds AS srs_cls_ds,
        srs_cls_no AS srs_cls_no,
        srs_div_nm AS srs_div_nm,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_ln_ds AS srs_ln_ds,
        srs_ln_no AS srs_ln_no,
        srs_sku_no AS srs_sku_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_sbl_no AS srs_sbl_no,
        shc_cat_nm AS shc_cat_nm,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_no AS shc_cat_no,
        shc_dept_nm AS shc_dept_nm,
        shc_dept_no AS shc_dept_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dvsn_no AS shc_dvsn_no,
        descr AS descr,
        item AS item,
        shc_subcat_nm AS shc_subcat_nm,
        shc_subcat_no AS shc_subcat_no,
        str_fcst_cd AS str_fcst_cd,
        similar_ksn_pct AS similar_ksn_pct,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        similar_ksn_id AS similar_ksn_id,
        sub_seas_id AS sub_seas_id,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        seas_yr AS seas_yr,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        seas_cd AS seas_cd,
        ksn_id AS ksn_id,
        referred_package_id AS referred_package_id,
        value_nm AS brand_ds,
        attr_package_id_10 AS attr_package_id_10;

gen_attribute_relate_curr_tbl_ksn_valu_30 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_30 
    GENERATE  
        value_nm AS value_nm_30,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_30,
        attr_ksn_id AS attr_ksn_id_30;

join_with_attr_id_30 = 
    JOIN gen_join_with_attr_id_10 BY ksn_id 
         LEFT OUTER ,
         gen_attribute_relate_curr_tbl_ksn_valu_30 BY attr_ksn_id_30 PARALLEL $NUM_PARALLEL;

group_data_by_ksn_id_to_find_size  =
    GROUP join_with_attr_id_30
    BY ksn_id;

flatten_data_to_find_size =
        FOREACH group_data_by_ksn_id_to_find_size
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER $1 BY attribute_relate_alternate_id_30 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };

gen_join_with_attr_id_30 = 
    FOREACH flatten_data_to_find_size
    GENERATE 
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        dist_typ_cd AS dist_typ_cd,
        can_carr_mdl_id AS can_carr_mdl_id,
        dd_ind AS dd_ind,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        groc_crossover_ind AS groc_crossover_ind,
        instl_ind AS instl_ind,
        pln_id AS pln_id,
        owner_cd AS owner_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        md_style_ref_cd AS md_style_ref_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        srs_bus_nm AS srs_bus_nm,
        srs_bus_no AS srs_bus_no,
        srs_cls_ds AS srs_cls_ds,
        srs_cls_no AS srs_cls_no,
        srs_div_nm AS srs_div_nm,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_ln_ds AS srs_ln_ds,
        srs_ln_no AS srs_ln_no,
        srs_sku_no AS srs_sku_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_sbl_no AS srs_sbl_no,
        shc_cat_nm AS shc_cat_nm,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_no AS shc_cat_no,
        shc_dept_nm AS shc_dept_nm,
        shc_dept_no AS shc_dept_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dvsn_no AS shc_dvsn_no,
        descr AS descr,
        item AS item,
        shc_subcat_nm AS shc_subcat_nm,
        shc_subcat_no AS shc_subcat_no,
        str_fcst_cd AS str_fcst_cd,
        similar_ksn_pct AS similar_ksn_pct,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        similar_ksn_id AS similar_ksn_id,
        sub_seas_id AS sub_seas_id,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        seas_yr AS seas_yr,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        seas_cd AS seas_cd,
        ksn_id AS ksn_id,
        referred_package_id AS referred_package_id,
        brand_ds AS brand_ds,
        value_nm_30 AS size;

gen_attribute_relate_curr_tbl_ksn_valu_50 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_50 
    GENERATE  
        value_nm AS value_nm_50,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_50,
	attr_ksn_id AS attr_ksn_id_50;

join_with_attr_id_50 = 
    JOIN gen_join_with_attr_id_30 BY ksn_id 
         LEFT OUTER ,
	 gen_attribute_relate_curr_tbl_ksn_valu_50 BY attr_ksn_id_50 PARALLEL $NUM_PARALLEL;

group_data_by_ksn_id_to_find_color_ds  =
    GROUP join_with_attr_id_50
    BY ksn_id;

flatten_data_to_find_color_ds =
        FOREACH group_data_by_ksn_id_to_find_color_ds
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER $1 BY attribute_relate_alternate_id_50 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };

gen_join_with_attr_id_50 = 
    FOREACH flatten_data_to_find_color_ds
    GENERATE 
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        dist_typ_cd AS dist_typ_cd,
        can_carr_mdl_id AS can_carr_mdl_id,
        dd_ind AS dd_ind,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        groc_crossover_ind AS groc_crossover_ind,
        instl_ind AS instl_ind,
        pln_id AS pln_id,
        owner_cd AS owner_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        md_style_ref_cd AS md_style_ref_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        srs_bus_nm AS srs_bus_nm,
        srs_bus_no AS srs_bus_no,
        srs_cls_ds AS srs_cls_ds,
        srs_cls_no AS srs_cls_no,
        srs_div_nm AS srs_div_nm,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_ln_ds AS srs_ln_ds,
        srs_ln_no AS srs_ln_no,
        srs_sku_no AS srs_sku_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_sbl_no AS srs_sbl_no,
        shc_cat_nm AS shc_cat_nm,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_no AS shc_cat_no,
        shc_dept_nm AS shc_dept_nm,
        shc_dept_no AS shc_dept_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dvsn_no AS shc_dvsn_no,
        descr AS descr,
        item AS item,
        shc_subcat_nm AS shc_subcat_nm,
        shc_subcat_no AS shc_subcat_no,
        str_fcst_cd AS str_fcst_cd,
        similar_ksn_pct AS similar_ksn_pct,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        similar_ksn_id AS similar_ksn_id,
        sub_seas_id AS sub_seas_id,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        seas_yr AS seas_yr,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        seas_cd AS seas_cd,
        ksn_id AS ksn_id,
        referred_package_id AS referred_package_id,
        brand_ds AS brand_ds,
        size AS size,
        value_nm_50 AS color_ds;
        

gen_attribute_relate_curr_tbl_ksn_valu_90 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_90 
    GENERATE  
        value_nm AS value_nm_90,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_90,
        attr_ksn_id AS attr_ksn_id_90;

join_with_attr_id_90 = 
    JOIN gen_join_with_attr_id_50 BY ksn_id 
         LEFT OUTER ,
	 gen_attribute_relate_curr_tbl_ksn_valu_90 BY attr_ksn_id_90 PARALLEL $NUM_PARALLEL;


group_data_by_ksn_id_to_find_style  =
    GROUP join_with_attr_id_90
    BY ksn_id;

flatten_data_to_find_style =
        FOREACH group_data_by_ksn_id_to_find_style
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER $1 BY attribute_relate_alternate_id_90 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };

gen_join_with_attr_id_90 = 
    FOREACH flatten_data_to_find_style
    GENERATE 
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        dist_typ_cd AS dist_typ_cd,
        can_carr_mdl_id AS can_carr_mdl_id,
        dd_ind AS dd_ind,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        groc_crossover_ind AS groc_crossover_ind,
        instl_ind AS instl_ind,
        pln_id AS pln_id,
        owner_cd AS owner_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        md_style_ref_cd AS md_style_ref_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        srs_bus_nm AS srs_bus_nm,
        srs_bus_no AS srs_bus_no,
        srs_cls_ds AS srs_cls_ds,
        srs_cls_no AS srs_cls_no,
        srs_div_nm AS srs_div_nm,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_ln_ds AS srs_ln_ds,
        srs_ln_no AS srs_ln_no,
        srs_sku_no AS srs_sku_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_sbl_no AS srs_sbl_no,
        shc_cat_nm AS shc_cat_nm,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_no AS shc_cat_no,
        shc_dept_nm AS shc_dept_nm,
        shc_dept_no AS shc_dept_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dvsn_no AS shc_dvsn_no,
        descr AS descr,
        item AS item,
        shc_subcat_nm AS shc_subcat_nm,
        shc_subcat_no AS shc_subcat_no,
        str_fcst_cd AS str_fcst_cd,
        similar_ksn_pct AS similar_ksn_pct,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        similar_ksn_id AS similar_ksn_id,
        sub_seas_id AS sub_seas_id,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        seas_yr AS seas_yr,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        seas_cd AS seas_cd,
        ksn_id AS ksn_id,
        referred_package_id AS referred_package_id,
        brand_ds AS brand_ds,
        size AS size,
        color_ds AS color_ds,
        value_nm_90 AS style;

gen_attribute_relate_curr_tbl_ksn_valu_220 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_220 
    GENERATE  
        value_definition_tx AS value_definition_tx_220,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_220,
        attr_ksn_id AS attr_ksn_id_220;

join_with_attr_id_220 = 
    JOIN gen_join_with_attr_id_90 BY ksn_id 
         LEFT OUTER ,
	 gen_attribute_relate_curr_tbl_ksn_valu_220 BY attr_ksn_id_220 PARALLEL $NUM_PARALLEL;

group_data_by_ksn_id_to_find_cross_mdse_attr_cd  =
    GROUP join_with_attr_id_220
    BY ksn_id;

flatten_data_to_find_cross_mdse_attr_cd =
        FOREACH group_data_by_ksn_id_to_find_cross_mdse_attr_cd
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER $1 BY attribute_relate_alternate_id_220 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };

gen_join_with_attr_id_220 = 
    FOREACH flatten_data_to_find_cross_mdse_attr_cd
    GENERATE 
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        dist_typ_cd AS dist_typ_cd,
        can_carr_mdl_id AS can_carr_mdl_id,
        dd_ind AS dd_ind,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        groc_crossover_ind AS groc_crossover_ind,
        instl_ind AS instl_ind,
        pln_id AS pln_id,
        owner_cd AS owner_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        md_style_ref_cd AS md_style_ref_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        srs_bus_nm AS srs_bus_nm,
        srs_bus_no AS srs_bus_no,
        srs_cls_ds AS srs_cls_ds,
        srs_cls_no AS srs_cls_no,
        srs_div_nm AS srs_div_nm,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_ln_ds AS srs_ln_ds,
        srs_ln_no AS srs_ln_no,
        srs_sku_no AS srs_sku_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_sbl_no AS srs_sbl_no,
        shc_cat_nm AS shc_cat_nm,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_no AS shc_cat_no,
        shc_dept_nm AS shc_dept_nm,
        shc_dept_no AS shc_dept_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dvsn_no AS shc_dvsn_no,
        descr AS descr,
        item AS item,
        shc_subcat_nm AS shc_subcat_nm,
        shc_subcat_no AS shc_subcat_no,
        str_fcst_cd AS str_fcst_cd,
        similar_ksn_pct AS similar_ksn_pct,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        similar_ksn_id AS similar_ksn_id,
        sub_seas_id AS sub_seas_id,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        seas_yr AS seas_yr,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        seas_cd AS seas_cd,
        ksn_id AS ksn_id,
        referred_package_id AS referred_package_id,
        brand_ds AS brand_ds,
        size AS size,
        color_ds AS color_ds,
        style AS style,
        (value_definition_tx_220 == 'KM1000' OR value_definition_tx_220 == 'KM4001' OR value_definition_tx_220 == 'KM4005' OR value_definition_tx_220 == 'KM4009' OR value_definition_tx_220 == 'KM5000' OR value_definition_tx_220 == 'KM9000' OR value_definition_tx_220 == 'SK3000' OR value_definition_tx_220 == 'SK1400' ? value_definition_tx_220 : '' ) AS cross_mdse_attr_cd;

gen_attribute_relate_curr_tbl_ksn_valu_360 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_360 
    GENERATE  
        attribute_relate_alternate_id AS attribute_relate_alternate_id_360,
        value_definition_tx AS value_definition_tx_360,
        attr_ksn_id AS attr_ksn_id_360;
		
join_with_attr_id_360 = 
    JOIN gen_join_with_attr_id_220 BY ksn_id 
         LEFT OUTER ,
	 gen_attribute_relate_curr_tbl_ksn_valu_360 BY attr_ksn_id_360 PARALLEL $NUM_PARALLEL;

group_data_by_ksn_id_to_find_itm_pgm  =
    GROUP join_with_attr_id_360
    BY ksn_id;

flatten_data_to_find_itm_pgm =
        FOREACH group_data_by_ksn_id_to_find_itm_pgm
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER $1 BY attribute_relate_alternate_id_360 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };
gen_join_with_attr_id_360 = 
    FOREACH flatten_data_to_find_itm_pgm
    GENERATE 
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        dist_typ_cd AS dist_typ_cd,
        can_carr_mdl_id AS can_carr_mdl_id,
        dd_ind AS dd_ind,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        groc_crossover_ind AS groc_crossover_ind,
        instl_ind AS instl_ind,
        pln_id AS pln_id,
        owner_cd AS owner_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        md_style_ref_cd AS md_style_ref_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        srs_bus_nm AS srs_bus_nm,
        srs_bus_no AS srs_bus_no,
        srs_cls_ds AS srs_cls_ds,
        srs_cls_no AS srs_cls_no,
        srs_div_nm AS srs_div_nm,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_ln_ds AS srs_ln_ds,
        srs_ln_no AS srs_ln_no,
        srs_sku_no AS srs_sku_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_sbl_no AS srs_sbl_no,
        shc_cat_nm AS shc_cat_nm,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_no AS shc_cat_no,
        shc_dept_nm AS shc_dept_nm,
        shc_dept_no AS shc_dept_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dvsn_no AS shc_dvsn_no,
        descr AS descr,
        item AS item,
        shc_subcat_nm AS shc_subcat_nm,
        shc_subcat_no AS shc_subcat_no,
        str_fcst_cd AS str_fcst_cd,
        similar_ksn_pct AS similar_ksn_pct,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        similar_ksn_id AS similar_ksn_id,
        sub_seas_id AS sub_seas_id,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        seas_yr AS seas_yr,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        seas_cd AS seas_cd,
        ksn_id AS ksn_id,
        referred_package_id AS referred_package_id,
        brand_ds AS brand_ds,
        size AS size,
        color_ds AS color_ds,
        style AS style,
        cross_mdse_attr_cd AS cross_mdse_attr_cd,
        value_definition_tx_360 AS itm_pgm;
 	    

gen_attribute_relate_curr_tbl_ksn_valu_400 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_400 
    GENERATE  
        value_definition_tx,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_400,
        attr_ksn_id AS attr_ksn_id_400;
		
join_with_attr_id_400 = 
    JOIN gen_join_with_attr_id_360 BY ksn_id 
         LEFT OUTER ,
	 gen_attribute_relate_curr_tbl_ksn_valu_400 BY attr_ksn_id_400 PARALLEL $NUM_PARALLEL;

group_data_by_ksn_id_to_find_sls_pfm_seg_cd  =
    GROUP join_with_attr_id_400 
    BY ksn_id;

flatten_data_to_find_sls_pfm_seg_cd = 
	FOREACH group_data_by_ksn_id_to_find_sls_pfm_seg_cd 
	    {
		    sort_data_desc_on_attr_relate_alt_id = ORDER $1 BY attribute_relate_alternate_id_400 DESC ;
			take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
			GENERATE FLATTEN (take_first_row);
            };
		 
gen_join_with_attr_id_400 = 
    FOREACH flatten_data_to_find_sls_pfm_seg_cd
    GENERATE 
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        dist_typ_cd AS dist_typ_cd,
        can_carr_mdl_id AS can_carr_mdl_id,
        dd_ind AS dd_ind,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        groc_crossover_ind AS groc_crossover_ind,
        instl_ind AS instl_ind,
        pln_id AS pln_id,
        owner_cd AS owner_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        md_style_ref_cd AS md_style_ref_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        srs_bus_nm AS srs_bus_nm,
        srs_bus_no AS srs_bus_no,
        srs_cls_ds AS srs_cls_ds,
        srs_cls_no AS srs_cls_no,
        srs_div_nm AS srs_div_nm,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_ln_ds AS srs_ln_ds,
        srs_ln_no AS srs_ln_no,
        srs_sku_no AS srs_sku_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_sbl_no AS srs_sbl_no,
        shc_cat_nm AS shc_cat_nm,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_no AS shc_cat_no,
        shc_dept_nm AS shc_dept_nm,
        shc_dept_no AS shc_dept_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dvsn_no AS shc_dvsn_no,
        descr AS descr,
        item AS item,
        shc_subcat_nm AS shc_subcat_nm,
        shc_subcat_no AS shc_subcat_no,
        str_fcst_cd AS str_fcst_cd,
        similar_ksn_pct AS similar_ksn_pct,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        similar_ksn_id AS similar_ksn_id,
        sub_seas_id AS sub_seas_id,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        seas_yr AS seas_yr,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        seas_cd AS seas_cd,
        ksn_id AS ksn_id,
        referred_package_id AS referred_package_id,
        brand_ds AS brand_ds,
        size AS size,
        color_ds AS color_ds,
        style AS style,
        cross_mdse_attr_cd AS cross_mdse_attr_cd,
        itm_pgm AS itm_pgm,
        (value_definition_tx IS NULL ? '' : (value_definition_tx == 'SQ5100' OR value_definition_tx == 'SQ5101' OR value_definition_tx == 'SQ9300' ? 'BB' : (value_definition_tx == 'SQ5102' OR value_definition_tx == 'SQ9301' ? 'B2' : (value_definition_tx == 'SQ5103' OR value_definition_tx == 'SQ9302' ? 'B3' : (value_definition_tx == 'SQ5104' OR value_definition_tx == 'SQ9303' ? 'B4' : (value_definition_tx == 'SQ5105' OR value_definition_tx == 'SQ5106' OR value_definition_tx == 'SQ5107' OR value_definition_tx == 'SQ5108' OR value_definition_tx == 'SQ5109' OR value_definition_tx == 'SQ5110' OR value_definition_tx == 'SQ5120' OR value_definition_tx == 'SQ5125' OR  value_definition_tx == 'SQ9304' ? 'B5' : '' )))))) AS sls_pfm_seg_cd,
        (value_definition_tx IS NULL ? 'SHARED' : (value_definition_tx == 'SQ5106' ? 'ICS' : (value_definition_tx == 'SQ5107' ? 'TGI' : (value_definition_tx == 'SQ5125' ? 'INT' : 'SHARED' )))) AS fmt_excl_cd;
 
gen_attribute_relate_curr_tbl_ksn_valu_420 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_420 
    GENERATE  
        value_definition_tx AS value_definition_tx_420,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_420,
        attr_ksn_id AS attr_ksn_id_420;

join_with_attr_id_420 = 
    JOIN gen_join_with_attr_id_400 BY ksn_id 
         LEFT OUTER ,
	 gen_attribute_relate_curr_tbl_ksn_valu_420 BY attr_ksn_id_420 PARALLEL $NUM_PARALLEL;

group_data_by_ksn_id_to_find_key_pgm  =
    GROUP join_with_attr_id_420
    BY ksn_id;

flatten_data_to_find_key_pgm =
        FOREACH group_data_by_ksn_id_to_find_key_pgm
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER $1 BY attribute_relate_alternate_id_420 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };
		 
gen_join_with_attr_id_420 = 
    FOREACH flatten_data_to_find_key_pgm
    GENERATE 
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        dist_typ_cd AS dist_typ_cd,
        can_carr_mdl_id AS can_carr_mdl_id,
        dd_ind AS dd_ind,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        groc_crossover_ind AS groc_crossover_ind,
        instl_ind AS instl_ind,
        pln_id AS pln_id,
        owner_cd AS owner_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        md_style_ref_cd AS md_style_ref_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        srs_bus_nm AS srs_bus_nm,
        srs_bus_no AS srs_bus_no,
        srs_cls_ds AS srs_cls_ds,
        srs_cls_no AS srs_cls_no,
        srs_div_nm AS srs_div_nm,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_ln_ds AS srs_ln_ds,
        srs_ln_no AS srs_ln_no,
        srs_sku_no AS srs_sku_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_sbl_no AS srs_sbl_no,
        shc_cat_nm AS shc_cat_nm,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_no AS shc_cat_no,
        shc_dept_nm AS shc_dept_nm,
        shc_dept_no AS shc_dept_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dvsn_no AS shc_dvsn_no,
        descr AS descr,
        item AS item,
        shc_subcat_nm AS shc_subcat_nm,
        shc_subcat_no AS shc_subcat_no,
        str_fcst_cd AS str_fcst_cd,
        similar_ksn_pct AS similar_ksn_pct,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        similar_ksn_id AS similar_ksn_id,
        sub_seas_id AS sub_seas_id,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        seas_yr AS seas_yr,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        seas_cd AS seas_cd,
        ksn_id AS ksn_id,
        referred_package_id AS referred_package_id,
        brand_ds AS brand_ds,
        size AS size,
        color_ds AS color_ds,
        style AS style,
        cross_mdse_attr_cd AS cross_mdse_attr_cd,
        itm_pgm AS itm_pgm,
        sls_pfm_seg_cd AS sls_pfm_seg_cd,
        fmt_excl_cd AS fmt_excl_cd,
        value_definition_tx_420 AS key_pgm;

gen_attribute_relate_curr_tbl_ksn_valu_430 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_430 
    GENERATE 
        value_definition_tx AS value_definition_tx_430,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_430,
        attr_ksn_id AS attr_ksn_id_430;
		
join_with_attr_id_430 = 
    JOIN gen_join_with_attr_id_420 BY ksn_id 
         LEFT OUTER ,
	 gen_attribute_relate_curr_tbl_ksn_valu_430 BY attr_ksn_id_430 PARALLEL $NUM_PARALLEL;

group_data_by_ksn_id_to_find_whse_sizing  =
    GROUP join_with_attr_id_430
    BY ksn_id;

flatten_data_to_find_whse_sizing =
        FOREACH group_data_by_ksn_id_to_find_whse_sizing
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER $1 BY attribute_relate_alternate_id_430 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };

gen_join_with_attr_id_430 = 
    FOREACH flatten_data_to_find_whse_sizing
    GENERATE 
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        dist_typ_cd AS dist_typ_cd,
        can_carr_mdl_id AS can_carr_mdl_id,
        dd_ind AS dd_ind,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        groc_crossover_ind AS groc_crossover_ind,
        instl_ind AS instl_ind,
        pln_id AS pln_id,
        owner_cd AS owner_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        md_style_ref_cd AS md_style_ref_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        srs_bus_nm AS srs_bus_nm,
        srs_bus_no AS srs_bus_no,
        srs_cls_ds AS srs_cls_ds,
        srs_cls_no AS srs_cls_no,
        srs_div_nm AS srs_div_nm,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_ln_ds AS srs_ln_ds,
        srs_ln_no AS srs_ln_no,
        srs_sku_no AS srs_sku_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_sbl_no AS srs_sbl_no,
        shc_cat_nm AS shc_cat_nm,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_no AS shc_cat_no,
        shc_dept_nm AS shc_dept_nm,
        shc_dept_no AS shc_dept_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dvsn_no AS shc_dvsn_no,
        descr AS descr,
        item AS item,
        shc_subcat_nm AS shc_subcat_nm,
        shc_subcat_no AS shc_subcat_no,
        str_fcst_cd AS str_fcst_cd,
        similar_ksn_pct AS similar_ksn_pct,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        similar_ksn_id AS similar_ksn_id,
        sub_seas_id AS sub_seas_id,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        seas_yr AS seas_yr,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        seas_cd AS seas_cd,
        ksn_id AS ksn_id,
        referred_package_id AS referred_package_id,
        brand_ds AS brand_ds,
        size AS size,
        color_ds AS color_ds,
        style AS style,
        cross_mdse_attr_cd AS cross_mdse_attr_cd,
        itm_pgm AS itm_pgm,
        sls_pfm_seg_cd AS sls_pfm_seg_cd,
        fmt_excl_cd AS fmt_excl_cd,
        key_pgm AS key_pgm,
        (value_definition_tx_430 IS NULL ? '' : (value_definition_tx_430 == 'WG8800' OR value_definition_tx_430 == 'WG8801' OR value_definition_tx_430 == 'WG8804'  ? value_definition_tx_430 : '')) AS whse_sizing;

/****BELOW IS NEW LOGIC FOR CALCULATING itm_cd_fl and itm_rpd_fl *****/	

gen_attribute_relate_curr_tbl_ksn_valu_710 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_710 
    GENERATE 
        value_definition_tx AS value_definition_tx_710,
        attr_ksn_id AS attr_ksn_id_710, 
        attribute_relate_alternate_id AS attribute_relate_alternate_id_710,
        attribute_id AS attribute_id_710;

		
join_with_attr_id_710 = 
    JOIN gen_join_with_attr_id_430 BY ksn_id 
         LEFT OUTER ,
	 gen_attribute_relate_curr_tbl_ksn_valu_710 BY attr_ksn_id_710 PARALLEL $NUM_PARALLEL;

group_data_by_ksn_id_to_find_itm_rpd_fl_and_itm_cd_fl  =
    GROUP join_with_attr_id_710
    BY ksn_id;

flatten_data_to_find_itm_rpd_fl_and_itm_cd_fl =
        FOREACH group_data_by_ksn_id_to_find_itm_rpd_fl_and_itm_cd_fl
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER $1 BY attribute_relate_alternate_id_710 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };
		 
gen_join_with_attr_id_710 = 
    FOREACH flatten_data_to_find_itm_rpd_fl_and_itm_cd_fl
    GENERATE 
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        dist_typ_cd AS dist_typ_cd,
        can_carr_mdl_id AS can_carr_mdl_id,
        dd_ind AS dd_ind,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        groc_crossover_ind AS groc_crossover_ind,
        instl_ind AS instl_ind,
        pln_id AS pln_id,
        owner_cd AS  owner_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        md_style_ref_cd AS md_style_ref_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        srs_bus_nm AS srs_bus_nm,
        srs_bus_no AS srs_bus_no,
        srs_cls_ds AS srs_cls_ds,
        srs_cls_no AS srs_cls_no,
        srs_div_nm AS srs_div_nm,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_ln_ds AS srs_ln_ds,
        srs_ln_no AS srs_ln_no,
        srs_sku_no AS srs_sku_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_sbl_no AS srs_sbl_no,
        shc_cat_nm AS shc_cat_nm,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_no AS shc_cat_no,
        shc_dept_nm AS shc_dept_nm,
        shc_dept_no AS shc_dept_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dvsn_no AS shc_dvsn_no,
        descr AS descr,
        item AS item,
        shc_subcat_nm AS shc_subcat_nm,
        shc_subcat_no AS shc_subcat_no,
        str_fcst_cd AS str_fcst_cd,
        similar_ksn_pct AS similar_ksn_pct,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        similar_ksn_id AS similar_ksn_id,
        sub_seas_id AS sub_seas_id,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        seas_yr AS seas_yr,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        seas_cd AS seas_cd,
        ksn_id AS ksn_id,
        referred_package_id AS referred_package_id,
        brand_ds AS brand_ds,
        size AS size,
        color_ds AS color_ds,
        style AS style,
        cross_mdse_attr_cd AS cross_mdse_attr_cd,
        itm_pgm AS itm_pgm,
        sls_pfm_seg_cd AS sls_pfm_seg_cd,
        fmt_excl_cd AS fmt_excl_cd,
        key_pgm AS key_pgm,
        whse_sizing  AS whse_sizing,
        (value_definition_tx_710 IS NULL OR owner_cd IS NULL ? '0' :  ((owner_cd == 'S' OR owner_cd == 'B') ? ((value_definition_tx_710 MATCHES 'RP00([0-9]+)' OR value_definition_tx_710 MATCHES 'RP10([0-9]+)' ? '1' : '0')) : '0')) AS itm_rpd_fl,
        (value_definition_tx_710 IS NULL OR owner_cd IS NULL ? '0' : ((owner_cd == 'S' OR owner_cd == 'B') ? ((value_definition_tx_710 MATCHES 'RP10([0-9]+)' ? '1' : '0' )) : '0')) AS itm_cd_fl;

gen_attribute_relate_curr_tbl_ksn_valu_720 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_720 
    GENERATE 
        value_definition_tx AS value_definition_tx_720,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_720,
        attr_ksn_id AS attr_ksn_id_720;
		
join_with_attr_id_720 = 
    JOIN gen_join_with_attr_id_710 BY ksn_id 
         LEFT OUTER ,
	 gen_attribute_relate_curr_tbl_ksn_valu_720 BY attr_ksn_id_720 PARALLEL $NUM_PARALLEL;

group_data_by_ksn_id_to_find_obn_830_dur  =
    GROUP join_with_attr_id_720
    BY ksn_id;

flatten_data_to_find_obn_830_dur =
        FOREACH group_data_by_ksn_id_to_find_obn_830_dur
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER $1 BY attribute_relate_alternate_id_720 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };
		 
gen_join_with_attr_id_720 = 
    FOREACH flatten_data_to_find_obn_830_dur
    GENERATE 
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        dist_typ_cd AS dist_typ_cd,
        can_carr_mdl_id AS can_carr_mdl_id,
        dd_ind AS dd_ind,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        groc_crossover_ind AS groc_crossover_ind,
        instl_ind AS instl_ind,
        pln_id AS pln_id,
        owner_cd AS owner_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        md_style_ref_cd AS md_style_ref_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        srs_bus_nm AS srs_bus_nm,
        srs_bus_no AS srs_bus_no,
        srs_cls_ds AS srs_cls_ds,
        srs_cls_no AS srs_cls_no,
        srs_div_nm AS srs_div_nm,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_ln_ds AS srs_ln_ds,
        srs_ln_no AS srs_ln_no,
        srs_sku_no AS srs_sku_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_sbl_no AS srs_sbl_no,
        shc_cat_nm AS shc_cat_nm,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_no AS shc_cat_no,
        shc_dept_nm AS shc_dept_nm,
        shc_dept_no AS shc_dept_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dvsn_no AS shc_dvsn_no,
        descr AS descr,
        item AS item,
        shc_subcat_nm AS shc_subcat_nm,
        shc_subcat_no AS shc_subcat_no,
        str_fcst_cd AS str_fcst_cd,
        similar_ksn_pct AS similar_ksn_pct ,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        similar_ksn_id AS similar_ksn_id,
        sub_seas_id AS sub_seas_id,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        seas_yr AS seas_yr,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        seas_cd AS seas_cd,
        ksn_id AS ksn_id,
        referred_package_id AS referred_package_id,
        brand_ds AS brand_ds,
        size AS size,
        color_ds AS color_ds,
        style AS style,
        cross_mdse_attr_cd AS cross_mdse_attr_cd,
        itm_pgm AS itm_pgm,
        sls_pfm_seg_cd AS sls_pfm_seg_cd,
        fmt_excl_cd AS fmt_excl_cd,
        key_pgm AS key_pgm,
        whse_sizing AS whse_sizing,
        itm_rpd_fl AS itm_rpd_fl,
        itm_cd_fl AS itm_cd_fl,
        (value_definition_tx_720 IS NULL OR value_definition_tx_720 == '' ? '0' : (SUBSTRING(value_definition_tx_720,0,2) == 'RQ' ? (chararray)((int)(SUBSTRING(value_definition_tx_720,(int)(SIZE(value_definition_tx_720)-2),(int)SIZE(value_definition_tx_720))) * 10080) : '0')) AS obn_830_dur;


gen_attribute_relate_curr_tbl_ksn_valu_730 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_730 
    GENERATE 
        value_definition_tx AS value_definition_tx_730,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_730,
        attr_ksn_id AS attr_ksn_id_730;
		
join_with_attr_id_730 = 
    JOIN gen_join_with_attr_id_720 BY ksn_id 
         LEFT OUTER ,
	 gen_attribute_relate_curr_tbl_ksn_valu_730 BY attr_ksn_id_730 PARALLEL $NUM_PARALLEL;

group_data_by_ksn_id_to_find_rpd_frz_dur  =
    GROUP join_with_attr_id_730
    BY ksn_id;

flatten_data_to_find_rpd_frz_dur =
        FOREACH group_data_by_ksn_id_to_find_rpd_frz_dur
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER $1 BY attribute_relate_alternate_id_730 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };
gen_join_with_attr_id_730 = 
    FOREACH flatten_data_to_find_rpd_frz_dur
    GENERATE 
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        dist_typ_cd AS dist_typ_cd,
        can_carr_mdl_id AS can_carr_mdl_id,
        dd_ind AS dd_ind,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        groc_crossover_ind AS groc_crossover_ind,
        instl_ind AS instl_ind,
        pln_id AS pln_id,
        owner_cd AS owner_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        md_style_ref_cd AS md_style_ref_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        srs_bus_nm AS srs_bus_nm,
        srs_bus_no AS srs_bus_no,
        srs_cls_ds AS srs_cls_ds,
        srs_cls_no AS srs_cls_no,
        srs_div_nm AS srs_div_nm,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_ln_ds AS srs_ln_ds,
        srs_ln_no AS srs_ln_no,
        srs_sku_no AS srs_sku_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_sbl_no AS srs_sbl_no,
        shc_cat_nm AS shc_cat_nm,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_no AS shc_cat_no,
        shc_dept_nm AS shc_dept_nm,
        shc_dept_no AS shc_dept_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dvsn_no AS shc_dvsn_no,
        descr AS descr,
        item AS item,
        shc_subcat_nm AS shc_subcat_nm,
        shc_subcat_no AS shc_subcat_no,
        str_fcst_cd AS str_fcst_cd,
        similar_ksn_pct AS similar_ksn_pct,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        similar_ksn_id AS similar_ksn_id,
        sub_seas_id AS sub_seas_id,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        seas_yr AS seas_yr,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        seas_cd AS seas_cd,
        ksn_id AS ksn_id,
        referred_package_id AS referred_package_id,
        brand_ds AS brand_ds,
        size AS size,
        color_ds AS color_ds,
        style AS style,
        cross_mdse_attr_cd AS cross_mdse_attr_cd,
        itm_pgm AS itm_pgm,
        sls_pfm_seg_cd AS sls_pfm_seg_cd,
        fmt_excl_cd AS fmt_excl_cd,
        key_pgm AS key_pgm,
        whse_sizing AS whse_sizing,
        itm_rpd_fl AS itm_rpd_fl,
        itm_cd_fl AS itm_cd_fl,
        obn_830_dur AS obn_830_dur,
        (value_definition_tx_730 IS NULL OR value_definition_tx_730 == '' ? '0' : (SUBSTRING(value_definition_tx_730,0,4) == 'RR00' OR  SUBSTRING(value_definition_tx_730,0,4) == 'RR10'  ?  (chararray)((int)SUBSTRING(value_definition_tx_730,(int)(SIZE(value_definition_tx_730)-2),(int)SIZE(value_definition_tx_730)) * 10080) : '0')) AS rpd_frz_dur;

gen_attribute_relate_curr_tbl_ksn_valu_1610 = 
    FOREACH gen_attribute_relate_curr_tbl_ksn_valu_1610 
    GENERATE 
        value_nm AS value_nm_1610,
        value_definition_tx AS value_definition_tx_1610,
        attribute_relate_alternate_id AS attribute_relate_alternate_id_1610,
        attr_ksn_id AS attr_ksn_id_1610;
		
join_with_attr_id_1610 = 
    JOIN gen_join_with_attr_id_730 BY ksn_id 
         LEFT OUTER ,
         gen_attribute_relate_curr_tbl_ksn_valu_1610 BY attr_ksn_id_1610 PARALLEL $NUM_PARALLEL;

group_data_by_ksn_id_to_find_tire_size_ds  =
    GROUP join_with_attr_id_1610
    BY ksn_id;

flatten_data_to_find_tire_size_ds =
        FOREACH group_data_by_ksn_id_to_find_tire_size_ds
            {
                    sort_data_desc_on_attr_relate_alt_id = ORDER $1 BY attribute_relate_alternate_id_1610 DESC ;
                    take_first_row = LIMIT sort_data_desc_on_attr_relate_alt_id 1;
                    GENERATE FLATTEN (take_first_row);
            };
	 
gen_join_with_attr_id_1610 = 
    FOREACH flatten_data_to_find_tire_size_ds
    GENERATE 
        spc_ord_cdt_fl AS spc_ord_cdt_fl,
        itm_emp_fl AS itm_emp_fl,
        dist_typ_cd AS dist_typ_cd,
        can_carr_mdl_id AS can_carr_mdl_id,
        dd_ind AS dd_ind,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        groc_crossover_ind AS groc_crossover_ind,
        instl_ind AS instl_ind,
        pln_id AS pln_id,
        owner_cd AS owner_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        md_style_ref_cd AS md_style_ref_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        str_reord_auth_cd AS str_reord_auth_cd,
        srs_bus_nm AS srs_bus_nm,
        srs_bus_no AS srs_bus_no,
        srs_cls_ds AS srs_cls_ds,
        srs_cls_no AS srs_cls_no,
        srs_div_nm AS srs_div_nm,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_ln_ds AS srs_ln_ds,
        srs_ln_no AS srs_ln_no,
        srs_sku_no AS srs_sku_no,
        srs_sbl_ds AS srs_sbl_ds,
        srs_sbl_no AS srs_sbl_no,
        shc_cat_nm AS shc_cat_nm,
        shc_cat_grp_nm AS shc_cat_grp_nm,
        shc_cat_grp_no AS shc_cat_grp_no,
        shc_cat_no AS shc_cat_no,
        shc_dept_nm AS shc_dept_nm,
        shc_dept_no AS shc_dept_no,
        shc_dvsn_nm AS shc_dvsn_nm,
        shc_dvsn_no AS shc_dvsn_no,
        descr AS descr,
        item AS item,
        shc_subcat_nm AS shc_subcat_nm,
        shc_subcat_no AS shc_subcat_no,
        str_fcst_cd AS str_fcst_cd,
        similar_ksn_pct AS similar_ksn_pct,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        similar_ksn_id AS similar_ksn_id,
        sub_seas_id AS sub_seas_id,
        dotcom_eligibility_cd AS dotcom_eligibility_cd,
        item_desc AS item_desc,
        seas_yr AS seas_yr,
        srs_div_itm AS srs_div_itm,
        srs_div_itm_sku AS srs_div_itm_sku,
        ima_smt_itm_no AS ima_smt_itm_no,
        ima_smt_itm_ds AS ima_smt_itm_ds,
        ima_smt_fac_qt AS ima_smt_fac_qt,
        uom AS uom,
        vol AS vol,
        wgt AS wgt,
        seas_cd AS seas_cd,
        ksn_id AS ksn_id,
        referred_package_id AS referred_package_id,
        brand_ds AS brand_ds,
        size AS size,
        color_ds AS color_ds,
        style AS style,
        cross_mdse_attr_cd AS cross_mdse_attr_cd,
        itm_pgm AS itm_pgm,
        sls_pfm_seg_cd AS sls_pfm_seg_cd,
        fmt_excl_cd AS fmt_excl_cd,
        key_pgm AS key_pgm,
        whse_sizing AS whse_sizing,
        itm_rpd_fl AS itm_rpd_fl,
        itm_cd_fl AS itm_cd_fl,
        obn_830_dur AS obn_830_dur,
        rpd_frz_dur AS rpd_frz_dur,
        value_nm_1610 AS tire_size_ds;
        
/******** 44. JOIN SMITH__ITEM_COMBINED_HIERARCHY_CURRENT TO GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT 
               ON SRS_DIV_NO, SRS_ITM_NO, AND SRS_SKU_NO AND EXTRACT COLUMN NON_STOCK_SOURCE_CD.
                 IF AT LEAST ONE ROW ON GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT  HAS A NON_STOCK_SOURCE_CD NOT EQUAL TO BLANKS,
                    THE ITEM MUST BE CONSIDERED CENTRALLY STOCKED AND THE ITM_CS_FL SET TO 1.
                         IF THERE ARE NO WAREHOUSES WITH A NON-BLANK NON STOCK SOURCE CODE, THE FLAG MUST BE SET TO 0. *********/

gen_gold__inventory_sears_dc_item_facility_current_table = 
    FOREACH gold__inventory_sears_dc_item_facility_current_data 
    GENERATE 
        sears_division_nbr AS in_division_nbr,
        sears_item_nbr AS in_item_nbr,
        sears_sku_nbr AS in_sku_nbr,
	non_stock_source_cd;
		
filter_gold__inventory_sears_dc_item_facility_current_table = 
    FILTER gen_gold__inventory_sears_dc_item_facility_current_table 
	BY non_stock_source_cd != ' ';
	
grp_filter_gold__inventory_sears_dc_item_facility_current_table = 
    GROUP filter_gold__inventory_sears_dc_item_facility_current_table 
	      BY (in_division_nbr,in_item_nbr,in_sku_nbr);
	
flatten_grp_filter_gold__inventory_sears_dc_item_facility_current_table = 
    FOREACH grp_filter_gold__inventory_sears_dc_item_facility_current_table 
            { a = LIMIT $1 1;
              GENERATE flatten(a);
            };


join_inv_item_shc_combil_div_sku_itm = 
    JOIN gen_join_with_attr_id_1610 BY ((long)srs_div_no,(long)srs_itm_no,(long)srs_sku_no) 
         LEFT OUTER ,
	 flatten_grp_filter_gold__inventory_sears_dc_item_facility_current_table BY ((long)in_division_nbr,(long)in_item_nbr,(long)in_sku_nbr);

gen_join_inv_item_shc_combil_div_sku_itm = 
    FOREACH join_inv_item_shc_combil_div_sku_itm 
    GENERATE
        (non_stock_source_cd IS NULL ? '0' : '1' ) AS itm_cs_fl,
        itm_rpd_fl,
        itm_cd_fl,
        can_carr_mdl_id,
        dd_ind,
        fut_ntwk_dist_cd,
        fut_ntwk_eff_dt,
        groc_crossover_ind,
        instl_ind,
        pln_id,
        owner_cd,
        ima_itm_typ_cd,
        jit_ntwk_dist_cd,
        md_style_ref_cd,
        ntwk_dist_cd,
        itm_purch_sts_cd,
        str_reord_auth_cd,
        srs_bus_nm,
        srs_bus_no,
        srs_cls_ds,
        srs_cls_no,
        srs_div_nm,
        srs_div_no,
        srs_itm_no,
        srs_ln_ds,
        srs_ln_no,
        srs_sku_no,
        srs_sbl_ds,
        srs_sbl_no,
        shc_cat_nm,
        shc_cat_grp_nm,
        shc_cat_grp_no,
        shc_cat_no,
        shc_dept_nm,
        shc_dept_no,
        shc_dvsn_nm,
        shc_dvsn_no ,
        descr,
        item,
        shc_subcat_nm,
        shc_subcat_no,
        str_fcst_cd,
        similar_ksn_pct,
        special_retail_order_system_ind,
        similar_ksn_id,
        sub_seas_id,
        dotcom_eligibility_cd AS dot_com_cd,
        item_desc,
        seas_yr,
        srs_div_itm,
        srs_div_itm_sku,
        ima_smt_itm_no,
        ima_smt_itm_ds,
        ima_smt_fac_qt,
        uom,
        vol,
        wgt,
        spc_ord_cdt_fl,
        itm_emp_fl,
        dist_typ_cd,
        obn_830_dur,
        ( obn_830_dur IS NULL OR obn_830_dur == '0' ? '0' : '1') AS obn_830_fl,
        rpd_frz_dur,
        sls_pfm_seg_cd,
        fmt_excl_cd,
        cross_mdse_attr_cd,
        whse_sizing,
        itm_pgm,
        key_pgm,
        size,
        style,
        brand_ds,
        color_ds,
        tire_size_ds,
        seas_cd,
        ksn_id AS ref_ksn_id,
        --extra
        referred_package_id;
		
/****** END 44 *********/
/**** FILTERING GOLD__ITEM_CORE_BRIDGE_ITEM TABLE ON EFF DATE AND EXP DATE ***************************/

filter_gold__item_core_bridge_item_data = 
    FILTER gold__item_core_bridge_item_data 
	BY '$CURRENT_TIMESTAMP_AT_6_PM' >= effective_ts AND '$CURRENT_TIMESTAMP_AT_6_PM' <= expiration_ts;
	
/************** JOIN PREVIOUS DATA TO GOLD__ITEM_CORE_BRIDGE_ITEM **********************************/

join_core_bridge_gen = 
    JOIN gen_join_inv_item_shc_combil_div_sku_itm BY ((long)srs_itm_no,(long)srs_div_no) 
         LEFT OUTER ,
	 filter_gold__item_core_bridge_item_data BY ((long)sears_item_nbr,(long)sears_division_nbr);
		 
gen_join_core_bridge_gen = 
    FOREACH join_core_bridge_gen 
    GENERATE
        brand_ds,
        can_carr_mdl_id,
        color_ds,
        cross_mdse_attr_cd,
        dd_ind,
        descr,
        dist_typ_cd,
        dot_com_cd,
        fmt_excl_cd,
        fut_ntwk_dist_cd,
        fut_ntwk_eff_dt,
        groc_crossover_ind,
        ima_itm_typ_cd,
        ima_smt_fac_qt,
        ima_smt_itm_ds,
        ima_smt_itm_no,
        instl_ind,
        item,
        itm_cd_fl,
        itm_cs_fl,
        itm_emp_fl,
        itm_pgm,
        itm_purch_sts_cd,
        itm_rpd_fl,
        jit_ntwk_dist_cd,
        key_pgm,
        md_style_ref_cd,
        ntwk_dist_cd,
        obn_830_dur,
        obn_830_fl,
        owner_cd,
        pln_id,
        ref_ksn_id,
        rpd_frz_dur,
        seas_cd,
        seas_yr,
        shc_cat_grp_nm,
        shc_cat_grp_no,
        shc_cat_nm,
        shc_cat_no,
        shc_dept_nm,
        shc_dept_no,
        shc_dvsn_nm,
        shc_dvsn_no,
        shc_subcat_nm,
        shc_subcat_no,
        size,
        sls_pfm_seg_cd,
        spc_ord_cdt_fl,
        srs_bus_nm,
        srs_bus_no,
        srs_cls_ds,
        srs_cls_no,
        srs_div_itm,
        srs_div_itm_sku,
        srs_div_nm,
        srs_div_no,
        srs_itm_no,
        srs_ln_ds,
        srs_ln_no,
        srs_sbl_ds,
        srs_sbl_no,
        srs_sku_no,
        str_fcst_cd,
        str_reord_auth_cd,
        style,
        sub_seas_id,
        tire_size_ds,
        uom,
        vol,
        wgt,
        whse_sizing,
        (cost_pointing_method_cd IS NULL OR owner_cd IS NULL OR owner_cd == 'K' ? '0' : (owner_cd == 'S' OR owner_cd == 'B' ? (TRIM(cost_pointing_method_cd) == 'E' ? '1' : '0' ) : '0' )) AS easy_ord_fl;
        
gen_join_core_bridge_gen = 
    FOREACH gen_join_core_bridge_gen 
    GENERATE 
        brand_ds,
        can_carr_mdl_id,
        color_ds,
        cross_mdse_attr_cd,
        dd_ind,
        descr,
        dist_typ_cd,
        dot_com_cd,
        fmt_excl_cd,
        fut_ntwk_dist_cd,
        fut_ntwk_eff_dt,
        groc_crossover_ind,
        ima_itm_typ_cd,
        ima_smt_fac_qt,
        ima_smt_itm_ds,
        ima_smt_itm_no,
        instl_ind,
        item,
        itm_cd_fl,
        itm_cs_fl,
        itm_emp_fl,
        itm_pgm,
        itm_purch_sts_cd,
        itm_rpd_fl,
        jit_ntwk_dist_cd,
        key_pgm,
        md_style_ref_cd,
        ntwk_dist_cd,
        obn_830_dur,
        obn_830_fl,
        owner_cd,
        pln_id,
        ref_ksn_id,
        rpd_frz_dur,
        seas_cd,
        seas_yr,
        shc_cat_grp_nm,
        shc_cat_grp_no,
        shc_cat_nm,
        shc_cat_no,
        shc_dept_nm,
        shc_dept_no,
        shc_dvsn_nm,
        shc_dvsn_no,
        shc_subcat_nm,
        shc_subcat_no,
        size,
        sls_pfm_seg_cd,
        spc_ord_cdt_fl,
        srs_bus_nm,
        srs_bus_no,
        srs_cls_ds,
        srs_cls_no,
        srs_div_itm,
        srs_div_itm_sku,
        srs_div_nm,
        srs_div_no,
        srs_itm_no,
        srs_ln_ds,
        srs_ln_no,
        srs_sbl_ds,
        srs_sbl_no,
        srs_sku_no,
        str_fcst_cd,
        str_reord_auth_cd,
        style,
        sub_seas_id,
        tire_size_ds,
        uom,
        vol,
        wgt,
        whse_sizing,
        (easy_ord_fl IS NULL ? '0' : easy_ord_fl) AS easy_ord_fl;

/*********** JOIN PREVIOUS DATA TO ITEM_RPT_COST TABLE ON ITEM_ID *****************/
join_item_rpt_cost_with_data = 
    JOIN gen_join_core_bridge_gen BY item 
         LEFT OUTER ,
         item_rpt_cost_data BY item_id; 
		 


grp_join_item_rpt_cost_with_data = 
    GROUP join_item_rpt_cost_with_data 
    BY item;


grp_join_item_rpt_cost_with_data = 
    FOREACH grp_join_item_rpt_cost_with_data 
	    {
		    a = ORDER $1 BY fisc_wk_end_dt DESC ;
			b = LIMIT a 1;
			GENERATE FLATTEN (b);
		};					


gen_join_item_rpt_cost_with_data = 
    FOREACH grp_join_item_rpt_cost_with_data 
    GENERATE 
        (corp_90dy_avg_cost IS NULL ? '0.0000' : corp_90dy_avg_cost) AS natl_un_cst_am,
        brand_ds,
        can_carr_mdl_id,
        color_ds,
        cross_mdse_attr_cd,
        dd_ind,
        descr,
        dist_typ_cd,
        dot_com_cd,
        fmt_excl_cd,
        fut_ntwk_dist_cd,
        fut_ntwk_eff_dt,
        groc_crossover_ind,
        ima_itm_typ_cd,
        ima_smt_fac_qt,
        ima_smt_itm_ds,
        ima_smt_itm_no,
        instl_ind,
        item,
        itm_cs_fl,
        itm_pgm,
        itm_purch_sts_cd,
        jit_ntwk_dist_cd,
        key_pgm,
        md_style_ref_cd,
        ntwk_dist_cd,
        obn_830_dur,
        obn_830_fl,
        owner_cd,
        pln_id,
        ref_ksn_id,
        rpd_frz_dur,
        seas_cd,
        seas_yr,
        shc_cat_grp_nm,
        shc_cat_grp_no,
        shc_cat_nm,
        shc_cat_no,
        shc_dept_nm,
        shc_dept_no,
        shc_dvsn_nm,
        shc_dvsn_no,
        shc_subcat_nm,
        shc_subcat_no,
        size,
        sls_pfm_seg_cd,
        srs_bus_nm,
        srs_bus_no,
        srs_cls_ds,
        srs_cls_no,
        srs_div_itm,
        srs_div_itm_sku,
        srs_div_nm,
        srs_div_no,
        srs_itm_no,
        srs_ln_ds,
        srs_ln_no,
        srs_sbl_ds,
        srs_sbl_no,
        srs_sku_no,
        str_fcst_cd,
        str_reord_auth_cd,
        style,
        sub_seas_id,
        tire_size_ds,
        uom,
        vol,
        wgt,
        whse_sizing,
        itm_cd_fl,
        itm_emp_fl,
        itm_rpd_fl,
        spc_ord_cdt_fl,
        easy_ord_fl;

/***** 
    SINCE DEFAULT PRICE DATA DOES NOT HAVE ITEM ID, WE NEED TO SIMULATE A VIEW IN HADOOP FOR THE BASE TABLES:
	1) GOLD__ITEM_PRICE_LINK_DEFAULT_PRICE
	2) GOLD__ITEM_PRICE_LINK_MEMBER

VIEW IS GIVEN BELOW:

CREATE VIEW PROD.DFLT_PRC_V2 AS SELECT A.ITEM_ID , A.PRC_LINK_ID , B.PRC_SRC_ID , B.EFF_TS , B.EXPIR_TS ,
B.PRC_AMT , B.PRC_MULT_QTY , B.LAST_CHG_USER_ID , B.WIN_PRC_STAT_CD FROM DB2.C_PRC_LINK_MBR A ,
DB2.PRC_LINK_DFLT_PRC B WHERE A.PRC_LINK_ID = B.PRC_LINK_ID AND CURRENT TIMESTAMP BETWEEN B.EFF_TS AND
B.EXPIR_TS;    ******/
---------------------------------------------------------------------------------------------------------
/**** NEW CHNAGE ***/

DEFAULT_DATA = 
    LOAD '$GOLD__ITEM_PRICE_LINK_DEFAULT_PRICE_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($GOLD__ITEM_PRICE_LINK_DEFAULT_PRICE_SCHEMA);
	
DEFAULT_DATA = 
    FILTER DEFAULT_DATA  
	BY ('$CURRENT_TIMESTAMP' >= effective_ts and '$CURRENT_TIMESTAMP' <= expiration_ts);
	
MEMBER_DATA  = 
    LOAD '$GOLD__ITEM_PRICE_LINK_MEMBER_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($GOLD__ITEM_PRICE_LINK_MEMBER_SCHEMA);
	
MEMBER_DATA = 
    FILTER MEMBER_DATA  
	BY ('$CURRENT_TIMESTAMP' >= effective_ts AND '$CURRENT_TIMESTAMP' <= expiration_ts);
	
DEFAULT_MEMBER_JOIN_TEMP = 
    JOIN DEFAULT_DATA BY price_link_id, 
         MEMBER_DATA BY price_link_id;


/*** PROJECTION OF DEFAULT PRICE DATA *********/

PRC_DATA = 
    FOREACH DEFAULT_MEMBER_JOIN_TEMP 
    GENERATE
        MEMBER_DATA::item_id AS item_id,
        DEFAULT_DATA::price_link_id AS price_link_id,
        DEFAULT_DATA::price_source_id AS price_source_id,
        DEFAULT_DATA::price_amt AS price_amt,
        DEFAULT_DATA::price_multiple_qty AS price_multiple_qty,
        DEFAULT_DATA::last_change_user_id AS last_change_user_id,
        DEFAULT_DATA::winning_price_status_cd AS winning_price_status_cd;

---------------------------------------------------------------------------------------------------------
join_dflt_prc_v2_oi_item_item_id = 
    JOIN gen_join_item_rpt_cost_with_data BY (long)item 
         LEFT OUTER ,
	 PRC_DATA BY (long)item_id;
		 
gen_prd_sll_am = 
    FOREACH join_dflt_prc_v2_oi_item_item_id 
    GENERATE
        (chararray)((float)price_amt/(float)price_multiple_qty) AS prd_sll_am,
        brand_ds,
        can_carr_mdl_id,
        color_ds,
        cross_mdse_attr_cd,
        dd_ind,
        descr,
        dist_typ_cd,
        dot_com_cd,
        fmt_excl_cd,
        fut_ntwk_dist_cd,
        fut_ntwk_eff_dt,
        groc_crossover_ind,
        ima_itm_typ_cd,
        ima_smt_fac_qt,
        ima_smt_itm_ds,
        ima_smt_itm_no,
        instl_ind,
        item,
        itm_cd_fl,
        itm_cs_fl,
        itm_emp_fl,
        itm_pgm,
        itm_purch_sts_cd,
        itm_rpd_fl,
        jit_ntwk_dist_cd,
        key_pgm,
        md_style_ref_cd,
        ntwk_dist_cd,
        obn_830_dur,
        obn_830_fl,
        owner_cd,
        pln_id,
        ref_ksn_id,
        rpd_frz_dur,
        seas_cd,
        seas_yr,
        shc_cat_grp_nm,
        shc_cat_grp_no,
        shc_cat_nm,
        shc_cat_no,
        shc_dept_nm,
        shc_dept_no,
        shc_dvsn_nm,
        shc_dvsn_no,
        shc_subcat_nm,
        shc_subcat_no,
        size,
        sls_pfm_seg_cd,
        spc_ord_cdt_fl,
        srs_bus_nm,
        srs_bus_no,
        srs_cls_ds,
        srs_cls_no,
        srs_div_itm,
        srs_div_itm_sku,
        srs_div_nm,
        srs_div_no,
        srs_itm_no,
        srs_ln_ds,
        srs_ln_no,
        srs_sbl_ds,
        srs_sbl_no,
        srs_sku_no,
        str_fcst_cd,
        str_reord_auth_cd,
        style,
        sub_seas_id,
        tire_size_ds,
        uom,
        vol,
        wgt,
        whse_sizing,
        easy_ord_fl,
        natl_un_cst_am;
		
join_data_to_rpt_grp = 
    JOIN gen_prd_sll_am BY (long)item 
         LEFT OUTER ,
	 item_rpt_grp_data BY (long)item_id;
		 
item_trans_op = 
    FOREACH join_data_to_rpt_grp  
    GENERATE
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
        NULL AS vnd_no,
        NULL AS vnd_nm,
        NULL AS vnd_itm_no,
        (spc_ord_cdt_fl IS NULL ? '0' : spc_ord_cdt_fl) AS spc_ord_cdt_fl,
        (itm_emp_fl IS NULL ? '0' : itm_emp_fl) AS itm_emp_fl,
        (easy_ord_fl IS NULL ? '0' : easy_ord_fl) AS easy_ord_fl,
        itm_rpd_fl AS itm_rpd_fl,
        itm_cd_fl AS itm_cd_fl,
        NULL AS itm_imp_fl,
        itm_cs_fl AS itm_cs_fl,
        dd_ind  AS dd_ind ,
        instl_ind  AS instl_ind, 
        NULL AS cheetah_elgbl_fl,
        dot_com_cd AS dot_com_cd,
        obn_830_fl AS obn_830_fl,
        obn_830_dur AS obn_830_dur,
        rpd_frz_dur AS rpd_frz_dur,
        dist_typ_cd AS dist_typ_cd,
        sls_pfm_seg_cd AS sls_pfm_seg_cd,
        fmt_excl_cd AS fmt_excl_cd,
        str_fcst_cd AS str_fcst_cd,
        NULL AS inv_mgmt_srvc_cd,
        ima_itm_typ_cd AS ima_itm_typ_cd,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        ntwk_dist_cd AS ntwk_dist_cd,
        fut_ntwk_dist_cd AS fut_ntwk_dist_cd,
        fut_ntwk_eff_dt AS fut_ntwk_eff_dt,
        jit_ntwk_dist_cd AS jit_ntwk_dist_cd,
        NULL AS cust_dir_ntwk_cd,
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
        rpt_grp_id AS rpt_id,
        rpt_grp_seq_nbr AS rpt_id_seq_no,
        NULL AS itm_fcst_grp_id,
        NULL AS itm_fcst_grp_ds,
        NULL AS idrp_itm_typ_ds,
        brand_ds AS brand_ds,
        color_ds AS color_ds,
        tire_size_ds AS tire_size_ds,
        'A' AS elig_sts_cd,
        NULL AS lst_sts_chg_dt,
        NULL AS itm_del_fl,
        NULL AS prd_prg_dt;
		
------------------------------------------------------------------------------------------------------------------------

--CR 808
------------------------------------------------------------------------------------------------------------------------		


JOIN_808 = JOIN smith__item_combined_hierarchy_current_data BY referred_package_id, gold__item_package_current_data BY package_id USING 'skewed';

JOIN_808_GEN = FOREACH JOIN_808 GENERATE 
gold__item_package_current_data::ksn_id AS ksn_id;

gold__item_attribute_relate_current_data_filter_808 = FILTER gold__item_attribute_relate_current_data BY attribute_id == '270' AND attribute_relate_level_cd == 'K' ;

gold__item_attribute_join_808 = JOIN JOIN_808_GEN BY ksn_id, gold__item_attribute_relate_current_data_filter_808 BY ksn_id USING 'skewed';

gold__item_attribute_join_808_gen = FOREACH gold__item_attribute_join_808 GENERATE

JOIN_808_GEN::ksn_id AS ksn_id,
gold__item_attribute_relate_current_data_filter_808::value_definition_tx AS value_definition_tx;


item_trans_op_join_808 = JOIN item_trans_op BY ref_ksn_id LEFT OUTER, gold__item_attribute_join_808_gen BY ksn_id;

item_trans_op_final = FOREACH item_trans_op_join_808 GENERATE

'$CURRENT_TIMESTAMP'	AS	load_ts	,
item_trans_op::item	AS	item	,
item_trans_op::descr	AS	descr	,
item_trans_op::shc_dvsn_no	AS	shc_dvsn_no	,
item_trans_op::shc_dvsn_nm	AS	shc_dvsn_nm	,
item_trans_op::shc_dept_no	AS	shc_dept_no	,
item_trans_op::shc_dept_nm	AS	shc_dept_nm	,
item_trans_op::shc_cat_grp_no	AS	shc_cat_grp_no	,
item_trans_op::shc_cat_grp_nm	AS	shc_cat_grp_nm	,
item_trans_op::shc_cat_no	AS	shc_cat_no	,
item_trans_op::shc_cat_nm	AS	shc_cat_nm	,
item_trans_op::shc_subcat_no	AS	shc_subcat_no	,
item_trans_op::shc_subcat_nm	AS	shc_subcat_nm	,
item_trans_op::ref_ksn_id	AS	ref_ksn_id	,
item_trans_op::srs_bus_no	AS	srs_bus_no	,
item_trans_op::srs_bus_nm	AS	srs_bus_nm	,
item_trans_op::srs_div_no	AS	srs_div_no	,
item_trans_op::srs_div_nm	AS	srs_div_nm	,
item_trans_op::srs_ln_no	AS	srs_ln_no	,
item_trans_op::srs_ln_ds	AS	srs_ln_ds	,
item_trans_op::srs_sbl_no	AS	srs_sbl_no	,
item_trans_op::srs_sbl_ds	AS	srs_sbl_ds	,
item_trans_op::srs_cls_no	AS	srs_cls_no	,
item_trans_op::srs_cls_ds	AS	srs_cls_ds	,
item_trans_op::srs_itm_no	AS	srs_itm_no	,
item_trans_op::srs_sku_no	AS	srs_sku_no	,
item_trans_op::srs_div_itm	AS	srs_div_itm	,
item_trans_op::srs_div_itm_sku	AS	srs_div_itm_sku	,
item_trans_op::ima_smt_itm_no	AS	ima_smt_itm_no	,
item_trans_op::ima_smt_itm_ds	AS	ima_smt_itm_ds	,
item_trans_op::ima_smt_fac_qt	AS	ima_smt_fac_qt	,
item_trans_op::uom	AS	uom	,
item_trans_op::vol	AS	vol	,
item_trans_op::wgt	AS	wgt	,
item_trans_op::vnd_no	AS	vnd_no	,
item_trans_op::vnd_nm	AS	vnd_nm	,
item_trans_op::vnd_itm_no	AS	vnd_itm_no	,
item_trans_op::spc_ord_cdt_fl	AS	spc_ord_cdt_fl	,
item_trans_op::itm_emp_fl	AS	itm_emp_fl	,
item_trans_op::easy_ord_fl	AS	easy_ord_fl	,
item_trans_op::itm_rpd_fl	AS	itm_rpd_fl	,
item_trans_op::itm_cd_fl	AS	itm_cd_fl	,
item_trans_op::itm_imp_fl	AS	itm_imp_fl	,
item_trans_op::itm_cs_fl	AS	itm_cs_fl	,
item_trans_op::dd_ind 	AS	dd_ind 	,
item_trans_op::instl_ind 	AS	instl_ind 	,
item_trans_op::cheetah_elgbl_fl	AS	cheetah_elgbl_fl	,
item_trans_op::dot_com_cd	AS	dot_com_cd	,
item_trans_op::obn_830_fl	AS	obn_830_fl	,
item_trans_op::obn_830_dur	AS	obn_830_dur	,
item_trans_op::rpd_frz_dur	AS	rpd_frz_dur	,
item_trans_op::dist_typ_cd	AS	dist_typ_cd	,
item_trans_op::sls_pfm_seg_cd	AS	sls_pfm_seg_cd	,
item_trans_op::fmt_excl_cd	AS	fmt_excl_cd	,
item_trans_op::str_fcst_cd	AS	str_fcst_cd	,
item_trans_op::inv_mgmt_srvc_cd	AS	inv_mgmt_srvc_cd	,
item_trans_op::ima_itm_typ_cd	AS	ima_itm_typ_cd	,
item_trans_op::itm_purch_sts_cd	AS	itm_purch_sts_cd	,
item_trans_op::ntwk_dist_cd	AS	ntwk_dist_cd	,
item_trans_op::fut_ntwk_dist_cd	AS	fut_ntwk_dist_cd	,
item_trans_op::fut_ntwk_eff_dt	AS	fut_ntwk_eff_dt	,
item_trans_op::jit_ntwk_dist_cd	AS	jit_ntwk_dist_cd	,
item_trans_op::cust_dir_ntwk_cd	AS	cust_dir_ntwk_cd	,
item_trans_op::str_reord_auth_cd	AS	str_reord_auth_cd	,
item_trans_op::cross_mdse_attr_cd	AS	cross_mdse_attr_cd	,
item_trans_op::whse_sizing	AS	whse_sizing	,
item_trans_op::can_carr_mdl_id	AS	can_carr_mdl_id	,
item_trans_op::groc_crossover_ind	AS	groc_crossover_ind	,
item_trans_op::owner_cd	AS	owner_cd	,
item_trans_op::pln_id	AS	pln_id	,
item_trans_op::itm_pgm	AS	itm_pgm	,
item_trans_op::key_pgm	AS	key_pgm	,
item_trans_op::natl_un_cst_am	AS	natl_un_cst_am	,
item_trans_op::prd_sll_am	AS	prd_sll_am	,
item_trans_op::size	AS	size	,
item_trans_op::style	AS	style	,
item_trans_op::md_style_ref_cd	AS	md_style_ref_cd	,
item_trans_op::seas_cd	AS	seas_cd	,
item_trans_op::seas_yr	AS	seas_yr	,
item_trans_op::sub_seas_id	AS	sub_seas_id	,
item_trans_op::rpt_id 	AS	rpt_id 	,
item_trans_op::rpt_id_seq_no	AS	rpt_id_seq_no	,
item_trans_op::itm_fcst_grp_id	AS	itm_fcst_grp_id	,
item_trans_op::itm_fcst_grp_ds	AS	itm_fcst_grp_ds	,
item_trans_op::idrp_itm_typ_ds	AS	idrp_itm_typ_ds	,
item_trans_op::brand_ds	AS	brand_ds	,
item_trans_op::color_ds	AS	color_ds	,
item_trans_op::tire_size_ds	AS	tire_size_ds	,
item_trans_op::elig_sts_cd	AS	elig_sts_cd	,
item_trans_op::lst_sts_chg_dt	AS	lst_sts_chg_dt	,
item_trans_op::itm_del_fl	AS	itm_del_fl	,
item_trans_op::prd_prg_dt	AS	prd_prg_dt	,
NULL AS	item_order_system_cd	,
NULL AS	idrp_order_method_cd	,
NULL AS	dotcom_assorted_cd,
NULL AS	dotcom_orderable_ind ,
NULL AS	roadrunner_eligible_ind	,
NULL AS	us_dot_ship_type_cd	,
NULL AS	package_weight_pounds_qty	,
NULL AS	package_depth_inch_qty	,
NULL AS	package_height_inch_qty	,
NULL AS	package_width_inch_qty	,
NULL AS	mailable_ind	,
NULL AS	temporary_online_fulfillment_type_cd	,
NULL AS	default_online_fulfillment_type_cd	,
NULL AS	default_online_ts	,
NULL AS	demand_online_fulfillment_cd	,
NULL AS	temporary_ups_billable_weight_qty	,
NULL AS	ups_billable_weight_qty	,
NULL AS	ups_billable_weight_ts	,
NULL AS	demand_ups_billable_weight_qty	,
NULL AS	web_exclusive_ind	,
((item_trans_op::ref_ksn_id is NOT NULL AND item_trans_op::ref_ksn_id !='')?gold__item_attribute_join_808_gen::value_definition_tx :NULL)	AS	price_type_desc	,
'$batchid'	AS	idrp_batch_id ;


-----------------------------------------------------------------------------------------------------------------------



item_trans_op = 
    DISTINCT item_trans_op_final;

/***************STORING ITEM OUTPUT **************************************/

STORE item_trans_op 
INTO '$WORK__IDRP_ELIGIBLE_ITEM_1_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*************** END OF SCRIPT *******************************************/


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
