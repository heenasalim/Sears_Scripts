/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_loc_kmart_store.pig
# AUTHOR NAME:         Onkar Malewadikar
# CREATION DATE:       Tue Dec 31 00:22:36 EST 2013
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

DEFINE AddDays com.searshc.supplychain.idrp.udf.AddOrRemoveDaysToDate();
DEFINE TrimLeadingZeros com.searshc.supplychain.idrp.udf.TrimLeadingZeros();

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

----------------------------------------------------Load for all tables and files required--------------------------------------------

inforem_store_driver_file = 
        LOAD '$WORK__IDRP_INFOREM_STORE_DRIVER_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($WORK__IDRP_INFOREM_STORE_DRIVER_SCHEMA);


inforem_store_master_file = 
        LOAD '$WORK__IDRP_INFOREM_STORE_MASTER_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($WORK__IDRP_INFOREM_STORE_MASTER_SCHEMA);


gold__item_ksn_collection_data = 
        LOAD '$GOLD__ITEM_KSN_COLLECTION_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($GOLD__ITEM_KSN_COLLECTION_SCHEMA);


gold__item_sears_hierarchy_current_data = 
        LOAD '$GOLD__ITEM_SEARS_HIERARCHY_CURRENT_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($GOLD__ITEM_SEARS_HIERARCHY_CURRENT_SCHEMA);


smith__item_combined_hierarchy_current_data = 
        LOAD '$SMITH__ITEM_COMBINED_HIERARCHY_CURRENT_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($SMITH__ITEM_COMBINED_HIERARCHY_CURRENT_SCHEMA);


gold__item_vendor_package_current_data = 
        LOAD '$GOLD__ITEM_VENDOR_PACKAGE_CURRENT_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($GOLD__ITEM_VENDOR_PACKAGE_CURRENT_SCHEMA);


smith__idrp_eligible_item_data = 
        LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($SMITH__IDRP_ELIGIBLE_ITEM_SCHEMA);


smith__idrp_eligible_loc_data = 
        LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);


smith__idrp_eligible_dc_loc_data = 
        LOAD '$WORK__IDRP_DC_LOCN_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($WORK__IDRP_DC_LOCN_SCHEMA);


gold__item_kmart_ksn_dc_package_data = 
        LOAD '$GOLD__ITEM_KMART_KSN_DC_PACKAGE_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($GOLD__ITEM_KMART_KSN_DC_PACKAGE_SCHEMA);


gold__item_aprk_current_data = 
        LOAD '$GOLD__ITEM_APRK_CURRENT_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
		AS ($GOLD__ITEM_APRK_CURRENT_SCHEMA);

----------------------------------------------------gold__item_aprk_current_data filter---------------------------------------------

gold__item_aprk_current_data_fltr = 
        FILTER gold__item_aprk_current_data 
		BY aprk_type_cd == 'ORD';

------------------------------------------------------Join and Transformation logic-------------------------------------------------

inforem_store_driver_file_gen = 
        FOREACH inforem_store_driver_file 
        GENERATE
                driver_item_id, 
                driver_store_nbr, 
                driver_ksn_id, 
                driver_vend_pack_id, 
                driver_reord_mthd_cd, 
                driver_dtc_num,
                driver_dtce_num;


inforem_store_master_file_gen = 
        FOREACH inforem_store_master_file 
        GENERATE
                m_repl_item_id,
                m_repl_store,
                m_store_auth_ind,
                m_store_supplier_cd,
                m_serv_dc,
                m_orderable_ksn_pack_id;


join_file1_file2 = 
        JOIN inforem_store_driver_file_gen BY (driver_item_id,driver_store_nbr), 
             inforem_store_master_file_gen BY (m_repl_item_id,m_repl_store);


join_file1_file2_gen = 
        FOREACH join_file1_file2 
		GENERATE 
                driver_item_id AS item,
                driver_store_nbr AS loc,
                (m_store_auth_ind == 'N' ? 'D' : 'A') AS elig_sts_cd,
                driver_ksn_id AS ksn_id,
                driver_dtc_num AS days_to_check_begin_date,
                driver_dtce_num AS days_to_check_end_date,
                driver_vend_pack_id AS vend_pack_id,
                driver_reord_mthd_cd AS recorder_method_code,
                m_store_supplier_cd AS str_supplier_cd,
                m_serv_dc AS serv_dc,
                m_orderable_ksn_pack_id AS m_ksn_id;


gold__item_vendor_package_current_data_gen = 
        FOREACH gold__item_vendor_package_current_data 
        GENERATE
                vendor_package_id,
                flow_type_cd,
                purchase_status_cd,
                aprk_id,
                import_cd,
                vendor_carton_qty,
                vendor_stock_nbr,
                ksn_package_id;

join_file_vend_pack_tbl = 
        JOIN join_file1_file2_gen BY (int)vend_pack_id, 
             gold__item_vendor_package_current_data_gen BY (int)vendor_package_id;


join_file_vend_pack_tbl_gen = 
        FOREACH join_file_vend_pack_tbl 
		GENERATE
                item,
                loc,
                elig_sts_cd,
                ksn_id,
                days_to_check_begin_date,
                days_to_check_end_date,
                vend_pack_id,
                recorder_method_code,
                str_supplier_cd,
                serv_dc,
                flow_type_cd AS vendor_pack_flow_type,
                purchase_status_cd AS vend_pack_purch_stat_cd,
                aprk_id AS aprk_id,
                (import_cd == 'I' ? '1' : '0') AS imp_fl,
                vendor_carton_qty AS vendor_pack_qty,
                vendor_stock_nbr AS vend_stk_nbr,
                ksn_package_id AS ksn_pack_id,
                m_ksn_id;


SPLIT join_file_vend_pack_tbl_gen INTO 
      join_file_vend_pack_tbl_gen_D IF str_supplier_cd == 'D',
	  join_file_vend_pack_tbl_gen_V IF str_supplier_cd == 'V';


join_file_vend_pack_tbl_gen_D_gen = 
        FOREACH join_file_vend_pack_tbl_gen_D 
		GENERATE 
	            item,
	            loc,
	            elig_sts_cd,
	            ksn_id,
	            days_to_check_begin_date,
	            days_to_check_end_date,
	            vend_pack_id,
	            recorder_method_code,
	            str_supplier_cd,
	            serv_dc as src_loc,
	            vendor_pack_flow_type,
	            vend_pack_purch_stat_cd,
	            aprk_id,
	            imp_fl,
	            vendor_pack_qty,
	            vend_stk_nbr,
	            ksn_pack_id,
	            m_ksn_id,
	            '' AS po_vnd_no;


gold__item_aprk_current_data_fltr_gen = 
        FOREACH gold__item_aprk_current_data_fltr 
		GENERATE
                aprk_id,
                duns_nbr;


join_vend_tbl_V_aprk_tbl = 
        JOIN join_file_vend_pack_tbl_gen_V BY (int)aprk_id, 
             gold__item_aprk_current_data_fltr_gen BY (int)aprk_id;


join_vend_tbl_V_aprk_tbl_gen = 
        FOREACH join_vend_tbl_V_aprk_tbl 
		GENERATE 
                item,
                loc,
                elig_sts_cd,
                ksn_id,
                days_to_check_begin_date,
                days_to_check_end_date,
                vend_pack_id,
                recorder_method_code,
                str_supplier_cd,
                CONCAT(duns_nbr,'_O') AS src_loc,
                vendor_pack_flow_type,
                vend_pack_purch_stat_cd,
                join_file_vend_pack_tbl_gen_V::aprk_id AS aprk_id,
                imp_fl,
                vendor_pack_qty,
                vend_stk_nbr,
                ksn_pack_id,
                m_ksn_id,
                CONCAT(duns_nbr,'_O') AS po_vnd_no;


final_join_data = 
        UNION join_file_vend_pack_tbl_gen_D_gen,
              join_vend_tbl_V_aprk_tbl_gen;
			  
final_join_data_dist = DISTINCT final_join_data;

----------------------------------------------------set the dotcom_order_indicator--------------------------------------------------

join_final_data_ksn_tbl = 
        JOIN final_join_data_dist BY ksn_id, 
             smith__item_combined_hierarchy_current_data BY ksn_id;


join_final_data_ksn_tbl_gen = 
        FOREACH join_final_data_ksn_tbl 
		GENERATE 
                item,
                loc,
                elig_sts_cd,
                smith__item_combined_hierarchy_current_data::ksn_id AS ksn_id,
                days_to_check_begin_date,
                days_to_check_end_date,
                vend_pack_id,
                recorder_method_code,
                str_supplier_cd,
                src_loc,
                vendor_pack_flow_type,
                vend_pack_purch_stat_cd,
                aprk_id,
                imp_fl,
                vendor_pack_qty,
                vend_stk_nbr,
                ksn_pack_id,
                m_ksn_id,
                po_vnd_no,
                purchase_status_cd AS oi_item_purch_stat_cd,
                dotcom_allocation_ind AS dotcom_order_ind;


join_final_data_ksn_tbl_gen = DISTINCT join_final_data_ksn_tbl_gen;

--------------------------------------------------To create a cigarette conversion file---------------------------------------------


smith__item_combined_hierarchy_current_data_fltr = 
        FILTER smith__item_combined_hierarchy_current_data 
		BY shc_item_type_cd == 'INVC';


join_ksn_collect_oi_item_ksn = 
        JOIN gold__item_ksn_collection_data BY external_ksn_id, 
             smith__item_combined_hierarchy_current_data_fltr BY ksn_id;


cigarette_file_gen = 
        FOREACH join_ksn_collect_oi_item_ksn 
		GENERATE
                external_ksn_id AS external_ksn_id,
                internal_ksn_id AS internal_ksn_id,
                internal_qty AS internal_qty;


join_final_data_ksn_tbl_gen_cigrt_file = 
        JOIN join_final_data_ksn_tbl_gen BY ksn_id LEFT OUTER, 
             cigarette_file_gen BY internal_ksn_id;


join_final_data_ksn_tbl_gen_cigrt_file_gen = 
        FOREACH join_final_data_ksn_tbl_gen_cigrt_file 
		GENERATE
                item,
                loc,
                elig_sts_cd,
                ksn_id,
                days_to_check_begin_date,
                days_to_check_end_date,
                vend_pack_id,
                recorder_method_code,
                str_supplier_cd,
                src_loc,
                vendor_pack_flow_type,
                vend_pack_purch_stat_cd,
                aprk_id,
                imp_fl,
                vendor_pack_qty,
                vend_stk_nbr,
                ksn_pack_id,
                m_ksn_id,
                po_vnd_no,
                oi_item_purch_stat_cd,
                dotcom_order_ind,
                (internal_ksn_id IS NOT NULL AND ksn_id == internal_ksn_id ? internal_qty : '0') AS intrnl_qty;

---------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------TO retrieve DC pack information-------------------------------------------------------

SPLIT join_final_data_ksn_tbl_gen_cigrt_file_gen INTO 
      DC_supplied IF TRIM(str_supplier_cd) == 'D', 
	  VEND_supplied IF (TRIM(str_supplier_cd) != 'D' OR TRIM(str_supplier_cd) =='V' OR str_supplier_cd IS NULL);


gold__item_kmart_ksn_dc_package_data_gen = 
        FOREACH gold__item_kmart_ksn_dc_package_data 
		GENERATE
                ksn_package_id,
                location_nbr,
                effective_ts,
                expiration_ts,
                purchase_status_cd,
                stocked_ind,
                outbound_package_qty;


gold__item_kmart_ksn_dc_package_data_fltr = 
        FILTER gold__item_kmart_ksn_dc_package_data_gen 
		BY '$CURRENT_TIMESTAMP_6PM' >= effective_ts AND '$CURRENT_TIMESTAMP_6PM' <= expiration_ts;


join_dc_supplied_ksn_dc_pack = 
        JOIN DC_supplied BY (m_ksn_id,src_loc), 
		gold__item_kmart_ksn_dc_package_data_fltr BY (ksn_package_id,location_nbr);


join_dc_supplied_ksn_dc_pack_gen = 
        FOREACH join_dc_supplied_ksn_dc_pack 
		GENERATE 
                item,
                loc,
                elig_sts_cd,
                ksn_id,
                days_to_check_begin_date,
                days_to_check_end_date,
                vend_pack_id,
                recorder_method_code,
                str_supplier_cd,
                src_loc,
                vendor_pack_flow_type,
                vend_pack_purch_stat_cd,
                aprk_id,
                imp_fl,
                vendor_pack_qty,
                vend_stk_nbr,
                ksn_pack_id,
                po_vnd_no,
                oi_item_purch_stat_cd,
                dotcom_order_ind,
                intrnl_qty,
                purchase_status_cd AS ksn_dc_pack_purch_stat_cd,
                stocked_ind AS stk_ind,
                outbound_package_qty AS outbnd_pack_qty;


-------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------To retrieve DC facility data----------------------------------------------------------

smith__idrp_eligible_dc_loc_data_gen = 
        FOREACH smith__idrp_eligible_dc_loc_data 
		GENERATE
                dc_locn_nbr,
                enable_jif_dc_ind;

join_dc_pack_info_dc_locn = 
        JOIN join_dc_supplied_ksn_dc_pack_gen BY src_loc, 
		     smith__idrp_eligible_dc_loc_data_gen BY dc_locn_nbr USING 'skewed';


join_dc_pack_info_dc_locn_gen = 
        FOREACH join_dc_pack_info_dc_locn 
		GENERATE
                item,
                loc,
                elig_sts_cd,
                ksn_id,
                days_to_check_begin_date,
                days_to_check_end_date,
                vend_pack_id,
                recorder_method_code,
                str_supplier_cd,
                src_loc,
                vendor_pack_flow_type,
                vend_pack_purch_stat_cd,
                aprk_id,
                imp_fl,
                vendor_pack_qty,
                vend_stk_nbr,
                ksn_pack_id,
                po_vnd_no,
                oi_item_purch_stat_cd,
                dotcom_order_ind,
                intrnl_qty,
                ksn_dc_pack_purch_stat_cd,
                stk_ind,
                outbnd_pack_qty,
                enable_jif_dc_ind AS enable_jif_dc_ind;

---------------------------------------------------------------------------------------------------------------------------------------

VEND_supplied_gen = 
        FOREACH VEND_supplied 
		GENERATE
                item,
                loc,
                elig_sts_cd,
                ksn_id,
                days_to_check_begin_date,
                days_to_check_end_date,
                vend_pack_id,
                recorder_method_code,
                str_supplier_cd,
                src_loc,
                vendor_pack_flow_type,
                vend_pack_purch_stat_cd,
                aprk_id,
                imp_fl,
                vendor_pack_qty,
                vend_stk_nbr,
                ksn_pack_id,
                po_vnd_no,
                oi_item_purch_stat_cd,
                dotcom_order_ind,
                intrnl_qty,
                '' AS ksn_dc_pack_purch_stat_cd,
                '' AS stk_ind,
                '' AS outbnd_pack_qty,
                '' AS enable_jif_dc_ind;

----------------------------------------------------------------------------------------------------------------------------------------
apppend_result_data = 
        UNION join_dc_pack_info_dc_locn_gen,
		      VEND_supplied_gen;


apppend_result_data_dist = DISTINCT apppend_result_data;

---------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------Retrieve srs_div/item/sku for shc item_id------------------------------------------------

smith__idrp_eligible_item_data_dist = 
        FOREACH smith__idrp_eligible_item_data 
		GENERATE 
                item AS item,
                ref_ksn_id AS ref_ksn_id,
                srs_div_no as srs_div_no,
                srs_itm_no AS srs_itm_no,
                srs_sku_no AS srs_sku_no, 
                itm_purch_sts_cd AS itm_purch_sts_cd,
                cross_mdse_attr_cd AS cross_mdse_attr_cd;


smith__idrp_eligible_item_data_dist_1 = DISTINCT smith__idrp_eligible_item_data_dist;


join_IE_item_result_data = 
        JOIN apppend_result_data_dist BY item, 
		     smith__idrp_eligible_item_data_dist_1 BY item USING 'skewed';


join_IE_item_result_data_gen = 
        FOREACH join_IE_item_result_data 
		GENERATE
                smith__idrp_eligible_item_data_dist_1::item AS item,
                loc,
                elig_sts_cd,
                ksn_id,
                days_to_check_begin_date,
                days_to_check_end_date,
                vend_pack_id,
                recorder_method_code,
                str_supplier_cd,
                src_loc,
                vendor_pack_flow_type,
                vend_pack_purch_stat_cd,
                aprk_id,
                imp_fl,
                vendor_pack_qty,
                vend_stk_nbr,
                ksn_pack_id,
                po_vnd_no,
                oi_item_purch_stat_cd,
                dotcom_order_ind,
                intrnl_qty,
                ksn_dc_pack_purch_stat_cd,
                stk_ind,
                outbnd_pack_qty,
                enable_jif_dc_ind,
                srs_div_no AS srs_div_no,
                srs_itm_no AS srs_itm_no,
                srs_sku_no AS srs_sku_no,
                itm_purch_sts_cd AS item_purchase_status,
                cross_mdse_attr_cd AS cross_merc_attr;

----------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------Retrieve sears location no for shc locn nbr------------------------------------------------

smith__idrp_eligible_loc_data_gen = 
        FOREACH smith__idrp_eligible_loc_data 
		GENERATE
                loc,
                srs_loc,
                loc_fmt_typ_cd;


join_IE_tbl_result_IE_LOC_tbl = 
        JOIN join_IE_item_result_data_gen BY loc, 
		     smith__idrp_eligible_loc_data_gen BY loc USING 'skewed';


join_IE_tbl_result_IE_LOC_tbl_gen = 
        FOREACH join_IE_tbl_result_IE_LOC_tbl 
		GENERATE
                item,
                smith__idrp_eligible_loc_data_gen::loc AS loc,
                elig_sts_cd,
                ksn_id,
                days_to_check_begin_date,
                days_to_check_end_date,
                vend_pack_id,
                recorder_method_code,
                str_supplier_cd,
                src_loc,
                vendor_pack_flow_type,
                vend_pack_purch_stat_cd,
                aprk_id,
                imp_fl,
                vendor_pack_qty,
                vend_stk_nbr,
                ksn_pack_id,
                po_vnd_no,
                oi_item_purch_stat_cd,
                dotcom_order_ind,
                intrnl_qty,
                ksn_dc_pack_purch_stat_cd,
                stk_ind,
                outbnd_pack_qty,
                enable_jif_dc_ind,
                srs_div_no,
                srs_itm_no,
                srs_sku_no,
                item_purchase_status,
                cross_merc_attr,
                srs_loc AS srs_location_nbr,
                loc_fmt_typ_cd AS sub_type,
                '1' AS src_pack_qty,
                '' AS dc_config_cd;

-------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------To determine cross merchandise eligibility-----------------------------------------------------

SPLIT join_IE_tbl_result_IE_LOC_tbl_gen INTO 
        cross_merc_eligibilty_data IF ((TRIM(cross_merc_attr) != 'SK3000' AND TRIM(sub_type) != '002') OR (cross_merc_attr IS NULL OR sub_type IS NULL)), 
		invalid_data IF (TRIM(cross_merc_attr) == 'SK3000' AND TRIM(sub_type) == '002');

-------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------To Generate the IE item/loc table with required columns----------------------------------------

smith__idrp_eligible_item_loc_final_tbl_gen = 
        FOREACH cross_merc_eligibilty_data 
		GENERATE
                item,
                loc,
                elig_sts_cd,
                src_pack_qty,
                src_loc,
                po_vnd_no,
                ksn_id,
                oi_item_purch_stat_cd as purch_stat_cd,
                intrnl_qty as retail_crtn_intrnl_pack_qty,
                days_to_check_begin_date,
                days_to_check_end_date,
                dotcom_order_ind,
                vend_pack_id,
                vend_pack_purch_stat_cd,
                vendor_pack_flow_type,
                vendor_pack_qty,
                recorder_method_code,
                str_supplier_cd,
                vend_stk_nbr,
                ksn_pack_id,
                ksn_dc_pack_purch_stat_cd,
                enable_jif_dc_ind,
                stk_ind,
                outbnd_pack_qty,
                dc_config_cd,
                imp_fl,
                srs_div_no,
                srs_itm_no,
                srs_sku_no,
                srs_location_nbr;


kmart_stores_final_output_gen = 
    FOREACH smith__idrp_eligible_item_loc_final_tbl_gen
	GENERATE
            '$CURRENT_TIMESTAMP' AS load_ts,
			item,
            loc,
            'K' AS src_owner_cd,
            elig_sts_cd,
            (str_supplier_cd=='V' and (int)retail_crtn_intrnl_pack_qty==0 ? (int)vendor_pack_qty : (str_supplier_cd=='V' and (int)retail_crtn_intrnl_pack_qty>0 ?  (int)retail_crtn_intrnl_pack_qty * (int)vendor_pack_qty : (str_supplier_cd=='D' ? (int)outbnd_pack_qty : 1))) AS src_pack_qty,
            src_loc,
            po_vnd_no,
            ksn_id,
            purch_stat_cd,
            retail_crtn_intrnl_pack_qty,
            (days_to_check_begin_date == '0' ? '1970-01-01' : AddDays('$CURRENT_DATE',(int)days_to_check_begin_date)) AS days_to_check_begin_date,
            (days_to_check_end_date == '365' ? '1970-01-01' : AddDays('$CURRENT_DATE',(int)days_to_check_end_date)) AS days_to_check_end_date,
            dotcom_order_ind,
            vend_pack_id,
            vend_pack_purch_stat_cd,
            vendor_pack_flow_type,
            vendor_pack_qty,
            recorder_method_code,
            str_supplier_cd,
            vend_stk_nbr,
            ksn_pack_id,
            ksn_dc_pack_purch_stat_cd,
            '' AS  inbnd_ord_uom_cd,
            enable_jif_dc_ind,
            stk_ind,
            '' AS  crtn_per_layer_qty,
            '' AS  layer_per_pall_qty,
--            (vendor_pack_flow_type IS NULL ? '' : (vendor_pack_flow_type == 'DSD' OR vendor_pack_flow_type == 'DSDS' ? 'DSD' : (str_supplier_cd IS NULL ? '' : (vendor_pack_flow_type == 'VCDC' AND str_supplier_cd == 'V' ? 'DSD' : (stk_ind IS NULL ? '' : ( vendor_pack_flow_type == 'VCDC' AND str_supplier_cd == 'D' AND stk_ind == 'Y' ? 'STK' : (vendor_pack_flow_type == 'DC' AND stk_ind == 'Y' ? 'STK' : (enable_jif_dc_ind IS NULL ? '' : (vendor_pack_flow_type == 'DC' AND stk_ind == 'N' AND enable_jif_dc_ind == 'N' ? 'FLT' : (vendor_pack_flow_type == 'DC' AND stk_ind == 'N' AND enable_jif_dc_ind == 'Y' ? 'JIF' : (vendor_pack_flow_type == 'VCDC' AND str_supplier_cd == 'D' AND stk_ind == 'N' AND enable_jif_dc_ind == 'N'  ?  'FLT' : (vendor_pack_flow_type == 'VCDC' AND str_supplier_cd == 'D' AND stk_ind == 'N' AND enable_jif_dc_ind == 'Y' ? 'JIF' : '')))))))))))) AS dc_config_cd,
        (vendor_pack_flow_type IS NULL ? '' : (vendor_pack_flow_type == 'JIT' ? 'JIT' : (vendor_pack_flow_type == 'DSD' OR vendor_pack_flow_type == 'DSDS' ? 'DSD' : (str_supplier_cd IS NULL ? '' : (vendor_pack_flow_type == 'VCDC' AND str_supplier_cd == 'V' ? 'DSD' : (stk_ind IS NULL ? '' : ( vendor_pack_flow_type == 'VCDC' AND str_supplier_cd == 'D' AND stk_ind == 'Y' ? 'STK' : (vendor_pack_flow_type == 'DC' AND stk_ind == 'Y' ? 'STK' : (enable_jif_dc_ind IS NULL ? '' : (vendor_pack_flow_type == 'DC' AND stk_ind == 'N' AND enable_jif_dc_ind == 'N' ? 'FLT' : (vendor_pack_flow_type == 'DC' AND stk_ind == 'N' AND enable_jif_dc_ind == 'Y' ? 'JIF' : (vendor_pack_flow_type == 'VCDC' AND str_supplier_cd == 'D' AND stk_ind == 'N' AND enable_jif_dc_ind == 'N'  ?  'FLT' : (vendor_pack_flow_type == 'VCDC' AND str_supplier_cd == 'D' AND stk_ind == 'N' AND enable_jif_dc_ind == 'Y' ? 'JIF' : ''))))))))))))) AS dc_config_cd,
            imp_fl,
            srs_div_no,
            srs_itm_no,
            srs_sku_no,
            TrimLeadingZeros(srs_location_nbr) AS srs_location_nbr,
            '' AS srs_source_nbr,
            '' AS rim_sts_cd,
            '' AS non_stock_source_cd,
            '' AS item_active_ind,
            '' AS item_reserve_cd,
            '' AS item_next_period_on_hand_qty,
            '' AS item_reserve_qty,
            '' AS item_back_order_qty,
            '' AS item_next_period_future_order_qty,
            '' AS item_on_order_qty,
            '' AS item_next_period_in_transit_qty,
            '' AS item_last_receive_dt,
            '' AS item_last_ship_dt,
            '' AS stk_typ_cd,
			'$batchid' AS batch_id;

---------------------------------------------- STORING THE OUTPUT FILE ON HDFS ---------------------------------

STORE kmart_stores_final_output_gen 
INTO '$WORK__IDRP_ELIGIBLE_ITEM_LOC_KMART_STORE_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

----------------------------------------------------------------------------------------------------------------


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
