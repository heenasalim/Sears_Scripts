/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_loc_kmart_dc.pig
# AUTHOR NAME:         Onkar Malewadikar
# CREATION DATE:       Tue Dec 31 02:29:05 EST 2013
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
DEFINE TrimLeadingZeros com.searshc.supplychain.idrp.udf.TrimLeadingZeros();

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

--------------------------------------Load for all tables and files required-----------------------------------------------------------

inforem_dc_inbound_vend_pack_file = 
    LOAD '$WORK__IDRP_INFOREM_DC_INBOUND_VEND_PACK_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($WORK__IDRP_INFOREM_DC_INBOUND_VEND_PACK_SCHEMA);
	
gold__item_vendor_package_current_data = 
    LOAD '$GOLD__ITEM_VENDOR_PACKAGE_CURRENT_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($GOLD__ITEM_VENDOR_PACKAGE_CURRENT_SCHEMA);
	
gold__item_kmart_vendor_package_dc_location_data = 
    LOAD '$GOLD__ITEM_KMART_VENDOR_PACKAGE_DC_LOCATION_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($GOLD__ITEM_KMART_VENDOR_PACKAGE_DC_LOCATION_SCHEMA);
	
gold__item_aprk_current_data = 
    LOAD '$GOLD__ITEM_APRK_CURRENT_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($GOLD__ITEM_APRK_CURRENT_SCHEMA);
	
gold__item_kmart_ksn_dc_package_data = 
    LOAD '$GOLD__ITEM_KMART_KSN_DC_PACKAGE_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($GOLD__ITEM_KMART_KSN_DC_PACKAGE_SCHEMA);
	
smith__idrp_eligible_dc_loc_data = 
    LOAD '$WORK__IDRP_DC_LOCN_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($WORK__IDRP_DC_LOCN_SCHEMA);
	
smith__idrp_eligible_item_data = 
    LOAD '$WORK__IDRP_ELIGIBLE_ITEM_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($SMITH__IDRP_ELIGIBLE_ITEM_SCHEMA);
	
smith__idrp_eligible_loc_data = 
    LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' 
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
	AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);
	
----------------------------GENERATING REQUIRED FIELDS FROM INFOREM_DC_INBOUND_VEND_PACK FILE------------------------------

gen_inforem_dc_inbound_vend_pack_file = 
        FOREACH inforem_dc_inbound_vend_pack_file 
	    GENERATE 
                inbnd_item_id AS item,
                inbnd_dc_locn_nbr AS loc,
                inbnd_vend_pack_id AS in_vend_pack_id;
		
----------------------------GENERATING REQUIRED FIELDS FROM gold__item_vendor_package_current table-----------------------

gen_gold__item_vendor_package_current_table = 
        FOREACH gold__item_vendor_package_current_data 
	    GENERATE 						
                vendor_package_id,
                ksn_id,
                flow_type_cd,
                purchase_status_cd AS v_purchase_status_cd,  /*--to avoid clashing of names */
                aprk_id,
                import_cd,
                (import_cd IS NULL ? '0' : (import_cd == 'I' ? '1' : '0')) AS imp_fl,
                vendor_carton_qty,
                vendor_stock_nbr,
                ksn_package_id,
                carton_per_layer_qty,
                layer_per_pallet_qty;
		
-------------------------JOINING FILE AND VEND_PACK TABLE----------------------------------------------------------------

join_infm_dc_inb_ved_pk_fl_vd_pk_tbl = 
        JOIN gen_inforem_dc_inbound_vend_pack_file BY (long)in_vend_pack_id,
	         gen_gold__item_vendor_package_current_table BY (long)vendor_package_id;

		 
gen_join_infm_dc_inb_ved_pk_fl_vd_pk_tbl = 
        FOREACH join_infm_dc_inb_ved_pk_fl_vd_pk_tbl 
	    GENERATE 
                item AS item,
                loc AS loc,
                vendor_package_id AS vendor_package_id,
                ksn_id AS ksn_id,
                flow_type_cd AS flow_type_cd,
                v_purchase_status_cd AS v_purchase_status_cd,
                aprk_id AS aprk_id,
                import_cd AS import_cd,
                imp_fl AS imp_fl,
                vendor_carton_qty AS vendor_carton_qty,
                vendor_stock_nbr AS vendor_stock_nbr,
                ksn_package_id AS ksn_package_id,
                carton_per_layer_qty AS carton_per_layer_qty,
                layer_per_pallet_qty AS layer_per_pallet_qty,
                in_vend_pack_id AS in_vend_pack_id;


gen_join_infm_dc_inb_ved_pk_fl_vd_pk_tbl = DISTINCT gen_join_infm_dc_inb_ved_pk_fl_vd_pk_tbl;	
	
---------------------------------JOINING TO VEND_PACK_DC_LOC-------------------------------------------------------------

gold__item_kmart_vendor_package_dc_location_data = 
        FILTER gold__item_kmart_vendor_package_dc_location_data 
	    BY '$CURRENT_TIMESTAMP_6PM' >= effective_ts AND 
		'$CURRENT_TIMESTAMP_6PM' <= expiration_ts;


gold__item_kmart_vendor_package_dc_location_data = 
    DISTINCT gold__item_kmart_vendor_package_dc_location_data ;


join_infrm_dc_vnd_pk_dc_loc_tbl = 
        JOIN gen_join_infm_dc_inb_ved_pk_fl_vd_pk_tbl BY ((long)in_vend_pack_id,loc), 
             gold__item_kmart_vendor_package_dc_location_data BY ((long)vendor_package_id,location_nbr);

gen_join_infrm_dc_vnd_pk_dc_loc_tbl = 
        FOREACH join_infrm_dc_vnd_pk_dc_loc_tbl 
	    GENERATE 
                item,
                loc,
                gen_join_infm_dc_inb_ved_pk_fl_vd_pk_tbl::vendor_package_id AS vendor_package_id,
                gen_join_infm_dc_inb_ved_pk_fl_vd_pk_tbl::ksn_id AS ksn_id,
                flow_type_cd,
                v_purchase_status_cd,
                aprk_id,
                import_cd,
                imp_fl,
                vendor_carton_qty,
                vendor_stock_nbr,
                ksn_package_id,
                (gold__item_kmart_vendor_package_dc_location_data::carton_per_layer_qty IS NULL OR gold__item_kmart_vendor_package_dc_location_data::layer_per_pallet_qty IS NULL ? gen_join_infm_dc_inb_ved_pk_fl_vd_pk_tbl::carton_per_layer_qty : (chararray)(((long)gold__item_kmart_vendor_package_dc_location_data::carton_per_layer_qty > 0 and (long)gold__item_kmart_vendor_package_dc_location_data::layer_per_pallet_qty > 0) ? gold__item_kmart_vendor_package_dc_location_data::carton_per_layer_qty : gen_join_infm_dc_inb_ved_pk_fl_vd_pk_tbl::carton_per_layer_qty )) AS carton_per_layer_qty,
                (gold__item_kmart_vendor_package_dc_location_data::carton_per_layer_qty IS NULL OR gold__item_kmart_vendor_package_dc_location_data::layer_per_pallet_qty IS NULL ? gen_join_infm_dc_inb_ved_pk_fl_vd_pk_tbl::layer_per_pallet_qty  : (chararray)(((long)gold__item_kmart_vendor_package_dc_location_data::carton_per_layer_qty > 0 and (long)gold__item_kmart_vendor_package_dc_location_data::layer_per_pallet_qty > 0) ? gold__item_kmart_vendor_package_dc_location_data::layer_per_pallet_qty : gen_join_infm_dc_inb_ved_pk_fl_vd_pk_tbl::layer_per_pallet_qty )) AS layer_per_pallet_qty,
                in_vend_pack_id,
                ship_aprk_id,
                inbound_order_uom_cd;


gen_join_infrm_dc_vnd_pk_dc_loc_tbl = DISTINCT gen_join_infrm_dc_vnd_pk_dc_loc_tbl;
	
---------------------JOINING PREVIOUS DATA WITH smith__idrp_eligible_item_table ON ITEM---------------------------

join_item_join_dc_locn_ksn_pk_join_aprk_eql = 
        JOIN gen_join_infrm_dc_vnd_pk_dc_loc_tbl BY item,
	         smith__idrp_eligible_item_data BY item;


gen_join_item_join_dc_locn_ksn_pk_join_aprk_eql = 
        FOREACH join_item_join_dc_locn_ksn_pk_join_aprk_eql 
	    GENERATE 
                gen_join_infrm_dc_vnd_pk_dc_loc_tbl::gen_join_infm_dc_inb_ved_pk_fl_vd_pk_tbl::item AS item,
                loc AS loc,
                vendor_package_id AS vendor_package_id,
                ksn_id AS ksn_id,
                flow_type_cd AS flow_type_cd,
                v_purchase_status_cd AS v_purchase_status_cd,
                aprk_id AS aprk_id,
                import_cd AS import_cd,
                imp_fl AS imp_fl,
                vendor_carton_qty AS vendor_carton_qty,
                vendor_stock_nbr AS vendor_stock_nbr,
                ksn_package_id AS ksn_package_id,
                carton_per_layer_qty AS carton_per_layer_qty,
                layer_per_pallet_qty AS layer_per_pallet_qty,
                in_vend_pack_id AS in_vend_pack_id,
                ship_aprk_id AS ship_aprk_id,
                inbound_order_uom_cd AS inbound_order_uom_cd,
                srs_div_no AS srs_div_no,
                srs_itm_no AS srs_itm_no,
                srs_sku_no AS srs_sku_no,
                itm_purch_sts_cd AS itm_purch_sts_cd,
                cross_mdse_attr_cd AS cross_mdse_attr_cd;

---------------------select all Ship Duns and their related order aprk id and duns nbrs---------------------------------

filter_aprk_curr_tbl_ship_aprk_type_cd = 
        FILTER gold__item_aprk_current_data 
	    BY aprk_type_cd == 'SHIP' AND 
		related_aprk_type_cd == 'ORD';

----------------------select all Order duns and their related pay duns--------------------------------------------------

filter_aprk_curr_tbl_ord_aprk_type_cd = 
        FILTER gold__item_aprk_current_data 
	    BY aprk_type_cd == 'ORD' AND 
		related_aprk_type_cd == 'PAY';

----------------------join ship duns to order duns to create ship to order to Pay cross reference-----------------------

join_aprk_ship_to_pay_xref = 
    JOIN filter_aprk_curr_tbl_ship_aprk_type_cd BY related_aprk_id,
         filter_aprk_curr_tbl_ord_aprk_type_cd BY aprk_id;


gen_aprk_ship_to_pay_xref = 
        FOREACH join_aprk_ship_to_pay_xref 
	    GENERATE
                filter_aprk_curr_tbl_ship_aprk_type_cd::aprk_id AS xref_ship_aprk_id  ,              
                filter_aprk_curr_tbl_ship_aprk_type_cd::duns_nbr AS xref_ship_duns_nbr,
                filter_aprk_curr_tbl_ord_aprk_type_cd::duns_nbr AS xref_ord_duns_nbr,
                filter_aprk_curr_tbl_ord_aprk_type_cd::related_duns_nbr AS xref_pay_duns_nbr;

--------------------------join to ship aprk id to ship-to-pay xref table------------------------------------------------

join_ship_aprk_id_equal = 
        JOIN gen_join_item_join_dc_locn_ksn_pk_join_aprk_eql BY ship_aprk_id, 
	         gen_aprk_ship_to_pay_xref BY xref_ship_aprk_id;
			 
------------------------------------------------------------------------------------------------------------------------

generate_ship_aprk_id_equal = 
        FOREACH join_ship_aprk_id_equal
	    GENERATE
	            item AS item,
                loc AS loc,
                vendor_package_id AS vendor_package_id,
                ksn_id AS ksn_id,
                flow_type_cd AS flow_type_cd,
                v_purchase_status_cd AS v_purchase_status_cd,
                aprk_id AS aprk_id,
                import_cd AS import_cd,
                imp_fl AS imp_fl,
                vendor_carton_qty AS vendor_carton_qty,
                vendor_stock_nbr AS vendor_stock_nbr,
                ksn_package_id AS ksn_package_id,
                carton_per_layer_qty AS carton_per_layer_qty,
                layer_per_pallet_qty AS layer_per_pallet_qty,
                in_vend_pack_id AS in_vend_pack_id,
                ship_aprk_id AS ship_aprk_id,
                inbound_order_uom_cd AS inbound_order_uom_cd,
                (imp_fl == '0' ? (cross_mdse_attr_cd IS NULL ? CONCAT(xref_ship_duns_nbr,'_S') : (cross_mdse_attr_cd == 'SK1400' AND (long)xref_pay_duns_nbr == 374392 ? '447' : CONCAT(xref_ship_duns_nbr,'_S'))) : (loc == '8277' ? CONCAT(xref_ship_duns_nbr,'_S') : '8277' ) ) AS src_loc,
                srs_div_no AS srs_div_no,
                srs_itm_no AS srs_itm_no,
                srs_sku_no AS srs_sku_no,
                itm_purch_sts_cd AS itm_purch_sts_cd,
                cross_mdse_attr_cd AS cross_mdse_attr_cd;
-------------------------------------------------------------------------------
 smith__idrp_eligible_loc_data_gen_loc_lvl_cd = 
     FOREACH  smith__idrp_eligible_loc_data
	 GENERATE 
	      loc AS loc,
		  loc_lvl_cd AS loc_lvl_cd;
		  
 smith__idrp_eligible_loc_data_filter_on_loc_lvl_cd = 
     FILTER smith__idrp_eligible_loc_data_gen_loc_lvl_cd 
	 BY loc_lvl_cd == 'VENDOR';
	 
join_with_item_loc_on_src_loc = 
    JOIN  generate_ship_aprk_id_equal BY src_loc,
	      smith__idrp_eligible_loc_data_filter_on_loc_lvl_cd BY loc;
		  
gen_join_with_item_loc_on_src_loc = 
    FOREACH join_with_item_loc_on_src_loc
    GENERATE
        generate_ship_aprk_id_equal::item AS item,
        generate_ship_aprk_id_equal::loc AS loc,
        generate_ship_aprk_id_equal::src_loc AS src_loc;
join_again_with_item_loc = 
    JOIN generate_ship_aprk_id_equal BY (item,src_loc) 
	     LEFT OUTER ,
		 gen_join_with_item_loc_on_src_loc BY (item,loc);

generate_po_vnd_no = 
    FOREACH join_again_with_item_loc
	GENERATE
        generate_ship_aprk_id_equal::item AS item,
        generate_ship_aprk_id_equal::loc AS loc,
        vendor_package_id AS vendor_package_id,
        ksn_id AS ksn_id,
        flow_type_cd AS flow_type_cd,
        v_purchase_status_cd AS v_purchase_status_cd,
        aprk_id AS aprk_id,
        import_cd AS import_cd,
        imp_fl AS imp_fl,
        vendor_carton_qty AS vendor_carton_qty,
        vendor_stock_nbr AS vendor_stock_nbr,
        ksn_package_id AS ksn_package_id,
        carton_per_layer_qty AS carton_per_layer_qty,
        layer_per_pallet_qty AS layer_per_pallet_qty,
        in_vend_pack_id AS in_vend_pack_id,
        ship_aprk_id AS ship_aprk_id,
        inbound_order_uom_cd AS inbound_order_uom_cd,
        generate_ship_aprk_id_equal::src_loc AS src_loc,
        (gen_join_with_item_loc_on_src_loc::src_loc IS NULL ? generate_ship_aprk_id_equal::src_loc : gen_join_with_item_loc_on_src_loc::src_loc) AS po_vnd_no,
        srs_div_no AS srs_div_no,
        srs_itm_no AS srs_itm_no,
        srs_sku_no AS srs_sku_no,
        itm_purch_sts_cd AS itm_purch_sts_cd,
        cross_mdse_attr_cd AS cross_mdse_attr_cd;	
	     

-------------------------------------------------------------------------------
---------------------JOINING PREVIOUS DATA WITH gold__item_kmart_ksn_dc_package_table--------------------------
gold__item_kmart_ksn_dc_package_data = 
    FILTER gold__item_kmart_ksn_dc_package_data 
	       BY '$CURRENT_TIMESTAMP_6PM' >= effective_ts AND '$CURRENT_TIMESTAMP_6PM' <= expiration_ts;


join_ksn_dc_pck_join_aprk_eql = 
        JOIN gold__item_kmart_ksn_dc_package_data BY  (ksn_package_id,location_nbr),
	     generate_po_vnd_no BY (ksn_package_id,loc);


gen_join_ksn_dc_pck_join_aprk_eql = 
        FOREACH join_ksn_dc_pck_join_aprk_eql 
	    GENERATE
                item AS item,
                loc AS loc,
                vendor_package_id AS vendor_package_id,
                generate_po_vnd_no::ksn_id AS ksn_id,
                flow_type_cd AS flow_type_cd,
                v_purchase_status_cd AS v_purchase_status_cd,
                aprk_id AS aprk_id,
                import_cd AS import_cd,
                imp_fl AS imp_fl,
                generate_po_vnd_no::vendor_carton_qty AS vendor_carton_qty,
                vendor_stock_nbr AS vendor_stock_nbr,
                gold__item_kmart_ksn_dc_package_data::ksn_package_id AS ksn_package_id,
                carton_per_layer_qty AS carton_per_layer_qty,
                layer_per_pallet_qty AS layer_per_pallet_qty,
                in_vend_pack_id AS in_vend_pack_id,
                ship_aprk_id AS ship_aprk_id,
                inbound_order_uom_cd AS inbound_order_uom_cd,
                src_loc AS src_loc,
                po_vnd_no AS po_vnd_no,
                purchase_status_cd AS purchase_status_cd,
                stocked_ind AS stocked_ind,
                outbound_package_qty AS outbound_package_qty,
                srs_div_no AS srs_div_no,
                srs_itm_no AS srs_itm_no,
                srs_sku_no AS srs_sku_no,
                itm_purch_sts_cd AS itm_purch_sts_cd,
                cross_mdse_attr_cd AS cross_mdse_attr_cd;

---------------------JOINING PREVIOUS DATA WIH smith__idrp_eligible_dc_loc_table---------------------------

join_dc_locn_ksn_pk_join_aprk_eql = 
        JOIN smith__idrp_eligible_dc_loc_data BY dc_locn_nbr,
	         gen_join_ksn_dc_pck_join_aprk_eql BY loc;


gen_join_dc_locn_ksn_pk_join_aprk_eql = 
        FOREACH join_dc_locn_ksn_pk_join_aprk_eql 
	    GENERATE 
                item AS item,
                loc AS loc,
                vendor_package_id AS vendor_package_id,
                ksn_id AS ksn_id,
                flow_type_cd AS flow_type_cd,
                v_purchase_status_cd AS v_purchase_status_cd,
                aprk_id AS aprk_id,
                import_cd AS import_cd,
                imp_fl AS imp_fl,
                vendor_carton_qty AS vendor_carton_qty,
                vendor_stock_nbr AS vendor_stock_nbr,
                ksn_package_id AS ksn_package_id,
                carton_per_layer_qty AS carton_per_layer_qty,
                layer_per_pallet_qty AS layer_per_pallet_qty,
                in_vend_pack_id AS in_vend_pack_id,
                ship_aprk_id AS ship_aprk_id,
                inbound_order_uom_cd AS inbound_order_uom_cd,
                src_loc AS src_loc,
                po_vnd_no AS po_vnd_no,
                purchase_status_cd AS purchase_status_cd,
                stocked_ind AS stocked_ind,
                outbound_package_qty AS outbound_package_qty,
                enable_jif_dc_ind AS enable_jif_dc_ind,
                srs_div_no AS srs_div_no,
                srs_itm_no AS srs_itm_no,
                srs_sku_no AS srs_sku_no,
                itm_purch_sts_cd AS itm_purch_sts_cd,
                cross_mdse_attr_cd AS cross_mdse_attr_cd;
		
-------------------JOINING PREVIOUS DATA WITH smith__idrp_eligible_loc_table AND APPLYING TRANSFORMATION LOGIC--------------------

join_loc_gen_join_item_dc_locn_ksn_pk_aprk_eql = 
        JOIN smith__idrp_eligible_loc_data BY loc,
	         gen_join_dc_locn_ksn_pk_join_aprk_eql BY loc;


join_loc_gen_join_item_dc_locn_ksn_pk_aprk_eql = 
        FOREACH join_loc_gen_join_item_dc_locn_ksn_pk_aprk_eql 
	    GENERATE 
                item AS item,
                gen_join_dc_locn_ksn_pk_join_aprk_eql::loc AS loc,
                vendor_package_id AS vendor_package_id,
                ksn_id AS ksn_id,
                stocked_ind AS stocked_ind,
                flow_type_cd AS flow_type_cd,
                v_purchase_status_cd AS v_purchase_status_cd,
                aprk_id AS aprk_id,
                import_cd AS import_cd,
                imp_fl AS imp_fl,
                vendor_carton_qty AS vendor_carton_qty,
                vendor_stock_nbr AS vendor_stock_nbr,
                ksn_package_id AS ksn_package_id,
                carton_per_layer_qty AS carton_per_layer_qty,
                layer_per_pallet_qty AS layer_per_pallet_qty,
                in_vend_pack_id AS in_vend_pack_id,
                ship_aprk_id AS ship_aprk_id,
                inbound_order_uom_cd AS inbound_order_uom_cd,
                src_loc AS src_loc,
                po_vnd_no AS po_vnd_no,
                purchase_status_cd AS purchase_status_cd,
                outbound_package_qty AS outbound_package_qty,
                enable_jif_dc_ind AS enable_jif_dc_ind,
                srs_div_no AS srs_div_no,
                srs_itm_no AS srs_itm_no,
                srs_sku_no AS srs_sku_no,
                itm_purch_sts_cd AS itm_purch_sts_cd,
                cross_mdse_attr_cd AS cross_mdse_attr_cd,
                srs_loc AS srs_loc;


join_loc_gen_join_item_dc_locn_ksn_pk_aprk_eql_dist = DISTINCT join_loc_gen_join_item_dc_locn_ksn_pk_aprk_eql;

-----------------------------------------EXTRACTING THE REQUIRED COLUMNS----------------------------------------------

gen_join_loc_gen_join_item_dc_locn_ksn_pk_aprk_eql = 
        FOREACH join_loc_gen_join_item_dc_locn_ksn_pk_aprk_eql_dist
	    GENERATE
                item AS item,
                loc AS loc,
                vendor_package_id AS vendor_package_id,
                ksn_id AS ksn_id,
                stocked_ind AS stocked_ind,
                flow_type_cd AS flow_type_cd,
                (flow_type_cd IS NULL ? '' : (flow_type_cd == 'DSDS' ? 'DSD' : (stocked_ind IS NULL ? '' : (flow_type_cd == 'DC' AND stocked_ind == 'Y' ? 'STK' : (flow_type_cd == 'VCDC' AND stocked_ind == 'Y' ? 'STK' : (enable_jif_dc_ind IS NULL ? '' : (flow_type_cd == 'DC' AND stocked_ind == 'N' AND enable_jif_dc_ind == 'N' ? 'FLT' : (flow_type_cd == 'DC' AND stocked_ind == 'N' AND enable_jif_dc_ind == 'Y' ? 'JIF' : (flow_type_cd == 'VCDC' AND stocked_ind == 'N' AND enable_jif_dc_ind == 'N' ? 'FLT' : (flow_type_cd == 'VCDC' AND stocked_ind == 'N' AND enable_jif_dc_ind == 'Y' ? 'JIF' : ''))))))))))  AS dc_config_cd,
                v_purchase_status_cd AS vend_pack_purch_stat_cd,
                aprk_id AS aprk_id,
                import_cd AS import_cd,
                imp_fl AS imp_fl,
                vendor_carton_qty AS vendor_carton_qty,
                vendor_stock_nbr AS vendor_stock_nbr,
                ksn_package_id AS ksn_package_id,
                carton_per_layer_qty AS carton_per_layer_qty,
                layer_per_pallet_qty AS layer_per_pallet_qty,
                in_vend_pack_id AS vend_pack_id,
                ship_aprk_id AS ship_aprk_id,
                inbound_order_uom_cd AS inbound_order_uom_cd,
                src_loc AS src_loc,
                po_vnd_no AS po_vnd_no,
                purchase_status_cd AS purchase_status_cd,
                outbound_package_qty AS outbound_package_qty,
                (inbound_order_uom_cd IS NULL ? vendor_carton_qty : (inbound_order_uom_cd == 'CRTN' ? vendor_carton_qty : (inbound_order_uom_cd == 'LAYR' ? (chararray)((int)vendor_carton_qty * (int) carton_per_layer_qty) : (inbound_order_uom_cd == 'PALL' ? (chararray)((int)vendor_carton_qty * (int)carton_per_layer_qty * (int)layer_per_pallet_qty) : '1') ))) AS src_pack_qty,
                enable_jif_dc_ind AS enable_jif_dc_ind,
                srs_div_no AS srs_div_no,
                srs_itm_no AS srs_itm_no,
                srs_sku_no AS srs_sku_no,
                itm_purch_sts_cd AS purch_stat_cd,
                cross_mdse_attr_cd AS cross_mdse_attr_cd,
                TrimLeadingZeros(srs_loc) AS srs_location_nbr,
                '' AS dotcom_order_indicator,
                '' AS elig_sts_cd,
                '' AS retail_crtn_intrnl_pack_qty,
                '' AS days_to_check_begin_date,
                '' AS days_to_check_end_date,
                '' AS reorder_method_code,
                '' AS str_supplier_cd,
                'K' AS  src_owner_cd,
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
                '' AS  stk_typ_cd;
        
--------------------GENERATING REQUIRED COLUMNS FOR smith__idrp_eligible_item_loc TABLE---------------------------------------

smith__idrp_eligible_item_loc = 
    FOREACH gen_join_loc_gen_join_item_dc_locn_ksn_pk_aprk_eql 
	GENERATE
	    '$CURRENT_TIMESTAMP' AS load_ts,
        item,
        loc,
        'K' AS  src_owner_cd,
        'A' AS elig_sts_cd,
        (src_pack_qty IS NULL ? '1' : src_pack_qty) AS src_pack_qty,
        src_loc,
        po_vnd_no,
        ksn_id,
        purch_stat_cd,
        '' AS retail_crtn_intrnl_pack_qty,
        '' AS days_to_check_begin_date,
        '' AS days_to_check_end_date,
        '' AS dotcom_order_indicator,
        vend_pack_id,
        vend_pack_purch_stat_cd,
        flow_type_cd AS vendor_pack_flow_type,
        vendor_carton_qty AS vendor_pack_qty,
        '' AS reorder_method_code,
        '' AS str_supplier_cd,
        vendor_stock_nbr AS vend_stk_nbr,
        ksn_package_id AS ksn_pack_id,
        purchase_status_cd AS ksn_dc_pack_purch_stat_cd,
        inbound_order_uom_cd AS inbnd_ord_uom_cd,
        enable_jif_dc_ind,
        stocked_ind AS stk_ind,
        carton_per_layer_qty AS crtn_per_layer_qty,
        layer_per_pallet_qty AS layer_per_pall_qty,
        dc_config_cd,
        imp_fl,
        srs_div_no AS srs_division_nbr,
        srs_itm_no AS srs_item_nbr,
        srs_sku_no AS srs_sku_cd,
        srs_location_nbr,
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
        '' AS  stk_typ_cd,
		'$batchid' AS batch_id;
		
smith__idrp_eligible_item_loc_dist = DISTINCT smith__idrp_eligible_item_loc;
	
------------------------------------------------STOREING DATA--------------------------------------------------------

STORE smith__idrp_eligible_item_loc_dist 
INTO '$WORK__IDRP_ELIGIBLE_ITEM_LOC_KMART_DC_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

---------------------------------------------------------------------------------------------------------------------



/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/

