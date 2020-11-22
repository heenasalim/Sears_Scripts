/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_work__idrp_candidate_sears_warehouse.pig
# AUTHOR NAME:         Neera Singh
# CREATION DATE:       Wed Jun 25 09:37:58 EST 2014
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
#        DATE         BY             MODIFICATION
#		22-10-2014   Priyanka Gurjar CR3205 – eliminate easy order filter
#                    Sushauvik Deb   CR3517 modify minimum vendor and warehouse sourcing to use item type to determine whether to use DOS or SRIM source.
#		28-08-2015	 Meghana D		 CR 5079 Modify the candidate warehouse and candidate store logic to use the gold__item_aprk_current table to determine which items are Import 
#		04-09-2015	 Priyanka Gurjar CR 5120 Allow TPW locations to be active for EMP and RSOS items 
#		11-09-2015	 Meghana D       CR 5138 Modify the candidate warehouse logic to capture the on-hand, on-order qtys and extract date from the SRIM data to determine active_ind
#		30-08-2016	Pankaj Gupta	 IPS -700 Increased reducer to 99
#		01/19/2017	Srujan Dussa	 IPS-779 . Adding rim_last_record_create_dt from gold__inventory_rim_daily_current to be included in the Extract File to Shared Items.
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

SET default_parallel 99;

--register the jar containing all PIG UDFs
REGISTER $UDF_JAR;

--trim spaces around string
DEFINE TRIM_STRING $TRIM_STRING ;

--trim leading zeros
DEFINE TRIM_INTEGER $TRIM_INTEGER ;

--trim leading and trailing zeros
DEFINE TRIM_DECIMAL $TRIM_DECIMAL ;

DEFINE TrimLeadingZeros com.searshc.supplychain.idrp.udf.TrimLeadingZeros();
DEFINE CompareTwoDates com.searshc.supplychain.idrp.udf.CompareTwoDates();
DEFINE AddDays com.searshc.supplychain.idrp.udf.AddOrRemoveDaysToDate();

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

/*********Loading gold__inventory_sears_dc_item_facility_current*****************************************************************/

gold__inventory_sears_dc_item_facility_current = 
        LOAD '$GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT_LOCATION'
        USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
        $GOLD__INVENTORY_SEARS_DC_ITEM_FACILITY_CURRENT_SCHEMA
       );


/*********Loading gold__inventory_dos_code_category_current*****************************************************************************/

gold__inventory_dos_code_category_current = 
        LOAD '$GOLD__INVENTORY_DOS_CODE_CATEGORY_CURRENT_LOCATION'
        USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS(
        $GOLD__INVENTORY_DOS_CODE_CATEGORY_CURRENT_SCHEMA
          );


/********Loading smith__idrp_ksn_attribute_current *********************************************************************************/


smith__idrp_ksn_attribute_current  = 
	LOAD '$SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS(
	 $SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_SCHEMA
	  );



/*******Loading work__idrp_sears_location_xref*************************************************************************************/

work__idrp_sears_location_xref = 
	LOAD '$WORK__IDRP_SEARS_LOCATION_XREF_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS(
	  $WORK__IDRP_SEARS_LOCATION_XREF_SCHEMA
	  );

gen_work__idrp_sears_location_xref = 
	FOREACH
	work__idrp_sears_location_xref
	GENERATE
	location_id,
	TrimLeadingZeros(sears_location_id) AS sears_location_id,
	location_level_cd,
	location_owner_cd,
	location_format_type_cd;


/*******Loading gold__inventory_sears_dc_item_owner_current**************************************************************************/

gold__inventory_sears_dc_item_owner_current = LOAD 
	'$GOLD__INVENTORY_SEARS_DC_ITEM_OWNER_CURRENT_LOCATION'
	 USING PigStorage('$FIELD_DELIMITER_TAB')
         AS(
	 $GOLD__INVENTORY_SEARS_DC_ITEM_OWNER_CURRENT_SCHEMA
	   );

gen_gold__inventory_sears_dc_item_owner_current = 
	FOREACH
	gold__inventory_sears_dc_item_owner_current
	GENERATE
	dos_division_nbr,
	sears_item_nbr,
	dos_sku_cd,
	dos_warehouse_nbr,
	corporate_cd,
	owner_cd,
	item_active_ind,
	item_next_period_on_hand_qty,
	item_on_order_qty,
	item_reserve_qty,
	item_back_order_qty,
	item_next_period_future_order_qty,
	item_next_period_in_transit_qty,
	item_last_receive_dt,
	item_last_ship_dt,
	item_reserve_cd,
	sears_sku_nbr;

/**********loading and generating gold__inventory_srim_daily*********************************************************************/


gold__inventory_srim_daily = LOAD
	'$GOLD__INVENTORY_SRIM_DAILY_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
	AS(
	$GOLD__INVENTORY_SRIM_DAILY_SCHEMA
	  );


gen_gold__inventory_srim_daily = 
	FOREACH
	gold__inventory_srim_daily
	GENERATE
	division_nbr,
	TrimLeadingZeros(item_nbr) AS item_nbr,
	sku_nbr,
	TrimLeadingZeros(warehouse_nbr) AS warehouse_nbr,
	source_nbr,
	status_cd,
	store_pack_size_qty,
	on_hand_qty,
	regular_on_order_qty,
	promo_on_order_qty,
	last_on_hand_adjustment_dt,
	extract_dt,
        last_record_creation_dt AS rim_last_record_creation_dt;
		
--CR 5079
	
/*********Loading gold__item_aprk_current*************************************/

gold__item_aprk_current = 
	LOAD '$GOLD__ITEM_APRK_CURRENT_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
	AS ($GOLD__ITEM_APRK_CURRENT_SCHEMA);

gen_gold__item_aprk_current = 
	FOREACH gold__item_aprk_current
	GENERATE
	duns_nbr,
	aprk_type_cd,
	duns_owner_cd,
	import_ind;
	
gold__item_aprk_current_filter = 
	FILTER gen_gold__item_aprk_current 
	BY TRIM(aprk_type_cd) == 'ORD' AND
	   TRIM(duns_owner_cd) == 'S' AND
	   TRIM(import_ind) == 'Y';
	
work__aprk_import_vendors = 
	FOREACH gold__item_aprk_current_filter
	GENERATE
		SUBSTRING(duns_nbr, 1, 10) AS sears_vendor_nbr,
		'1' AS import_ind;		

/********generating gold__inventory_sears_dc_item_facility_current****************************************************************/

work__idrp_candidate_sears_warehouse_step1b = 
        FOREACH
        gold__inventory_sears_dc_item_facility_current
        GENERATE
	sears_division_nbr,
	sears_item_nbr,
	sears_sku_nbr,
	dos_division_nbr,
	dos_sku_cd,
	dos_warehouse_nbr,
	corporate_cd,
	non_stock_source_cd,
	vendor_nbr AS dos_original_source_nbr,
	vendor_nbr AS dos_source_nbr,
        ((TRIM(flash_cd)=='' )?'00001':flash_cd)AS dos_source_package_qty;


work__idrp_candidate_sears_warehouse_step2 = 
        FILTER
        gen_gold__inventory_sears_dc_item_owner_current
        BY(TRIM(corporate_cd)=='01'
		AND
	   TRIM(dos_division_nbr)!='605'
		AND
	   TRIM(owner_cd)=='0490R');


join_candidate_sears_warehouse_step1b_step2 = 
        JOIN
        work__idrp_candidate_sears_warehouse_step1b
        BY(TrimLeadingZeros(dos_division_nbr), TrimLeadingZeros(sears_item_nbr), sears_sku_nbr, dos_warehouse_nbr, TRIM(corporate_cd)),
        work__idrp_candidate_sears_warehouse_step2
        BY(TrimLeadingZeros(dos_division_nbr), TrimLeadingZeros(sears_item_nbr), sears_sku_nbr, dos_warehouse_nbr, TRIM(corporate_cd));

work__idrp_candidate_sears_warehouse_step3 = 
	FOREACH
	join_candidate_sears_warehouse_step1b_step2
	GENERATE
	work__idrp_candidate_sears_warehouse_step1b::sears_division_nbr AS sears_division_nbr,
	TrimLeadingZeros(work__idrp_candidate_sears_warehouse_step1b::sears_item_nbr) AS sears_item_nbr,
	work__idrp_candidate_sears_warehouse_step1b::sears_sku_nbr AS sears_sku_nbr,
	work__idrp_candidate_sears_warehouse_step1b::dos_division_nbr AS dos_division_nbr,
	work__idrp_candidate_sears_warehouse_step1b::dos_sku_cd AS dos_sku_cd,
	TrimLeadingZeros(work__idrp_candidate_sears_warehouse_step1b::dos_warehouse_nbr) AS dos_warehouse_nbr,
	work__idrp_candidate_sears_warehouse_step1b::dos_original_source_nbr AS dos_original_source_nbr,
	work__idrp_candidate_sears_warehouse_step1b::corporate_cd AS corporate_cd,
	work__idrp_candidate_sears_warehouse_step1b::non_stock_source_cd AS non_stock_source_cd,
	work__idrp_candidate_sears_warehouse_step1b::dos_source_nbr AS dos_source_nbr,
    work__idrp_candidate_sears_warehouse_step1b::dos_source_package_qty AS dos_source_package_qty,
	work__idrp_candidate_sears_warehouse_step2::owner_cd AS owner_cd,
	work__idrp_candidate_sears_warehouse_step2::item_active_ind AS item_active_ind,
	work__idrp_candidate_sears_warehouse_step2::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
	work__idrp_candidate_sears_warehouse_step2::item_on_order_qty AS item_on_order_qty,
	work__idrp_candidate_sears_warehouse_step2::item_reserve_qty AS item_reserve_qty,
	work__idrp_candidate_sears_warehouse_step2::item_back_order_qty AS item_back_order_qty,
	work__idrp_candidate_sears_warehouse_step2::item_next_period_future_order_qty AS item_next_period_future_order_qty,
	work__idrp_candidate_sears_warehouse_step2::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
	work__idrp_candidate_sears_warehouse_step2::item_last_receive_dt AS item_last_receive_dt,
	work__idrp_candidate_sears_warehouse_step2::item_last_ship_dt AS item_last_ship_dt,
	work__idrp_candidate_sears_warehouse_step2::item_reserve_cd AS item_reserve_cd;


join_inventory_srim_daily_warehouse_step3 = 
	JOIN
	work__idrp_candidate_sears_warehouse_step3
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr), TrimLeadingZeros(dos_warehouse_nbr))
	FULL OUTER,
	gen_gold__inventory_srim_daily
        BY(TrimLeadingZeros(division_nbr), TrimLeadingZeros(item_nbr), TrimLeadingZeros(sku_nbr), TrimLeadingZeros(warehouse_nbr));

gen_inventory_srim_daily_warehouse_step3_dos_dc = 
	FOREACH
	join_inventory_srim_daily_warehouse_step3
	GENERATE
	(work__idrp_candidate_sears_warehouse_step3::sears_division_nbr IS NOT NULL?
	 work__idrp_candidate_sears_warehouse_step3::sears_division_nbr: 
	 gen_gold__inventory_srim_daily::division_nbr) AS sears_division_nbr,
	(work__idrp_candidate_sears_warehouse_step3::sears_item_nbr IS NOT NULL?
	 work__idrp_candidate_sears_warehouse_step3::sears_item_nbr:
	 gen_gold__inventory_srim_daily::item_nbr) AS sears_item_nbr,
	(work__idrp_candidate_sears_warehouse_step3::sears_sku_nbr IS NOT NULL?
	 work__idrp_candidate_sears_warehouse_step3::sears_sku_nbr:
	 gen_gold__inventory_srim_daily::sku_nbr) AS sears_sku_nbr,
	(work__idrp_candidate_sears_warehouse_step3::dos_warehouse_nbr IS NOT NULL?
	 work__idrp_candidate_sears_warehouse_step3::dos_warehouse_nbr:
	 gen_gold__inventory_srim_daily::warehouse_nbr) AS sears_location_id,
	work__idrp_candidate_sears_warehouse_step3::dos_original_source_nbr AS dos_original_source_nbr,
	work__idrp_candidate_sears_warehouse_step3::dos_source_nbr AS dos_source_nbr,
        work__idrp_candidate_sears_warehouse_step3::dos_source_package_qty AS dos_source_package_qty,
	gen_gold__inventory_srim_daily::source_nbr AS srim_source_nbr,
	gen_gold__inventory_srim_daily::status_cd AS srim_status_cd,
	(gen_gold__inventory_srim_daily::store_pack_size_qty IS NOT NULL?
	 gen_gold__inventory_srim_daily::store_pack_size_qty : 1) AS srim_source_package_qty,
	work__idrp_candidate_sears_warehouse_step3::item_active_ind AS item_active_ind,
	work__idrp_candidate_sears_warehouse_step3::item_reserve_cd AS item_reserve_cd,
	work__idrp_candidate_sears_warehouse_step3::non_stock_source_cd AS non_stock_source_cd,
	work__idrp_candidate_sears_warehouse_step3::owner_cd AS owner_cd,
	work__idrp_candidate_sears_warehouse_step3::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
	work__idrp_candidate_sears_warehouse_step3::item_on_order_qty AS item_on_order_qty,
	work__idrp_candidate_sears_warehouse_step3::item_reserve_qty AS item_reserve_qty,
	work__idrp_candidate_sears_warehouse_step3::item_back_order_qty AS item_back_order_qty,
	work__idrp_candidate_sears_warehouse_step3::item_next_period_future_order_qty AS item_next_period_future_order_qty,
	work__idrp_candidate_sears_warehouse_step3::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
	work__idrp_candidate_sears_warehouse_step3::item_last_receive_dt AS item_last_receive_dt,
	work__idrp_candidate_sears_warehouse_step3::item_last_ship_dt AS item_last_ship_dt,
	gen_gold__inventory_srim_daily::on_hand_qty AS srim_on_hand_qty,
	gen_gold__inventory_srim_daily::regular_on_order_qty AS srim_regular_on_order_qty,
	gen_gold__inventory_srim_daily::promo_on_order_qty AS srim_promo_on_order_qty,
	gen_gold__inventory_srim_daily::last_on_hand_adjustment_dt AS srim_last_on_hand_adj_dt,
	gen_gold__inventory_srim_daily::rim_last_record_creation_dt AS rim_last_record_creation_dt;

work__idrp_candidate_sears_warehouse_step4 = 
	FOREACH
	gen_inventory_srim_daily_warehouse_step3_dos_dc
	GENERATE
	sears_division_nbr,
	TrimLeadingZeros(sears_item_nbr) AS sears_item_nbr,
	sears_sku_nbr,
	sears_location_id,
	dos_original_source_nbr,
	dos_source_nbr,
        dos_source_package_qty,
	srim_source_nbr,
	srim_status_cd,
	srim_source_package_qty,
	item_active_ind,
	item_reserve_cd,
	non_stock_source_cd,
	owner_cd,
	item_next_period_on_hand_qty,
	item_on_order_qty,
	item_reserve_qty,
	item_back_order_qty,
	item_next_period_future_order_qty,
	item_next_period_in_transit_qty,
	item_last_receive_dt,
	item_last_ship_dt,
	srim_on_hand_qty,
	srim_regular_on_order_qty,
	srim_promo_on_order_qty,
	srim_last_on_hand_adj_dt,
	rim_last_record_creation_dt;


join_candidate_sears_warehouse_step4_ksn_attribute_current = 
	JOIN
	work__idrp_candidate_sears_warehouse_step4
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr)),
	smith__idrp_ksn_attribute_current
	BY(TrimLeadingZeros(sears_division_nbr), TrimLeadingZeros(sears_item_nbr), TrimLeadingZeros(sears_sku_nbr));


work__idrp_candidate_sears_warehouse_step5 = 
	FOREACH
	join_candidate_sears_warehouse_step4_ksn_attribute_current
	GENERATE
	work__idrp_candidate_sears_warehouse_step4::sears_division_nbr AS sears_division_nbr,
	work__idrp_candidate_sears_warehouse_step4::sears_item_nbr AS sears_item_nbr,
	work__idrp_candidate_sears_warehouse_step4::sears_sku_nbr AS sears_sku_nbr,
	TrimLeadingZeros(work__idrp_candidate_sears_warehouse_step4::sears_location_id) AS sears_location_id,
	work__idrp_candidate_sears_warehouse_step4::dos_original_source_nbr AS dos_original_source_nbr,
	work__idrp_candidate_sears_warehouse_step4::dos_source_nbr AS dos_source_nbr,
        work__idrp_candidate_sears_warehouse_step4::dos_source_package_qty AS dos_source_package_qty,
	work__idrp_candidate_sears_warehouse_step4::srim_source_nbr AS srim_source_nbr,
	work__idrp_candidate_sears_warehouse_step4::srim_status_cd AS srim_status_cd,
	work__idrp_candidate_sears_warehouse_step4::srim_source_package_qty AS srim_source_package_qty,
	work__idrp_candidate_sears_warehouse_step4::item_active_ind AS item_active_ind,
	work__idrp_candidate_sears_warehouse_step4::item_reserve_cd AS item_reserve_cd,
	work__idrp_candidate_sears_warehouse_step4::non_stock_source_cd AS non_stock_source_cd,
	work__idrp_candidate_sears_warehouse_step4::owner_cd AS owner_cd,
	work__idrp_candidate_sears_warehouse_step4::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
	work__idrp_candidate_sears_warehouse_step4::item_on_order_qty AS item_on_order_qty,
	work__idrp_candidate_sears_warehouse_step4::item_reserve_qty AS item_reserve_qty,
	work__idrp_candidate_sears_warehouse_step4::item_back_order_qty AS item_back_order_qty,
	work__idrp_candidate_sears_warehouse_step4::item_next_period_future_order_qty AS item_next_period_future_order_qty,
	work__idrp_candidate_sears_warehouse_step4::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
	work__idrp_candidate_sears_warehouse_step4::item_last_receive_dt AS item_last_receive_dt,
	work__idrp_candidate_sears_warehouse_step4::item_last_ship_dt AS item_last_ship_dt,
	work__idrp_candidate_sears_warehouse_step4::srim_on_hand_qty AS srim_on_hand_qty,
	work__idrp_candidate_sears_warehouse_step4::srim_regular_on_order_qty AS srim_regular_on_order_qty,
	work__idrp_candidate_sears_warehouse_step4::srim_promo_on_order_qty AS srim_promo_on_order_qty,
	work__idrp_candidate_sears_warehouse_step4::srim_last_on_hand_adj_dt AS srim_last_on_hand_adj_dt,
	smith__idrp_ksn_attribute_current::ksn_id AS ksn_id,
	smith__idrp_ksn_attribute_current::shc_item_id AS shc_item_id,
	smith__idrp_ksn_attribute_current::special_retail_order_system_ind AS special_retail_order_system_ind,
	smith__idrp_ksn_attribute_current::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
	smith__idrp_ksn_attribute_current::dot_com_allocation_ind AS dot_com_allocation_ind,
	smith__idrp_ksn_attribute_current::distribution_type_cd AS distribution_type_cd,
	smith__idrp_ksn_attribute_current::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
	smith__idrp_ksn_attribute_current::special_order_candidate_ind AS special_order_candidate_ind,
	smith__idrp_ksn_attribute_current::item_emp_ind AS item_emp_ind,
	smith__idrp_ksn_attribute_current::easy_order_ind AS easy_order_ind,
	smith__idrp_ksn_attribute_current::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
	smith__idrp_ksn_attribute_current::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
	smith__idrp_ksn_attribute_current::rapid_item_ind AS rapid_item_ind,
	smith__idrp_ksn_attribute_current::constrained_item_ind AS constrained_item_ind,
	smith__idrp_ksn_attribute_current::sears_import_ind AS sears_import_ind,
	TRIM(smith__idrp_ksn_attribute_current::idrp_item_type_desc) AS idrp_item_type_desc,
	smith__idrp_ksn_attribute_current::sams_migration_ind AS sams_migration_ind,
	smith__idrp_ksn_attribute_current::emp_to_jit_ind AS emp_to_jit_ind,
	smith__idrp_ksn_attribute_current::rim_flow_ind AS rim_flow_ind,
	smith__idrp_ksn_attribute_current::cross_merchandising_cd AS cross_merchandising_cd,
	work__idrp_candidate_sears_warehouse_step4::rim_last_record_creation_dt AS rim_last_record_creation_dt; 


join_candidate_sears_warehouse_sears_location_xref  = 
	JOIN
	work__idrp_candidate_sears_warehouse_step5
	BY(TrimLeadingZeros(sears_location_id)),
	gen_work__idrp_sears_location_xref 
	BY(TrimLeadingZeros(sears_location_id));


SPLIT join_candidate_sears_warehouse_sears_location_xref
INTO rec_dos_dc IF(TRIM(gen_work__idrp_sears_location_xref::location_format_type_cd)=='RRC'
			OR TRIM(gen_work__idrp_sears_location_xref::location_format_type_cd)=='DDC'
			OR TRIM(gen_work__idrp_sears_location_xref::location_format_type_cd)=='MDO'), 

rec_non_dos_dc IF(NOT(TRIM(gen_work__idrp_sears_location_xref::location_format_type_cd)=='RRC'
                        OR TRIM(gen_work__idrp_sears_location_xref::location_format_type_cd)=='DDC'
                        OR TRIM(gen_work__idrp_sears_location_xref::location_format_type_cd)=='MDO'));
						
filter_rec_dos_dc = filter rec_dos_dc by IsNull(item_active_ind,'') != '';

gen_rec_dos_dc = 
	FOREACH
	filter_rec_dos_dc
	GENERATE
	work__idrp_candidate_sears_warehouse_step5::sears_division_nbr AS sears_division_nbr,
        work__idrp_candidate_sears_warehouse_step5::sears_item_nbr AS sears_item_nbr,
        work__idrp_candidate_sears_warehouse_step5::sears_sku_nbr AS sears_sku_nbr,
        work__idrp_candidate_sears_warehouse_step5::sears_location_id AS sears_location_id,
        work__idrp_candidate_sears_warehouse_step5::dos_original_source_nbr AS dos_original_source_nbr,
        TrimLeadingZeros(work__idrp_candidate_sears_warehouse_step5::dos_source_nbr) AS dos_source_nbr,
        work__idrp_candidate_sears_warehouse_step5::dos_source_package_qty AS dos_source_package_qty,
        work__idrp_candidate_sears_warehouse_step5::srim_source_nbr AS srim_source_nbr,
        work__idrp_candidate_sears_warehouse_step5::srim_status_cd AS srim_status_cd,
        work__idrp_candidate_sears_warehouse_step5::srim_source_package_qty AS srim_source_package_qty,
        work__idrp_candidate_sears_warehouse_step5::item_active_ind AS item_active_ind,
        work__idrp_candidate_sears_warehouse_step5::item_reserve_cd AS item_reserve_cd,
	((TRIM(work__idrp_candidate_sears_warehouse_step5::item_active_ind)=='Y' AND work__idrp_candidate_sears_warehouse_step5::item_active_ind IS NOT NULL)
                                AND
         ((int)work__idrp_candidate_sears_warehouse_step5::item_reserve_cd==1 OR (int)work__idrp_candidate_sears_warehouse_step5::item_reserve_cd==2 OR
          (int)work__idrp_candidate_sears_warehouse_step5::item_reserve_cd==4 AND work__idrp_candidate_sears_warehouse_step5::item_reserve_cd IS NOT NULL)
                                AND
         (TRIM(work__idrp_candidate_sears_warehouse_step5::non_stock_source_cd)=='' OR work__idrp_candidate_sears_warehouse_step5::non_stock_source_cd 
	IS NULL)?
        'STK' :'NONSTK') AS stock_type_cd,
        work__idrp_candidate_sears_warehouse_step5::non_stock_source_cd AS non_stock_source_cd,
        work__idrp_candidate_sears_warehouse_step5::owner_cd AS owner_cd,
        work__idrp_candidate_sears_warehouse_step5::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
        work__idrp_candidate_sears_warehouse_step5::item_on_order_qty AS item_on_order_qty,
        work__idrp_candidate_sears_warehouse_step5::item_reserve_qty AS item_reserve_qty,
        work__idrp_candidate_sears_warehouse_step5::item_back_order_qty AS item_back_order_qty,
        work__idrp_candidate_sears_warehouse_step5::item_next_period_future_order_qty AS item_next_period_future_order_qty,
        work__idrp_candidate_sears_warehouse_step5::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
        work__idrp_candidate_sears_warehouse_step5::item_last_receive_dt AS item_last_receive_dt,
        work__idrp_candidate_sears_warehouse_step5::item_last_ship_dt AS item_last_ship_dt,
		work__idrp_candidate_sears_warehouse_step5::srim_on_hand_qty AS srim_on_hand_qty,
		work__idrp_candidate_sears_warehouse_step5::srim_regular_on_order_qty AS srim_regular_on_order_qty,
		work__idrp_candidate_sears_warehouse_step5::srim_promo_on_order_qty AS srim_promo_on_order_qty,
		work__idrp_candidate_sears_warehouse_step5::srim_last_on_hand_adj_dt AS srim_last_on_hand_adj_dt,
        work__idrp_candidate_sears_warehouse_step5::ksn_id AS ksn_id,
        work__idrp_candidate_sears_warehouse_step5::shc_item_id AS shc_item_id,
        work__idrp_candidate_sears_warehouse_step5::special_retail_order_system_ind AS special_retail_order_system_ind,
        work__idrp_candidate_sears_warehouse_step5::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        work__idrp_candidate_sears_warehouse_step5::dot_com_allocation_ind AS dot_com_allocation_ind,
        work__idrp_candidate_sears_warehouse_step5::distribution_type_cd AS distribution_type_cd,
        work__idrp_candidate_sears_warehouse_step5::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
        work__idrp_candidate_sears_warehouse_step5::special_order_candidate_ind AS special_order_candidate_ind,
        work__idrp_candidate_sears_warehouse_step5::item_emp_ind AS item_emp_ind,
        work__idrp_candidate_sears_warehouse_step5::easy_order_ind AS easy_order_ind,
        work__idrp_candidate_sears_warehouse_step5::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
        work__idrp_candidate_sears_warehouse_step5::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
        work__idrp_candidate_sears_warehouse_step5::rapid_item_ind AS rapid_item_ind,
        work__idrp_candidate_sears_warehouse_step5::constrained_item_ind AS constrained_item_ind,
        work__idrp_candidate_sears_warehouse_step5::sears_import_ind AS sears_import_ind,
        work__idrp_candidate_sears_warehouse_step5::idrp_item_type_desc AS idrp_item_type_desc,
        work__idrp_candidate_sears_warehouse_step5::sams_migration_ind AS sams_migration_ind,
        work__idrp_candidate_sears_warehouse_step5::emp_to_jit_ind AS emp_to_jit_ind,
        work__idrp_candidate_sears_warehouse_step5::rim_flow_ind AS rim_flow_ind,
        work__idrp_candidate_sears_warehouse_step5::cross_merchandising_cd AS cross_merchandising_cd,
        gen_work__idrp_sears_location_xref::location_id AS location_id,
        gen_work__idrp_sears_location_xref::location_level_cd AS location_level_cd,
		gen_work__idrp_sears_location_xref::location_owner_cd AS location_owner_cd,
        gen_work__idrp_sears_location_xref::location_format_type_cd AS location_format_type_cd,
	work__idrp_candidate_sears_warehouse_step5::rim_last_record_creation_dt AS rim_last_record_creation_dt;

		
gen_rec_non_dos_dc = 
	FOREACH
	rec_non_dos_dc
	GENERATE
	work__idrp_candidate_sears_warehouse_step5::sears_division_nbr AS sears_division_nbr,
	work__idrp_candidate_sears_warehouse_step5::sears_item_nbr AS sears_item_nbr,
	work__idrp_candidate_sears_warehouse_step5::sears_sku_nbr AS sears_sku_nbr,
	work__idrp_candidate_sears_warehouse_step5::sears_location_id AS sears_location_id,
	work__idrp_candidate_sears_warehouse_step5::dos_original_source_nbr AS dos_original_source_nbr,
	TrimLeadingZeros(work__idrp_candidate_sears_warehouse_step5::dos_source_nbr) AS dos_source_nbr,
    work__idrp_candidate_sears_warehouse_step5::dos_source_package_qty AS dos_source_package_qty,
	work__idrp_candidate_sears_warehouse_step5::srim_source_nbr AS srim_source_nbr,
	work__idrp_candidate_sears_warehouse_step5::srim_status_cd AS srim_status_cd,
	work__idrp_candidate_sears_warehouse_step5::srim_source_package_qty AS srim_source_package_qty,
	work__idrp_candidate_sears_warehouse_step5::item_active_ind AS item_active_ind,
	work__idrp_candidate_sears_warehouse_step5::item_reserve_cd AS item_reserve_cd,
	 ((TRIM(work__idrp_candidate_sears_warehouse_step5::srim_status_cd)=='D')
                OR
         (TRIM(work__idrp_candidate_sears_warehouse_step5::srim_status_cd)=='F')
                OR
         (TRIM(work__idrp_candidate_sears_warehouse_step5::srim_status_cd)=='T')
                OR
         (TRIM(work__idrp_candidate_sears_warehouse_step5::srim_status_cd)=='Q')
		AND
	  work__idrp_candidate_sears_warehouse_step5::srim_status_cd IS NOT NULL?'NONSTK':'STK') AS stock_type_cd,

	work__idrp_candidate_sears_warehouse_step5::non_stock_source_cd AS non_stock_source_cd,	
	work__idrp_candidate_sears_warehouse_step5::owner_cd AS owner_cd,
	work__idrp_candidate_sears_warehouse_step5::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
	work__idrp_candidate_sears_warehouse_step5::item_on_order_qty AS item_on_order_qty,
	work__idrp_candidate_sears_warehouse_step5::item_reserve_qty AS item_reserve_qty,
	work__idrp_candidate_sears_warehouse_step5::item_back_order_qty AS item_back_order_qty,
	work__idrp_candidate_sears_warehouse_step5::item_next_period_future_order_qty AS item_next_period_future_order_qty,
	work__idrp_candidate_sears_warehouse_step5::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
	work__idrp_candidate_sears_warehouse_step5::item_last_receive_dt AS item_last_receive_dt,
	work__idrp_candidate_sears_warehouse_step5::item_last_ship_dt AS item_last_ship_dt,
	work__idrp_candidate_sears_warehouse_step5::srim_on_hand_qty AS srim_on_hand_qty,
	work__idrp_candidate_sears_warehouse_step5::srim_regular_on_order_qty AS srim_regular_on_order_qty,
	work__idrp_candidate_sears_warehouse_step5::srim_promo_on_order_qty AS srim_promo_on_order_qty,
	work__idrp_candidate_sears_warehouse_step5::srim_last_on_hand_adj_dt AS srim_last_on_hand_adj_dt,
	work__idrp_candidate_sears_warehouse_step5::ksn_id AS ksn_id,
	work__idrp_candidate_sears_warehouse_step5::shc_item_id AS shc_item_id,
	work__idrp_candidate_sears_warehouse_step5::special_retail_order_system_ind AS special_retail_order_system_ind,
	work__idrp_candidate_sears_warehouse_step5::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
	work__idrp_candidate_sears_warehouse_step5::dot_com_allocation_ind AS dot_com_allocation_ind,
	work__idrp_candidate_sears_warehouse_step5::distribution_type_cd AS distribution_type_cd,
	work__idrp_candidate_sears_warehouse_step5::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
	work__idrp_candidate_sears_warehouse_step5::special_order_candidate_ind AS special_order_candidate_ind,
	work__idrp_candidate_sears_warehouse_step5::item_emp_ind AS item_emp_ind,
	work__idrp_candidate_sears_warehouse_step5::easy_order_ind AS easy_order_ind,
	work__idrp_candidate_sears_warehouse_step5::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
	work__idrp_candidate_sears_warehouse_step5::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
	work__idrp_candidate_sears_warehouse_step5::rapid_item_ind AS rapid_item_ind,
	work__idrp_candidate_sears_warehouse_step5::constrained_item_ind AS constrained_item_ind,
	work__idrp_candidate_sears_warehouse_step5::sears_import_ind AS sears_import_ind,
	work__idrp_candidate_sears_warehouse_step5::idrp_item_type_desc AS idrp_item_type_desc,
	work__idrp_candidate_sears_warehouse_step5::sams_migration_ind AS sams_migration_ind,
	work__idrp_candidate_sears_warehouse_step5::emp_to_jit_ind AS emp_to_jit_ind,
	work__idrp_candidate_sears_warehouse_step5::rim_flow_ind AS rim_flow_ind,
	work__idrp_candidate_sears_warehouse_step5::cross_merchandising_cd AS cross_merchandising_cd,
	gen_work__idrp_sears_location_xref::location_id AS location_id,
	gen_work__idrp_sears_location_xref::location_level_cd AS location_level_cd,
	gen_work__idrp_sears_location_xref::location_owner_cd AS location_owner_cd,
	gen_work__idrp_sears_location_xref::location_format_type_cd AS location_format_type_cd,
	work__idrp_candidate_sears_warehouse_step5::rim_last_record_creation_dt AS rim_last_record_creation_dt;
	

work__idrp_candidate_sears_warehouse_step6a = 
	UNION
	gen_rec_dos_dc,
	gen_rec_non_dos_dc;


fltr_gen_work__idrp_sears_location_xref = FILTER gen_work__idrp_sears_location_xref by TRIM(location_level_cd) != 'STORE';
	
join_candidate_sears_warehouse_step6a_xref = 
	JOIN
	work__idrp_candidate_sears_warehouse_step6a
	BY(TrimLeadingZeros(dos_source_nbr))
	LEFT OUTER,
	fltr_gen_work__idrp_sears_location_xref
	BY(TrimLeadingZeros(sears_location_id));
	
work__idrp_candidate_sears_warehouse_step6b = 
	FOREACH
	join_candidate_sears_warehouse_step6a_xref
	GENERATE
	work__idrp_candidate_sears_warehouse_step6a::sears_division_nbr AS sears_division_nbr,
	work__idrp_candidate_sears_warehouse_step6a::sears_item_nbr AS sears_item_nbr,
	work__idrp_candidate_sears_warehouse_step6a::sears_sku_nbr AS sears_sku_nbr,
	work__idrp_candidate_sears_warehouse_step6a::sears_location_id AS sears_location_id,
	work__idrp_candidate_sears_warehouse_step6a::dos_original_source_nbr AS dos_original_source_nbr,
	work__idrp_candidate_sears_warehouse_step6a::dos_source_nbr AS dos_source_nbr,
    work__idrp_candidate_sears_warehouse_step6a::dos_source_package_qty AS dos_source_package_qty,
	work__idrp_candidate_sears_warehouse_step6a::srim_source_nbr AS srim_source_nbr,
	work__idrp_candidate_sears_warehouse_step6a::srim_status_cd AS srim_status_cd,
	work__idrp_candidate_sears_warehouse_step6a::srim_source_package_qty AS srim_source_package_qty,
	work__idrp_candidate_sears_warehouse_step6a::item_active_ind AS item_active_ind,
	work__idrp_candidate_sears_warehouse_step6a::item_reserve_cd AS item_reserve_cd,
	work__idrp_candidate_sears_warehouse_step6a::stock_type_cd AS stock_type_cd,
	work__idrp_candidate_sears_warehouse_step6a::non_stock_source_cd AS non_stock_source_cd,
	work__idrp_candidate_sears_warehouse_step6a::owner_cd AS owner_cd,
	work__idrp_candidate_sears_warehouse_step6a::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
	work__idrp_candidate_sears_warehouse_step6a::item_on_order_qty AS item_on_order_qty,
	work__idrp_candidate_sears_warehouse_step6a::item_reserve_qty AS item_reserve_qty,
	work__idrp_candidate_sears_warehouse_step6a::item_back_order_qty AS item_back_order_qty,
	work__idrp_candidate_sears_warehouse_step6a::item_next_period_future_order_qty AS item_next_period_future_order_qty,
	work__idrp_candidate_sears_warehouse_step6a::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
	work__idrp_candidate_sears_warehouse_step6a::item_last_receive_dt AS item_last_receive_dt,
	work__idrp_candidate_sears_warehouse_step6a::item_last_ship_dt AS item_last_ship_dt,
	work__idrp_candidate_sears_warehouse_step6a::srim_on_hand_qty AS srim_on_hand_qty,
	work__idrp_candidate_sears_warehouse_step6a::srim_regular_on_order_qty AS srim_regular_on_order_qty,
	work__idrp_candidate_sears_warehouse_step6a::srim_promo_on_order_qty AS srim_promo_on_order_qty,
	work__idrp_candidate_sears_warehouse_step6a::srim_last_on_hand_adj_dt AS srim_last_on_hand_adj_dt,
	work__idrp_candidate_sears_warehouse_step6a::ksn_id AS ksn_id,
	work__idrp_candidate_sears_warehouse_step6a::shc_item_id AS shc_item_id,
	work__idrp_candidate_sears_warehouse_step6a::special_retail_order_system_ind AS special_retail_order_system_ind,
	work__idrp_candidate_sears_warehouse_step6a::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
	work__idrp_candidate_sears_warehouse_step6a::dot_com_allocation_ind AS dot_com_allocation_ind,
	work__idrp_candidate_sears_warehouse_step6a::distribution_type_cd AS distribution_type_cd,
	work__idrp_candidate_sears_warehouse_step6a::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
	work__idrp_candidate_sears_warehouse_step6a::special_order_candidate_ind AS special_order_candidate_ind,
	work__idrp_candidate_sears_warehouse_step6a::item_emp_ind AS item_emp_ind,
	work__idrp_candidate_sears_warehouse_step6a::easy_order_ind AS easy_order_ind,
	work__idrp_candidate_sears_warehouse_step6a::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
	work__idrp_candidate_sears_warehouse_step6a::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
	work__idrp_candidate_sears_warehouse_step6a::rapid_item_ind AS rapid_item_ind,
	work__idrp_candidate_sears_warehouse_step6a::constrained_item_ind AS constrained_item_ind,
	work__idrp_candidate_sears_warehouse_step6a::sears_import_ind AS sears_import_ind,
	work__idrp_candidate_sears_warehouse_step6a::idrp_item_type_desc AS idrp_item_type_desc,
	work__idrp_candidate_sears_warehouse_step6a::sams_migration_ind AS sams_migration_ind,
	work__idrp_candidate_sears_warehouse_step6a::emp_to_jit_ind AS emp_to_jit_ind,
	work__idrp_candidate_sears_warehouse_step6a::rim_flow_ind AS rim_flow_ind,
	work__idrp_candidate_sears_warehouse_step6a::cross_merchandising_cd AS cross_merchandising_cd,
	work__idrp_candidate_sears_warehouse_step6a::location_id AS location_id,
	work__idrp_candidate_sears_warehouse_step6a::location_level_cd AS location_level_cd,
	work__idrp_candidate_sears_warehouse_step6a::location_owner_cd AS location_owner_cd,
	work__idrp_candidate_sears_warehouse_step6a::location_format_type_cd AS location_format_type_cd,
	fltr_gen_work__idrp_sears_location_xref::location_id AS dos_source_location_id,
	fltr_gen_work__idrp_sears_location_xref::location_level_cd AS dos_source_location_level_cd,
	work__idrp_candidate_sears_warehouse_step6a::rim_last_record_creation_dt AS rim_last_record_creation_dt;


join_candidate_sears_warehouse_step6b_xref = 
        JOIN
        work__idrp_candidate_sears_warehouse_step6b
        BY(TrimLeadingZeros(srim_source_nbr))
        LEFT OUTER,
        fltr_gen_work__idrp_sears_location_xref
        BY(TrimLeadingZeros(sears_location_id));
		
work__idrp_candidate_sears_warehouse_step6c = 
        FOREACH
        join_candidate_sears_warehouse_step6b_xref
        GENERATE
        work__idrp_candidate_sears_warehouse_step6b::sears_division_nbr AS sears_division_nbr,
        work__idrp_candidate_sears_warehouse_step6b::sears_item_nbr AS sears_item_nbr,
        work__idrp_candidate_sears_warehouse_step6b::sears_sku_nbr AS sears_sku_nbr,
        work__idrp_candidate_sears_warehouse_step6b::sears_location_id AS sears_location_id,
        work__idrp_candidate_sears_warehouse_step6b::dos_original_source_nbr AS dos_original_source_nbr,
        work__idrp_candidate_sears_warehouse_step6b::dos_source_nbr AS dos_source_nbr,
        work__idrp_candidate_sears_warehouse_step6b::dos_source_package_qty AS dos_source_package_qty,
        work__idrp_candidate_sears_warehouse_step6b::srim_source_nbr AS srim_source_nbr,
        work__idrp_candidate_sears_warehouse_step6b::srim_status_cd AS srim_status_cd,
        work__idrp_candidate_sears_warehouse_step6b::srim_source_package_qty AS srim_source_package_qty,
        work__idrp_candidate_sears_warehouse_step6b::item_active_ind AS item_active_ind,
        work__idrp_candidate_sears_warehouse_step6b::item_reserve_cd AS item_reserve_cd,
        work__idrp_candidate_sears_warehouse_step6b::stock_type_cd AS stock_type_cd,
        work__idrp_candidate_sears_warehouse_step6b::non_stock_source_cd AS non_stock_source_cd,
        work__idrp_candidate_sears_warehouse_step6b::owner_cd AS owner_cd,
        work__idrp_candidate_sears_warehouse_step6b::item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
        work__idrp_candidate_sears_warehouse_step6b::item_on_order_qty AS item_on_order_qty,
        work__idrp_candidate_sears_warehouse_step6b::item_reserve_qty AS item_reserve_qty,
        work__idrp_candidate_sears_warehouse_step6b::item_back_order_qty AS item_back_order_qty,
        work__idrp_candidate_sears_warehouse_step6b::item_next_period_future_order_qty AS item_next_period_future_order_qty,
        work__idrp_candidate_sears_warehouse_step6b::item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
        work__idrp_candidate_sears_warehouse_step6b::item_last_receive_dt AS item_last_receive_dt,
        work__idrp_candidate_sears_warehouse_step6b::item_last_ship_dt AS item_last_ship_dt,
		work__idrp_candidate_sears_warehouse_step6b::srim_on_hand_qty AS srim_on_hand_qty,
		work__idrp_candidate_sears_warehouse_step6b::srim_regular_on_order_qty AS srim_regular_on_order_qty,
		work__idrp_candidate_sears_warehouse_step6b::srim_promo_on_order_qty AS srim_promo_on_order_qty,
		work__idrp_candidate_sears_warehouse_step6b::srim_last_on_hand_adj_dt AS srim_last_on_hand_adj_dt,
        work__idrp_candidate_sears_warehouse_step6b::ksn_id AS ksn_id,
        work__idrp_candidate_sears_warehouse_step6b::shc_item_id AS shc_item_id,
        work__idrp_candidate_sears_warehouse_step6b::special_retail_order_system_ind AS special_retail_order_system_ind,
        work__idrp_candidate_sears_warehouse_step6b::shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        work__idrp_candidate_sears_warehouse_step6b::dot_com_allocation_ind AS dot_com_allocation_ind,
        work__idrp_candidate_sears_warehouse_step6b::distribution_type_cd AS distribution_type_cd,
        work__idrp_candidate_sears_warehouse_step6b::only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
        work__idrp_candidate_sears_warehouse_step6b::special_order_candidate_ind AS special_order_candidate_ind,
        work__idrp_candidate_sears_warehouse_step6b::item_emp_ind AS item_emp_ind,
        work__idrp_candidate_sears_warehouse_step6b::easy_order_ind AS easy_order_ind,
        work__idrp_candidate_sears_warehouse_step6b::warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
        work__idrp_candidate_sears_warehouse_step6b::cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
        work__idrp_candidate_sears_warehouse_step6b::rapid_item_ind AS rapid_item_ind,
        work__idrp_candidate_sears_warehouse_step6b::constrained_item_ind AS constrained_item_ind,
        work__idrp_candidate_sears_warehouse_step6b::sears_import_ind AS sears_import_ind,
        work__idrp_candidate_sears_warehouse_step6b::idrp_item_type_desc AS idrp_item_type_desc,
        work__idrp_candidate_sears_warehouse_step6b::sams_migration_ind AS sams_migration_ind,
        work__idrp_candidate_sears_warehouse_step6b::emp_to_jit_ind AS emp_to_jit_ind,
		work__idrp_candidate_sears_warehouse_step6b::rim_flow_ind AS rim_flow_ind,
        work__idrp_candidate_sears_warehouse_step6b::cross_merchandising_cd AS cross_merchandising_cd,
        work__idrp_candidate_sears_warehouse_step6b::location_id AS location_id,
        work__idrp_candidate_sears_warehouse_step6b::location_level_cd AS location_level_cd,
        work__idrp_candidate_sears_warehouse_step6b::location_owner_cd AS location_owner_cd,
        work__idrp_candidate_sears_warehouse_step6b::location_format_type_cd AS location_format_type_cd,
		work__idrp_candidate_sears_warehouse_step6b::dos_source_location_id AS dos_source_location_id,
        work__idrp_candidate_sears_warehouse_step6b::dos_source_location_level_cd AS dos_source_location_level_cd,
        fltr_gen_work__idrp_sears_location_xref::location_id AS srim_source_location_id,
        fltr_gen_work__idrp_sears_location_xref::location_level_cd AS srim_source_location_level_cd,
	work__idrp_candidate_sears_warehouse_step6b::rim_last_record_creation_dt AS rim_last_record_creation_dt;


SPLIT work__idrp_candidate_sears_warehouse_step6c
INTO dos_err_records IF((dos_source_nbr IS NOT NULL) AND (dos_source_location_id IS NULL)),
     srim_err_records IF(((srim_source_nbr IS NOT NULL )OR ((long)srim_source_nbr!=0)) AND (srim_source_location_id IS NULL)),
     work__idrp_candidate_sears_warehouse_step6d 
     IF(((dos_source_nbr IS NOT NULL) AND (dos_source_location_id IS NOT NULL))
			OR
	(((srim_source_nbr IS NOT NULL )OR ((long)srim_source_nbr!=0)) AND (srim_source_location_id IS NOT NULL)));
     


gen_dos_err_records = 
	FOREACH
	dos_err_records
	GENERATE
	sears_division_nbr,
	sears_item_nbr,
        sears_sku_nbr,
        sears_location_id AS sears_location_nbr,
        dos_source_nbr AS sears_source_nbr,
        'DOS' AS sears_source_system_cd,
        'source vendor not found in eligible loc table' AS error_message;


gen_srim_err_records = 
        FOREACH
        srim_err_records
        GENERATE
        sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        sears_location_id AS sears_location_nbr,
        srim_source_nbr AS sears_source_nbr,
        'SRIM' AS sears_source_system_cd,
        'source vendor not found in eligible loc table' AS error_message;


work__idrp_candidate_sears_warehouse_missing_source_error = 
        UNION 
	gen_dos_err_records,
	gen_srim_err_records;

STORE work__idrp_candidate_sears_warehouse_missing_source_error
INTO '$WORK__IDRP_CANDIDATE_SEARS_WAREHOUSE_MISSING_SOURCE_ERROR_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

SPLIT work__idrp_candidate_sears_warehouse_step6d
INTO location_format_type_cd_RRC 
     IF(TRIM(location_format_type_cd)=='RRC'),
     location_format_type_cd_DDC_MDO
     IF(TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO'),
     location_format_type_cd_CDFC
     IF(TRIM(location_format_type_cd)=='CDFC'),
     location_format_type_cd_others
     IF(NOT(TRIM(location_format_type_cd)=='RRC' OR TRIM(location_format_type_cd)=='DDC' OR TRIM(location_format_type_cd)=='MDO' OR TRIM(location_format_type_cd)=='CDFC'));


gen_location_format_type_cd_RRC = 
        FOREACH
        location_format_type_cd_RRC
        GENERATE
        sears_division_nbr AS sears_division_nbr,
        sears_item_nbr AS sears_item_nbr,
        sears_sku_nbr AS sears_sku_nbr,
        sears_location_id AS sears_location_id,
        location_id AS location_id,
        location_level_cd AS location_level_cd,
		location_format_type_cd AS location_format_type_cd,
        location_owner_cd AS location_owner_cd,
        dos_original_source_nbr AS dos_original_source_nbr,
        dos_source_nbr AS dos_source_nbr,
        dos_source_package_qty AS dos_source_package_qty,
        dos_source_location_id AS dos_source_location_id,
        dos_source_location_level_cd AS dos_source_location_level_cd,
        srim_source_nbr AS srim_source_nbr,
		srim_source_location_id AS srim_source_location_id,
		srim_source_location_level_cd AS srim_source_location_level_cd,
		' ' AS purchase_order_vendor_location_id,
        srim_status_cd AS srim_status_cd,
	(((int)special_order_candidate_ind==0 AND (int)item_emp_ind==0 AND (TRIM(item_active_ind)=='Y' OR (TRIM(item_active_ind)=='N' AND ((int)item_next_period_on_hand_qty>0 OR (int)item_on_order_qty>0 OR (int)item_reserve_qty>0 OR (int)item_back_order_qty>0 OR (int)item_next_period_future_order_qty>0 OR (int)item_next_period_in_transit_qty>0) AND (CompareTwoDates(item_last_receive_dt,AddDays('$CURRENT_DATE' ,-365))>=1 OR CompareTwoDates(item_last_ship_dt,AddDays('$CURRENT_DATE' ,-365))>=1)))) ? 'Y' : 'N') AS active_ind,
	srim_source_package_qty  AS srim_source_package_qty,
	item_active_ind AS item_active_ind,
	shc_item_id AS shc_item_id,
	ksn_id AS ksn_id,
	special_retail_order_system_ind AS special_retail_order_system_ind,
	shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
	dot_com_allocation_ind AS dot_com_allocation_ind,
	distribution_type_cd AS distribution_type_cd,
	only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
	special_order_candidate_ind AS special_order_candidate_ind,
	item_emp_ind AS item_emp_ind,
	easy_order_ind as easy_order_ind,
	warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
	rapid_item_ind AS rapid_item_ind,
	constrained_item_ind AS constrained_item_ind,
	sears_import_ind as sears_import_ind,
        (IsNull(idrp_item_type_desc,'') == ''
			? ''
			: idrp_item_type_desc) AS idrp_item_type_desc,
	cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
	sams_migration_ind AS sams_migration_ind,
	emp_to_jit_ind AS emp_to_jit_ind,
	rim_flow_ind AS rim_flow_ind,
	cross_merchandising_cd AS cross_merchandising_cd,
	stock_type_cd AS stock_type_cd,
	item_reserve_cd AS item_reserve_cd,
	non_stock_source_cd AS non_stock_source_cd,
	owner_cd AS owner_cd,
	item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
	item_on_order_qty AS item_on_order_qty,
	item_reserve_qty AS item_reserve_qty,
	item_back_order_qty AS item_back_order_qty,
	item_next_period_future_order_qty AS item_next_period_future_order_qty,
	item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
    item_last_receive_dt AS item_last_receive_dt,
	item_last_ship_dt AS item_last_ship_dt,
	rim_last_record_creation_dt AS rim_last_record_creation_dt;
        	
	
gen_location_format_type_cd_DDC_MDO = 
	FOREACH
	location_format_type_cd_DDC_MDO
	GENERATE
		sears_division_nbr AS sears_division_nbr,
        sears_item_nbr AS sears_item_nbr,
        sears_sku_nbr AS sears_sku_nbr,
        sears_location_id AS sears_location_id,
        location_id AS location_id,
        location_level_cd AS location_level_cd,
		location_format_type_cd AS location_format_type_cd,
        location_owner_cd AS location_owner_cd,
        dos_original_source_nbr AS dos_original_source_nbr,
        dos_source_nbr AS dos_source_nbr,
        dos_source_package_qty AS dos_source_package_qty,
        dos_source_location_id AS dos_source_location_id,
        dos_source_location_level_cd AS dos_source_location_level_cd,
        srim_source_nbr AS srim_source_nbr,
        srim_source_location_id AS srim_source_location_id,
        srim_source_location_level_cd AS srim_source_location_level_cd,
        ' ' AS purchase_order_vendor_location_id,
        srim_status_cd AS srim_status_cd,
        (((int)special_order_candidate_ind==0
                AND
         (int)item_emp_ind==0
                AND
	     (TRIM(item_active_ind)=='Y'
                OR
         (TRIM(item_active_ind)=='N'
               AND
                ((int)item_next_period_on_hand_qty > 0 OR (int)item_on_order_qty > 0 OR (int)item_reserve_qty > 0 OR (int)item_back_order_qty > 0 OR
           (int)item_next_period_future_order_qty > 0 OR (int)item_next_period_in_transit_qty > 0 )
               AND
                (CompareTwoDates(item_last_receive_dt,AddDays('$CURRENT_DATE' ,-365))>=1 OR CompareTwoDates(item_last_ship_dt,AddDays('$CURRENT_DATE' ,-365))>=1)))) ? 'Y' : 'N') AS active_ind,
        srim_source_package_qty  AS srim_source_package_qty,
        item_active_ind AS item_active_ind,
        shc_item_id AS shc_item_id,
        ksn_id AS ksn_id,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        dot_com_allocation_ind AS dot_com_allocation_ind,
        distribution_type_cd AS distribution_type_cd,
        only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
        special_order_candidate_ind AS special_order_candidate_ind,
        item_emp_ind AS item_emp_ind,
        easy_order_ind as easy_order_ind,
        warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
        rapid_item_ind AS rapid_item_ind,
        constrained_item_ind AS constrained_item_ind,
        sears_import_ind as sears_import_ind,
        (IsNull(idrp_item_type_desc,'') == ''
			? ''
			: idrp_item_type_desc) AS idrp_item_type_desc,
		cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
        sams_migration_ind AS sams_migration_ind,
        emp_to_jit_ind AS emp_to_jit_ind,
        rim_flow_ind AS rim_flow_ind,
        cross_merchandising_cd AS cross_merchandising_cd,
        stock_type_cd AS stock_type_cd,
        item_reserve_cd AS item_reserve_cd,
        non_stock_source_cd AS non_stock_source_cd,
        owner_cd AS owner_cd,
        item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
        item_on_order_qty AS item_on_order_qty,
        item_reserve_qty AS item_reserve_qty,
        item_back_order_qty AS item_back_order_qty,
        item_next_period_future_order_qty AS item_next_period_future_order_qty,
        item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
        item_last_receive_dt AS item_last_receive_dt,
        item_last_ship_dt AS item_last_ship_dt,
	rim_last_record_creation_dt AS rim_last_record_creation_dt;
        


gen_location_format_type_cd_CDFC = 
	FOREACH
	location_format_type_cd_CDFC
	GENERATE	
	sears_division_nbr AS sears_division_nbr,
        sears_item_nbr AS sears_item_nbr,
        sears_sku_nbr AS sears_sku_nbr,
        sears_location_id AS sears_location_id,
        location_id AS location_id,
        location_level_cd AS location_level_cd,
		location_format_type_cd AS location_format_type_cd,
        location_owner_cd AS location_owner_cd,
        dos_original_source_nbr AS dos_original_source_nbr,
        dos_source_nbr AS dos_source_nbr,
        dos_source_package_qty AS dos_source_package_qty,
        dos_source_location_id AS dos_source_location_id,
        dos_source_location_level_cd AS dos_source_location_level_cd,
        srim_source_nbr AS srim_source_nbr,
        srim_source_location_id AS srim_source_location_id,
        srim_source_location_level_cd AS srim_source_location_level_cd,
        ' ' AS purchase_order_vendor_location_id,
        srim_status_cd AS srim_status_cd,
	((IsNull(warehouse_sizing_attribute_cd,'') != '' AND TRIM(warehouse_sizing_attribute_cd)=='WG8800'
		OR
	  IsNull(warehouse_sizing_attribute_cd,'') != '' AND TRIM(warehouse_sizing_attribute_cd)=='WG8808'
		OR
	  IsNull(warehouse_sizing_attribute_cd,'') != '' AND TRIM(warehouse_sizing_attribute_cd)=='WG8809'
		OR
	  IsNull(warehouse_sizing_attribute_cd,'') != '' AND TRIM(warehouse_sizing_attribute_cd)=='WG8810'
	 )
		AND
	(NOT(TRIM(srim_status_cd)=='D'
		OR
	 TRIM(srim_status_cd)=='F'
		OR
	 TRIM(srim_status_cd)=='T'
		OR
	 TRIM(srim_status_cd)=='Q'))
		AND
	((int)special_order_candidate_ind==0)
	    ? 'Y' : 'N') AS active_ind,
		srim_source_package_qty  AS srim_source_package_qty,
        item_active_ind AS item_active_ind,
        shc_item_id AS shc_item_id,
        ksn_id AS ksn_id,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        dot_com_allocation_ind AS dot_com_allocation_ind,
        distribution_type_cd AS distribution_type_cd,
        only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
        special_order_candidate_ind AS special_order_candidate_ind,
        item_emp_ind AS item_emp_ind,
        easy_order_ind as easy_order_ind,
        warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
        rapid_item_ind AS rapid_item_ind,
        constrained_item_ind AS constrained_item_ind,
        sears_import_ind as sears_import_ind,
        (IsNull(idrp_item_type_desc,'') == ''
			? ''
			: idrp_item_type_desc) AS idrp_item_type_desc,
        cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
        sams_migration_ind AS sams_migration_ind,
        emp_to_jit_ind AS emp_to_jit_ind,
        rim_flow_ind AS rim_flow_ind,
        cross_merchandising_cd AS cross_merchandising_cd,
        stock_type_cd AS stock_type_cd,
        item_reserve_cd AS item_reserve_cd,
        non_stock_source_cd AS non_stock_source_cd,
        owner_cd AS owner_cd,
        item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
        item_on_order_qty AS item_on_order_qty,
        item_reserve_qty AS item_reserve_qty,
        item_back_order_qty AS item_back_order_qty,
        item_next_period_future_order_qty AS item_next_period_future_order_qty,
        item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
        item_last_receive_dt AS item_last_receive_dt,
        item_last_ship_dt AS item_last_ship_dt,
	rim_last_record_creation_dt AS rim_last_record_creation_dt;
        

gen_location_format_type_cd_others = 
	FOREACH
	location_format_type_cd_others
	GENERATE
	sears_division_nbr AS sears_division_nbr,
        sears_item_nbr AS sears_item_nbr,
        sears_sku_nbr AS sears_sku_nbr,
        sears_location_id AS sears_location_id,
        location_id AS location_id,
        location_level_cd AS location_level_cd,
		location_format_type_cd AS location_format_type_cd,
        location_owner_cd AS location_owner_cd,
        dos_original_source_nbr AS dos_original_source_nbr,
        dos_source_nbr AS dos_source_nbr,
        dos_source_package_qty AS dos_source_package_qty,
        dos_source_location_id AS dos_source_location_id,
        dos_source_location_level_cd AS dos_source_location_level_cd,
        srim_source_nbr AS srim_source_nbr,
        srim_source_location_id AS srim_source_location_id,
        srim_source_location_level_cd AS srim_source_location_level_cd,
        ' ' AS purchase_order_vendor_location_id,
        srim_status_cd AS srim_status_cd,
		((NOT(TRIM(srim_status_cd)=='D'
		  OR
		  TRIM(srim_status_cd)=='F'
		  OR
	      TRIM(srim_status_cd)=='T'
		  OR
	      TRIM(srim_status_cd)=='Q')) OR
		  (((int)srim_on_hand_qty > 0 OR (int)srim_regular_on_order_qty > 0 OR (int)srim_promo_on_order_qty > 0) AND
		  (CompareTwoDates(srim_last_on_hand_adj_dt, AddDays('$CURRENT_DATE',-365))>=1))  
			? 'Y' : 'N') AS active_ind,  ---CR5120, CR5138
		srim_source_package_qty  AS srim_source_package_qty,
        item_active_ind AS item_active_ind,
        shc_item_id AS shc_item_id,
        ksn_id AS ksn_id,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        dot_com_allocation_ind AS dot_com_allocation_ind,
        distribution_type_cd AS distribution_type_cd,
        only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
        special_order_candidate_ind AS special_order_candidate_ind,
        item_emp_ind AS item_emp_ind,
        easy_order_ind as easy_order_ind,
        warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
        rapid_item_ind AS rapid_item_ind,
        constrained_item_ind AS constrained_item_ind,
        sears_import_ind as sears_import_ind,
        (IsNull(idrp_item_type_desc,'') == ''
			? ''
			: idrp_item_type_desc) AS idrp_item_type_desc,
        cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
        sams_migration_ind AS sams_migration_ind,
        emp_to_jit_ind AS emp_to_jit_ind,
        rim_flow_ind AS rim_flow_ind,
        cross_merchandising_cd AS cross_merchandising_cd,
        stock_type_cd AS stock_type_cd,
        item_reserve_cd AS item_reserve_cd,
        non_stock_source_cd AS non_stock_source_cd,
        owner_cd AS owner_cd,
        item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
        item_on_order_qty AS item_on_order_qty,
        item_reserve_qty AS item_reserve_qty,
        item_back_order_qty AS item_back_order_qty,
        item_next_period_future_order_qty AS item_next_period_future_order_qty,
        item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
        item_last_receive_dt AS item_last_receive_dt,
        item_last_ship_dt AS item_last_ship_dt,
	rim_last_record_creation_dt AS rim_last_record_creation_dt;
        

work__idrp_candidate_sears_warehouse_1  = 
	UNION
	gen_location_format_type_cd_RRC,
	gen_location_format_type_cd_DDC_MDO,
	gen_location_format_type_cd_CDFC,
	gen_location_format_type_cd_others;
	                                                                                                                                                  	
--CR 5079		
work__idrp_candidate_sears_warehouse_filter = 
	FILTER work__idrp_candidate_sears_warehouse_1
	BY active_ind == 'Y';
	
work__idrp_candidate_active_warehouse = 
	FOREACH work__idrp_candidate_sears_warehouse_filter
	GENERATE
		sears_division_nbr,	
		sears_item_nbr,	
		sears_sku_nbr,	
		dos_source_nbr,
		srim_source_nbr;

join_aprk_candidate_active_warehouse_1 = 
	JOIN  work__aprk_import_vendors BY ((int)sears_vendor_nbr),
		  work__idrp_candidate_active_warehouse BY ((int)dos_source_nbr);	

join_aprk_candidate_active_warehouse_2 = 
	JOIN  work__aprk_import_vendors BY ((int)sears_vendor_nbr),
		  work__idrp_candidate_active_warehouse BY ((int)srim_source_nbr);

join_aprk_candidate_active_warehouse = UNION join_aprk_candidate_active_warehouse_1, join_aprk_candidate_active_warehouse_2;

gen_aprk_candidate_active_warehouse =
	FOREACH join_aprk_candidate_active_warehouse
	GENERATE
		work__idrp_candidate_active_warehouse::sears_division_nbr AS sears_division_nbr,	
		work__idrp_candidate_active_warehouse::sears_item_nbr AS sears_item_nbr,	
		work__idrp_candidate_active_warehouse::sears_sku_nbr AS sears_sku_nbr;		

work__idrp_import_items = DISTINCT gen_aprk_candidate_active_warehouse;

join_candidate_sears_warehouse_import_items = 
	JOIN work__idrp_candidate_sears_warehouse_1 BY (sears_division_nbr, sears_item_nbr, sears_sku_nbr) LEFT OUTER,
		 work__idrp_import_items BY (sears_division_nbr, sears_item_nbr, sears_sku_nbr);

work__idrp_candidate_sears_warehouse =
	FOREACH join_candidate_sears_warehouse_import_items
	GENERATE		 
		work__idrp_candidate_sears_warehouse_1::sears_division_nbr AS sears_division_nbr,
        work__idrp_candidate_sears_warehouse_1::sears_item_nbr AS sears_item_nbr,
        work__idrp_candidate_sears_warehouse_1::sears_sku_nbr AS sears_sku_nbr,
        sears_location_id AS sears_location_id,
        location_id AS location_id,
        location_level_cd AS location_level_cd,
		location_format_type_cd AS location_format_type_cd,
        location_owner_cd AS location_owner_cd,
        dos_original_source_nbr AS dos_original_source_nbr,
        dos_source_nbr AS dos_source_nbr,
        dos_source_package_qty AS dos_source_package_qty,
        dos_source_location_id AS dos_source_location_id,
        dos_source_location_level_cd AS dos_source_location_level_cd,
        srim_source_nbr AS srim_source_nbr,
        srim_source_location_id AS srim_source_location_id,
        srim_source_location_level_cd AS srim_source_location_level_cd,
        purchase_order_vendor_location_id,
        srim_status_cd AS srim_status_cd,
		active_ind,
		srim_source_package_qty AS srim_source_package_qty,
        item_active_ind AS item_active_ind,
        shc_item_id AS shc_item_id,
        ksn_id AS ksn_id,
        special_retail_order_system_ind AS special_retail_order_system_ind,
        shc_item_corporate_owner_cd AS shc_item_corporate_owner_cd,
        dot_com_allocation_ind AS dot_com_allocation_ind,
        distribution_type_cd AS distribution_type_cd,
        only_rsu_distribution_channel_ind AS only_rsu_distribution_channel_ind,
        special_order_candidate_ind AS special_order_candidate_ind,
        item_emp_ind AS item_emp_ind,
        easy_order_ind as easy_order_ind,
        warehouse_sizing_attribute_cd AS warehouse_sizing_attribute_cd,
        rapid_item_ind AS rapid_item_ind,
        constrained_item_ind AS constrained_item_ind,
        ((IsNull(work__idrp_import_items::sears_division_nbr,'') != '')
			? '1'
			: '0') AS sears_import_ind,
		(((IsNull(work__idrp_import_items::sears_division_nbr,'') != '') AND
		(idrp_item_type_desc != 'RSOS' AND idrp_item_type_desc != 'EASY ORDER'))
			? 'IMPORT'
			: idrp_item_type_desc) AS idrp_item_type_desc,
        cross_merchandising_attribute_cd AS cross_merchandising_attribute_cd,
        sams_migration_ind AS sams_migration_ind,
        emp_to_jit_ind AS emp_to_jit_ind,
        rim_flow_ind AS rim_flow_ind,
        cross_merchandising_cd AS cross_merchandising_cd,
        stock_type_cd AS stock_type_cd,
        item_reserve_cd AS item_reserve_cd,
        non_stock_source_cd AS non_stock_source_cd,
        owner_cd AS owner_cd,
        item_next_period_on_hand_qty AS item_next_period_on_hand_qty,
        item_on_order_qty AS item_on_order_qty,
        item_reserve_qty AS item_reserve_qty,
        item_back_order_qty AS item_back_order_qty,
        item_next_period_future_order_qty AS item_next_period_future_order_qty,
        item_next_period_in_transit_qty AS item_next_period_in_transit_qty,
        item_last_receive_dt AS item_last_receive_dt,
        item_last_ship_dt AS item_last_ship_dt,
	rim_last_record_creation_dt AS rim_last_record_creation_dt;		 

STORE work__idrp_candidate_sears_warehouse
INTO '$WORK__IDRP_CANDIDATE_SEARS_WAREHOUSE_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
