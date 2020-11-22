/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_smith__idrp_i2k_sears_rebuy_vendor_package_current.pig
# AUTHOR NAME:         Neera Singh
# CREATION DATE:       Thu Jun 23 09:37:58 EST 2014
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
#        DATE          BY               MODIFICATION
#        2014-09-05    Meghana Dhage    Defect 2955
#        2014-09-12    Meghana Dhage    CR 2956
#        2014-12-15    Sushauvik Deb    CR 3451
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

--register the jar containing all PIG UDFs
REGISTER $UDF_JAR;

--trim spaces around string
DEFINE TRIM_STRING $TRIM_STRING ;

--trim leading zeros
DEFINE TRIM_INTEGER $TRIM_INTEGER ;

--trim leading and trailing zeros
DEFINE TRIM_DECIMAL $TRIM_DECIMAL ;

DEFINE TrimLeadingZeros com.searshc.supplychain.idrp.udf.TrimLeadingZeros();

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

smith__idrp_eligible_loc = LOAD
	'$SMITH__IDRP_ELIGIBLE_LOC_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
	AS($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);


	smith__idrp_batchdate = LOAD 
    '$SMITH__IDRP_BATCH_DATE_LOCATION'
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
    AS ($SMITH__IDRP_BATCH_DATE_SCHEMA);

gen_smith__idrp_batchdate = foreach smith__idrp_batchdate generate
							batch_dt;


work__idrp_item_eligibility_sears_vendor_package_ta_shared_location = LOAD
	'$WORK__IDRP_ITEM_ELIGIBILITY_SEARS_VENDOR_PACKAGE_TA_SHARED_LOCATION_LOCATION'
	AS (ta_shared_location:chararray);
	
work__idrp_item_eligibility_sears_vendor_package_ta_shared_location = foreach work__idrp_item_eligibility_sears_vendor_package_ta_shared_location generate SUBSTRING(ta_shared_location, 0, 7) as sears_location_nbr, SUBSTRING(ta_shared_location, 7, 8) as store_type_cd;



smith__idrp_i2k_valid_rebuy_vendor_package_ids_current = LOAD
	'$smith__idrp_i2k_valid_rebuy_vendor_package_ids_current_location'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
	AS($smith__idrp_i2k_valid_rebuy_vendor_package_ids_current_schema);



smith__idrp_vend_pack_combined = LOAD
	'$SMITH__IDRP_VEND_PACK_COMBINED_LOCATION'
	USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
	AS($SMITH__IDRP_VEND_PACK_COMBINED_SCHEMA);


gen_smith__idrp_eligible_loc = 
	FOREACH
	smith__idrp_eligible_loc
	GENERATE
	loc,
	srs_loc,
	srs_vndr_nbr,
	loc_lvl_cd,
	loc_fmt_typ_cd,
	duns_type_cd,
	duns_owner_cd,
	loc_owner_cd,
	loc_cls_dt;

fltr_gen_smith__idrp_eligible_loc_STORE = 
        FILTER
        gen_smith__idrp_eligible_loc
        BY(TRIM(loc_lvl_cd)=='STORE');
		
join_fltr_gen_smith__idrp_eligible_loc_STORE_batchdate = CROSS fltr_gen_smith__idrp_eligible_loc_STORE, gen_smith__idrp_batchdate;

filter_join_fltr_gen_smith__idrp_eligible_loc_STORE_batchdate = filter join_fltr_gen_smith__idrp_eligible_loc_STORE_batchdate by
											(GetDateFromTimeStamp(loc_cls_dt) > batch_dt OR GetDateFromTimeStamp(loc_cls_dt) == '1970-01-01');
																
gen_fltr_smith__idrp_eligible_loc_STORE = 
	FOREACH
	filter_join_fltr_gen_smith__idrp_eligible_loc_STORE_batchdate
	GENERATE
	loc AS location_id,
	TrimLeadingZeros(srs_loc) AS sears_location_id,
	loc_lvl_cd,
	loc_fmt_typ_cd,
	loc_owner_cd AS location_owner_cd;


--[CR3242] Filter out closed warehouse and warehouses with a blank location format type code when creating  work__idrp_sears_location_xref 	

fltr_gen_smith__idrp_eligible_loc_WAREHOUSE = 
        FILTER
        gen_smith__idrp_eligible_loc
        BY((TRIM(loc_lvl_cd)=='WAREHOUSE') AND (loc_fmt_typ_cd IS NOT NULL OR loc_fmt_typ_cd!=' '));


join_fltr_gen_smith__idrp_eligible_loc_WAREHOUSE_batchdate = CROSS fltr_gen_smith__idrp_eligible_loc_WAREHOUSE,gen_smith__idrp_batchdate;

filter_join_fltr_gen_smith__idrp_eligible_loc_WAREHOUSE_batchdate =
           FILTER
           join_fltr_gen_smith__idrp_eligible_loc_WAREHOUSE_batchdate
           BY((GetDateFromTimeStamp(loc_cls_dt) > batch_dt) OR (GetDateFromTimeStamp(loc_cls_dt) == '1970-01-01'));


gen_smith__idrp_eligible_loc_WAREHOUSE = 
	FOREACH
	filter_join_fltr_gen_smith__idrp_eligible_loc_WAREHOUSE_batchdate
	GENERATE
	loc AS location_id,
        TrimLeadingZeros(srs_loc) AS sears_location_id,
        loc_lvl_cd,
        loc_fmt_typ_cd,
        loc_owner_cd AS location_owner_cd;
		
SPLIT gen_smith__idrp_eligible_loc_WAREHOUSE into 
	gen_smith__idrp_eligible_loc_WAREHOUSE_sears if IsNull(sears_location_id,'') != '',
	gen_smith__idrp_eligible_loc_WAREHOUSE_kmart if IsNull(sears_location_id,'') == '';

group_smith__idrp_eligible_loc_WAREHOUSE_sears = GROUP gen_smith__idrp_eligible_loc_WAREHOUSE_sears by sears_location_id;

limit_group_smith__idrp_eligible_loc_WAREHOUSE_sears = foreach group_smith__idrp_eligible_loc_WAREHOUSE_sears
									{ sorted = ORDER gen_smith__idrp_eligible_loc_WAREHOUSE_sears by location_owner_cd;
									  unq = limit sorted 1;
									  GENERATE FLATTEN(unq);
									};
									
union_WAREHOUSE_sears_kmart = union limit_group_smith__idrp_eligible_loc_WAREHOUSE_sears, gen_smith__idrp_eligible_loc_WAREHOUSE_kmart;		
									
fltr_gen_smith__idrp_eligible_loc_VENDOR = 
        FILTER
        gen_smith__idrp_eligible_loc
        BY(TRIM(loc_lvl_cd)=='VENDOR'
                AND
           TRIM(duns_owner_cd)=='S'
                AND
           TRIM(duns_type_cd)=='ORD');

gen_smith__idrp_eligible_loc_VENDOR = 
	FOREACH
	fltr_gen_smith__idrp_eligible_loc_VENDOR
	GENERATE
	loc AS location_id,
	TrimLeadingZeros(srs_vndr_nbr) AS sears_location_id,
	loc_lvl_cd,
	loc_fmt_typ_cd,
	duns_owner_cd AS location_owner_cd;

un_fltr_gen_smith__idrp_eligible_loc = 
        UNION
        gen_fltr_smith__idrp_eligible_loc_STORE,
        union_WAREHOUSE_sears_kmart,
        gen_smith__idrp_eligible_loc_VENDOR;

join_grp_eligible_loc_vendr_pkg_shared = 
	JOIN
	un_fltr_gen_smith__idrp_eligible_loc
	BY(sears_location_id)
	LEFT OUTER,
	work__idrp_item_eligibility_sears_vendor_package_ta_shared_location
	BY(TrimLeadingZeros(sears_location_nbr));

work__idrp_sears_location_xref = 
	FOREACH
	join_grp_eligible_loc_vendr_pkg_shared
	GENERATE
	un_fltr_gen_smith__idrp_eligible_loc::location_id AS location_id,
	un_fltr_gen_smith__idrp_eligible_loc::sears_location_id AS sears_location_id,
	un_fltr_gen_smith__idrp_eligible_loc::loc_lvl_cd AS location_level_cd,
	un_fltr_gen_smith__idrp_eligible_loc::location_owner_cd AS location_owner_cd,
	un_fltr_gen_smith__idrp_eligible_loc::loc_fmt_typ_cd AS location_format_type_cd,
	work__idrp_item_eligibility_sears_vendor_package_ta_shared_location::store_type_cd AS cross_merchandise_store_type_cd;


STORE work__idrp_sears_location_xref
INTO '$WORK__IDRP_SEARS_LOCATION_XREF_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


fltr_smith__idrp_i2k_valid_rebuy_vendor_package_ids_current = 
	FILTER
	smith__idrp_i2k_valid_rebuy_vendor_package_ids_current
	BY(TRIM(owner_cd)=='SRS');


gen_smith__idrp_vend_pack_combined = 
	FOREACH
	smith__idrp_vend_pack_combined	
	GENERATE
	sears_division_nbr,
	sears_item_nbr,
	sears_sku_nbr,
	purchase_status_cd,
	purchase_status_dt,
	vendor_package_id,
	shc_item_id;

-- CR 3451 --Added filter logic
filter_gen_smith__idrp_vend_pack_combined = FILTER
                                            gen_smith__idrp_vend_pack_combined
                                            BY((sears_division_nbr IS NOT NULL AND sears_division_nbr !='')AND 
                                                (sears_item_nbr IS NOT NULL AND sears_item_nbr !='') AND (sears_sku_nbr IS NOT NULL AND sears_sku_nbr !=''));


join_vend_pack_combnd_pkg_id_currnt = 
	JOIN
	fltr_smith__idrp_i2k_valid_rebuy_vendor_package_ids_current
	BY((long)vendor_pack_id),
	filter_gen_smith__idrp_vend_pack_combined
	BY((long)vendor_package_id);



grp_join_vend_pack_combnd_pkg_id_currnt = 
	GROUP
	join_vend_pack_combnd_pkg_id_currnt
	BY(filter_gen_smith__idrp_vend_pack_combined::sears_division_nbr,filter_gen_smith__idrp_vend_pack_combined::sears_item_nbr,filter_gen_smith__idrp_vend_pack_combined::sears_sku_nbr);


unq_grp_join_vend_pack_combnd_pkg_id_currnt = 
	FOREACH
	grp_join_vend_pack_combnd_pkg_id_currnt{
		sorted = ORDER join_vend_pack_combnd_pkg_id_currnt BY create_dt desc, last_order_dt desc, 
				filter_gen_smith__idrp_vend_pack_combined::purchase_status_cd, filter_gen_smith__idrp_vend_pack_combined::purchase_status_dt desc,filter_gen_smith__idrp_vend_pack_combined::vendor_package_id desc ;
		unq = LIMIT sorted 1;
		GENERATE FLATTEN(unq);
		};

join_vend_pack_combnd_pkg_id_currnt_xref = 
	JOIN
	work__idrp_sears_location_xref
	BY((long)sears_location_id),
	unq_grp_join_vend_pack_combnd_pkg_id_currnt
	BY((long)sears_vendor_duns_nbr);
	
filter_join_vend_pack_combnd_pkg_id_currnt_xref = filter join_vend_pack_combnd_pkg_id_currnt_xref by TRIM(location_level_cd) == 'VENDOR';


smith__idrp_i2k_sears_rebuy_vendor_package_current = 
	FOREACH
	filter_join_vend_pack_combnd_pkg_id_currnt_xref
	GENERATE
	filter_gen_smith__idrp_vend_pack_combined::sears_division_nbr AS sears_division_nbr,
	filter_gen_smith__idrp_vend_pack_combined::sears_item_nbr AS sears_item_nbr,
	filter_gen_smith__idrp_vend_pack_combined::sears_sku_nbr AS sears_sku_nbr,
	unq_grp_join_vend_pack_combnd_pkg_id_currnt::unq::fltr_smith__idrp_i2k_valid_rebuy_vendor_package_ids_current::shc_item_id AS shc_item_id,
	unq_grp_join_vend_pack_combnd_pkg_id_currnt::unq::fltr_smith__idrp_i2k_valid_rebuy_vendor_package_ids_current::ksn_id AS ksn_id,
	unq_grp_join_vend_pack_combnd_pkg_id_currnt::unq::filter_gen_smith__idrp_vend_pack_combined::vendor_package_id AS vendor_package_id,
	unq_grp_join_vend_pack_combnd_pkg_id_currnt::unq::fltr_smith__idrp_i2k_valid_rebuy_vendor_package_ids_current::sears_vendor_duns_nbr AS sears_vendor_duns_nbr,
	unq_grp_join_vend_pack_combnd_pkg_id_currnt::unq::fltr_smith__idrp_i2k_valid_rebuy_vendor_package_ids_current::source_pack_qty AS source_pack_qty,
	unq_grp_join_vend_pack_combnd_pkg_id_currnt::unq::fltr_smith__idrp_i2k_valid_rebuy_vendor_package_ids_current::create_dt AS create_dt,
	unq_grp_join_vend_pack_combnd_pkg_id_currnt::unq::fltr_smith__idrp_i2k_valid_rebuy_vendor_package_ids_current::last_order_dt AS last_order_dt,
	unq_grp_join_vend_pack_combnd_pkg_id_currnt::unq::filter_gen_smith__idrp_vend_pack_combined::purchase_status_cd AS purchase_status_cd,
	unq_grp_join_vend_pack_combnd_pkg_id_currnt::unq::filter_gen_smith__idrp_vend_pack_combined::purchase_status_dt AS purchase_status_dt,
	unq_grp_join_vend_pack_combnd_pkg_id_currnt::unq::fltr_smith__idrp_i2k_valid_rebuy_vendor_package_ids_current::owner_cd AS owner_cd,
	work__idrp_sears_location_xref::location_id AS location_id;


STORE 
smith__idrp_i2k_sears_rebuy_vendor_package_current
INTO '$SMITH__IDRP_I2K_SEARS_REBUY_VENDOR_PACKAGE_CURRENT_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

