/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_loc_format.pig
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       Mon Oct 14 05:09:17 EDT 2013
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
#		 2015-04-22   Meghana		CR 4171 (Added additional data requirements to support Rapid processing)
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

--register the jar containing all PIG UDFs
REGISTER $UDF_JAR;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

--load existing data
existing_data = 
    LOAD '$SMITH__IDRP_ELIGIBLE_LOC_INCOMING_LOCATION' 
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
    AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);

--apply formatting to each field              
formatted_data = 
    FOREACH existing_data
    GENERATE 
        TrimLeadingZeros(TRIM(loc)),
        TRIM(srs_loc),
        TRIM(shc_vndr_nbr),
        TRIM(srs_vndr_nbr),
        TRIM(descr),
        TRIM(loc_opn_dt),
        TRIM(loc_cls_dt),
        TRIM(loc_temp_opn_dt),
        TRIM(loc_temp_cls_dt),
        TRIM(loc_lvl_cd),
        TRIM(loc_fmt_typ_cd),
        TRIM(fmt_typ_cd),
        TRIM(fmt_sub_typ_cd),
        TRIM(fmt_mod_cd),
        TRIM(loc_cty),
        TRIM(loc_ste_cd),
        TRIM(loc_zip_cd),
        TRIM(region_cd),
        TRIM(region_nm),
        TRIM(district_cd),
        TRIM(district_nm),
        TRIM(climazone_cd),
        TRIM(climazone_nm),
        TRIM(merch_area_cd),
        TRIM(merch_area_nm),
        TRIM(elig_usr_cd),
        (IsNull(TRIM(elig_fnl_cd),'') == ''
			? 'A' 
			: TRIM(elig_fnl_cd)) AS elig_fnl_cd,
        TRIM(duns_type_cd),
        TRIM(duns_owner_cd),
        TRIM(loc_owner_cd),
		TRIM(parent_vendor_nbr),
		TRIM(parent_vendor_nm),
		TRIM(edi_route_vendor_nbr),
		TRIM(edi_route_cd),
		TRIM(edi_830_eligible_fl),
		TRIM(edi_862_eligible_fl),
		'$CURRENT_TIMESTAMP' AS load_ts,
        '$batchid' AS idrp_batch_id;
               
--store formatted data
STORE formatted_data 
INTO '$SMITH__IDRP_ELIGIBLE_LOC_WORK_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
