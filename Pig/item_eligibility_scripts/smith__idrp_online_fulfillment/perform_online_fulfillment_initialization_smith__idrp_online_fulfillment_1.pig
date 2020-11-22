/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_online_fulfillment_initialization_smith__idrp_online_fulfillment_1.pig
# AUTHOR NAME:         Mudit Mangal
# CREATION DATE:       Mon July 07 05:25:42 EST 2014
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
#        DATE         BY                     MODIFICATION
#  22/01/2015		Siddhivinayak Karpe	     CR#3628 Source Changed from smith__idrp_ie_item_combined_hierarchy_all_current to 
#									         work__idrp_item_hierarchy_combined_all_current 
#  27/03/2015       Meghana Dhage            CR#3703 Updated code to separate Sears and Kmart Online Fulfillment Type Codes
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

kmart_online_fulfillmnt = 
	LOAD '$SMITH__IDRP_OBU_DEFAULT_FULFILLMENT_KMART_DAILY_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
			AS ($SMITH__IDRP_OBU_DEFAULT_FULFILLMENT_KMART_DAILY_SCHEMA);

sears_online_fulfillmnt = 
	LOAD '$SMITH__IDRP_OBU_DEFAULT_FULFILLMENT_SEARS_DAILY_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
			AS ($SMITH__IDRP_OBU_DEFAULT_FULFILLMENT_SEARS_DAILY_SCHEMA);

smith__item_combined_hierarchy_current = 
	LOAD '$WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 
			AS ($WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_SCHEMA);
			
smith__item_combined_hierarchy_current_gen = 
	FOREACH smith__item_combined_hierarchy_current 
	GENERATE
		shc_item_id AS shc_item_id,
		ksn_id AS ksn_id,
		sears_division_nbr AS sears_division_nbr,
		sears_item_nbr AS sears_item_nbr,
		sears_sku_nbr AS sears_sku_nbr;

/* CR 3703 */

/* Sears Online Fulfillment file */

sears_online_fulfillmnt_filter = 
	FILTER sears_online_fulfillmnt
	BY (default_fulfillment_type_cd == 'TW' OR
		default_fulfillment_type_cd == 'VD' OR
		default_fulfillment_type_cd == 'SPU' OR
		default_fulfillment_type_cd == 'DDC');
		
join_sears_online_fulfillmnt_smith__item_combined_hierarchy = 
	JOIN sears_online_fulfillmnt_filter
		BY ((int)sears_division_nbr,(int)sears_item_nbr,(int)sears_sku_nbr)
		LEFT OUTER,
		 smith__item_combined_hierarchy_current_gen
		BY ((int)sears_division_nbr,(int)sears_item_nbr,(int)sears_sku_nbr);
		
gen_1 = 
	FOREACH join_sears_online_fulfillmnt_smith__item_combined_hierarchy
	GENERATE
		(((IsNull(smith__item_combined_hierarchy_current_gen::ksn_id,'') != '') AND
		  (IsNull(sears_online_fulfillmnt_filter::sears_division_nbr,'') != '' AND
		   IsNull(sears_online_fulfillmnt_filter::sears_item_nbr,'') != '' AND
		   IsNull(sears_online_fulfillmnt_filter::sears_sku_nbr,'') != ''))
			? smith__item_combined_hierarchy_current_gen::ksn_id
			: '') AS ksn_id,
		sears_online_fulfillmnt_filter::sears_division_nbr AS sears_division_nbr,
		sears_online_fulfillmnt_filter::sears_item_nbr AS sears_item_nbr,
		sears_online_fulfillmnt_filter::sears_sku_nbr AS sears_sku_nbr,
		'' AS web_sku_id,
		upc_nbr AS upc_nbr,
		default_fulfillment_type_cd AS default_fulfillment_type,
		first_online_ts AS last_change_ts,
        web_exclusive_ind AS web_exclusive_ind;
		
/* Generating error file: KSN not found for Sears item in Sears Online Fulfillment file */

SPLIT gen_1 INTO
	valid_data_sears_1 IF ksn_id != '',
	error_data_sears_1 IF ksn_id == '';
	
smith__idrp_item_eligibility_online_process_error_gen_1 = 
	FOREACH error_data_sears_1 
	GENERATE 
		'$CURRENT_TIMESTAMP' AS load_ts,
		'' AS item_id,
		'' AS ksn_id,
		sears_division_nbr AS sears_division_nbr,
		sears_item_nbr AS sears_item_nbr,
		sears_sku_nbr AS sears_sku_nbr,
		'' AS websku,
		'' AS package_id,
		'' AS error_value,
		'KSN not found for Sears item in Sears Online Fulfillment file' AS error_desc,
		'$batchid' AS idrp_batch_id;
																
join_valid_data_sears_smith__item_combined_hierarchy = 
	JOIN valid_data_sears_1
		BY ((int)ksn_id) LEFT OUTER,
		 smith__item_combined_hierarchy_current_gen
		BY ((int)ksn_id);
		
join_valid_data_sears_smith__item_combined_hierarchy_gen = 
	FOREACH join_valid_data_sears_smith__item_combined_hierarchy
	GENERATE
		shc_item_id AS item_id,
		valid_data_sears_1::ksn_id AS ksn_id,
		valid_data_sears_1::sears_division_nbr AS sears_division_nbr,
		valid_data_sears_1::sears_item_nbr AS sears_item_nbr,
		valid_data_sears_1::sears_sku_nbr AS sears_sku_nbr,
		valid_data_sears_1::web_sku_id AS web_sku_id,
		valid_data_sears_1::upc_nbr AS upc_nbr,
		valid_data_sears_1::default_fulfillment_type AS default_fulfillment_type,
		valid_data_sears_1::last_change_ts AS last_change_ts,
		valid_data_sears_1::web_exclusive_ind AS web_exclusive_ind;
		
/* Generating error file: SHC ITEM not found for KSN in Sears Online Fulfillment file */
		
SPLIT join_valid_data_sears_smith__item_combined_hierarchy_gen INTO
	valid_data_sears_2 IF (IsNull(item_id,'') != ''),
	error_data_sears_2 IF (IsNull(item_id,'') == '');
	
smith__idrp_item_eligibility_online_process_error_gen_2 = 
	FOREACH error_data_sears_2 
	GENERATE 
		'$CURRENT_TIMESTAMP' AS load_ts,
		'' AS item_id,
		ksn_id AS ksn_id,
		'' AS sears_division_nbr,
		'' AS sears_item_nbr,
		'' AS sears_sku_nbr,
		'' AS websku,
		'' AS package_id,
		'' AS error_value,
		'SHC ITEM not found for KSN in Sears Online Fulfillment file' AS error_desc,
		'$batchid' AS idrp_batch_id;
		
valid_data_sears_2_gen = 
	FOREACH valid_data_sears_2
	GENERATE
		item_id,
		ksn_id,
		sears_division_nbr,
		sears_item_nbr,
		sears_sku_nbr,
		web_sku_id,
		upc_nbr,
		default_fulfillment_type,
		last_change_ts,
		web_exclusive_ind;
		
/* Generating error file: Multiple Default Fulfillment Types found at Item level */
		
default_fulfillment_type_chk = 
	GROUP valid_data_sears_2_gen 
	BY item_id;

default_fulfillment_type_chk_gen = 
	FOREACH default_fulfillment_type_chk 
	GENERATE 
		group AS item_id,
		com.searshc.supplychain.idrp.udf.HasMultipleValues(valid_data_sears_2_gen.default_fulfillment_type) AS default_fulfillment_type;

final_default_fulfillment_type_data = 
	JOIN valid_data_sears_2_gen 
		BY item_id, 
		 default_fulfillment_type_chk_gen 
		BY item_id;

final_default_fulfillment_type_data_gen = 
	FOREACH final_default_fulfillment_type_data 
	GENERATE 
		valid_data_sears_2_gen::item_id AS item_id,
		valid_data_sears_2_gen::ksn_id AS ksn_id,
		valid_data_sears_2_gen::sears_division_nbr AS sears_division_nbr,
		valid_data_sears_2_gen::sears_item_nbr AS sears_item_nbr,
		valid_data_sears_2_gen::sears_sku_nbr AS sears_sku_nbr,
		valid_data_sears_2_gen::web_sku_id AS web_sku_id,
		valid_data_sears_2_gen::upc_nbr AS upc_nbr,
		(default_fulfillment_type_chk_gen::default_fulfillment_type == 'MULTIPLE' 
			? 'NONE' 
			: valid_data_sears_2_gen::default_fulfillment_type) AS default_fulfillment_type,
		valid_data_sears_2_gen::last_change_ts AS last_change_ts,
		valid_data_sears_2_gen::web_exclusive_ind AS web_exclusive_ind,
		(default_fulfillment_type_chk_gen::default_fulfillment_type == 'MULTIPLE' 
			? valid_data_sears_2_gen::default_fulfillment_type 
			: default_fulfillment_type_chk_gen::default_fulfillment_type) AS error_value;

default_fulfillment_type_error = 
	FILTER final_default_fulfillment_type_data_gen 
	BY default_fulfillment_type == 'NONE';

smith__idrp_item_eligibility_online_process_error_gen_3 = 
	FOREACH default_fulfillment_type_error 
	GENERATE 
		'$CURRENT_TIMESTAMP' AS load_ts,
		item_id AS item_id,
		ksn_id AS ksn_id,
		'' AS sears_division_nbr,
		'' AS sears_item_nbr,
		'' AS sears_sku_nbr,
		'' AS websku,
		'' AS package_id,
		error_value AS error_value,
		'Multiple Default Fulfillment Types found for an Item in Sears Online Fulfillment file' AS error_desc,
		'$batchid' AS idrp_batch_id;
		
/* Generating Valid File: Multiple Default Fulfillment Types found at Item level */
 		
join_valid_data_sears_2_final_default_fulfillment_type_data = 
	JOIN valid_data_sears_2_gen BY (item_id, ksn_id),
		 final_default_fulfillment_type_data_gen BY (item_id, ksn_id);
		
default_fulfillment_type_data_TW =
	FILTER join_valid_data_sears_2_final_default_fulfillment_type_data
	BY (final_default_fulfillment_type_data_gen::default_fulfillment_type == 'NONE' AND    
		final_default_fulfillment_type_data_gen::error_value == 'TW');
			
join_valid_data_sears_2_final_default_fulfillment_type_data_gen	=
	FOREACH join_valid_data_sears_2_final_default_fulfillment_type_data
	GENERATE
		valid_data_sears_2_gen::item_id AS item_id,
		valid_data_sears_2_gen::ksn_id AS ksn_id,
		valid_data_sears_2_gen::sears_division_nbr AS sears_division_nbr,
		valid_data_sears_2_gen::sears_item_nbr AS sears_item_nbr,
		valid_data_sears_2_gen::sears_sku_nbr AS sears_sku_nbr,
		valid_data_sears_2_gen::web_sku_id AS web_sku_id,
		valid_data_sears_2_gen::upc_nbr AS upc_nbr,		
		final_default_fulfillment_type_data_gen::default_fulfillment_type AS default_fulfillment_type,
		valid_data_sears_2_gen::last_change_ts AS last_change_ts,
		valid_data_sears_2_gen::web_exclusive_ind AS web_exclusive_ind,
		final_default_fulfillment_type_data_gen::error_value AS error_value;
		
default_fulfillment_type_data_TW_gen = 
	FOREACH default_fulfillment_type_data_TW
	GENERATE
		final_default_fulfillment_type_data_gen::item_id AS item_id,
		final_default_fulfillment_type_data_gen::ksn_id AS ksn_id,
		final_default_fulfillment_type_data_gen::default_fulfillment_type AS default_fulfillment_type,
		final_default_fulfillment_type_data_gen::error_value AS error_value;
		
grp_default_fulfillment_type_data_TW = 
	GROUP default_fulfillment_type_data_TW_gen 
	BY item_id;
	
gen_grp_default_fulfillment_type_data_TW =
	FOREACH grp_default_fulfillment_type_data_TW
	{	
		a = LIMIT default_fulfillment_type_data_TW_gen 1;
		GENERATE FLATTEN(a);
	};

join_default_fulfillment_type_data_TW = 
	JOIN join_valid_data_sears_2_final_default_fulfillment_type_data_gen
		BY (item_id) LEFT OUTER,
		 gen_grp_default_fulfillment_type_data_TW
		BY (item_id);
		
join_default_fulfillment_type_data_TW_filter = 
	FILTER join_default_fulfillment_type_data_TW
	BY ((IsNull(gen_grp_default_fulfillment_type_data_TW::a::item_id,'') != '') AND 
	   (join_valid_data_sears_2_final_default_fulfillment_type_data_gen::error_value == 'TW')) OR
	   (IsNull(gen_grp_default_fulfillment_type_data_TW::a::item_id,'') == '');
	   
valid_data_sears_3 = 
	FOREACH join_default_fulfillment_type_data_TW_filter
	GENERATE
		join_valid_data_sears_2_final_default_fulfillment_type_data_gen::item_id AS item_id,
		join_valid_data_sears_2_final_default_fulfillment_type_data_gen::ksn_id AS ksn_id,
		join_valid_data_sears_2_final_default_fulfillment_type_data_gen::sears_division_nbr AS sears_division_nbr,
		join_valid_data_sears_2_final_default_fulfillment_type_data_gen::sears_item_nbr AS sears_item_nbr,
		join_valid_data_sears_2_final_default_fulfillment_type_data_gen::sears_sku_nbr AS sears_sku_nbr,
		join_valid_data_sears_2_final_default_fulfillment_type_data_gen::web_sku_id AS web_sku_id,
		join_valid_data_sears_2_final_default_fulfillment_type_data_gen::upc_nbr AS upc_nbr,
		((join_valid_data_sears_2_final_default_fulfillment_type_data_gen::default_fulfillment_type == 'NONE' AND
		 join_valid_data_sears_2_final_default_fulfillment_type_data_gen::error_value == 'TW')
			? 'TW'
			: ((join_valid_data_sears_2_final_default_fulfillment_type_data_gen::default_fulfillment_type == 'NONE' AND
			   join_valid_data_sears_2_final_default_fulfillment_type_data_gen::error_value != 'TW')
					? 'NONE'
					: join_valid_data_sears_2_final_default_fulfillment_type_data_gen::default_fulfillment_type)) AS default_fulfillment_type,
		join_valid_data_sears_2_final_default_fulfillment_type_data_gen::last_change_ts AS last_change_ts,
		join_valid_data_sears_2_final_default_fulfillment_type_data_gen::web_exclusive_ind AS web_exclusive_ind;
			   
/* Generating error file: Multiple Web Exclusive Flag found at Item level */

web_exclusive_ind_chk = 
	GROUP valid_data_sears_3 
	BY item_id;

web_exclusive_ind_chk_gen = 
	FOREACH web_exclusive_ind_chk 
	GENERATE 
		group AS item_id,
		com.searshc.supplychain.idrp.udf.HasMultipleValues(valid_data_sears_3.web_exclusive_ind) AS web_exclusive_ind;

final_web_exclusive_ind_data = 
	JOIN valid_data_sears_3 
		BY item_id, 
		 web_exclusive_ind_chk_gen 
		BY item_id;

final_web_exclusive_ind_data_gen = 
	FOREACH final_web_exclusive_ind_data 
	GENERATE 
		valid_data_sears_3::item_id AS item_id,
		valid_data_sears_3::ksn_id AS ksn_id,
		valid_data_sears_3::sears_division_nbr AS sears_division_nbr,
		valid_data_sears_3::sears_item_nbr AS sears_item_nbr,
		valid_data_sears_3::sears_sku_nbr AS sears_sku_nbr,
		valid_data_sears_3::web_sku_id AS web_sku_id,
		valid_data_sears_3::upc_nbr AS upc_nbr,
		valid_data_sears_3::default_fulfillment_type AS default_fulfillment_type,
		valid_data_sears_3::last_change_ts AS last_change_ts,
		valid_data_sears_3::web_exclusive_ind AS web_exclusive_ind_sears_valid_value,
		web_exclusive_ind_chk_gen::web_exclusive_ind AS web_exclusive_ind_sears_error_value,
        (web_exclusive_ind_chk_gen::web_exclusive_ind == 'MULTIPLE' 
			? valid_data_sears_3::web_exclusive_ind 
			: web_exclusive_ind_chk_gen::web_exclusive_ind) AS error_value;

web_exclusive_ind_error = 
	FILTER final_web_exclusive_ind_data_gen 
	BY web_exclusive_ind_sears_error_value == 'MULTIPLE';

smith__idrp_item_eligibility_online_process_error_gen_4 = 
	FOREACH web_exclusive_ind_error 
	GENERATE 
		'$CURRENT_TIMESTAMP' AS load_ts,
		item_id AS item_id,
		ksn_id AS ksn_id,
		'' AS sears_division_nbr,
		'' AS sears_item_nbr,
		'' AS sears_sku_nbr,
		'' AS websku,
		'' AS package_id,
		error_value AS error_value,
		'Multiple Web Exclusive Flag values found for an Item in Sears Online Fulfillment file' AS error_desc,
		'$batchid' AS idrp_batch_id;

valid_data_sears_4 = 
	FOREACH final_web_exclusive_ind_data_gen 
	GENERATE 
		item_id,
		ksn_id,
		sears_division_nbr,
		sears_item_nbr,
		sears_sku_nbr,
		web_sku_id,
		upc_nbr,
		default_fulfillment_type,
		last_change_ts,
		(web_exclusive_ind_sears_error_value == 'MULTIPLE'
			? 'N'
			: web_exclusive_ind_sears_valid_value) AS web_exclusive_ind;
		
/* Taking the first record for each shc_item_id */

grp_valid_data_sears_4 = 
	GROUP valid_data_sears_4
	BY item_id;
	
valid_data_sears = 
	FOREACH grp_valid_data_sears_4
	{
		ordered_data_sears = ORDER valid_data_sears_4 BY ksn_id ASC, last_change_ts DESC, upc_nbr DESC;
		first_record_sears = LIMIT ordered_data_sears 1;
		GENERATE FLATTEN(first_record_sears);
	};
	
gen_valid_data_sears = 
	FOREACH valid_data_sears
	GENERATE
		item_id AS item_id,
		ksn_id AS ksn_id,
		sears_division_nbr AS sears_division_nbr,
		sears_item_nbr AS sears_item_nbr,
		sears_sku_nbr AS sears_sku_nbr,
		web_sku_id AS web_sku_id,
		upc_nbr AS upc_nbr,
		default_fulfillment_type AS default_fulfillment_type,
		last_change_ts AS last_change_ts,
		web_exclusive_ind AS web_exclusive_ind;		
	
/* SEARS ERROR FILE */

union_error_data_sears = UNION
	smith__idrp_item_eligibility_online_process_error_gen_1,
	smith__idrp_item_eligibility_online_process_error_gen_2,
	smith__idrp_item_eligibility_online_process_error_gen_3,
	smith__idrp_item_eligibility_online_process_error_gen_4;
	
/* Kmart Online Fulfillment file */

kmart_online_fulfillmnt_filter = 
	FILTER kmart_online_fulfillmnt
	BY (default_fulfillment_type_cd == 'TW' OR
		default_fulfillment_type_cd == 'VD' OR
		default_fulfillment_type_cd == 'SPU' OR
		default_fulfillment_type_cd == 'KHD');
																		
join_valid_data_kmart_smith__item_combined_hierarchy = 
	JOIN kmart_online_fulfillmnt_filter
		BY ((int)ksn_id) LEFT OUTER,
		 smith__item_combined_hierarchy_current_gen
		BY ((int)ksn_id);
		
join_valid_data_kmart_smith__item_combined_hierarchy_gen = 
	FOREACH join_valid_data_kmart_smith__item_combined_hierarchy
	GENERATE
		shc_item_id AS item_id,
		smith__item_combined_hierarchy_current_gen::ksn_id AS ksn_id,
		kmart_online_fulfillmnt_filter::web_sku_id AS web_sku_id,
		kmart_online_fulfillmnt_filter::upc_nbr AS upc_nbr,
		kmart_online_fulfillmnt_filter::default_fulfillment_type_cd AS default_fulfillment_type,
		kmart_online_fulfillmnt_filter::first_online_ts AS last_change_ts,
		kmart_online_fulfillmnt_filter::web_exclusive_ind AS web_exclusive_ind;
		
/* Generating error file: SHC ITEM not found for KSN in Kmart Online Fulfillment file */
		
SPLIT join_valid_data_kmart_smith__item_combined_hierarchy_gen INTO
	valid_data_kmart_1 IF (IsNull(item_id,'') != ''),
	error_data_kmart_1 IF (IsNull(item_id,'') == '');
	
smith__idrp_item_eligibility_online_process_error_gen_5 = 
	FOREACH error_data_kmart_1 
	GENERATE 
		'$CURRENT_TIMESTAMP' AS load_ts,
		'' AS item_id,
		ksn_id AS ksn_id,
		'' AS sears_division_nbr,
		'' AS sears_item_nbr,
		'' AS sears_sku_nbr,
		'' AS websku,
		'' AS package_id,
		'' AS error_value,
		'SHC ITEM not found for KSN in Kmart Online Fulfillment file' AS error_desc,
		'$batchid' AS idrp_batch_id;
		
valid_data_kmart_1_gen = 
	FOREACH valid_data_kmart_1
	GENERATE
		item_id,
		ksn_id,
		web_sku_id,
		upc_nbr,
		default_fulfillment_type,
		last_change_ts,
		web_exclusive_ind;
		
/* Generating error file: Multiple Web Exclusive Flag values at Item level */

web_exclusive_ind_chk_kmart = 
	GROUP valid_data_kmart_1_gen 
	BY item_id;

web_exclusive_ind_chk_kmart_gen = 
	FOREACH web_exclusive_ind_chk_kmart 
	GENERATE 
		group AS item_id,
		com.searshc.supplychain.idrp.udf.HasMultipleValues(valid_data_kmart_1_gen.web_exclusive_ind) AS web_exclusive_ind;

final_web_exclusive_ind_kmart_data = 
	JOIN valid_data_kmart_1_gen 
		BY item_id, 
		 web_exclusive_ind_chk_kmart_gen 
		BY item_id;

final_web_exclusive_ind_kmart_data_gen = 
	FOREACH final_web_exclusive_ind_kmart_data 
	GENERATE 
		valid_data_kmart_1_gen::item_id AS item_id,
		valid_data_kmart_1_gen::ksn_id AS ksn_id,
		valid_data_kmart_1_gen::web_sku_id AS web_sku_id,
		valid_data_kmart_1_gen::upc_nbr AS upc_nbr,
		valid_data_kmart_1_gen::default_fulfillment_type AS default_fulfillment_type,
		valid_data_kmart_1_gen::last_change_ts AS last_change_ts,
		web_exclusive_ind_chk_kmart_gen::web_exclusive_ind AS web_exclusive_ind_kmart_error_value,
		valid_data_kmart_1_gen::web_exclusive_ind AS web_exclusive_ind_kmart_valid_value,
        (web_exclusive_ind_chk_kmart_gen::web_exclusive_ind == 'MULTIPLE' 
			? valid_data_kmart_1_gen::web_exclusive_ind 
			: web_exclusive_ind_chk_kmart_gen::web_exclusive_ind) AS error_value;

web_exclusive_ind_kmart_error = 
	FILTER final_web_exclusive_ind_kmart_data_gen 
	BY web_exclusive_ind_kmart_error_value == 'MULTIPLE';

smith__idrp_item_eligibility_online_process_error_gen_6 = 
	FOREACH web_exclusive_ind_kmart_error 
	GENERATE 
		'$CURRENT_TIMESTAMP' AS load_ts,
		item_id AS item_id,
		ksn_id AS ksn_id,
		'' AS sears_division_nbr,
		'' AS sears_item_nbr,
		'' AS sears_sku_nbr,
		'' AS websku,
		'' AS package_id,
		error_value AS error_value,
		'Multiple Web Exclusive Flag values found for an Item in Kmart Online Fulfillment file' AS error_desc,
		'$batchid' AS idrp_batch_id;

valid_data_kmart_2 = 
	FOREACH final_web_exclusive_ind_kmart_data_gen 
	GENERATE 
		item_id,
		ksn_id,
		web_sku_id,
		upc_nbr,
		default_fulfillment_type,
		last_change_ts,
		(web_exclusive_ind_kmart_error_value == 'MULTIPLE'
			? 'N'
			: web_exclusive_ind_kmart_valid_value) AS web_exclusive_ind;
		
/* Generating error file: Multiple Default Fulfillment Types found for a KSN level */
		
default_fulfillment_type_chk_kmart_1 = 
	GROUP valid_data_kmart_2 
	BY ksn_id;

default_fulfillment_type_chk_kmart_1_gen = 
	FOREACH default_fulfillment_type_chk_kmart_1 
	GENERATE 
		group AS ksn_id,
		com.searshc.supplychain.idrp.udf.HasMultipleValues(valid_data_kmart_2.default_fulfillment_type) AS default_fulfillment_type;

final_default_fulfillment_type_chk_kmart_1_data = 
	JOIN valid_data_kmart_2 
		BY ksn_id, 
		 default_fulfillment_type_chk_kmart_1_gen 
		BY ksn_id;

final_default_fulfillment_type_chk_kmart_1_data_gen = 
	FOREACH final_default_fulfillment_type_chk_kmart_1_data 
	GENERATE 
		valid_data_kmart_2::item_id AS item_id,
		valid_data_kmart_2::ksn_id AS ksn_id,
		valid_data_kmart_2::web_sku_id AS web_sku_id,
		valid_data_kmart_2::upc_nbr AS upc_nbr,
		(default_fulfillment_type_chk_kmart_1_gen::default_fulfillment_type == 'MULTIPLE' 
			? 'NONE' 
			: valid_data_kmart_2::default_fulfillment_type) AS default_fulfillment_type,
		valid_data_kmart_2::last_change_ts AS last_change_ts,
		valid_data_kmart_2::web_exclusive_ind AS web_exclusive_ind,
		(default_fulfillment_type_chk_kmart_1_gen::default_fulfillment_type == 'MULTIPLE' 
			? valid_data_kmart_2::default_fulfillment_type 
			: default_fulfillment_type_chk_kmart_1_gen::default_fulfillment_type) AS error_value;

default_fulfillment_type_kmart_1_error = 
	FILTER final_default_fulfillment_type_chk_kmart_1_data_gen 
	BY default_fulfillment_type == 'NONE';

smith__idrp_item_eligibility_online_process_error_gen_7 = 
	FOREACH default_fulfillment_type_kmart_1_error 
	GENERATE 
		'$CURRENT_TIMESTAMP' AS load_ts,
		item_id AS item_id,
		ksn_id AS ksn_id,
		'' AS sears_division_nbr,
		'' AS sears_item_nbr,
		'' AS sears_sku_nbr,
		'' AS websku,
		'' AS package_id,
		error_value AS error_value,
		'Multiple Default Fulfillment Types found for a KSN in Kmart Online Fulfillment file' AS error_desc,
		'$batchid' AS idrp_batch_id;

/* Generating Valid File: Multiple Default Fulfillment Types found for a KSN level */
 		
join_valid_data_kmart_2_final_default_fulfillment_type_data = 
	JOIN valid_data_kmart_2 BY (ksn_id, web_sku_id),
		 final_default_fulfillment_type_chk_kmart_1_data_gen BY (ksn_id, web_sku_id);
		
default_fulfillment_type_data_kmart_1_TW =
	FILTER join_valid_data_kmart_2_final_default_fulfillment_type_data
	BY (final_default_fulfillment_type_chk_kmart_1_data_gen::default_fulfillment_type == 'NONE' AND    
		final_default_fulfillment_type_chk_kmart_1_data_gen::error_value == 'TW');
			
join_valid_data_kmart_2_final_default_fulfillment_type_data_gen	=
	FOREACH join_valid_data_kmart_2_final_default_fulfillment_type_data
	GENERATE
		valid_data_kmart_2::item_id AS item_id,
		valid_data_kmart_2::ksn_id AS ksn_id,
		valid_data_kmart_2::web_sku_id AS web_sku_id,
		valid_data_kmart_2::upc_nbr AS upc_nbr,		
		final_default_fulfillment_type_chk_kmart_1_data_gen::default_fulfillment_type AS default_fulfillment_type,
		valid_data_kmart_2::last_change_ts AS last_change_ts,
		valid_data_kmart_2::web_exclusive_ind AS web_exclusive_ind,
		final_default_fulfillment_type_chk_kmart_1_data_gen::error_value AS error_value;
		
default_fulfillment_type_data_kmart_1_TW_gen = 
	FOREACH default_fulfillment_type_data_kmart_1_TW
	GENERATE
		final_default_fulfillment_type_chk_kmart_1_data_gen::ksn_id AS ksn_id,
		final_default_fulfillment_type_chk_kmart_1_data_gen::web_sku_id AS web_sku_id,
		final_default_fulfillment_type_chk_kmart_1_data_gen::default_fulfillment_type AS default_fulfillment_type,
		final_default_fulfillment_type_chk_kmart_1_data_gen::error_value AS error_value;
		
grp_default_fulfillment_type_data_kmart_1_TW = 
	GROUP default_fulfillment_type_data_kmart_1_TW_gen 
	BY ksn_id;
	
gen_default_fulfillment_type_data_kmart_1_TW = 
	FOREACH grp_default_fulfillment_type_data_kmart_1_TW
	{	
		a = LIMIT default_fulfillment_type_data_kmart_1_TW_gen 1;
		GENERATE FLATTEN(a);
	};

join_default_fulfillment_type_data_kmart_1_TW = 
	JOIN join_valid_data_kmart_2_final_default_fulfillment_type_data_gen
		BY (ksn_id) LEFT OUTER,
		 gen_default_fulfillment_type_data_kmart_1_TW
		BY (ksn_id);
		
join_default_fulfillment_type_data_kmart_1_TW_filter = 
	FILTER join_default_fulfillment_type_data_kmart_1_TW
	BY ((IsNull(gen_default_fulfillment_type_data_kmart_1_TW::a::ksn_id,'') != '') AND 
	   (join_valid_data_kmart_2_final_default_fulfillment_type_data_gen::error_value == 'TW')) OR
	   (IsNull(gen_default_fulfillment_type_data_kmart_1_TW::a::ksn_id,'') == '');
	   
valid_data_kmart_3 = 
	FOREACH join_default_fulfillment_type_data_kmart_1_TW_filter
	GENERATE
		join_valid_data_kmart_2_final_default_fulfillment_type_data_gen::item_id AS item_id,
		join_valid_data_kmart_2_final_default_fulfillment_type_data_gen::ksn_id AS ksn_id,
		join_valid_data_kmart_2_final_default_fulfillment_type_data_gen::web_sku_id AS web_sku_id,
		join_valid_data_kmart_2_final_default_fulfillment_type_data_gen::upc_nbr AS upc_nbr,
		((join_valid_data_kmart_2_final_default_fulfillment_type_data_gen::default_fulfillment_type == 'NONE' AND
		 join_valid_data_kmart_2_final_default_fulfillment_type_data_gen::error_value == 'TW')
			? 'TW'
			: ((join_valid_data_kmart_2_final_default_fulfillment_type_data_gen::default_fulfillment_type == 'NONE' AND
			   join_valid_data_kmart_2_final_default_fulfillment_type_data_gen::error_value != 'TW')
					? 'NONE'
					: join_valid_data_kmart_2_final_default_fulfillment_type_data_gen::default_fulfillment_type)) AS default_fulfillment_type,
		join_valid_data_kmart_2_final_default_fulfillment_type_data_gen::last_change_ts AS last_change_ts,
		join_valid_data_kmart_2_final_default_fulfillment_type_data_gen::web_exclusive_ind AS web_exclusive_ind;
		
/* Generating error file: Multiple Default Fulfillment Types found at Item level */
		
default_fulfillment_type_chk_kmart_2 = 
	GROUP valid_data_kmart_3 
	BY item_id;

default_fulfillment_type_chk_kmart_2_gen = 
	FOREACH default_fulfillment_type_chk_kmart_2 
	GENERATE 
		group AS item_id,
		com.searshc.supplychain.idrp.udf.HasMultipleValues(valid_data_kmart_3.default_fulfillment_type) AS default_fulfillment_type;

final_default_fulfillment_type_chk_kmart_2_data = 
	JOIN valid_data_kmart_3 
		BY item_id, 
		 default_fulfillment_type_chk_kmart_2_gen 
		BY item_id;

final_default_fulfillment_type_chk_kmart_2_data_gen = 
	FOREACH final_default_fulfillment_type_chk_kmart_2_data 
	GENERATE 
		valid_data_kmart_3::item_id AS item_id,
		valid_data_kmart_3::ksn_id AS ksn_id,
		valid_data_kmart_3::web_sku_id AS web_sku_id,
		valid_data_kmart_3::upc_nbr AS upc_nbr,
		(default_fulfillment_type_chk_kmart_2_gen::default_fulfillment_type == 'MULTIPLE' 
			? 'NONE' 
			: valid_data_kmart_3::default_fulfillment_type) AS default_fulfillment_type,
		valid_data_kmart_3::last_change_ts AS last_change_ts,
		valid_data_kmart_3::web_exclusive_ind AS web_exclusive_ind,
		(default_fulfillment_type_chk_kmart_2_gen::default_fulfillment_type == 'MULTIPLE' 
			? valid_data_kmart_3::default_fulfillment_type 
			: default_fulfillment_type_chk_kmart_2_gen::default_fulfillment_type) AS error_value;

default_fulfillment_type_kmart_2_error = 
	FILTER final_default_fulfillment_type_chk_kmart_2_data_gen 
	BY default_fulfillment_type == 'NONE';

smith__idrp_item_eligibility_online_process_error_gen_8 = 
	FOREACH default_fulfillment_type_kmart_2_error 
	GENERATE 
		'$CURRENT_TIMESTAMP' AS load_ts,
		item_id AS item_id,
		ksn_id AS ksn_id,
		'' AS sears_division_nbr,
		'' AS sears_item_nbr,
		'' AS sears_sku_nbr,
		'' AS websku,
		'' AS package_id,
		error_value AS error_value,
		'Multiple Default Fulfillment Types found for an Item in Kmart Online Fulfillment file' AS error_desc,
		'$batchid' AS idrp_batch_id;
		
/* Generating Valid File: Multiple Default Fulfillment Types found at Item level */
 		
join_valid_data_kmart_3_final_default_fulfillment_type_data = 
	JOIN valid_data_kmart_3 BY (item_id, ksn_id),
		 final_default_fulfillment_type_chk_kmart_2_data_gen BY (item_id, ksn_id);
		
default_fulfillment_type_data_kmart_2_TW =
	FILTER join_valid_data_kmart_3_final_default_fulfillment_type_data
	BY (final_default_fulfillment_type_chk_kmart_2_data_gen::default_fulfillment_type == 'NONE' AND    
		final_default_fulfillment_type_chk_kmart_2_data_gen::error_value == 'TW');
			
join_valid_data_kmart_3_final_default_fulfillment_type_data_gen	=
	FOREACH join_valid_data_kmart_3_final_default_fulfillment_type_data
	GENERATE
		valid_data_kmart_3::item_id AS item_id,
		valid_data_kmart_3::ksn_id AS ksn_id,
		valid_data_kmart_3::web_sku_id AS web_sku_id,
		valid_data_kmart_3::upc_nbr AS upc_nbr,		
		final_default_fulfillment_type_chk_kmart_2_data_gen::default_fulfillment_type AS default_fulfillment_type,
		valid_data_kmart_3::last_change_ts AS last_change_ts,
		valid_data_kmart_3::web_exclusive_ind AS web_exclusive_ind,
		final_default_fulfillment_type_chk_kmart_2_data_gen::error_value AS error_value;
		
default_fulfillment_type_data_kmart_2_TW_gen = 
	FOREACH default_fulfillment_type_data_kmart_2_TW
	GENERATE
		final_default_fulfillment_type_chk_kmart_2_data_gen::item_id AS item_id,
		final_default_fulfillment_type_chk_kmart_2_data_gen::ksn_id AS ksn_id,
		final_default_fulfillment_type_chk_kmart_2_data_gen::default_fulfillment_type AS default_fulfillment_type,
		final_default_fulfillment_type_chk_kmart_2_data_gen::error_value AS error_value;
		
grp_default_fulfillment_type_data_kmart_2_TW = 
	GROUP default_fulfillment_type_data_kmart_2_TW_gen 
	BY item_id;
	
gen_grp_default_fulfillment_type_data_kmart_2_TW = 
	FOREACH grp_default_fulfillment_type_data_kmart_2_TW
	{	
		a = LIMIT default_fulfillment_type_data_kmart_2_TW_gen 1;
		GENERATE FLATTEN(a);
	};

join_default_fulfillment_type_data_kmart_2_TW = 
	JOIN join_valid_data_kmart_3_final_default_fulfillment_type_data_gen
		BY (item_id) LEFT OUTER,
		 gen_grp_default_fulfillment_type_data_kmart_2_TW
		BY (item_id);
		
join_default_fulfillment_type_data_kmart_2_TW_filter = 
	FILTER join_default_fulfillment_type_data_kmart_2_TW
	BY ((IsNull(gen_grp_default_fulfillment_type_data_kmart_2_TW::a::item_id,'') != '') AND 
	   (join_valid_data_kmart_3_final_default_fulfillment_type_data_gen::error_value == 'TW')) OR
	   (IsNull(gen_grp_default_fulfillment_type_data_kmart_2_TW::a::item_id,'') == '');
	   
valid_data_kmart_4 = 
	FOREACH join_default_fulfillment_type_data_kmart_2_TW_filter
	GENERATE
		join_valid_data_kmart_3_final_default_fulfillment_type_data_gen::item_id AS item_id,
		join_valid_data_kmart_3_final_default_fulfillment_type_data_gen::ksn_id AS ksn_id,
		join_valid_data_kmart_3_final_default_fulfillment_type_data_gen::web_sku_id AS web_sku_id,
		join_valid_data_kmart_3_final_default_fulfillment_type_data_gen::upc_nbr AS upc_nbr,
		((join_valid_data_kmart_3_final_default_fulfillment_type_data_gen::default_fulfillment_type == 'NONE' AND
		 join_valid_data_kmart_3_final_default_fulfillment_type_data_gen::error_value == 'TW')
			? 'TW'
			: ((join_valid_data_kmart_3_final_default_fulfillment_type_data_gen::default_fulfillment_type == 'NONE' AND
			   join_valid_data_kmart_3_final_default_fulfillment_type_data_gen::error_value != 'TW')
					? 'NONE'
					: join_valid_data_kmart_3_final_default_fulfillment_type_data_gen::default_fulfillment_type)) AS default_fulfillment_type,
		join_valid_data_kmart_3_final_default_fulfillment_type_data_gen::last_change_ts AS last_change_ts,
		join_valid_data_kmart_3_final_default_fulfillment_type_data_gen::web_exclusive_ind AS web_exclusive_ind;
		
/* Taking the first record for each shc_item_id */

grp_valid_data_kmart_4 = 
	GROUP valid_data_kmart_4
	BY item_id;
	
valid_data_kmart = 
	FOREACH grp_valid_data_kmart_4
	{
		ordered_data_kmart = ORDER valid_data_kmart_4 BY ksn_id ASC, last_change_ts DESC, upc_nbr DESC;
		first_record_kmart = LIMIT ordered_data_kmart 1;
		GENERATE FLATTEN(first_record_kmart);
	};
	
gen_valid_data_kmart = 
	FOREACH valid_data_kmart
	GENERATE
		item_id AS item_id,
		ksn_id AS ksn_id,
		web_sku_id AS web_sku_id,
		upc_nbr AS upc_nbr,
		default_fulfillment_type AS default_fulfillment_type,
		last_change_ts AS last_change_ts,
		web_exclusive_ind AS web_exclusive_ind;
		
/* KMART ERROR FILE */

union_error_data_kmart = UNION
	smith__idrp_item_eligibility_online_process_error_gen_5,
	smith__idrp_item_eligibility_online_process_error_gen_6,
	smith__idrp_item_eligibility_online_process_error_gen_7,
	smith__idrp_item_eligibility_online_process_error_gen_8;


/* Merging SEARS and KMART files */

full_outer_join = 
	JOIN gen_valid_data_sears
		BY (item_id) FULL OUTER,
		 gen_valid_data_kmart
		BY (item_id);
		
final_valid_data = 
	FOREACH full_outer_join
	GENERATE
		GetCurrentDate() AS load_ts,
		(IsNull(gen_valid_data_sears::item_id,'') != ''
			? gen_valid_data_sears::item_id
			: gen_valid_data_kmart::item_id) AS item_id,
		((IsNull(gen_valid_data_sears::item_id,'') != '' AND IsNull(gen_valid_data_kmart::item_id,'') != '')
			? (((int)gen_valid_data_sears::ksn_id >= (int)gen_valid_data_kmart::ksn_id)
				? gen_valid_data_sears::ksn_id
				: gen_valid_data_kmart::ksn_id)
			: ((IsNull(gen_valid_data_sears::item_id,'') != '' AND IsNull(gen_valid_data_kmart::item_id,'') == '')
				? gen_valid_data_sears::ksn_id
				: gen_valid_data_kmart::ksn_id)) AS ksn_id,
		(IsNull(gen_valid_data_sears::item_id,'') != ''
			? gen_valid_data_sears::sears_division_nbr
			: '') AS sears_division_nbr,
		(IsNull(gen_valid_data_sears::item_id,'') != ''
			? gen_valid_data_sears::sears_item_nbr
			: '') AS sears_item_nbr,		
		(IsNull(gen_valid_data_sears::item_id,'') != ''
			? gen_valid_data_sears::sears_sku_nbr
			: '') AS sears_sku_nbr,	
		(IsNull(gen_valid_data_kmart::item_id,'') != ''
			? gen_valid_data_kmart::web_sku_id : '') AS web_sku_id,	
		((IsNull(gen_valid_data_sears::item_id,'') != '' AND IsNull(gen_valid_data_kmart::item_id,'') != '')
			? (((long)gen_valid_data_sears::upc_nbr >= (long)gen_valid_data_kmart::upc_nbr)
				? gen_valid_data_sears::upc_nbr
				: gen_valid_data_kmart::upc_nbr)
			: ((IsNull(gen_valid_data_sears::item_id,'') != '' AND IsNull(gen_valid_data_kmart::item_id,'') == '')
				? gen_valid_data_sears::upc_nbr
				: gen_valid_data_kmart::upc_nbr)) AS upc_nbr,	
		gen_valid_data_sears::default_fulfillment_type AS sears_default_online_fulfillment_type_cd,
		gen_valid_data_sears::last_change_ts AS sears_default_online_fulfillment_type_cd_ts,
		'' AS sears_temporary_online_fulfillment_type_cd,
		gen_valid_data_kmart::default_fulfillment_type AS kmart_default_online_fulfillment_type_cd,
		gen_valid_data_kmart::last_change_ts AS kmart_default_online_fulfillment_type_cd_ts,
		'' AS kmart_temporary_online_fulfillment_type_cd,
		(IsNull(gen_valid_data_sears::web_exclusive_ind,'') == 'Y' OR IsNull(gen_valid_data_kmart::web_exclusive_ind,'') == 'Y'
			? 'Y'
			: 'N') AS web_exclusive_ind,
		'$batchid' AS idrp_batch_id;
				
/* Merging SEARS and KMART ERROR files */

smith__idrp_item_eligibility_online_process_error_final = UNION
	union_error_data_sears,
	union_error_data_kmart;
	
/* STORING OUTPUTS */
		
STORE final_valid_data 
	INTO '$TEMP_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

STORE smith__idrp_item_eligibility_online_process_error_final 
	INTO '$WORK_ERROR_LOCATION' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A');
	
/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/

