/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:        perform_ie_master_data_smith__idrp_ksn_attribute_current.pig
# AUTHOR NAME:         Bhagyashree Phapale
# CREATION DATE:       6/11/2014
# CURRENT REVISION NO: 1
#
# DESCRIPTION:  The pig script retrieve IDRP Eligible KSN Attributes (smith__idrp_ksn_attribute_current) for item Eligibility
#                               Buid Master Data based on the filter criteria and transformation that are applied to the hadoop input file as
#                               per the functional requirements.
#
# Param files  - smith__idrp_item_eligibility_batchdate.schema
#                               work__idrp_ie_item_hierarchy_combined_all_current.schema
#                               gold__item_attribute_relate_current.schema
#                               gold__item_sears_channel_distribution_current.schema
#                               work__idrp_ksn_core_bridge_item.schema
#                               gold__item_vendor_package_current.schema
#                               work__idrp_prod_mgrtn.schema
#
#
# DEPENDENCIES:
#
#
# REV LIST:
# DATE          BY            	 MODIFICATION
# 2015-06-22   	Khim          	 CR  4468
# 2014-08-12   	Meghana       	 CR  2789 and Defect 2776
# 2015-08-28   	Meghana       	 CR  5033 (Set sears_import_ind = '0')
# 2016-11-29   	Bhagwan       	 IPS 734  (Kmart constrained items appearing as domestic)
# 08/02/2018   	Piyush Solanki   IPS-3157: Populate amazon_brand_attribute_cd column in attribute file
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/
SET job.name 'perform_ie_master_data_smith__idrp_ksn_attribute_current.pig';
REGISTER $UDF_JAR
SET default_parallel $NUM_PARALLEL;

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

---------------------------------  LOADS --------------------------------------

-- work__idrp_item_hierarchy_combined_all_current

LOAD_CMB_HIERARCHY = load '$WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_LOCATION' USING PigStorage('$work__idrp_item_hierarchy_combined_all_current_delimiter') as ($WORK__IDRP_ITEM_HIERARCHY_COMBINED_ALL_CURRENT_SCHEMA);

CMB_HIERARCHY_DATA = foreach LOAD_CMB_HIERARCHY generate ksn_id,
														 sears_division_nbr,
														 sears_item_nbr,
														 sears_sku_nbr,
														 shc_item_id,
														 ksn_purchase_status_cd,
														 TRIM(special_retail_order_system_ind) as special_retail_order_system_ind,
														 TRIM(shc_item_corporate_owner_cd) as shc_item_corporate_owner_cd,
														 dotcom_allocation_ind,
														 TRIM(shc_item_type_cd) as shc_item_type_cd,
														 TRIM(idrp_order_method_cd) as idrp_order_method_cd;

-- gold__item_attribute_relate_current

LOAD_ATTR_RELATE = load '$GOLD__ITEM_ATTRIBUTE_RELATE_CURRENT_LOCATION' USING PigStorage('$gold__item_attribute_relate_current_delimiter') as ($GOLD__ITEM_ATTRIBUTE_RELATE_CURRENT_SCHEMA);

ATTR_RELATE_DATA = foreach 	LOAD_ATTR_RELATE generate 
							ksn_id as ksn_id,
							attribute_id as attribute_id,
							sub_attribute_id as sub_attribute_id,
							TRIM(value_definition_tx) as value_definition_tx,
							attribute_relate_alternate_id as attribute_relate_alternate_id,
							attribute_nm as attribute_nm,        		--IPS-3157
							value_nm as value_nm,        				--IPS-3157
							package_id as package_id,        			--IPS-3157
							( ((attribute_nm == 'BRAND') AND (value_nm MATCHES '.*KENMORE.*' OR value_nm MATCHES '.*DIEHARD.*')) ? product_group_attribute_cd : ' ' ) AS product_group_attribute_cd;
							--IPS-3157: Added attribute_nm, value_nm, package_id, product_group_attribute_cd

-- gold__item_sears_channel_distribution_current

CHANNEL_DIST_DATA = load '$GOLD__ITEM_SEARS_CHANNEL_DISTRIBUTION_CURRENT_LOCATION' USING PigStorage('$gold__item_sears_channel_distribution_current_delimiter') as ($GOLD__ITEM_SEARS_CHANNEL_DISTRIBUTION_CURRENT_SCHEMA);

-- gold__item_core_bridge_item

LOAD_CORE_BRIDGE_ITEM = load '$WORK__IDRP_KSN_CORE_BRIDGE_ITEM_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') as ($WORK__IDRP_KSN_CORE_BRIDGE_ITEM_SCHEMA);

CORE_BRIDGE_ITEM_DATA = foreach LOAD_CORE_BRIDGE_ITEM generate load_ts as  load_ts,
																		   ksn_id as ksn_id,
																		   TRIM(cost_pointing_method_cd) as cost_pointing_method_cd,
																		   TRIM(item_order_system_cd) as item_order_system_cd,
																		   idrp_batch_id;

-- gold__item_vendor_package_current

LOAD_VENDOR_PKG = load '$GOLD__ITEM_VENDOR_PACKAGE_CURRENT_LOCATION' USING PigStorage('$gold__item_vendor_package_current_delimiter') as ($GOLD__ITEM_VENDOR_PACKAGE_CURRENT_SCHEMA);

VENDOR_PKG_DATA = foreach LOAD_VENDOR_PKG generate  ksn_id,
													TRIM(import_cd) as import_cd,
													TRIM(owner_cd) as owner_cd,
													TRIM(purchase_status_cd) as purchase_status_cd;
-- work__idrp_prod_mgrtn

LOAD_PROD_MGRTN = load '$WORK__IDRP_PROD_MGRTN_LOCATION' USING PigStorage('$work__idrp_prod_mgrtn_delimiter') as ($WORK__IDRP_PROD_MGRTN_SCHEMA);

PROD_MGRTN_DATA = foreach LOAD_PROD_MGRTN generate  ksn_id,
													TRIM(initiative_name) as initv_nm;

--IPS-3157: AMZ-Start: Added ITEM_PACKAGE_CURRENT -----------------------------------------

-- gold__item_package_current

LOAD_PKG = load '$GOLD__ITEM_PACKAGE_CURRENT_LOCATION' USING PigStorage('$gold__item_package_current_delimiter') as ($GOLD__ITEM_PACKAGE_CURRENT_SCHEMA);

WORK__PKG_DATA = foreach LOAD_PKG generate 
						 ksn_id,
						 package_id;

--IPS-3157: AMZ-End -----------------------------------------

--------------- SPLIT -------------------------------

--IPS-3157: added ATTR_RELATE_WITH_ATTR_10
split ATTR_RELATE_DATA into
        ATTR_RELATE_WITH_ATTR_430_SUBATTR_1566 if (attribute_id == '430' and sub_attribute_id == '1566'),
        ATTR_RELATE_WITH_ATTR_710 if (attribute_id == '710'),
        ATTR_RELATE_WITH_ATTR_220_SUBATTR_1535 if (attribute_id == '220' and  sub_attribute_id  == '1535'),
        ATTR_RELATE_WITH_ATTR_3610_SUBATTR_2055 if (attribute_id == '3610' and sub_attribute_id  == '2055'),
		ATTR_RELATE_WITH_ATTR_10 if ( (attribute_nm == 'BRAND') AND (value_nm MATCHES '.*KENMORE.*' OR value_nm MATCHES '.*DIEHARD.*') );

---------------------------------  STEP-1 ------------------------------------

CMB_HIERARCHY_FILTER = filter CMB_HIERARCHY_DATA by
            ((IsNull(shc_item_type_cd,'') == 'TYP' OR IsNull(shc_item_type_cd,'') == 'EXAS' OR IsNull(shc_item_type_cd,'') == 'IIRC' OR IsNull(shc_item_type_cd,'') == 'INVC')
                    AND (IsNull(idrp_order_method_cd,'') == 'A' OR IsNull(idrp_order_method_cd,'') == 'R'));

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP1 = foreach CMB_HIERARCHY_FILTER 	generate
																	 ksn_id,
																	 sears_division_nbr,
																	 sears_item_nbr,
																	 sears_sku_nbr,
																	 shc_item_id,
																	 ksn_purchase_status_cd,
																	 special_retail_order_system_ind,
																	 shc_item_corporate_owner_cd,
																	 dotcom_allocation_ind;


---------------- STEP-2 ---------------------------

CHANNEL_DIST_DATA_GROUPED_BY_DIV_ITEM = group CHANNEL_DIST_DATA
										by (sears_division_nbr,sears_item_nbr);

CHANNEL_DIST_WITH_DIST_TYPE_AND_RSU_CHANNEL = foreach  CHANNEL_DIST_DATA_GROUPED_BY_DIV_ITEM generate
											  FLATTEN(GetDistributionType(CHANNEL_DIST_DATA)) AS ($GOLD__ITEM_SEARS_CHANNEL_DISTRIBUTION_WITH_DISTRIBUTION_TYPE);

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP2 = foreach CHANNEL_DIST_WITH_DIST_TYPE_AND_RSU_CHANNEL generate
									sears_division_nbr,
									sears_item_nbr,
									distribution_type_code as distribution_type_cd,
									(only_rsu_distribution_channel == 'true'?'1':'0') as only_rsu_distribution_channel_ind;

---------------- STEP-3 ---------------------------

JOIN_STEP1_AND_STEP2 = join WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP1 by (sears_division_nbr, (long)sears_item_nbr) LEFT OUTER,
							WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP2 by (sears_division_nbr, (long)sears_item_nbr);

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3 = foreach JOIN_STEP1_AND_STEP2 generate
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP1::ksn_id as ksn_id,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP1::sears_division_nbr as sears_division_nbr,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP1::sears_item_nbr as sears_item_nbr,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP1::sears_sku_nbr as sears_sku_nbr,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP1::shc_item_id as shc_item_id,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP1::ksn_purchase_status_cd as ksn_purchase_status_cd,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP1::special_retail_order_system_ind as special_retail_order_system_ind,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP1::shc_item_corporate_owner_cd as shc_item_corporate_owner_cd,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP1::dotcom_allocation_ind as dotcom_allocation_ind,
                                                                        (WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP2::sears_division_nbr IS NULL ?'TW': distribution_type_cd)as distribution_type_cd,
                                                                        (WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP2::sears_division_nbr IS NULL ?'0': only_rsu_distribution_channel_ind) as only_rsu_distribution_channel_ind,
                                                                        (WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP2::sears_division_nbr IS NOT NULL
                                                                        ?
                                                                                (IsNull(special_retail_order_system_ind,'') == 'Y'
                                                                                        ?
                                                                                        ((IsNull(shc_item_corporate_owner_cd,'') == 'S' or IsNull(shc_item_corporate_owner_cd,'') == 'B')
                                                                                                ? (IsNull(only_rsu_distribution_channel_ind,'') == '1'?'1':'0')
                                                                                                :'0')
                                                                                        :'0')
                                                                        :'0')as special_order_candidate_ind,
                                                                        (special_retail_order_system_ind == 'N'
                                                                        ?
                                                                                ((IsNull(shc_item_corporate_owner_cd,'')== 'S' or IsNull(shc_item_corporate_owner_cd,'') == 'B')
                                                                                ? ( IsNull(only_rsu_distribution_channel_ind,'') == '1'?'1':'0')
                                                                                :'0')
                                                                        :'0') as item_emp_ind;

---------------- STEP-4 ---------------------------

JOIN_STEP3_CORE_BRIDGE_ITEM = join WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3 BY (int)ksn_id LEFT OUTER, CORE_BRIDGE_ITEM_DATA by (int)ksn_id;

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4 = foreach JOIN_STEP3_CORE_BRIDGE_ITEM generate
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3::ksn_id as ksn_id,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3::sears_division_nbr as sears_division_nbr,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3::sears_item_nbr as sears_item_nbr,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3::sears_sku_nbr as sears_sku_nbr,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3::shc_item_id as shc_item_id,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3::ksn_purchase_status_cd as ksn_purchase_status_cd,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3::special_retail_order_system_ind as special_retail_order_system_ind,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3::shc_item_corporate_owner_cd as shc_item_corporate_owner_cd,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3::dotcom_allocation_ind as dotcom_allocation_ind,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3::distribution_type_cd as distribution_type_cd,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3::only_rsu_distribution_channel_ind as only_rsu_distribution_channel_ind,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3::special_order_candidate_ind as special_order_candidate_ind,
                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP3::item_emp_ind as item_emp_ind,
                                                                        ((CORE_BRIDGE_ITEM_DATA::ksn_id IS NOT NULL)
                                                                                ?((IsNull(shc_item_corporate_owner_cd,'')== 'S' or IsNull(shc_item_corporate_owner_cd,'') == 'B')
                                                                                        ?(IsNull(cost_pointing_method_cd,'') == 'E'?'1':'0')
                                                                                        :'0')
                                                                        :'0')as easy_order_ind,
                                                                        (IsNull(CORE_BRIDGE_ITEM_DATA::item_order_system_cd,'') == 'SAMS'?'1':'0') as sams_migration_ind;



---------------- STEP-5 ---------------------------

ATTR_RELATE_FILTER_ATTR_430_SUBATTR_1566_BY_KSN = group ATTR_RELATE_WITH_ATTR_430_SUBATTR_1566 by ksn_id;

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5A_SINGLE_KSN = foreach ATTR_RELATE_FILTER_ATTR_430_SUBATTR_1566_BY_KSN{
                                                                        ORDER_BY_ATTR_RELATE_ALT_ID = order $1 by attribute_relate_alternate_id desc;
                                                                        SINGLE_RECORD_PER_KSN = limit ORDER_BY_ATTR_RELATE_ALT_ID 1;
                                                                        generate flatten(SINGLE_RECORD_PER_KSN);
                                                                }

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5A = foreach WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5A_SINGLE_KSN generate
                                                                                                        ksn_id as ksn_id,
                                                                                                        attribute_id as attribute_id,
                                                                                                        sub_attribute_id as sub_attribute_id,
                                                                                                        value_definition_tx as value_definition_tx,
                                                                                                        attribute_relate_alternate_id as attribute_relate_alternate_id;

JOIN_STEP4_AND_STEP5A = join WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4 by ksn_id LEFT OUTER,
                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5A by ksn_id;


WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B = foreach JOIN_STEP4_AND_STEP5A generate
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::ksn_id as ksn_id,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::sears_division_nbr as sears_division_nbr,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::sears_item_nbr as sears_item_nbr,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::sears_sku_nbr as sears_sku_nbr,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::shc_item_id as shc_item_id,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::ksn_purchase_status_cd as ksn_purchase_status_cd,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::special_retail_order_system_ind as special_retail_order_system_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::shc_item_corporate_owner_cd as shc_item_corporate_owner_cd,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::dotcom_allocation_ind as dotcom_allocation_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::distribution_type_cd as distribution_type_cd,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::only_rsu_distribution_channel_ind as only_rsu_distribution_channel_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::special_order_candidate_ind as special_order_candidate_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::item_emp_ind as item_emp_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::easy_order_ind as easy_order_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP4::sams_migration_ind as sams_migration_ind,
										(WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5A::ksn_id IS NULL ?'':value_definition_tx) as warehouse_sizing_attribute_cd;

---------------- STEP-6 ---------------------------

ATTR_RELATE_WITH_ATTR_710_BY_KSN = group ATTR_RELATE_WITH_ATTR_710 by ksn_id;

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP6A_SINGLE_KSN = foreach ATTR_RELATE_WITH_ATTR_710_BY_KSN {
                                                                                ORDER_BY_ATTR_RELATE_ALT_ID = order $1 by attribute_relate_alternate_id desc;
                                                                                SINGLE_RECORD_PER_KSN = limit ORDER_BY_ATTR_RELATE_ALT_ID 1;
                                                                                generate flatten(SINGLE_RECORD_PER_KSN);
                                                                        }

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP6A = foreach WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP6A_SINGLE_KSN generate
                                                                                                        ksn_id as ksn_id,
                                                                                                        attribute_id as attribute_id,
                                                                                                        sub_attribute_id as sub_attribute_id,
                                                                                                        value_definition_tx as value_definition_tx,
                                                                                                        attribute_relate_alternate_id as attribute_relate_alternate_id;

JOIN_STEP5B_AND_STEP6A = join WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B by ksn_id LEFT OUTER,
                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP6A by ksn_id;

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP6B = foreach JOIN_STEP5B_AND_STEP6A generate
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::ksn_id as ksn_id,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::sears_division_nbr as sears_division_nbr,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::sears_item_nbr as sears_item_nbr,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::sears_sku_nbr as sears_sku_nbr,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::shc_item_id as shc_item_id,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::ksn_purchase_status_cd as ksn_purchase_status_cd,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::special_retail_order_system_ind as special_retail_order_system_ind,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::shc_item_corporate_owner_cd as shc_item_corporate_owner_cd,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::dotcom_allocation_ind as dotcom_allocation_ind,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::distribution_type_cd as distribution_type_cd,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::only_rsu_distribution_channel_ind as only_rsu_distribution_channel_ind,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::special_order_candidate_ind as special_order_candidate_ind,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::item_emp_ind as item_emp_ind,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::easy_order_ind as easy_order_ind,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::sams_migration_ind as sams_migration_ind,
                                                                                        WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP5B::warehouse_sizing_attribute_cd as warehouse_sizing_attribute_cd,
                                                                                        (WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP6A::ksn_id IS NOT NULL
                                                                                                ?((IsNull(shc_item_corporate_owner_cd,'')== 'S' or IsNull(shc_item_corporate_owner_cd,'') == 'B')
                                                                                                        ?((IsNull(value_definition_tx,'') == 'RP0001' or IsNull(value_definition_tx,'') == 'RP0002' or IsNull(value_definition_tx,'') == 'RP0003' or IsNull(value_definition_tx,'') == 'RP0004' or IsNull(value_definition_tx,'') == 'RP0005' or IsNull(value_definition_tx,'') == 'RP0006' or IsNull(value_definition_tx,'') == 'RP0007' or IsNull(value_definition_tx,'') == 'RP0008' or IsNull(value_definition_tx,'') == 'RP0009' or IsNull(value_definition_tx,'') == 'RP0010' or IsNull(value_definition_tx,'') == 'RP0011' or IsNull(value_definition_tx,'') == 'RP1001' or IsNull(value_definition_tx,'') == 'RP1002' or IsNull(value_definition_tx,'') == 'RP1003' or IsNull(value_definition_tx,'') == 'RP1004' or IsNull(value_definition_tx,'') == 'RP1005' or IsNull(value_definition_tx,'') == 'RP1006' or IsNull(value_definition_tx,'') == 'RP1007' or IsNull(value_definition_tx,'') == 'RP1008' or IsNull(value_definition_tx,'') == 'RP1009' or IsNull(value_definition_tx,'') == 'RP1010' or IsNull(value_definition_tx,'') == 'RP1011')
                                                                                                        ?'1':'0')
                                                                                                :'0')
                                                                                        :'0')as rapid_item_ind,
                                                                                        (WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP6A::ksn_id IS NOT NULL
                                                                                        ?((shc_item_corporate_owner_cd == 'S' or 
                                                                                           shc_item_corporate_owner_cd == 'B' or
                                                                                           shc_item_corporate_owner_cd == 'K')                  -- IPS-734
                                                                                                ?((IsNull(value_definition_tx,'') == 'RP1001' or 
                                                                                                IsNull(value_definition_tx,'') == 'RP1002' or
                                                                                                IsNull(value_definition_tx,'') == 'RP1003' or
                                                                                                IsNull(value_definition_tx,'') == 'RP1004' or
                                                                                                IsNull(value_definition_tx,'') == 'RP1005' or
                                                                                                IsNull(value_definition_tx,'') == 'RP1006' or
                                                                                                IsNull(value_definition_tx,'') == 'RP1007' or
                                                                                                IsNull(value_definition_tx,'') == 'RP1008' or
                                                                                                IsNull(value_definition_tx,'') == 'RP1009' or
                                                                                                IsNull(value_definition_tx,'') == 'RP1010' or
                                                                                                IsNull(value_definition_tx,'') == 'RP1011')?'1':'0')
                                                                                        :'0')
                                                                                        :'0')as constrained_item_ind;

---------------- STEP-7 ---------------------------
--CR5033 (Default sears_import_ind to 0)

/*
--CR4668
VENDOR_PKG_FILTER = filter VENDOR_PKG_DATA by
                                        IsNull(import_cd,'') == 'I' and IsNull(owner_cd,'') == 'S';

VENDOR_PKG_KSN = foreach VENDOR_PKG_FILTER generate ksn_id;

DISTINCT_VENDOR_PKG_KSN = distinct VENDOR_PKG_KSN;

JOIN_STEP6B_AND_STEP7_1 = join WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP6B by ksn_id LEFT OUTER,
                                                        DISTINCT_VENDOR_PKG_KSN by ksn_id;
*/

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP7 = foreach WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP6B generate
                                                                                ksn_id as ksn_id,
                                                                                sears_division_nbr as sears_division_nbr,
                                                                                sears_item_nbr as sears_item_nbr,
                                                                                sears_sku_nbr as sears_sku_nbr,
                                                                                shc_item_id as shc_item_id,
                                                                                ksn_purchase_status_cd as ksn_purchase_status_cd,
                                                                                special_retail_order_system_ind as special_retail_order_system_ind,
                                                                                shc_item_corporate_owner_cd as shc_item_corporate_owner_cd,
                                                                                dotcom_allocation_ind as dotcom_allocation_ind,
                                                                                distribution_type_cd as distribution_type_cd,
                                                                                only_rsu_distribution_channel_ind as only_rsu_distribution_channel_ind,
                                                                                special_order_candidate_ind as special_order_candidate_ind,
                                                                                item_emp_ind as item_emp_ind,
                                                                                easy_order_ind as easy_order_ind,
                                                                                sams_migration_ind as sams_migration_ind,
                                                                                warehouse_sizing_attribute_cd as warehouse_sizing_attribute_cd,
                                                                                rapid_item_ind as rapid_item_ind,
                                                                                constrained_item_ind as constrained_item_ind,
                                                                                '0' as sears_import_ind;

---------------- STEP-8 ---------------------------

split WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP7 into
                                                SEARS_RECORDS if (IsNull(shc_item_corporate_owner_cd,'') == 'S' or IsNull(shc_item_corporate_owner_cd,'') == 'B'),
                                                KMART_RECORDS if (shc_item_corporate_owner_cd != 'S' and shc_item_corporate_owner_cd != 'B');


KSN_ATTR_BY_ITEM_TYPE_IND_PRECEDENCE_SEARS = foreach SEARS_RECORDS generate
                                                                                ksn_id,
                                                                                sears_division_nbr,
                                                                                sears_item_nbr,
                                                                                sears_sku_nbr,
                                                                                shc_item_id,
                                                                                ksn_purchase_status_cd,
                                                                                special_retail_order_system_ind,
                                                                                shc_item_corporate_owner_cd,
                                                                                dotcom_allocation_ind,
                                                                                distribution_type_cd,
                                                                                only_rsu_distribution_channel_ind,
                                                                                special_order_candidate_ind,
                                                                                (
                                                                                        (special_order_candidate_ind == '1') or
                                                                                        (special_order_candidate_ind == '0' and easy_order_ind == '1')
                                                                                        ?'0'
                                                                                        :item_emp_ind
                                                                                )as item_emp_ind,
                                                                                (
                                                                                        special_order_candidate_ind == '1'
                                                                                        ?'0'
                                                                                        :easy_order_ind
                                                                                )as easy_order_ind,
                                                                                sams_migration_ind,
                                                                                warehouse_sizing_attribute_cd,
                                                                                (
                                                                                        (special_order_candidate_ind == '1') or
                                                                                        (special_order_candidate_ind == '0' and easy_order_ind == '1') or
                                                                                        (special_order_candidate_ind == '0' and easy_order_ind == '0' and item_emp_ind == '1')
                                                                                        ?'0'
                                                                                        :rapid_item_ind
                                                                                )as rapid_item_ind,
                                                                                (
                                                                                        (special_order_candidate_ind == '1') or
                                                                                        (special_order_candidate_ind == '0' and easy_order_ind == '1') or
                                                                                        (special_order_candidate_ind == '0' and easy_order_ind == '0' and item_emp_ind == '1')
                                                                                        ?'0'
                                                                                        :constrained_item_ind
                                                                                )as constrained_item_ind,
                                                                                sears_import_ind;


WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8_SEARS = foreach KSN_ATTR_BY_ITEM_TYPE_IND_PRECEDENCE_SEARS generate
                                                                                ksn_id,
                                                                                sears_division_nbr,
                                                                                sears_item_nbr as sears_item_nbr,
                                                                                sears_sku_nbr as sears_sku_nbr,
                                                                                shc_item_id as shc_item_id,
                                                                                ksn_purchase_status_cd as ksn_purchase_status_cd,
                                                                                special_retail_order_system_ind as special_retail_order_system_ind,
                                                                                shc_item_corporate_owner_cd,
                                                                                dotcom_allocation_ind as dotcom_allocation_ind,
                                                                                distribution_type_cd,
                                                                                only_rsu_distribution_channel_ind,
                                                                                special_order_candidate_ind,
                                                                                item_emp_ind,
                                                                                easy_order_ind,
                                                                                sams_migration_ind,
                                                                                warehouse_sizing_attribute_cd,
                                                                                rapid_item_ind,
                                                                                constrained_item_ind,
                                                                                sears_import_ind,
                                                                                (
                                                                                (special_order_candidate_ind == '1')
                                                                                  ?'RSOS'
                                                                                  :(
                                                                                    easy_order_ind == '1'
                                                                                    ?'EASY ORDER'
                                                                                    :(
                                                                                      item_emp_ind == '1'
                                                                                      ?'EMP'
                                                                                      :(
                                                                                        ((shc_item_corporate_owner_cd == 'S' OR shc_item_corporate_owner_cd == 'B') AND  
                                                                                        constrained_item_ind == '1' AND rapid_item_ind == '1')
                                                                                        ?'CONSTRAINED'
                                                                                        :(
                                                                                          rapid_item_ind == '1'
                                                                                          ?'RAPID'
                                                                                          :'DOMESTIC')))))
                                                                                as idrp_item_type_desc;

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8_KMART = foreach KMART_RECORDS generate
                                                                                ksn_id,
                                                                                sears_division_nbr,
                                                                                sears_item_nbr,
                                                                                sears_sku_nbr,
                                                                                shc_item_id,
                                                                                ksn_purchase_status_cd,
                                                                                special_retail_order_system_ind,
                                                                                shc_item_corporate_owner_cd,
                                                                                dotcom_allocation_ind,
                                                                                distribution_type_cd,
                                                                                only_rsu_distribution_channel_ind,
                                                                                special_order_candidate_ind,
                                                                                item_emp_ind,
                                                                                easy_order_ind,
                                                                                sams_migration_ind,
                                                                                warehouse_sizing_attribute_cd,
                                                                                rapid_item_ind,
                                                                                constrained_item_ind,
                                                                                sears_import_ind,
                                                                                (shc_item_corporate_owner_cd == 'K' AND              -- IPS-734
                                                                                 constrained_item_ind == '1'
                                                                                 ? 'CONSTRAINED'
                                                                                 : 'DOMESTIC') as idrp_item_type_desc;

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8 = union WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8_KMART, WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8_SEARS;

---------------- STEP-9 ---------------------------

ATTR_RELATE_WITH_ATTR_220_SUBATTR_1535_BY_KSN = group ATTR_RELATE_WITH_ATTR_220_SUBATTR_1535 by ksn_id;

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9A_SINGLE_KSN = foreach ATTR_RELATE_WITH_ATTR_220_SUBATTR_1535_BY_KSN {
                                                                ORDER_BY_ATTR_RELATE_ALT_ID = order $1 by attribute_relate_alternate_id desc;
                                                                SINGLE_RECORD_PER_KSN = limit ORDER_BY_ATTR_RELATE_ALT_ID 1;
                                                                generate flatten(SINGLE_RECORD_PER_KSN);
                                                        }

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9A = foreach WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9A_SINGLE_KSN generate
                                                                                                        ksn_id as ksn_id,
                                                                                                        attribute_id as attribute_id,
                                                                                                        sub_attribute_id as sub_attribute_id,
                                                                                                        value_definition_tx as value_definition_tx,
                                                                                                        attribute_relate_alternate_id as attribute_relate_alternate_id;

JOIN_STEP8_AND_STEP9A = join WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8 by ksn_id LEFT OUTER,
                                                         WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9A by ksn_id;


WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B = foreach JOIN_STEP8_AND_STEP9A generate
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::ksn_id as ksn_id,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::sears_division_nbr as sears_division_nbr,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::sears_item_nbr as sears_item_nbr,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::sears_sku_nbr as sears_sku_nbr,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::shc_item_id as shc_item_id,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::ksn_purchase_status_cd as ksn_purchase_status_cd,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::special_retail_order_system_ind as special_retail_order_system_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::shc_item_corporate_owner_cd as shc_item_corporate_owner_cd,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::dotcom_allocation_ind as dotcom_allocation_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::distribution_type_cd as distribution_type_cd,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::only_rsu_distribution_channel_ind as only_rsu_distribution_channel_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::special_order_candidate_ind as special_order_candidate_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::item_emp_ind as item_emp_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::easy_order_ind as easy_order_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::sams_migration_ind as sams_migration_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::warehouse_sizing_attribute_cd as warehouse_sizing_attribute_cd,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::rapid_item_ind as rapid_item_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::constrained_item_ind as constrained_item_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::sears_import_ind as sears_import_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP8::idrp_item_type_desc as idrp_item_type_desc,
                                                                                (WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9A::ksn_id IS NULL ?'':value_definition_tx)as cross_merchandising_attribute_cd;

---------------- STEP-10 ---------------------------

PROD_MGRTN_FILTER = filter PROD_MGRTN_DATA by (initv_nm == 'EMPJITLIVE' or  initv_nm == 'KMTDC2SRS');

JOIN_STEP_9_AND_PROD_MGRTN = join WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B by ksn_id LEFT OUTER, PROD_MGRTN_FILTER by ksn_id;

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP10 = foreach JOIN_STEP_9_AND_PROD_MGRTN generate
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::ksn_id as ksn_id,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::sears_division_nbr as sears_division_nbr,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::sears_item_nbr as sears_item_nbr,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::sears_sku_nbr as sears_sku_nbr,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::shc_item_id as shc_item_id,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::ksn_purchase_status_cd as ksn_purchase_status_cd,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::special_retail_order_system_ind as special_retail_order_system_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::shc_item_corporate_owner_cd as shc_item_corporate_owner_cd,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::dotcom_allocation_ind as dotcom_allocation_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::distribution_type_cd as distribution_type_cd,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::only_rsu_distribution_channel_ind as only_rsu_distribution_channel_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::special_order_candidate_ind as special_order_candidate_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::item_emp_ind as item_emp_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::easy_order_ind as easy_order_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::sams_migration_ind as sams_migration_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::warehouse_sizing_attribute_cd as warehouse_sizing_attribute_cd,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::rapid_item_ind as rapid_item_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::constrained_item_ind as constrained_item_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::sears_import_ind as sears_import_ind,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::idrp_item_type_desc as idrp_item_type_desc,
                                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP9B::cross_merchandising_attribute_cd as cross_merchandising_attribute_cd,
                                                                                (
                                                                                        PROD_MGRTN_FILTER::ksn_id IS NULL
                                                                                        ?'0'
                                                                                        :(IsNull(initv_nm,'') == 'EMPJITLIVE'?'1':'0')
                                                                                )as emp_to_jit_ind,
                                                                                (
                                                                                        PROD_MGRTN_FILTER::ksn_id IS NULL
                                                                                        ?'0'
                                                                                        :(IsNull(initv_nm,'') == 'KMTDC2SRS'?'1':'0')
                                                                                )as rim_flow_ind;

---------------- STEP-11 ---------------------------

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11 = foreach WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP10 generate
                                                                                ksn_id,
                                                                                sears_division_nbr,
                                                                                sears_item_nbr,
                                                                                sears_sku_nbr,
                                                                                shc_item_id,
                                                                                ksn_purchase_status_cd,
                                                                                special_retail_order_system_ind,
                                                                                shc_item_corporate_owner_cd,
                                                                                dotcom_allocation_ind,
                                                                                distribution_type_cd,
                                                                                only_rsu_distribution_channel_ind,
                                                                                special_order_candidate_ind,
                                                                                item_emp_ind,
                                                                                easy_order_ind,
                                                                                sams_migration_ind,
                                                                                warehouse_sizing_attribute_cd,
                                                                                rapid_item_ind,
                                                                                constrained_item_ind,
                                                                                sears_import_ind,
                                                                                idrp_item_type_desc,
                                                                                cross_merchandising_attribute_cd,
                                                                                emp_to_jit_ind,
                                                                                rim_flow_ind,
                                                                                (
                                                                                        (rim_flow_ind == '1'
                                                                                        ?'RIMFLOW'
                                                                                        :(
                                                                                                        emp_to_jit_ind == '1'
                                                                                                        ?'EMP2JIT'
                                                                                                        :(
                                                                                                                sams_migration_ind == '1'
                                                                                                                ?'SAMS'
                                                                                                                :((cross_merchandising_attribute_cd IS NOT NULL or cross_merchandising_attribute_cd != '')
                                                                                                                        ?cross_merchandising_attribute_cd
                                                                                                                        :''
                                                                                                                )
                                                                                                        )
                                                                                                )
                                                                                        )
                                                                                )as cross_merchandising_cd;

---------------- STEP-12 ---------------------------

ATTR_RELATE_WITH_ATTR_3610_SUBATTR_2055_BY_KSN = group ATTR_RELATE_WITH_ATTR_3610_SUBATTR_2055 by ksn_id;

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12A_SINGLE_KSN = foreach ATTR_RELATE_WITH_ATTR_3610_SUBATTR_2055_BY_KSN {
                                                        ORDER_BY_ATTR_RELATE_ALT_ID = order $1 by attribute_relate_alternate_id desc;
                                                        SINGLE_RECORD_PER_KSN = limit ORDER_BY_ATTR_RELATE_ALT_ID 1;
                                                        generate flatten(SINGLE_RECORD_PER_KSN);
                                                        }

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12A = foreach WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12A_SINGLE_KSN generate
                                                                                                        ksn_id as ksn_id,
                                                                                                        attribute_id as attribute_id,
                                                                                                        sub_attribute_id as sub_attribute_id,
                                                                                                        value_definition_tx as value_definition_tx,
                                                                                                        attribute_relate_alternate_id as attribute_relate_alternate_id;

JOIN_STEP11_AND_STEP12A = join WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11 by ksn_id LEFT OUTER,
                                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12A by ksn_id;


-- IPS-3157 : Commented below dataset, added alias name in each column to be used further in joins

--WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B =  foreach JOIN_STEP11_AND_STEP12A generate
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::ksn_id,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::sears_division_nbr,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::sears_item_nbr,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::sears_sku_nbr,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::shc_item_id,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::ksn_purchase_status_cd,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::special_retail_order_system_ind,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::shc_item_corporate_owner_cd,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::dotcom_allocation_ind,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::distribution_type_cd,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::only_rsu_distribution_channel_ind,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::special_order_candidate_ind,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::item_emp_ind,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::easy_order_ind,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::sams_migration_ind,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::warehouse_sizing_attribute_cd,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::cross_merchandising_attribute_cd,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::rapid_item_ind,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::constrained_item_ind,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::sears_import_ind,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::idrp_item_type_desc,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::emp_to_jit_ind,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::rim_flow_ind,
--                                                WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::cross_merchandising_cd,
--                                                (WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12A::ksn_id IS NULL ? '' : value_definition_tx) as shop_your_way_exclusive_attribute_cd;

WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B = foreach JOIN_STEP11_AND_STEP12A generate
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::ksn_id as ksn_id,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::sears_division_nbr as sears_division_nbr,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::sears_item_nbr as sears_item_nbr,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::sears_sku_nbr as sears_sku_nbr,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::shc_item_id as shc_item_id,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::ksn_purchase_status_cd as ksn_purchase_status_cd,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::special_retail_order_system_ind as special_retail_order_system_ind,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::shc_item_corporate_owner_cd as shc_item_corporate_owner_cd,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::dotcom_allocation_ind as dotcom_allocation_ind,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::distribution_type_cd as distribution_type_cd,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::only_rsu_distribution_channel_ind as only_rsu_distribution_channel_ind,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::special_order_candidate_ind as special_order_candidate_ind,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::item_emp_ind as item_emp_ind,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::easy_order_ind as easy_order_ind,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::sams_migration_ind as sams_migration_ind,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::warehouse_sizing_attribute_cd as warehouse_sizing_attribute_cd,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::cross_merchandising_attribute_cd as cross_merchandising_attribute_cd,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::rapid_item_ind as rapid_item_ind,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::constrained_item_ind as constrained_item_ind,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::sears_import_ind as sears_import_ind,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::idrp_item_type_desc as idrp_item_type_desc,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::emp_to_jit_ind as emp_to_jit_ind,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::rim_flow_ind as rim_flow_ind,
									   WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP11::cross_merchandising_cd as cross_merchandising_cd,
									   (WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12A::ksn_id IS NULL ? '' : value_definition_tx) as shop_your_way_exclusive_attribute_cd;

-----------------------IPS-3157-AMZ-Start-----------------------------------------

WORK__ATTR_RELATE_AMZ =  foreach ATTR_RELATE_WITH_ATTR_10 generate
								 package_id as package_id_attr10,
								 product_group_attribute_cd as product_group_attribute_cd;

join_PKG__ATTR_RELATE_AMZ = join WORK__PKG_DATA        by (package_id), 
								 WORK__ATTR_RELATE_AMZ by (package_id_attr10);

gen_PKG_AMZ = foreach join_PKG__ATTR_RELATE_AMZ generate
					  WORK__PKG_DATA::ksn_id as ksn_id_amz,
					  WORK__ATTR_RELATE_AMZ::product_group_attribute_cd as product_group_attribute_cd;

WORK__PKG_AMZ = distinct gen_PKG_AMZ;

join_STEP12B__PKG_AMZ = join WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B by (ksn_id) LEFT OUTER,
							 WORK__PKG_AMZ          			  by (ksn_id_amz);

WORK__ELIGIBLE_KSN_ATTR_12B_AND_AMZ = foreach join_STEP12B__PKG_AMZ generate
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::ksn_id as ksn_id,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::sears_division_nbr as sears_division_nbr,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::sears_item_nbr as sears_item_nbr,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::sears_sku_nbr as sears_sku_nbr,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::shc_item_id as shc_item_id,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::ksn_purchase_status_cd as ksn_purchase_status_cd,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::special_retail_order_system_ind as special_retail_order_system_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::shc_item_corporate_owner_cd as shc_item_corporate_owner_cd,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::dotcom_allocation_ind as dotcom_allocation_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::distribution_type_cd as distribution_type_cd,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::only_rsu_distribution_channel_ind as only_rsu_distribution_channel_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::special_order_candidate_ind as special_order_candidate_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::item_emp_ind as item_emp_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::easy_order_ind as easy_order_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::sams_migration_ind as sams_migration_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::warehouse_sizing_attribute_cd as warehouse_sizing_attribute_cd,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::cross_merchandising_attribute_cd as cross_merchandising_attribute_cd,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::rapid_item_ind as rapid_item_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::constrained_item_ind as constrained_item_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::sears_import_ind as sears_import_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::idrp_item_type_desc as idrp_item_type_desc,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::emp_to_jit_ind as emp_to_jit_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::rim_flow_ind as rim_flow_ind,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::cross_merchandising_cd as cross_merchandising_cd,
										WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B::shop_your_way_exclusive_attribute_cd as shop_your_way_exclusive_attribute_cd,
										(WORK__PKG_AMZ::ksn_id_amz IS NULL ? ' ' : product_group_attribute_cd) as amazon_brand_attribute_cd;

-----------------------IPS-3157-AMZ-End-----------------------------------------

---------------- STEP-13 ---------------------------

--WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP13 = foreach WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP12B generate     	--IPS-3157: Commented
WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP13 = foreach WORK__ELIGIBLE_KSN_ATTR_12B_AND_AMZ generate    		--IPS-3157
												GetCurrentDate() as load_ts,
												ksn_id,
												sears_division_nbr,
												sears_item_nbr,
												sears_sku_nbr,
												shc_item_id,
												ksn_purchase_status_cd,
												special_retail_order_system_ind,
												shc_item_corporate_owner_cd,
												dotcom_allocation_ind,
												distribution_type_cd,
												only_rsu_distribution_channel_ind,
												special_order_candidate_ind,
												item_emp_ind,
												easy_order_ind,
												sams_migration_ind,
												warehouse_sizing_attribute_cd,
												cross_merchandising_attribute_cd,
												rapid_item_ind,
												constrained_item_ind,
												sears_import_ind,
												idrp_item_type_desc,
												emp_to_jit_ind,
												rim_flow_ind,
												cross_merchandising_cd,
												shop_your_way_exclusive_attribute_cd,
												amazon_brand_attribute_cd,  						--IPS-3157: New column amazon_brand_attribute_cd
												'$batchid' as batchid;

------------- Store results into output hdfs path ----------------------------

store WORK__IDRP_ELIGIBLE_KSN_ATTR_STEP13 into '$output_hdfs_path' using PigStorage('$output_field_delimiter');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/