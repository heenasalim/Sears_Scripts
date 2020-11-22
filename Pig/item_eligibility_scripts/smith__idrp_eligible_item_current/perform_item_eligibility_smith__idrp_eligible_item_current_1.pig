/*
###############################################################################
#<>                           START HEADER DOCUMENT                         <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_current.pig
# AUTHOR NAME:         Mudit Mangal
# CREATION DATE:       07-07-2014 06:20
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>
#
#
#
# DEPENDENCIES: None
# RESTARTABLE:  N/A
#
#
# REV LIST:
#        DATE         BY          		  MODIFICATION
#30/10/2014			Siddhi/Meghana		CR#3248 Changes made at line 292 CR#3216 Added special_retail_order_system_ind 
#07/11/2014			Siddhi				CR#3283 Null value handled for column web_exclusive_ind code changed at line 983
#21/01/2015			Siddhivinayak Karpe	CR#3626 join on sears_division_nbr and sears_item_nbr to set order_system_cd while the prior steps do not fetch those columns
#22/01/2015			Siddhivinayak Karpe	CR#3639	Records with error on mailable_ind being dropped
#23/01/2015			Meghana	            CR#3626	group by on shc_item_id (Line 359)
#30/01/2015			Meghana	            Spira#3694	Handled Nulls for dotcom_allocation_ind (Assigned nulls as 'N') (Line 862)
#30/03/2015         Meghana			    CR#3703 Added Sears and Kmart Online Fulfillment Columns
#18-05-2015         Meghana			    CR#4427 Group by clause added as per FSPEC 
###############################################################################
#<<                 START COMMON HEADER CODE - DO NOT MANUALLY EDIT         >>#
###############################################################################
*/

-- Register the jar containing all PIG UDFs
REGISTER $UDF_JAR;
SET default_parallel $NUM_PARALLEL;
DEFINE TrimLeadingZeros com.searshc.supplychain.idrp.udf.TrimLeadingZeros();
DEFINE AddDays com.searshc.supplychain.idrp.udf.AddOrRemoveDaysToDate();

/******************************* LOAD FOR ALL TABLES AND FILES REQUIRED ***********************************/

smith__idrp_shc_item_combined_data = LOAD '$SMITH__IDRP_SHC_ITEM_COMBINED_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_SHC_ITEM_COMBINED_SCHEMA);
	
item_rpt_cost_data = LOAD '$WORK__IDRP_ITEM_RPT_COST_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($WORK__IDRP_ITEM_RPT_COST_SCHEMA);	

item_rpt_grp_data = LOAD '$WORK__IDRP_ITEM_RPT_GRP_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($WORK__IDRP_ITEM_RPT_GRP_SCHEMA);

LOAD_CORE_BRIDGE_ITEM = LOAD '$GOLD__ITEM_CORE_BRIDGE_ITEM_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_CORE_BRIDGE_ITEM_SCHEMA);

--LOAD GOLD ITEM HIERARCHY Package file
LOAD_GOLD_ITEM = LOAD '$GOLD__ITEM_SHC_HIERARCHY_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_SHC_HIERARCHY_CURRENT_SCHEMA);

--LOAD ONLINE FULFILLMENT file
LOAD_ONLINE_FULFILLMENT = LOAD '$SMITH__IDRP_ONLINE_FULFILLMENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ONLINE_FULFILLMENT_SCHEMA);

--LOAD ONLINE BILLABLE WEIGHT
LOAD_ONLINE_BILL_WT = LOAD '$SMITH__IDRP_ONLINE_BILLABLE_WEIGHT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ONLINE_BILLABLE_WEIGHT_SCHEMA);

--LOAD ITEM PACKAGE CURRENT
LOAD_PACKAGE_CURRENT = LOAD '$GOLD__ITEM_PACKAGE_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_PACKAGE_CURRENT_SCHEMA);

--LOAD DROP SHIP ITEMS file
LOAD_DROP_SHIP = LOAD '$SMITH__IDRP_ONLINE_DROP_SHIP_ITEMS_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ONLINE_DROP_SHIP_ITEMS_SCHEMA);

DEFAULT_DATA = LOAD '$GOLD__ITEM_PRICE_LINK_DEFAULT_PRICE_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') 	AS ($GOLD__ITEM_PRICE_LINK_DEFAULT_PRICE_SCHEMA);
	
MEMBER_DATA  = LOAD '$GOLD__ITEM_PRICE_LINK_MEMBER_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_PRICE_LINK_MEMBER_SCHEMA);
	
/******************************************************************************************************************/

work_idrp_eligible_item_shc_item_step1 = 
	FOREACH smith__idrp_shc_item_combined_data 
	GENERATE
		shc_item_id AS shc_item_id,
		shc_item_desc AS shc_item_desc,
		shc_division_nbr AS shc_division_nbr,
		shc_division_desc AS shc_division_desc,
		shc_department_nbr AS shc_department_nbr,
		shc_department_desc AS shc_department_desc,
		shc_category_group_level_nbr AS shc_category_group_level_nbr,
		shc_category_group_desc AS shc_category_group_desc,
		shc_category_nbr AS shc_category_nbr,
		shc_category_desc AS shc_category_desc,
		shc_sub_category_nbr AS shc_sub_category_nbr,
		shc_sub_category_desc AS shc_sub_category_desc,
		delivered_direct_ind AS delivered_direct_ind,
		installation_ind  AS installation_ind,
		store_forecast_cd AS store_forecast_cd,
		idrp_order_method_cd AS idrp_order_method_cd,
		idrp_order_method_desc AS idrp_order_method_desc,
		shc_item_type_cd AS shc_item_type_cd,
		purchase_status_cd AS purchase_status_cd,
		network_distribution_cd AS network_distribution_cd,
		future_network_distribution_cd AS future_network_distribution_cd,
		future_network_distribution_effective_dt AS future_network_distribution_effective_dt,
		jit_network_distribution_cd AS jit_network_distribution_cd,
		sears_network_distribution_cd AS sears_network_distribution_cd,
		sears_future_network_distribution_effective_dt AS sears_future_network_effective_dt,
		sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd,
		sears_future_network_distribution_cd AS sears_future_network_distribution_cd,
		reorder_authentication_cd AS reorder_authorization_cd,
		can_carry_model_id AS can_carry_model_id,
		grocery_item_ind AS grocery_item_ind,
		iplan_id AS iplan_id,
		markdown_style_reference_cd AS markdown_style_reference_cd,
		forecast_group_format_id AS forecast_group_format_id,
		forecast_group_desc AS forecast_group_desc,
		referred_ksn_id AS referred_ksn_id,
		special_retail_order_system_ind as special_retail_order_system_ind,
		sears_division_nbr as sears_division_nbr,
		sears_item_nbr as sears_item_nbr;


/*********** JOIN PREVIOUS DATA TO ITEM_RPT_COST TABLE ON ITEM_ID *****************/
join_item_rpt_cost_with_data = 
    JOIN work_idrp_eligible_item_shc_item_step1 BY shc_item_id 
         LEFT OUTER,
         item_rpt_cost_data BY item_id; 

grp_join_item_rpt_cost_with_data = 
    GROUP join_item_rpt_cost_with_data 
    BY shc_item_id;

grp_join_item_rpt_cost_with_data = 
    FOREACH grp_join_item_rpt_cost_with_data 
	    {
		    a = ORDER $1 BY fisc_wk_end_dt DESC ;
			b = LIMIT a 1;
			GENERATE FLATTEN (b);
		};					

work_idrp_eligible_item_shc_item_step2 = 
    FOREACH grp_join_item_rpt_cost_with_data 
    GENERATE 
		shc_item_id AS shc_item_id,
		shc_item_desc AS shc_item_desc,
		shc_division_nbr AS shc_division_nbr,
		shc_division_desc AS shc_division_desc,
		shc_department_nbr AS shc_department_nbr,
		shc_department_desc AS shc_department_desc,
		shc_category_group_level_nbr AS shc_category_group_level_nbr,
		shc_category_group_desc AS shc_category_group_desc,
		shc_category_nbr AS shc_category_nbr,
		shc_category_desc AS shc_category_desc,
		shc_sub_category_nbr AS shc_sub_category_nbr,
		shc_sub_category_desc AS shc_sub_category_desc,
		delivered_direct_ind AS delivered_direct_ind,
		installation_ind  AS installation_ind,
		store_forecast_cd AS store_forecast_cd,
		idrp_order_method_cd AS idrp_order_method_cd,
		idrp_order_method_desc AS idrp_order_method_desc,
		shc_item_type_cd AS shc_item_type_cd,
		purchase_status_cd AS purchase_status_cd,
		network_distribution_cd AS network_distribution_cd,
		future_network_distribution_cd AS future_network_distribution_cd,
		future_network_distribution_effective_dt AS future_network_distribution_effective_dt,
		jit_network_distribution_cd AS jit_network_distribution_cd,
		sears_network_distribution_cd AS sears_network_distribution_cd,
		sears_future_network_effective_dt AS sears_future_network_effective_dt,
		sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd,
		sears_future_network_distribution_cd AS sears_future_network_distribution_cd,
		reorder_authorization_cd AS reorder_authorization_cd,
		can_carry_model_id AS can_carry_model_id,
		grocery_item_ind AS grocery_item_ind,
		iplan_id AS iplan_id,
		markdown_style_reference_cd AS markdown_style_reference_cd,
		forecast_group_format_id AS forecast_group_format_id,
		forecast_group_desc AS forecast_group_desc,
		referred_ksn_id AS referred_ksn_id,
		special_retail_order_system_ind as special_retail_order_system_ind,
		sears_division_nbr as sears_division_nbr,
		sears_item_nbr as sears_item_nbr,
		(corp_90dy_avg_cost IS NULL ? '0.0000' : corp_90dy_avg_cost) AS national_unit_cost_amt;

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
    FILTER DEFAULT_DATA  
	BY ('$CURRENT_TIMESTAMP' >= effective_ts and '$CURRENT_TIMESTAMP' <= expiration_ts);
	
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
    JOIN work_idrp_eligible_item_shc_item_step2 BY (long)shc_item_id 
         LEFT OUTER ,
		 PRC_DATA BY (long)item_id;

work_idrp_eligible_item_shc_item_step2_test = 
	FOREACH join_dflt_prc_v2_oi_item_item_id 
	GENERATE
		shc_item_id AS shc_item_id,
		shc_item_desc AS shc_item_desc,
		shc_division_nbr AS shc_division_nbr,
		shc_division_desc AS shc_division_desc,
		shc_department_nbr AS shc_department_nbr,
		shc_department_desc AS shc_department_desc,
		shc_category_group_level_nbr AS shc_category_group_level_nbr,
		shc_category_group_desc AS shc_category_group_desc,
		shc_category_nbr AS shc_category_nbr,
		shc_category_desc AS shc_category_desc,
		shc_sub_category_nbr AS shc_sub_category_nbr,
		shc_sub_category_desc AS shc_sub_category_desc,
		delivered_direct_ind AS delivered_direct_ind,
		installation_ind  AS installation_ind,
		store_forecast_cd AS store_forecast_cd,
		idrp_order_method_cd AS idrp_order_method_cd,
		idrp_order_method_desc AS idrp_order_method_desc,
		shc_item_type_cd AS shc_item_type_cd,
		purchase_status_cd AS purchase_status_cd,
		network_distribution_cd AS network_distribution_cd,
		future_network_distribution_cd AS future_network_distribution_cd,
		future_network_distribution_effective_dt AS future_network_distribution_effective_dt,
		jit_network_distribution_cd AS jit_network_distribution_cd,
		sears_network_distribution_cd AS sears_network_distribution_cd,
		sears_future_network_effective_dt AS sears_future_network_effective_dt,
		sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd,
		sears_future_network_distribution_cd AS sears_future_network_distribution_cd,
		reorder_authorization_cd AS reorder_authorization_cd,
		can_carry_model_id AS can_carry_model_id,
		grocery_item_ind AS grocery_item_ind,
		iplan_id AS iplan_id,
		markdown_style_reference_cd AS markdown_style_reference_cd,
		forecast_group_format_id AS forecast_group_format_id,
		forecast_group_desc AS forecast_group_desc,
		referred_ksn_id AS referred_ksn_id,
		special_retail_order_system_ind as special_retail_order_system_ind,
		national_unit_cost_amt AS national_unit_cost_amt,
		sears_division_nbr as sears_division_nbr,
		sears_item_nbr as sears_item_nbr,
		(chararray)((float)price_amt/(float)price_multiple_qty) AS product_selling_price_amt ;        

work_idrp_eligible_item_shc_item_step2 = 
	FOREACH work_idrp_eligible_item_shc_item_step2_test 
	GENERATE
		shc_item_id ,
		shc_item_desc ,
		shc_division_nbr ,
		shc_division_desc ,
		shc_department_nbr ,
		shc_department_desc ,
		shc_category_group_level_nbr ,
		shc_category_group_desc ,
		shc_category_nbr ,
		shc_category_desc ,
		shc_sub_category_nbr ,
		shc_sub_category_desc ,
		delivered_direct_ind ,
		installation_ind  ,
		store_forecast_cd ,
		idrp_order_method_cd ,
		idrp_order_method_desc ,
		shc_item_type_cd ,
		purchase_status_cd ,
		network_distribution_cd ,
		future_network_distribution_cd ,
		future_network_distribution_effective_dt ,
		jit_network_distribution_cd ,
		sears_network_distribution_cd ,
		sears_future_network_effective_dt ,
		sears_emp_network_distribution_cd ,
		sears_future_network_distribution_cd ,
		reorder_authorization_cd ,
		can_carry_model_id ,
		grocery_item_ind ,
		iplan_id ,
		markdown_style_reference_cd ,
		forecast_group_format_id ,
		forecast_group_desc ,
		referred_ksn_id ,
		special_retail_order_system_ind,
		national_unit_cost_amt,
		((IsNull(TRIM(product_selling_price_amt),'')=='')? '0': product_selling_price_amt) AS product_selling_price_amt,
		sears_division_nbr AS sears_division_nbr,
		sears_item_nbr AS sears_item_nbr;

----------------------------------------------------------------------------------------------------------
join_data_to_rpt_grp = 
    JOIN work_idrp_eligible_item_shc_item_step2 
		BY (long)shc_item_id 
    LEFT OUTER,
	     item_rpt_grp_data 
		BY (long)item_id;

work_idrp_eligible_item_shc_item_step3 = 
	FOREACH join_data_to_rpt_grp 
	GENERATE 
		shc_item_id AS shc_item_id ,
		shc_item_desc AS shc_item_desc ,
		shc_division_nbr AS shc_division_nbr ,
		shc_division_desc AS shc_division_desc ,
		shc_department_nbr AS shc_department_nbr ,
		shc_department_desc AS shc_department_desc ,
		shc_category_group_level_nbr AS shc_category_group_level_nbr ,
		shc_category_group_desc AS shc_category_group_desc ,
		shc_category_nbr AS shc_category_nbr ,
		shc_category_desc AS shc_category_desc ,
		shc_sub_category_nbr AS shc_sub_category_nbr ,
		shc_sub_category_desc AS shc_sub_category_desc ,
		delivered_direct_ind AS delivered_direct_ind ,
		installation_ind  AS installation_ind  ,
		store_forecast_cd AS store_forecast_cd ,
		idrp_order_method_cd AS idrp_order_method_cd ,
		idrp_order_method_desc AS idrp_order_method_desc ,
		shc_item_type_cd AS shc_item_type_cd ,
		purchase_status_cd AS purchase_status_cd ,
		network_distribution_cd AS network_distribution_cd ,
		future_network_distribution_cd AS future_network_distribution_cd ,
		future_network_distribution_effective_dt AS future_network_distribution_effective_dt ,
		jit_network_distribution_cd AS jit_network_distribution_cd ,
		sears_network_distribution_cd AS sears_network_distribution_cd ,
		sears_future_network_effective_dt AS sears_future_network_effective_dt ,
		sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd ,
		sears_future_network_distribution_cd AS sears_future_network_distribution_cd ,
		reorder_authorization_cd AS reorder_authorization_cd ,
		can_carry_model_id AS can_carry_model_id ,
		grocery_item_ind AS grocery_item_ind ,
		iplan_id AS iplan_id ,
		markdown_style_reference_cd AS markdown_style_reference_cd ,
		forecast_group_format_id AS forecast_group_format_id ,
		forecast_group_desc AS forecast_group_desc ,
		referred_ksn_id AS referred_ksn_id ,
		special_retail_order_system_ind as special_retail_order_system_ind,
		national_unit_cost_amt AS national_unit_cost_amt,	 
		product_selling_price_amt AS product_selling_price_amt,
		rpt_grp_id AS item_report_group_id,
		rpt_grp_seq_nbr AS item_report_sequence_nbr,
		sears_division_nbr as sears_division_nbr,
		sears_item_nbr as sears_item_nbr;	 

NEW_JOIN = 
	JOIN work_idrp_eligible_item_shc_item_step3 
		BY (sears_division_nbr,sears_item_nbr) LEFT OUTER, 
		 LOAD_CORE_BRIDGE_ITEM 
		BY (sears_division_nbr,sears_item_nbr) ;

NEW_JOIN_OUTPUT = GROUP NEW_JOIN BY (work_idrp_eligible_item_shc_item_step3::shc_item_id);

NEW_JOIN_OUTPUT_FINAL = 
	FOREACH NEW_JOIN_OUTPUT
		{	ord_data_1 = ORDER NEW_JOIN BY LOAD_CORE_BRIDGE_ITEM::item_order_system_cd ASC;
			ord_data_lmt_1 = LIMIT ord_data_1 1;
			GENERATE FLATTEN(ord_data_lmt_1);			
		};
		
work_idrp_eligible_item_shc_item_step4 = 
	FOREACH NEW_JOIN_OUTPUT_FINAL 
	GENERATE
		shc_item_id AS shc_item_id ,
		shc_item_desc AS shc_item_desc ,
		shc_division_nbr AS shc_division_nbr ,
		shc_division_desc AS shc_division_desc ,
		shc_department_nbr AS shc_department_nbr ,
		shc_department_desc AS shc_department_desc ,
		shc_category_group_level_nbr AS shc_category_group_level_nbr ,
		shc_category_group_desc AS shc_category_group_desc ,
		shc_category_nbr AS shc_category_nbr ,
		shc_category_desc AS shc_category_desc ,
		shc_sub_category_nbr AS shc_sub_category_nbr ,
		shc_sub_category_desc AS shc_sub_category_desc ,
		delivered_direct_ind AS delivered_direct_ind ,
		installation_ind  AS installation_ind  ,
		store_forecast_cd AS store_forecast_cd ,
		idrp_order_method_cd AS idrp_order_method_cd ,
		idrp_order_method_desc AS idrp_order_method_desc ,
		shc_item_type_cd AS shc_item_type_cd ,
		purchase_status_cd AS purchase_status_cd ,
		network_distribution_cd AS network_distribution_cd ,
		future_network_distribution_cd AS future_network_distribution_cd ,
		future_network_distribution_effective_dt AS future_network_distribution_effective_dt ,
		jit_network_distribution_cd AS jit_network_distribution_cd ,
		sears_network_distribution_cd AS sears_network_distribution_cd ,
		sears_future_network_effective_dt AS sears_future_network_effective_dt ,
		sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd ,
		sears_future_network_distribution_cd AS sears_future_network_distribution_cd ,
		reorder_authorization_cd AS reorder_authorization_cd ,
		can_carry_model_id AS can_carry_model_id ,
		grocery_item_ind AS grocery_item_ind ,
		iplan_id AS iplan_id ,
		markdown_style_reference_cd AS markdown_style_reference_cd ,
		forecast_group_format_id AS forecast_group_format_id ,
		forecast_group_desc AS forecast_group_desc ,
		referred_ksn_id AS referred_ksn_id ,
		special_retail_order_system_ind as special_retail_order_system_ind,
		national_unit_cost_amt AS national_unit_cost_amt,	 
		product_selling_price_amt AS product_selling_price_amt,
		item_report_group_id AS item_report_group_id,
		item_report_sequence_nbr AS item_report_sequence_nbr,
		LOAD_CORE_BRIDGE_ITEM::item_order_system_cd as order_system_cd ;

-- Calculating dotcom_assorted_cd 

--------------------------------------------------------------------------------------------------------------
LOAD_GOLD_ITEM_NEW = 
	FOREACH LOAD_GOLD_ITEM 
	GENERATE
		(item_id is NULL 
			? ''
			:item_id) as item_id,
		(ksn_purchase_status_cd is NULL 
			? ''
			: ksn_purchase_status_cd) as ksn_purchase_status_cd,
		(dotcom_eligibility_cd is NULL 
			? ''
			: dotcom_eligibility_cd) as dotcom_eligibility_cd,
		(ksn_id is NULL 
			? ''
			: ksn_id) as ksn_id ;

GOLD_ITEM = 
	FILTER LOAD_GOLD_ITEM_NEW 
	BY ksn_purchase_status_cd  != 'U' AND dotcom_eligibility_cd  == '1' ;

GOLD_ITEMS = 
	FOREACH GOLD_ITEM 
	GENERATE 
		item_id;

DISTINCT_GOLD_ITEMS = DISTINCT GOLD_ITEMS;

JOIN_SMITH_GOLD = 
	JOIN work_idrp_eligible_item_shc_item_step4 
		BY shc_item_id LEFT OUTER, 
		 DISTINCT_GOLD_ITEMS 
		BY item_id ;

work_idrp_eligible_item_shc_item_step4 = 
	FOREACH JOIN_SMITH_GOLD 
	GENERATE
		shc_item_id AS shc_item_id ,
		shc_item_desc AS shc_item_desc ,
		shc_division_nbr AS shc_division_nbr ,
		shc_division_desc AS shc_division_desc ,
		shc_department_nbr AS shc_department_nbr ,
		shc_department_desc AS shc_department_desc ,
		shc_category_group_level_nbr AS shc_category_group_level_nbr ,
		shc_category_group_desc AS shc_category_group_desc ,
		shc_category_nbr AS shc_category_nbr ,
		shc_category_desc AS shc_category_desc ,
		shc_sub_category_nbr AS shc_sub_category_nbr ,
		shc_sub_category_desc AS shc_sub_category_desc ,
		delivered_direct_ind AS delivered_direct_ind ,
		installation_ind  AS installation_ind  ,
		store_forecast_cd AS store_forecast_cd ,
		idrp_order_method_cd AS idrp_order_method_cd ,
		idrp_order_method_desc AS idrp_order_method_desc ,
		shc_item_type_cd AS shc_item_type_cd ,
		purchase_status_cd AS purchase_status_cd ,
		network_distribution_cd AS network_distribution_cd ,
		future_network_distribution_cd AS future_network_distribution_cd ,
		future_network_distribution_effective_dt AS future_network_distribution_effective_dt ,
		jit_network_distribution_cd AS jit_network_distribution_cd ,
		sears_network_distribution_cd AS sears_network_distribution_cd ,
		sears_future_network_effective_dt AS sears_future_network_effective_dt ,
		sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd ,
		sears_future_network_distribution_cd AS sears_future_network_distribution_cd ,
		reorder_authorization_cd AS reorder_authorization_cd ,
		can_carry_model_id AS can_carry_model_id ,
		grocery_item_ind AS grocery_item_ind ,
		iplan_id AS iplan_id ,
		markdown_style_reference_cd AS markdown_style_reference_cd ,
		forecast_group_format_id AS forecast_group_format_id ,
		forecast_group_desc AS forecast_group_desc ,
		referred_ksn_id AS referred_ksn_id ,
		special_retail_order_system_ind as special_retail_order_system_ind,
		national_unit_cost_amt AS national_unit_cost_amt,	 
		product_selling_price_amt AS product_selling_price_amt,
		item_report_group_id AS item_report_group_id,
		item_report_sequence_nbr AS item_report_sequence_nbr,
		order_system_cd AS order_system_cd,
		((DISTINCT_GOLD_ITEMS::item_id != '' AND DISTINCT_GOLD_ITEMS::item_id IS NOT NULL ) ? '1':'0') AS dotcom_assorted_ind ,
		'' as roadrunner_eligible_ind ;

-- Coding for US DOT SHIP TYPE CODE

LOAD_PACKAGE_CURRENT_FILTER = 
	FILTER LOAD_PACKAGE_CURRENT 
	BY package_type_cd == 'EACH' OR  package_type_cd == 'ECRT' ;

gold__item_package_current_data_reqd = 
	FOREACH LOAD_PACKAGE_CURRENT_FILTER
	GENERATE
		ksn_id AS ksn_id,
		us_dot_ship_type_cd AS us_dot_ship_type_cd;
		
/*** CR 3703 ********/

gold__item_shc_hierarchy_current_data = 
	FOREACH LOAD_GOLD_ITEM
	GENERATE
	ksn_id AS ksn_id,
	item_id AS item_id;
	
join_gold__item_shc_hierarchy_current_online_fulfillment = 
	JOIN gold__item_shc_hierarchy_current_data BY (item_id) LEFT OUTER,
		 LOAD_ONLINE_FULFILLMENT BY (item_id);
		 
join_gold__item_shc_hierarchy_current_online_fulfillment_filter = 
	FILTER join_gold__item_shc_hierarchy_current_online_fulfillment
	BY IsNull(LOAD_ONLINE_FULFILLMENT::item_id,'') == '';
	
gold__item_shc_hierarchy_current_data_reqd = 
	FOREACH join_gold__item_shc_hierarchy_current_online_fulfillment_filter
	GENERATE
		gold__item_shc_hierarchy_current_data::ksn_id AS ksn_id,
		gold__item_shc_hierarchy_current_data::item_id AS item_id;
	
--LOAD shc_hierarchy and left join to item table 
join_item_to_shc_hierarchy_current = 
    JOIN work_idrp_eligible_item_shc_item_step4 
		BY shc_item_id LEFT OUTER,
         gold__item_shc_hierarchy_current_data_reqd 
		BY item_id ;

join_shc_hierarchy_current_package_current =
    JOIN join_item_to_shc_hierarchy_current 
		BY ksn_id LEFT OUTER,
         gold__item_package_current_data_reqd 
		BY ksn_id;

generate_join_shc_hierarchy_current_package_current  = 
    FOREACH join_shc_hierarchy_current_package_current
	GENERATE 
		shc_item_id AS shc_item_id ,
		shc_item_desc AS shc_item_desc ,
		shc_division_nbr AS shc_division_nbr ,
		shc_division_desc AS shc_division_desc ,
		shc_department_nbr AS shc_department_nbr ,
		shc_department_desc AS shc_department_desc ,
		shc_category_group_level_nbr AS shc_category_group_level_nbr ,
		shc_category_group_desc AS shc_category_group_desc ,
		shc_category_nbr AS shc_category_nbr ,
		shc_category_desc AS shc_category_desc ,
		shc_sub_category_nbr AS shc_sub_category_nbr ,
		shc_sub_category_desc AS shc_sub_category_desc ,
		delivered_direct_ind AS delivered_direct_ind ,
		installation_ind  AS installation_ind  ,
		store_forecast_cd AS store_forecast_cd ,
		idrp_order_method_cd AS idrp_order_method_cd ,
		idrp_order_method_desc AS idrp_order_method_desc ,
		shc_item_type_cd AS shc_item_type_cd ,
		purchase_status_cd AS purchase_status_cd ,
		network_distribution_cd AS network_distribution_cd ,
		future_network_distribution_cd AS future_network_distribution_cd ,
		future_network_distribution_effective_dt AS future_network_distribution_effective_dt ,
		jit_network_distribution_cd AS jit_network_distribution_cd ,
		sears_network_distribution_cd AS sears_network_distribution_cd ,
		sears_future_network_effective_dt AS sears_future_network_effective_dt ,
		sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd ,
		sears_future_network_distribution_cd AS sears_future_network_distribution_cd ,
		reorder_authorization_cd AS reorder_authorization_cd ,
		can_carry_model_id AS can_carry_model_id ,
		grocery_item_ind AS grocery_item_ind ,
		iplan_id AS iplan_id ,
		markdown_style_reference_cd AS markdown_style_reference_cd ,
		forecast_group_format_id AS forecast_group_format_id ,
		forecast_group_desc AS forecast_group_desc ,
		referred_ksn_id AS referred_ksn_id ,
		special_retail_order_system_ind as special_retail_order_system_ind,
		national_unit_cost_amt AS national_unit_cost_amt,	 
		product_selling_price_amt AS product_selling_price_amt,
		item_report_group_id AS item_report_group_id,
		item_report_sequence_nbr AS item_report_sequence_nbr,
		order_system_cd AS order_system_cd,
		dotcom_assorted_ind AS dotcom_assorted_ind ,
		roadrunner_eligible_ind AS roadrunner_eligible_ind ,
		gold__item_package_current_data_reqd::us_dot_ship_type_cd AS us_dot_ship_type_cd ,
		(gold__item_package_current_data_reqd::us_dot_ship_type_cd IS NULL ? '1' : (gold__item_package_current_data_reqd::us_dot_ship_type_cd == 'H' ? '3' : (gold__item_package_current_data_reqd::us_dot_ship_type_cd == 'S' ? '2' : '1'))) AS severity,
		gold__item_package_current_data_reqd::ksn_id AS ksn_id;


order_data_by_severity =
	ORDER generate_join_shc_hierarchy_current_package_current
	BY shc_item_id ASC, severity DESC ;

distinct_order_data_by_severity = DISTINCT order_data_by_severity;
	
grp_distinct_order_data_by_severity =
	GROUP distinct_order_data_by_severity 	
	BY shc_item_id;

generate_valid_and_invalid_items =
	FOREACH grp_distinct_order_data_by_severity
    GENERATE         
	group AS shc_item_id ,
	com.searshc.supplychain.idrp.udf.HasMultipleValues(distinct_order_data_by_severity.us_dot_ship_type_cd) AS error_value;		

join_with_actual_data =
	JOIN generate_valid_and_invalid_items 
		BY shc_item_id,
		 distinct_order_data_by_severity 
		BY shc_item_id;

SPLIT join_with_actual_data INTO 
	invalid_records IF error_value == 'MULTIPLE',
	valid_records IF error_value != 'MULTIPLE';

group_valid_records_by_item = 
	GROUP valid_records
	BY distinct_order_data_by_severity::shc_item_id;

flatten_valid_records =
	FOREACH group_valid_records_by_item
		{
			order_data = ORDER $1 BY severity DESC ;
			limit_data = LIMIT order_data 1;
			GENERATE FLATTEN (limit_data);
		}; 
		
flatten_valid_records = 
    FOREACH flatten_valid_records
    GENERATE 
		limit_data::distinct_order_data_by_severity::shc_item_id AS shc_item_id ,
		limit_data::distinct_order_data_by_severity::shc_item_desc AS shc_item_desc ,
		limit_data::distinct_order_data_by_severity::shc_division_nbr AS shc_division_nbr ,
		limit_data::distinct_order_data_by_severity::shc_division_desc AS shc_division_desc ,
		limit_data::distinct_order_data_by_severity::shc_department_nbr AS shc_department_nbr ,
		limit_data::distinct_order_data_by_severity::shc_department_desc AS shc_department_desc ,
		limit_data::distinct_order_data_by_severity::shc_category_group_level_nbr AS shc_category_group_level_nbr ,
		limit_data::distinct_order_data_by_severity::shc_category_group_desc AS shc_category_group_desc ,
		limit_data::distinct_order_data_by_severity::shc_category_nbr AS shc_category_nbr ,
		limit_data::distinct_order_data_by_severity::shc_category_desc AS shc_category_desc ,
		limit_data::distinct_order_data_by_severity::shc_sub_category_nbr AS shc_sub_category_nbr ,
		limit_data::distinct_order_data_by_severity::shc_sub_category_desc AS shc_sub_category_desc ,
		limit_data::distinct_order_data_by_severity::delivered_direct_ind AS delivered_direct_ind ,
		limit_data::distinct_order_data_by_severity::installation_ind  AS installation_ind  ,
		limit_data::distinct_order_data_by_severity::store_forecast_cd AS store_forecast_cd ,
		limit_data::distinct_order_data_by_severity::idrp_order_method_cd AS idrp_order_method_cd ,
		limit_data::distinct_order_data_by_severity::idrp_order_method_desc AS idrp_order_method_desc ,
		limit_data::distinct_order_data_by_severity::shc_item_type_cd AS shc_item_type_cd ,
		limit_data::distinct_order_data_by_severity::purchase_status_cd AS purchase_status_cd ,
		limit_data::distinct_order_data_by_severity::network_distribution_cd AS network_distribution_cd ,
		limit_data::distinct_order_data_by_severity::future_network_distribution_cd AS future_network_distribution_cd ,
		limit_data::distinct_order_data_by_severity::future_network_distribution_effective_dt AS future_network_distribution_effective_dt ,
		limit_data::distinct_order_data_by_severity::jit_network_distribution_cd AS jit_network_distribution_cd ,
		limit_data::distinct_order_data_by_severity::sears_network_distribution_cd AS sears_network_distribution_cd ,
		limit_data::distinct_order_data_by_severity::sears_future_network_effective_dt AS sears_future_network_effective_dt ,
		limit_data::distinct_order_data_by_severity::sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd ,
		limit_data::distinct_order_data_by_severity::sears_future_network_distribution_cd AS sears_future_network_distribution_cd ,
		limit_data::distinct_order_data_by_severity::reorder_authorization_cd AS reorder_authorization_cd ,
		limit_data::distinct_order_data_by_severity::can_carry_model_id AS can_carry_model_id ,
		limit_data::distinct_order_data_by_severity::grocery_item_ind AS grocery_item_ind ,
		limit_data::distinct_order_data_by_severity::iplan_id AS iplan_id ,
		limit_data::distinct_order_data_by_severity::markdown_style_reference_cd AS markdown_style_reference_cd ,
		limit_data::distinct_order_data_by_severity::forecast_group_format_id AS forecast_group_format_id ,
		limit_data::distinct_order_data_by_severity::forecast_group_desc AS forecast_group_desc ,
		limit_data::distinct_order_data_by_severity::referred_ksn_id AS referred_ksn_id ,
		limit_data::distinct_order_data_by_severity::special_retail_order_system_ind as special_retail_order_system_ind,
		limit_data::distinct_order_data_by_severity::national_unit_cost_amt AS national_unit_cost_amt , 
		limit_data::distinct_order_data_by_severity::product_selling_price_amt AS product_selling_price_amt ,
		limit_data::distinct_order_data_by_severity::item_report_group_id AS item_report_group_id ,
		limit_data::distinct_order_data_by_severity::item_report_sequence_nbr AS item_report_sequence_nbr ,
		limit_data::distinct_order_data_by_severity::order_system_cd AS order_system_cd ,
		limit_data::distinct_order_data_by_severity::dotcom_assorted_ind AS dotcom_assorted_ind ,
		limit_data::distinct_order_data_by_severity::roadrunner_eligible_ind AS roadrunner_eligible_ind ,
		limit_data::distinct_order_data_by_severity::us_dot_ship_type_cd AS us_dot_ship_type_cd,
		limit_data::distinct_order_data_by_severity::severity AS severity,
		limit_data::distinct_order_data_by_severity::ksn_id AS ksn_id ;
        
join_again_with_item =     
    JOIN work_idrp_eligible_item_shc_item_step4 
		BY shc_item_id 
        LEFT OUTER ,
         flatten_valid_records 
		BY shc_item_id ;

work_idrp_eligible_item_shc_item_step5 = 
	FOREACH join_again_with_item 
	GENERATE
		work_idrp_eligible_item_shc_item_step4::shc_item_id AS shc_item_id ,
		work_idrp_eligible_item_shc_item_step4::shc_item_desc AS shc_item_desc ,
		work_idrp_eligible_item_shc_item_step4::shc_division_nbr AS shc_division_nbr ,
		work_idrp_eligible_item_shc_item_step4::shc_division_desc AS shc_division_desc ,
		work_idrp_eligible_item_shc_item_step4::shc_department_nbr AS shc_department_nbr ,
		work_idrp_eligible_item_shc_item_step4::shc_department_desc AS shc_department_desc ,
		work_idrp_eligible_item_shc_item_step4::shc_category_group_level_nbr AS shc_category_group_level_nbr ,
		work_idrp_eligible_item_shc_item_step4::shc_category_group_desc AS shc_category_group_desc ,
		work_idrp_eligible_item_shc_item_step4::shc_category_nbr AS shc_category_nbr ,
		work_idrp_eligible_item_shc_item_step4::shc_category_desc AS shc_category_desc ,
		work_idrp_eligible_item_shc_item_step4::shc_sub_category_nbr AS shc_sub_category_nbr ,
		work_idrp_eligible_item_shc_item_step4::shc_sub_category_desc AS shc_sub_category_desc ,
		work_idrp_eligible_item_shc_item_step4::delivered_direct_ind AS delivered_direct_ind ,
		work_idrp_eligible_item_shc_item_step4::installation_ind  AS installation_ind  ,
		work_idrp_eligible_item_shc_item_step4::store_forecast_cd AS store_forecast_cd ,
		work_idrp_eligible_item_shc_item_step4::idrp_order_method_cd AS idrp_order_method_cd ,
		work_idrp_eligible_item_shc_item_step4::idrp_order_method_desc AS idrp_order_method_desc ,
		work_idrp_eligible_item_shc_item_step4::shc_item_type_cd AS shc_item_type_cd ,
		work_idrp_eligible_item_shc_item_step4::purchase_status_cd AS purchase_status_cd ,
		work_idrp_eligible_item_shc_item_step4::network_distribution_cd AS network_distribution_cd ,
		work_idrp_eligible_item_shc_item_step4::future_network_distribution_cd AS future_network_distribution_cd ,
		work_idrp_eligible_item_shc_item_step4::future_network_distribution_effective_dt AS future_network_distribution_effective_dt ,
		work_idrp_eligible_item_shc_item_step4::jit_network_distribution_cd AS jit_network_distribution_cd ,
		work_idrp_eligible_item_shc_item_step4::sears_network_distribution_cd AS sears_network_distribution_cd ,
		work_idrp_eligible_item_shc_item_step4::sears_future_network_effective_dt AS sears_future_network_effective_dt ,
		work_idrp_eligible_item_shc_item_step4::sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd ,
		work_idrp_eligible_item_shc_item_step4::sears_future_network_distribution_cd AS sears_future_network_distribution_cd ,
		work_idrp_eligible_item_shc_item_step4::reorder_authorization_cd AS reorder_authorization_cd ,
		work_idrp_eligible_item_shc_item_step4::can_carry_model_id AS can_carry_model_id ,
		work_idrp_eligible_item_shc_item_step4::grocery_item_ind AS grocery_item_ind ,
		work_idrp_eligible_item_shc_item_step4::iplan_id AS iplan_id ,
		work_idrp_eligible_item_shc_item_step4::markdown_style_reference_cd AS markdown_style_reference_cd ,
		work_idrp_eligible_item_shc_item_step4::forecast_group_format_id AS forecast_group_format_id ,
		work_idrp_eligible_item_shc_item_step4::forecast_group_desc AS forecast_group_desc ,
		work_idrp_eligible_item_shc_item_step4::referred_ksn_id AS referred_ksn_id ,
		work_idrp_eligible_item_shc_item_step4::special_retail_order_system_ind as special_retail_order_system_ind,
		work_idrp_eligible_item_shc_item_step4::national_unit_cost_amt AS national_unit_cost_amt,	 
		work_idrp_eligible_item_shc_item_step4::product_selling_price_amt AS product_selling_price_amt,
		work_idrp_eligible_item_shc_item_step4::item_report_group_id AS item_report_group_id,
		work_idrp_eligible_item_shc_item_step4::item_report_sequence_nbr AS item_report_sequence_nbr,
		work_idrp_eligible_item_shc_item_step4::order_system_cd AS order_system_cd,
		work_idrp_eligible_item_shc_item_step4::dotcom_assorted_ind AS dotcom_assorted_ind ,
		work_idrp_eligible_item_shc_item_step4::roadrunner_eligible_ind AS roadrunner_eligible_ind ,
		flatten_valid_records::us_dot_ship_type_cd AS us_dot_ship_type_cd ;

/**** FOR INVALID OR ERROR DATA ***/

error_file_1 = 
    FOREACH invalid_records
    GENERATE
        '$CURRENT_TIMESTAMP' AS load_ts, 
        distinct_order_data_by_severity::shc_item_id AS item_id,
        distinct_order_data_by_severity::ksn_id AS ksn_id,
        NULL AS sears_division_nbr,
        NULL AS sears_item_nbr,
        NULL AS sears_sku_nbr,
        NULL AS websku,
        NULL AS package_id,
        distinct_order_data_by_severity::us_dot_ship_type_cd AS error_value,
        'Multiple US DOT Ship Type Code values were found for an Item' AS error_desc,
		'$batchid' AS idrp_batch_id;
		
----------------------------------------------------------------------------------------------------------------------------

-- Logic for Mailable Indicator

/**** CR 3703 *****/

join_gold_item_online_fulfillment = 
	JOIN GOLD_ITEM BY (item_id) LEFT OUTER,
		 LOAD_ONLINE_FULFILLMENT BY (item_id);
		 
join_gold_item_online_fulfillment_filter = 
	FILTER join_gold_item_online_fulfillment
	BY IsNull(LOAD_ONLINE_FULFILLMENT::item_id,'') == '';

GOLD_ITEM_gen = 
	FOREACH join_gold_item_online_fulfillment_filter
	GENERATE
		GOLD_ITEM::item_id AS item_id,
		ksn_purchase_status_cd AS ksn_purchase_status_cd,
		dotcom_eligibility_cd AS dotcom_eligibility_cd,
		GOLD_ITEM::ksn_id AS ksn_id;

--LOAD shc_hierarchy and left join to item table 
join_item_to_shc_hierarchy_current = 
    JOIN work_idrp_eligible_item_shc_item_step5 BY shc_item_id LEFT OUTER,
         GOLD_ITEM_gen BY item_id ;

join_shc_hierarchy_current_package_current =
    JOIN join_item_to_shc_hierarchy_current BY ksn_id LEFT OUTER,
         LOAD_PACKAGE_CURRENT_FILTER BY ksn_id;


work_idrp_eligible_item_shc_item_step15 = 
	FOREACH join_shc_hierarchy_current_package_current 
	GENERATE
		shc_item_id AS shc_item_id ,
		shc_item_desc AS shc_item_desc ,
		shc_division_nbr AS shc_division_nbr ,
		shc_division_desc AS shc_division_desc ,
		shc_department_nbr AS shc_department_nbr ,
		shc_department_desc AS shc_department_desc ,
		shc_category_group_level_nbr AS shc_category_group_level_nbr ,
		shc_category_group_desc AS shc_category_group_desc ,
		shc_category_nbr AS shc_category_nbr ,
		shc_category_desc AS shc_category_desc ,
		shc_sub_category_nbr AS shc_sub_category_nbr ,
		shc_sub_category_desc AS shc_sub_category_desc ,
		delivered_direct_ind AS delivered_direct_ind ,
		installation_ind  AS installation_ind  ,
		store_forecast_cd AS store_forecast_cd ,
		idrp_order_method_cd AS idrp_order_method_cd ,
		idrp_order_method_desc AS idrp_order_method_desc ,
		shc_item_type_cd AS shc_item_type_cd ,
		purchase_status_cd AS purchase_status_cd ,
		network_distribution_cd AS network_distribution_cd ,
		future_network_distribution_cd AS future_network_distribution_cd ,
		future_network_distribution_effective_dt AS future_network_distribution_effective_dt ,
		jit_network_distribution_cd AS jit_network_distribution_cd ,
		sears_network_distribution_cd AS sears_network_distribution_cd ,
		sears_future_network_effective_dt AS sears_future_network_effective_dt ,
		sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd ,
		sears_future_network_distribution_cd AS sears_future_network_distribution_cd ,
		reorder_authorization_cd AS reorder_authorization_cd ,
		can_carry_model_id AS can_carry_model_id ,
		grocery_item_ind AS grocery_item_ind ,
		iplan_id AS iplan_id ,
		markdown_style_reference_cd AS markdown_style_reference_cd ,
		forecast_group_format_id AS forecast_group_format_id ,
		forecast_group_desc AS forecast_group_desc ,
		referred_ksn_id AS referred_ksn_id ,
		special_retail_order_system_ind as special_retail_order_system_ind,
		national_unit_cost_amt AS national_unit_cost_amt,	 
		product_selling_price_amt AS product_selling_price_amt,
		item_report_group_id AS item_report_group_id,
		item_report_sequence_nbr AS item_report_sequence_nbr,
		order_system_cd AS order_system_cd,
		dotcom_assorted_ind AS dotcom_assorted_ind ,
		roadrunner_eligible_ind AS roadrunner_eligible_ind ,
		work_idrp_eligible_item_shc_item_step5::us_dot_ship_type_cd AS us_dot_ship_type_cd,
		((LOAD_PACKAGE_CURRENT_FILTER::package_height_inch_qty=='' OR LOAD_PACKAGE_CURRENT_FILTER::package_height_inch_qty IS NULL OR LOAD_PACKAGE_CURRENT_FILTER::package_depth_inch_qty=='' OR LOAD_PACKAGE_CURRENT_FILTER::package_depth_inch_qty IS NULL OR LOAD_PACKAGE_CURRENT_FILTER::package_width_inch_qty=='' OR LOAD_PACKAGE_CURRENT_FILTER::package_width_inch_qty IS NULL OR LOAD_PACKAGE_CURRENT_FILTER::package_weight_pounds_qty=='' OR LOAD_PACKAGE_CURRENT_FILTER::package_weight_pounds_qty IS NULL) ? 'N' : ((((double)LOAD_PACKAGE_CURRENT_FILTER::package_height_inch_qty >= (double)LOAD_PACKAGE_CURRENT_FILTER::package_depth_inch_qty) AND ((double)LOAD_PACKAGE_CURRENT_FILTER::package_height_inch_qty >= (double)LOAD_PACKAGE_CURRENT_FILTER::package_width_inch_qty)) ? (((double)LOAD_PACKAGE_CURRENT_FILTER::package_height_inch_qty + 2*(double)((double)LOAD_PACKAGE_CURRENT_FILTER::package_depth_inch_qty + (double)LOAD_PACKAGE_CURRENT_FILTER::package_width_inch_qty)) < 130 ? ((double)LOAD_PACKAGE_CURRENT_FILTER::package_height_inch_qty < 108 ? ((double)LOAD_PACKAGE_CURRENT_FILTER::package_weight_pounds_qty < 150 ? 'Y' : 'N') : 'N') : 'N') : ((((double)LOAD_PACKAGE_CURRENT_FILTER::package_depth_inch_qty >= (double)LOAD_PACKAGE_CURRENT_FILTER::package_height_inch_qty) AND ((double)LOAD_PACKAGE_CURRENT_FILTER::package_depth_inch_qty >= (double)LOAD_PACKAGE_CURRENT_FILTER::package_width_inch_qty)) ? (((double)LOAD_PACKAGE_CURRENT_FILTER::package_depth_inch_qty + 2*(double)((double)LOAD_PACKAGE_CURRENT_FILTER::package_height_inch_qty + (double)LOAD_PACKAGE_CURRENT_FILTER::package_width_inch_qty)) < 130 ? ((double)LOAD_PACKAGE_CURRENT_FILTER::package_depth_inch_qty < 108 ? ((double)LOAD_PACKAGE_CURRENT_FILTER::package_weight_pounds_qty < 150 ? 'Y' : 'N') : 'N') : 'N') : ((((double)LOAD_PACKAGE_CURRENT_FILTER::package_width_inch_qty >= (double)LOAD_PACKAGE_CURRENT_FILTER::package_height_inch_qty) AND ((double)LOAD_PACKAGE_CURRENT_FILTER::package_width_inch_qty >= (double)LOAD_PACKAGE_CURRENT_FILTER::package_depth_inch_qty)) ? (((double)LOAD_PACKAGE_CURRENT_FILTER::package_width_inch_qty + 2*(double)((double)LOAD_PACKAGE_CURRENT_FILTER::package_height_inch_qty + (double)LOAD_PACKAGE_CURRENT_FILTER::package_depth_inch_qty)) < 130 ? ((double)LOAD_PACKAGE_CURRENT_FILTER::package_width_inch_qty < 108 ? ((double)LOAD_PACKAGE_CURRENT_FILTER::package_weight_pounds_qty < 150 ? 'Y' : 'N') : 'N') : 'N') : 'N')))) AS dotcom_mailable_ind;  


-----------------------Checking for multiple dotcom_mailable_ind for an item----------------------------------------------

gruoped_data = 
	GROUP work_idrp_eligible_item_shc_item_step15 
	BY shc_item_id;

gruoped_data_gen = 
	FOREACH gruoped_data 
	GENERATE 
		group AS shc_item_id,
		com.searshc.supplychain.idrp.udf.HasMultipleValues(work_idrp_eligible_item_shc_item_step15.dotcom_mailable_ind) AS check_error;
						   
join_grouped_data_prev_data = 
	JOIN work_idrp_eligible_item_shc_item_step15 BY shc_item_id, 
		 gruoped_data_gen BY shc_item_id;

join_grouped_data_prev_data_gen = 
	FOREACH join_grouped_data_prev_data 
	GENERATE 
		work_idrp_eligible_item_shc_item_step15::shc_item_id AS	shc_item_id,
		work_idrp_eligible_item_shc_item_step15::shc_item_desc AS shc_item_desc,
		work_idrp_eligible_item_shc_item_step15::shc_division_nbr AS shc_division_nbr,
		work_idrp_eligible_item_shc_item_step15::shc_division_desc AS shc_division_desc,
		work_idrp_eligible_item_shc_item_step15::shc_department_nbr AS shc_department_nbr,
		work_idrp_eligible_item_shc_item_step15::shc_department_desc AS shc_department_desc,
		work_idrp_eligible_item_shc_item_step15::shc_category_group_level_nbr AS shc_category_group_level_nbr,
		work_idrp_eligible_item_shc_item_step15::shc_category_group_desc AS	shc_category_group_desc,
		work_idrp_eligible_item_shc_item_step15::shc_category_nbr AS shc_category_nbr,
		work_idrp_eligible_item_shc_item_step15::shc_category_desc AS shc_category_desc,
		work_idrp_eligible_item_shc_item_step15::shc_sub_category_nbr AS shc_sub_category_nbr,
		work_idrp_eligible_item_shc_item_step15::shc_sub_category_desc AS shc_sub_category_desc,
		work_idrp_eligible_item_shc_item_step15::delivered_direct_ind AS delivered_direct_ind,
		work_idrp_eligible_item_shc_item_step15::installation_ind AS installation_ind,
		work_idrp_eligible_item_shc_item_step15::store_forecast_cd AS store_forecast_cd,
		work_idrp_eligible_item_shc_item_step15::idrp_order_method_cd AS idrp_order_method_cd,
		work_idrp_eligible_item_shc_item_step15::idrp_order_method_desc AS idrp_order_method_desc,
		work_idrp_eligible_item_shc_item_step15::shc_item_type_cd AS shc_item_type_cd,
		work_idrp_eligible_item_shc_item_step15::purchase_status_cd AS purchase_status_cd,
		work_idrp_eligible_item_shc_item_step15::network_distribution_cd AS	network_distribution_cd,
		work_idrp_eligible_item_shc_item_step15::future_network_distribution_cd AS future_network_distribution_cd,
		work_idrp_eligible_item_shc_item_step15::future_network_distribution_effective_dt AS future_network_distribution_effective_dt,
		work_idrp_eligible_item_shc_item_step15::jit_network_distribution_cd AS	jit_network_distribution_cd,
		work_idrp_eligible_item_shc_item_step15::sears_network_distribution_cd AS sears_network_distribution_cd,
		work_idrp_eligible_item_shc_item_step15::sears_future_network_effective_dt AS sears_future_network_effective_dt,
		work_idrp_eligible_item_shc_item_step15::sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd,
		work_idrp_eligible_item_shc_item_step15::sears_future_network_distribution_cd AS sears_future_network_distribution_cd,
		work_idrp_eligible_item_shc_item_step15::reorder_authorization_cd AS reorder_authorization_cd,
		work_idrp_eligible_item_shc_item_step15::can_carry_model_id AS can_carry_model_id,
		work_idrp_eligible_item_shc_item_step15::grocery_item_ind AS grocery_item_ind,
		work_idrp_eligible_item_shc_item_step15::iplan_id AS iplan_id,
		work_idrp_eligible_item_shc_item_step15::markdown_style_reference_cd AS	markdown_style_reference_cd,
		work_idrp_eligible_item_shc_item_step15::forecast_group_format_id AS forecast_group_format_id,
		work_idrp_eligible_item_shc_item_step15::forecast_group_desc AS	forecast_group_desc,
		work_idrp_eligible_item_shc_item_step15::referred_ksn_id AS	referred_ksn_id,
		work_idrp_eligible_item_shc_item_step15::special_retail_order_system_ind as special_retail_order_system_ind,
		work_idrp_eligible_item_shc_item_step15::national_unit_cost_amt AS national_unit_cost_amt,	
		work_idrp_eligible_item_shc_item_step15::product_selling_price_amt AS product_selling_price_amt,	
		work_idrp_eligible_item_shc_item_step15::item_report_group_id AS item_report_group_id,	
		work_idrp_eligible_item_shc_item_step15::item_report_sequence_nbr AS item_report_sequence_nbr,	
		work_idrp_eligible_item_shc_item_step15::order_system_cd AS	order_system_cd,	
		work_idrp_eligible_item_shc_item_step15::dotcom_assorted_ind AS	dotcom_assorted_ind	,
		work_idrp_eligible_item_shc_item_step15::roadrunner_eligible_ind AS	roadrunner_eligible_ind	,
		work_idrp_eligible_item_shc_item_step15::us_dot_ship_type_cd AS	us_dot_ship_type_cd,
		work_idrp_eligible_item_shc_item_step15::dotcom_mailable_ind AS dotcom_mailable_ind ,
		(gruoped_data_gen::check_error == 'MULTIPLE' ? 'ERROR' : 'NO ERROR') AS check_error;

SPLIT join_grouped_data_prev_data_gen INTO 
	error_data IF (check_error == 'ERROR'),
	no_error_data IF (check_error == 'NO ERROR');

final_data_dist_join_item = 
	JOIN work_idrp_eligible_item_shc_item_step5 BY shc_item_id LEFT OUTER, 
	     no_error_data BY shc_item_id;

final_data_dist_join_item_gen_temp = 
	FOREACH final_data_dist_join_item 
	GENERATE 
		work_idrp_eligible_item_shc_item_step5::shc_item_id AS shc_item_id,
		work_idrp_eligible_item_shc_item_step5::shc_item_desc AS shc_item_desc,
		work_idrp_eligible_item_shc_item_step5::shc_division_nbr AS	shc_division_nbr,
		work_idrp_eligible_item_shc_item_step5::shc_division_desc AS shc_division_desc,
		work_idrp_eligible_item_shc_item_step5::shc_department_nbr AS shc_department_nbr,
		work_idrp_eligible_item_shc_item_step5::shc_department_desc AS shc_department_desc,
		work_idrp_eligible_item_shc_item_step5::shc_category_group_level_nbr AS	shc_category_group_level_nbr,
		work_idrp_eligible_item_shc_item_step5::shc_category_group_desc AS shc_category_group_desc,
		work_idrp_eligible_item_shc_item_step5::shc_category_nbr AS	shc_category_nbr,
		work_idrp_eligible_item_shc_item_step5::shc_category_desc AS shc_category_desc,
		work_idrp_eligible_item_shc_item_step5::shc_sub_category_nbr AS	shc_sub_category_nbr,
		work_idrp_eligible_item_shc_item_step5::shc_sub_category_desc AS shc_sub_category_desc,
		work_idrp_eligible_item_shc_item_step5::delivered_direct_ind AS	delivered_direct_ind,
		work_idrp_eligible_item_shc_item_step5::installation_ind AS	installation_ind,
		work_idrp_eligible_item_shc_item_step5::store_forecast_cd AS store_forecast_cd,
		work_idrp_eligible_item_shc_item_step5::idrp_order_method_cd AS	idrp_order_method_cd,
		work_idrp_eligible_item_shc_item_step5::idrp_order_method_desc AS idrp_order_method_desc,
		work_idrp_eligible_item_shc_item_step5::shc_item_type_cd AS	shc_item_type_cd,
		work_idrp_eligible_item_shc_item_step5::purchase_status_cd AS purchase_status_cd,
		work_idrp_eligible_item_shc_item_step5::network_distribution_cd AS network_distribution_cd,
		work_idrp_eligible_item_shc_item_step5::future_network_distribution_cd AS future_network_distribution_cd,
		work_idrp_eligible_item_shc_item_step5::future_network_distribution_effective_dt AS	future_network_distribution_effective_dt,
		work_idrp_eligible_item_shc_item_step5::jit_network_distribution_cd AS jit_network_distribution_cd,
		work_idrp_eligible_item_shc_item_step5::sears_network_distribution_cd AS sears_network_distribution_cd,
		work_idrp_eligible_item_shc_item_step5::sears_future_network_effective_dt AS sears_future_network_effective_dt,
		work_idrp_eligible_item_shc_item_step5::sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd,
		work_idrp_eligible_item_shc_item_step5::sears_future_network_distribution_cd AS	sears_future_network_distribution_cd,
		work_idrp_eligible_item_shc_item_step5::reorder_authorization_cd AS	reorder_authorization_cd,
		work_idrp_eligible_item_shc_item_step5::can_carry_model_id AS can_carry_model_id,
		work_idrp_eligible_item_shc_item_step5::grocery_item_ind AS	grocery_item_ind,
		work_idrp_eligible_item_shc_item_step5::iplan_id AS	iplan_id,
		work_idrp_eligible_item_shc_item_step5::markdown_style_reference_cd AS markdown_style_reference_cd,
		work_idrp_eligible_item_shc_item_step5::forecast_group_format_id AS	forecast_group_format_id,
		work_idrp_eligible_item_shc_item_step5::forecast_group_desc AS forecast_group_desc,
		work_idrp_eligible_item_shc_item_step5::referred_ksn_id AS referred_ksn_id,
		work_idrp_eligible_item_shc_item_step5::special_retail_order_system_ind AS special_retail_order_system_ind,
		work_idrp_eligible_item_shc_item_step5::national_unit_cost_amt AS national_unit_cost_amt,	
		work_idrp_eligible_item_shc_item_step5::product_selling_price_amt AS product_selling_price_amt,	
		work_idrp_eligible_item_shc_item_step5::item_report_group_id AS	item_report_group_id,	
		work_idrp_eligible_item_shc_item_step5::item_report_sequence_nbr AS	item_report_sequence_nbr,	
		work_idrp_eligible_item_shc_item_step5::order_system_cd AS order_system_cd,	
		work_idrp_eligible_item_shc_item_step5::dotcom_assorted_ind AS dotcom_assorted_ind,
		work_idrp_eligible_item_shc_item_step5::roadrunner_eligible_ind AS roadrunner_eligible_ind,
		work_idrp_eligible_item_shc_item_step5::us_dot_ship_type_cd AS us_dot_ship_type_cd,
		((no_error_data::check_error == 'NO ERROR' AND       
		IsNull(no_error_data::dotcom_mailable_ind,'') != '')
			? no_error_data::dotcom_mailable_ind
			: 'N') AS dotcom_mailable_ind;
	
final_data_dist_join_item_gen = DISTINCT final_data_dist_join_item_gen_temp;

error_data_gen = FOREACH error_data GENERATE 
                         '$CURRENT_TIMESTAMP' AS load_ts,
                         shc_item_id AS item_id,
                         '' AS ksn_id,
                         '' AS sears_division_nbr,
                         '' AS sears_item_nbr,
                         '' AS sears_sku_nbr,
                         '' AS websku,
                         '' AS package_id,
                         dotcom_mailable_ind AS error_value,
                         'Multiple Mailable Indicators found for an Item' AS error_desc,
						 '$batchid' AS idrp_batch_id;

-----------------------------------------------------------------------------------------------------------------------------------

work_idrp_eligible_item_shc_item_step_join5 = 
	JOIN final_data_dist_join_item_gen BY shc_item_id LEFT OUTER, 
		 LOAD_ONLINE_FULFILLMENT BY item_id ;

work_idrp_eligible_item_shc_item_step_join52 = 
	JOIN work_idrp_eligible_item_shc_item_step_join5 BY shc_item_id  LEFT OUTER, 
	     LOAD_ONLINE_BILL_WT BY  item_id ;
						 
work_idrp_eligible_item_shc_item_step_join5 = 
	FOREACH work_idrp_eligible_item_shc_item_step_join52 
	GENERATE
		shc_item_id AS shc_item_id,
		shc_item_desc AS shc_item_desc,
		shc_division_nbr AS	shc_division_nbr,
		shc_division_desc AS shc_division_desc,
		shc_department_nbr AS shc_department_nbr,
		shc_department_desc AS shc_department_desc,
		shc_category_group_level_nbr AS	shc_category_group_level_nbr,
		shc_category_group_desc AS shc_category_group_desc,
		shc_category_nbr AS	shc_category_nbr,
		shc_category_desc AS shc_category_desc,
		shc_sub_category_nbr AS	shc_sub_category_nbr,
		shc_sub_category_desc AS shc_sub_category_desc,
		delivered_direct_ind AS	delivered_direct_ind,
		installation_ind AS	installation_ind,
		store_forecast_cd AS store_forecast_cd,
		idrp_order_method_cd AS	idrp_order_method_cd,
		idrp_order_method_desc AS idrp_order_method_desc,
		shc_item_type_cd AS	shc_item_type_cd,
		purchase_status_cd AS purchase_status_cd,
		network_distribution_cd AS network_distribution_cd,
		future_network_distribution_cd AS future_network_distribution_cd,
		future_network_distribution_effective_dt AS	future_network_distribution_effective_dt,
		jit_network_distribution_cd AS jit_network_distribution_cd,
		sears_network_distribution_cd AS sears_network_distribution_cd,
		sears_future_network_effective_dt AS sears_future_network_effective_dt,
		sears_emp_network_distribution_cd AS sears_emp_network_distribution_cd,
		sears_future_network_distribution_cd AS	sears_future_network_distribution_cd,
		reorder_authorization_cd AS	reorder_authorization_cd,
		can_carry_model_id AS can_carry_model_id,
		grocery_item_ind AS	grocery_item_ind,
		iplan_id AS	iplan_id,
		markdown_style_reference_cd AS markdown_style_reference_cd,
		forecast_group_format_id AS	forecast_group_format_id,
		forecast_group_desc AS forecast_group_desc,
		referred_ksn_id AS referred_ksn_id,
		special_retail_order_system_ind AS special_retail_order_system_ind,
		national_unit_cost_amt AS national_unit_cost_amt,	
		product_selling_price_amt AS product_selling_price_amt,	
		item_report_group_id AS	item_report_group_id,	
		item_report_sequence_nbr AS	item_report_sequence_nbr,	
		order_system_cd AS order_system_cd,	
		dotcom_assorted_ind AS dotcom_assorted_ind,
		roadrunner_eligible_ind AS roadrunner_eligible_ind,
		us_dot_ship_type_cd AS us_dot_ship_type_cd,
		dotcom_mailable_ind AS dotcom_mailable_ind,

/** CR 3703 **/
		
		(IsNull(LOAD_ONLINE_FULFILLMENT::item_id,'') != '' 
			? LOAD_ONLINE_FULFILLMENT::sears_temporary_online_fulfillment_type_cd 
			: '') as sears_temporary_online_fulfillment_type_cd,	
		(IsNull(LOAD_ONLINE_FULFILLMENT::item_id,'') != '' 
			? LOAD_ONLINE_FULFILLMENT::sears_default_online_fulfillment_type_cd  
			: '') as sears_default_online_fulfillment_type_cd,
		(IsNull(LOAD_ONLINE_FULFILLMENT::item_id,'') != '' 
			? LOAD_ONLINE_FULFILLMENT::sears_default_online_fulfillment_type_cd_ts   
			: '') as sears_default_online_fulfillment_type_cd_ts,
		(IsNull(LOAD_ONLINE_FULFILLMENT::item_id,'') != '' 
			? LOAD_ONLINE_FULFILLMENT::kmart_temporary_online_fulfillment_type_cd   
			: '') as kmart_temporary_online_fulfillment_type_cd,
		(IsNull(LOAD_ONLINE_FULFILLMENT::item_id,'') != '' 
			? LOAD_ONLINE_FULFILLMENT::kmart_default_online_fulfillment_type_cd  
			: '') as kmart_default_online_fulfillment_type_cd,
		(IsNull(LOAD_ONLINE_FULFILLMENT::item_id,'') != '' 
			? LOAD_ONLINE_FULFILLMENT::kmart_default_online_fulfillment_type_cd_ts  
			: '') as kmart_default_online_fulfillment_type_cd_ts,
		(IsNull(LOAD_ONLINE_FULFILLMENT::item_id,'') != '' 
			? LOAD_ONLINE_FULFILLMENT::web_exclusive_ind  
			: 'N') as web_exclusive_ind,			
		(IsNull(LOAD_ONLINE_BILL_WT::item_id,'') !=''
			? LOAD_ONLINE_BILL_WT::ups_billable_weight
			: '') as  ups_billable_weight,
		(IsNull(LOAD_ONLINE_BILL_WT::item_id,'') !=''
			? LOAD_ONLINE_BILL_WT::last_change_ts
			: '') AS ups_billable_weight_ts ;

-----------------------------------------------------------------------------------------
/** 3703 **/

DROP_ITEMS = 
	FOREACH LOAD_DROP_SHIP 
	GENERATE 
		(item_id is NULL ? '' : item_id) AS item_id,
		service_area_restriction_model_id AS service_area_restriction_model_id;
		
DROP_ITEMS_FILTER = 
	FILTER DROP_ITEMS
	BY service_area_restriction_model_id == '46162' OR service_area_restriction_model_id == '78459';
	
DROP_ITEMS_FILTER_GEN = 
	FOREACH DROP_ITEMS_FILTER
	GENERATE
		item_id AS item_id,
		(service_area_restriction_model_id == '78459'
			? '1'
			: '0') AS sears_online_drop_ship_ind,
		(service_area_restriction_model_id == '46162'
			? '1'
			: '0') AS kmart_online_drop_ship_ind;			

/**4472***/			
			
DROP_ITEMS_DIST = GROUP DROP_ITEMS_FILTER_GEN by (item_id);
GRP_DROP_ITEMS_DIST = foreach DROP_ITEMS_DIST generate group as item_id,MAX(DROP_ITEMS_FILTER_GEN.sears_online_drop_ship_ind) as sears_online_drop_ship_ind,MAX(DROP_ITEMS_FILTER_GEN.kmart_online_drop_ship_ind) as kmart_online_drop_ship_ind;

JOIN_NEW_COLS_NEXT  = 
	JOIN work_idrp_eligible_item_shc_item_step_join5 BY shc_item_id LEFT OUTER, 
	     GRP_DROP_ITEMS_DIST BY item_id ;

work_idrp_eligible_item_shc_item_step4 = 
	FOREACH JOIN_NEW_COLS_NEXT 
	GENERATE
		REPLACE(shc_item_id,'\\|','') AS shc_item_id,
		REPLACE(shc_item_desc,'\\|','') AS shc_item_desc,
		REPLACE(shc_division_nbr,'\\|','') AS shc_division_nbr,
		REPLACE(shc_division_desc,'\\|','') AS shc_division_desc,
		REPLACE(shc_department_nbr,'\\|','') AS shc_department_nbr,
		REPLACE(shc_department_desc,'\\|','') AS shc_department_desc,
		REPLACE(shc_category_group_level_nbr,'\\|','') AS shc_category_group_level_nbr,
		REPLACE(shc_category_group_desc,'\\|','') AS shc_category_group_desc,
		REPLACE(shc_category_nbr,'\\|','') AS shc_category_nbr,
		REPLACE(shc_category_desc,'\\|','') AS shc_category_desc,
		REPLACE(shc_sub_category_nbr,'\\|','') AS shc_sub_category_nbr,
		REPLACE(shc_sub_category_desc,'\\|','') AS shc_sub_category_desc,
		REPLACE(delivered_direct_ind,'\\|','') AS delivered_direct_ind,
		REPLACE(installation_ind,'\\|','') AS installation_ind,
		REPLACE(store_forecast_cd,'\\|','') AS store_forecast_cd,
		REPLACE(idrp_order_method_cd,'\\|','') AS idrp_order_method_cd,
		REPLACE(idrp_order_method_desc,'\\|','') AS	idrp_order_method_desc,
		REPLACE(shc_item_type_cd,'\\|','') AS shc_item_type_cd,
		REPLACE(purchase_status_cd,'\\|','') AS	purchase_status_cd,
		REPLACE(network_distribution_cd,'\\|','') AS network_distribution_cd,
		REPLACE(future_network_distribution_cd,'\\|','') AS	future_network_distribution_cd,
		REPLACE(future_network_distribution_effective_dt,'\\|','') AS future_network_distribution_effective_dt,
		REPLACE(jit_network_distribution_cd,'\\|','') AS jit_network_distribution_cd,
		REPLACE(sears_network_distribution_cd,'\\|','') AS sears_network_distribution_cd,
		REPLACE(sears_future_network_effective_dt,'\\|','') AS sears_future_network_effective_dt,
		REPLACE(sears_emp_network_distribution_cd,'\\|','') AS sears_emp_network_distribution_cd,
		REPLACE(sears_future_network_distribution_cd,'\\|','') AS sears_future_network_distribution_cd,
		REPLACE(reorder_authorization_cd,'\\|','') AS reorder_authorization_cd,
		REPLACE(can_carry_model_id,'\\|','') AS	can_carry_model_id,
		REPLACE(grocery_item_ind,'\\|','') AS grocery_item_ind,
		REPLACE(iplan_id,'\\|','') AS iplan_id,
		REPLACE(markdown_style_reference_cd,'\\|','') AS markdown_style_reference_cd,
		REPLACE(forecast_group_format_id,'\\|','') AS forecast_group_format_id,
		REPLACE(forecast_group_desc,'\\|','') AS forecast_group_desc,
		REPLACE(referred_ksn_id,'\\|','') AS referred_ksn_id,		
		REPLACE(special_retail_order_system_ind,'\\|','') as special_retail_order_system_ind,
		REPLACE(national_unit_cost_amt,'\\|','') AS	national_unit_cost_amt,	
		REPLACE(product_selling_price_amt,'\\|','') AS product_selling_price_amt,	
		REPLACE(item_report_group_id,'\\|','') AS item_report_group_id,	
		REPLACE(item_report_sequence_nbr,'\\|','') AS item_report_sequence_nbr,
		REPLACE(order_system_cd,'\\|','') AS order_system_cd,	
		REPLACE(dotcom_assorted_ind,'\\|','') AS dotcom_assorted_ind,
		REPLACE(roadrunner_eligible_ind,'\\|','') AS roadrunner_eligible_ind,
		REPLACE(us_dot_ship_type_cd,'\\|','') AS us_dot_ship_type_cd,
		REPLACE(dotcom_mailable_ind,'\\|','') AS dotcom_mailable_ind,
		REPLACE(sears_temporary_online_fulfillment_type_cd,'\\|','') AS sears_temporary_online_fulfillment_type_cd,
		REPLACE(sears_default_online_fulfillment_type_cd,'\\|','') AS sears_default_online_fulfillment_type_cd,
		REPLACE(sears_default_online_fulfillment_type_cd_ts,'\\|','') AS sears_default_online_fulfillment_type_cd_ts,
		REPLACE(kmart_temporary_online_fulfillment_type_cd,'\\|','') AS kmart_temporary_online_fulfillment_type_cd,
		REPLACE(kmart_default_online_fulfillment_type_cd,'\\|','') AS kmart_default_online_fulfillment_type_cd,
		REPLACE(kmart_default_online_fulfillment_type_cd_ts,'\\|','') AS kmart_default_online_fulfillment_type_cd_ts,
		REPLACE(ups_billable_weight,'\\|','') AS ups_billable_weight,
		REPLACE(ups_billable_weight_ts,'\\|','') AS ups_billable_weight_ts,
		REPLACE(((IsNull(TRIM(web_exclusive_ind),'')=='') ? 'N': web_exclusive_ind),'\\|','') AS web_exclusive_ind,	
		(IsNull(GRP_DROP_ITEMS_DIST::item_id,'') != ''
			? GRP_DROP_ITEMS_DIST::sears_online_drop_ship_ind
			: '0') AS sears_online_drop_ship_ind,
		(IsNull(GRP_DROP_ITEMS_DIST::item_id,'') != ''
			? GRP_DROP_ITEMS_DIST::kmart_online_drop_ship_ind
			: '0') AS kmart_online_drop_ship_ind,
		'$batchid' AS batch_id;
		
----------------------------------------------------------------------------------------------------------------

work_idrp_eligible_item_shc_item_step4 = DISTINCT work_idrp_eligible_item_shc_item_step4 ;

----------------------------------------------------------------------------------------------------------------

ERROR_UNION = UNION error_file_1, error_data_gen ;

----------------------------------------------------------------------------------------------------------------

STORE ERROR_UNION 
	INTO '$WORK__IDRP_ELIGIBLE_ITEM_CURRENT_ERROR' 
		USING PigStorage('$FIELD_DELIMITER_CONTROL_A');  

STORE work_idrp_eligible_item_shc_item_step4 
	INTO '$WORK__IDRP_ELIGIBLE_ITEM_CURRENT_PART_1' 
		USING PigStorage('$FIELD_DELIMITER_PIPE'); 
						 
/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
