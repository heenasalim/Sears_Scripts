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
#        DATE         BY            MODIFICATION
#
#
#
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

--LOAD GOLD ITEM HIERARCHY Package file
LOAD_GOLD_ITEM = LOAD '$GOLD__ITEM_SHC_HIERARCHY_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_SHC_HIERARCHY_CURRENT_SCHEMA);

--LOAD ITEM PACKAGE CURRENT
LOAD_PACKAGE_CURRENT = LOAD '$GOLD__ITEM_PACKAGE_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__ITEM_PACKAGE_CURRENT_SCHEMA);

--------------------------------------------------------------------------------------------------------------
LOAD_GOLD_ITEM_NEW = FOREACH LOAD_GOLD_ITEM GENERATE

(item_id is NULL ? '':item_id) as item_id,
(ksn_purchase_status_cd is NULL ? '': ksn_purchase_status_cd) as ksn_purchase_status_cd,
(dotcom_eligibility_cd is NULL ? '': dotcom_eligibility_cd) as dotcom_eligibility_cd,
(ksn_id is NULL ? '': ksn_id) as ksn_id ;


GOLD_ITEM = FILTER LOAD_GOLD_ITEM_NEW BY ksn_purchase_status_cd  != 'U' AND dotcom_eligibility_cd  == '1' ;

GOLD_ITEMS = FOREACH GOLD_ITEM GENERATE item_id, ksn_id;

DISTINCT_GOLD_ITEMS = DISTINCT GOLD_ITEMS;

LOAD_PACKAGE_CURRENT_FILTER = FILTER LOAD_PACKAGE_CURRENT BY package_type_cd == 'EACH' OR  package_type_cd == 'ECRT' ;

JOIN_PACKAGE_GOLD = JOIN LOAD_PACKAGE_CURRENT_FILTER BY ksn_id, DISTINCT_GOLD_ITEMS BY ksn_id ;

FINAL = FOREACH JOIN_PACKAGE_GOLD GENERATE

DISTINCT_GOLD_ITEMS::item_id AS shc_item_id,
DISTINCT_GOLD_ITEMS::ksn_id AS ksn_id ,
LOAD_PACKAGE_CURRENT_FILTER::package_id AS package_id,
LOAD_PACKAGE_CURRENT_FILTER::package_weight_pounds_qty AS package_weight_pounds_qty,
LOAD_PACKAGE_CURRENT_FILTER::package_depth_inch_qty AS package_depth_inch_qty,
LOAD_PACKAGE_CURRENT_FILTER::package_height_inch_qty AS package_height_inch_qty,
LOAD_PACKAGE_CURRENT_FILTER::package_width_inch_qty AS package_width_inch_qty,
'$batchid' AS batch_id  ;
---------------------------------------------------------------------------------------------------------------------------------------------

FINAL = DISTINCT FINAL;

------------------------------------------------------------------------------------------------------------------------------------------------
STORE FINAL INTO '$WORK__IDRP_ELIGIBLE_ITEM_CURRENT_PART_2' USING PigStorage('$FIELD_DELIMITER_PIPE');  

						 
/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/