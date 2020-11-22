/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_inforem_store_master_format.pig
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       Mon Oct 14 05:09:26 EDT 2013
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

-- Register piggybank.jar.
-- It contains the definitions for pig ebcidic & text load & store functions
REGISTER '$PIGGYBANK_JAR';

-- Register jt400-7.1.0.9.jar 
-- It contains the EBCIDIC to TEXT conversion logic
REGISTER '$JT400_JAR';

-- Register cb2xml.jar.
-- It contains the COBOL copybook parsing logic
REGISTER '$CBL2XML_JAR';

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/



--load ebcidic data
ebcidic_data = LOAD '$WORK__IDRP_INFOREM_STORE_MASTER_INCOMING_LOCATION' 
           USING com.sears.hadoop.pig.piggybank.storage.ebcidic.EBCIDICStorage('$WORK__IDRP_INFOREM_STORE_MASTER_COPYBOOK_INCOMING_LOCATION');

--convert ebcidic data and store
STORE ebcidic_data 
INTO '$WORK__IDRP_INFOREM_STORE_MASTER_CONVERTED_WORK_LOCATION' 
USING com.sears.hadoop.pig.piggybank.storage.ebcidic.text.TextStorage('$WORK__IDRP_INFOREM_STORE_MASTER_COPYBOOK_INCOMING_LOCATION','$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
