/*
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_ie_master_data_work__idrp_sears_exploding_assortment_master_final_conversion.pig
# AUTHOR NAME:         Mayank Agarwal
# CREATION DATE:       18-06-2014 10:15
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>
#
#
#
# DEPENDENCIES: <<TODO>>
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

set mapred.child.java.opts '-Xmx512m';

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

--load ebcidic data
ebcidic_data = LOAD '$TA_JIT2_EXASST_MASTER_FINAL_INCOMING_LOCATION'
           USING com.sears.hadoop.pig.piggybank.storage.ebcidic.EBCIDICStorage('$TA_JIT2_EXASST_MASTER_FINAL_COPYBOOK_INCOMING_LOCATION');

--convert ebcidic data and store
STORE ebcidic_data
INTO '$TA_JIT2_EXASST_MASTER_FINAL_CONVERTED_WORK_LOCATION'
USING com.sears.hadoop.pig.piggybank.storage.ebcidic.text.TextStorage('$TA_JIT2_EXASST_MASTER_FINAL_COPYBOOK_INCOMING_LOCATION','$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
