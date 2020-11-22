/*
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_mips1_smith__idrp_replenishment_store_parameter_format.pig
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       19-09-2013 10:15
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


SET pig.splitCombination 'false';

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
ebcidic_data = LOAD '$SMITH__IDRP_REPLENISHMENT_STORE_PARAMETER_INCOMING_LOCATION'
           USING com.sears.hadoop.pig.piggybank.storage.ebcidic.EBCIDICStorage('$SMITH__IDRP_REPLENISHMENT_STORE_PARAMETER_COPYBOOK_INCOMING_LOCATION');

--convert ebcidic data and store
STORE ebcidic_data
INTO '$SMITH__IDRP_REPLENISHMENT_STORE_PARAMETER_CONVERTED_WORK_LOCATION'
USING com.sears.hadoop.pig.piggybank.storage.ebcidic.text.TextStorage('$SMITH__IDRP_REPLENISHMENT_STORE_PARAMETER_COPYBOOK_INCOMING_LOCATION','$FIELD_DELIMITER_CONTROL_A');

/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
