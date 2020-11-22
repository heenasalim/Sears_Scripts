/*
This tool converts MainFrame EBCIDIC files to TEXT format
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

rmf $OUTPUT_CONVERTED_TEXT_FILES_LOCATION;

-- Load the ebcidic file using pig ebcidic storage function
ebcidicData = LOAD '$INPUT_EBCIDIC_FILES_LOCATION'
                  USING com.sears.hadoop.pig.piggybank.storage.ebcidic.EBCIDICStorage('$COPYBOOK');

-- Store the ebcidic data into a file using text storage function
STORE ebcidicData INTO '$OUTPUT_CONVERTED_TEXT_FILES_LOCATION'
    USING com.sears.hadoop.pig.piggybank.storage.ebcidic.text.TextStorage('$COPYBOOK','$DELIMITER');

