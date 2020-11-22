##############################################################################################################################
#
#       Script Name   : perform_item_eligibility_sears_import_vendor.sh
#
#       Description   : This is a wrapper script for invoking hadoop job for item_eligibility_sears vendor application
#
#       Author        : Sambit Parida
#       Date created  : 14/10/2013
#
#       Parameters    : 1. Batch Control ID
#                       2. Access Server Output File Path
#                       3. Reset and Run / Run [ RESET / RUN ]
#
#       Modified on     Modified by           Description
#
################################################################################################################################

#!/bin/bash

if [ "$(id -un)" != "hdidrp" ]; then
   echo "This script must be run as hdidrp" 1>&2
   exit 1
fi

export PROJECT_CONF_DIR=$2

. ${PROJECT_CONF_DIR}/HDIDRP.cfg

. /appl/hdidrp/etc/idrp_funcs

PIG_SCRIPT_PATH=/appl/hdidrp/pig/scripts/item_eligibility/item_loc
PIG_PARAM_PATH=/appl/hdidrp/pig/params/item_eligibility/item_loc/sears
PIG_SCHEMA_PATH=/appl/hdidrp/pig/schema/item_eligibility/item_loc/sears
# Command to run the import vendor application



pig -x mapreduce -logfile /logs/hdidrp/pig -m /appl/hdidrp/pig/params/item_eligibility/item_loc/sears/item_eligibility_sears_vendor.param -m /appl/hdidrp/pig/schema/item_eligibility/item_loc/sears/smith__idrp_eligible_item_loc.param -m /appl/hdidrp/pig/schema/item_eligibility/item_loc/sears/smith__idrp_eligible_loc.param /appl/hdidrp/pig/scripts/item_eligibility/item_loc/perform_item_eligibility_kmart_and_sears_output_vendor.pig

# End of script

