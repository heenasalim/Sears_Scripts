##############################################################################################################################
#
#       Script Name   : perform_item_eligibility_sears_import_vendor.sh
#
#       Description   : This is a wrapper script for invoking all hadoop jobs for item_eligibility_sears import vendor application
#
#       Author        : Hemlata Chelwani
#       Date created  : 17/04/2013
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

#export PROJECT_CONF_DIR=$2

#. ${PROJECT_CONF_DIR}/HDIDRP.cfg
. /appl/hdidrp/etc/idrp_funcs

PIG_SCRIPT_PATH=/appl/hdidrp/pig/scripts/item_eligibility/item_loc/sears
PIG_PARAM_PATH=/appl/hdidrp/pig/params/item_eligibility/item_loc/sears
PIG_SCHEMA_PATH=/appl/hdidrp/pig/schema/item_eligibility/item_loc/sears
PKG_NAME=com.searshc.supplychain.idrp.udf


# Command to run the import vendor application 

hadoop fs -rmr /work/idrp/deconsolidation_centre;

pig -x mapreduce -logfile /logs/hdidrp/pig -param_file $PIG_PARAM_PATH/item_eligibility_sears_import_center.param  -m $PIG_SCHEMA_PATH/smith__idrp_eligible_item_loc.param -m $PIG_SCHEMA_PATH/smith__idrp_eligible_loc.param $PIG_SCRIPT_PATH/item_eligibility_sears_import_center.pig

# End of script


