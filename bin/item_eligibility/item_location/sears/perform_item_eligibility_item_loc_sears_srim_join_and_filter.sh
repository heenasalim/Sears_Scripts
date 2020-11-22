##############################################################################################################################
#
#       Script Name   : perform_item_eligibility_item_loc_sears_srim_join.sh
#
#       Description   : This is a wrapper script for invoking all hadoop jobs for item_eligibility_sears_srim join
#
#       Author        : Mudit Mangal
#       Date created  : 07/23/2013
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

PIG_SCRIPT_PATH=/appl/hdidrp/pig/scripts/item_eligibility/item_loc/sears
PIG_PARAM_PATH=/appl/hdidrp/pig/params/item_eligibility/item_loc/sears
PIG_SCHEMA_PATH=/appl/hdidrp/pig/schema/item_eligibility/item_loc/sears
PKG_NAME=com.searshc.supplychain.idrp.udf

# Joins for SRIM DATA

pig -Dpig.usenewlogicalplan=false -Dudf.import.list=$PKG_NAME -logfile $HDIDRP_LOG_FILE_DIR/pig -p batch_id=$1 -p WORK_DIR=$WORK_DIR -p SMITH_DIR=$SMITH_DIR -p GOLD_DIR=$GOLD_DIR -param_file $PIG_PARAM_PATH/item_eligibility_sears.param -m $PIG_SCHEMA_PATH/smith__idrp_eligible_item_loc_rim_join.param -m $PIG_SCHEMA_PATH/dummy_vend_whse_ref.param -m $PIG_SCHEMA_PATH/smith__idrp_eligible_item_loc.param -m $PIG_SCHEMA_PATH/gold__inventory_rim_daily_current.param -m $PIG_SCHEMA_PATH/gold__inventory_sears_dc_item_facility_current.param -m $PIG_SCHEMA_PATH/gold__inventory_sears_dc_item_owner_current.param -m $PIG_SCHEMA_PATH/gold__inventory_srim_daily.param -m $PIG_SCHEMA_PATH/gold__item_vendor_package_current.param -m $PIG_SCHEMA_PATH/smith__item_combined_hierarchy_current.param -m $PIG_SCHEMA_PATH/smith__idrp_eligible_item.param -m $PIG_SCHEMA_PATH/smith__idrp_eligible_loc.param $PIG_SCRIPT_PATH/item_eligibility_item_loc_sears_srim_join.pig

# End of script

