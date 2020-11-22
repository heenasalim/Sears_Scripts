#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################

# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_smith__idrp_i2k_sears_rebuy_vendor_package_current.sh
# AUTHOR NAME:         Neera Singh
# CREATION DATE:       Thu Jun 29 09:37:59 EST 2014
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>
#
#
#
# DEPENDENCIES: <<TODO>>
# RESTARTABLE:  <<TODO>>
#
#
# REV LIST:
#      DATE         BY            MODIFICATION
#  05/04/2020   Rajani Galpalli   IPS-4912: Added empty file check for few Hadoop files
#
#
###############################################################################
#<<                               INITIALIZE                                >>#
###############################################################################

#set project configuration directory
export PROJECT_CONF_DIR=$2

#check if the PROJECT_CONF_DIR is set to valid value
if [ -z "${PROJECT_CONF_DIR}" ]; then
  echo  "ERROR: PROJECT_CONF_DIR must be passed as second argument in the command line " \
    " But it is unset or set to the empty string."
  exit 1
fi

#load common configurations file
. ${PROJECT_CONF_DIR}/USER.cfg

#load common configurations file
. ${PROJECT_CONF_DIR}/PROJECT.cfg

#check if the PROJECT_HOME_DIR is set to valid value
if [ -z "${PROJECT_HOME_DIR}" ]; then
  echo  "ERROR: PROJECT_HOME_DIR must be set in ${PROJECT_CONF_DIR}/USER.cfg file" \
    "But it is unset or set to the empty string."
  exit 1
fi

. $PROJECT_BIN_DIR/shared/utility_functions

#trigger the initialization.
#pass all the command line arguments to fn_initialize function.
fn_initialize "$@"

###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################

#this is relative path of the module
#for example, if module is grand_grand_parent, sub module is grand_parent and sub sub module is parent
#then the module path will be grand_grand_parent/grand_parent/parent
#another example, in case of item eligiblity, there is item_eligibility module which has item_location as sub module.
#item_location has vendor, store,dc as sub sub modules.
#in above case following are all possible module paths
#1. item_eligibility/item_location/vendor
#2. item_eligibility/item_location/store
#3. item_eligibility/item_location/dc
module_relative_path=item_eligibility/srsvndrpklocn/smith__idrp_i2k_sears_rebuy_vendor_package_current

#this is the name of the script without extension
script_name=perform_item_eligibility_srsvndrpklocn_smith__idrp_i2k_sears_rebuy_vendor_package_current

#pig params file
#pig_params_file=${PIG_PARAMS_DIR}/${module_relative_path}/${script_name}.param

#pig script file
pig_script_file=${PIG_SCRIPTS_DIR}/${module_relative_path}/${script_name}.pig

#load params file as shell environment variables file
#pig params file has same syntax as shell environment variables file.
#. ${pig_params_file}

CURRENT_DATE=`date "+%Y-%m-%d"`

###############################################################################
#<>                                  BODY                                   <>#
###############################################################################

. ${PIG_SCHEMAS_DIR}/smith__idrp_eligible_loc.schema
. ${PIG_SCHEMAS_DIR}/smith__idrp_batchdate.schema
. ${PIG_SCHEMAS_DIR}/smith__idrp_i2k_valid_rebuy_vendor_package_ids_current.schema
. ${PIG_SCHEMAS_DIR}/smith__idrp_vend_pack_combined.schema
. ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_item_eligibility_sears_vendor_package_ta_shared_location.schema

#IPS-4912: Added an empty file check condition
fn_non_zero_file_check $module_relative_path "$SMITH__IDRP_ELIGIBLE_LOC_LOCATION $smith__idrp_i2k_valid_rebuy_vendor_package_ids_current_location $SMITH__IDRP_VEND_PACK_COMBINED_LOCATION $SMITH__IDRP_BATCH_DATE_LOCATION" $CURRENT_DATE

hadoop fs -test -z $WORK__IDRP_ITEM_ELIGIBILITY_SEARS_VENDOR_PACKAGE_TA_SHARED_LOCATION_LOCATION
    exec1=$?
hadoop fs -test -f $WORK__IDRP_ITEM_ELIGIBILITY_SEARS_VENDOR_PACKAGE_TA_SHARED_LOCATION_LOCATION
    exec2=$?
if [ $exec1 -eq 0 ]; then
     fn_log_info "****ERROR: process $module_relative_path - $WORK__IDRP_ITEM_ELIGIBILITY_SEARS_VENDOR_PACKAGE_TA_SHARED_LOCATION_LOCATION is empty for date $CURRENT_DATE,hence job failed****"
     exit 1

elif [ $exec2 -ne 0 ]; then
     fn_log_info "****ERROR: $WORK__IDRP_ITEM_ELIGIBILITY_SEARS_VENDOR_PACKAGE_TA_SHARED_LOCATION_LOCATION No such file or directory for date $CURRENT_DATE,hence job failed****"
     exit 1
fi

#IPS-4912: end

. ${PIG_SCHEMAS_DIR}/smith__idrp_i2k_sears_rebuy_vendor_package_current.schema

. ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sears_location_xref.schema

. ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_item_eligibility_sears_vendor_package_ta_shared_location.schema

#delete the output HDFS directory
fn_delete_hadoop_directory_if_it_already_exist $SMITH__IDRP_I2K_SEARS_REBUY_VENDOR_PACKAGE_CURRENT_LOCATION

#delete the output HDFS directory
fn_delete_hadoop_directory_if_it_already_exist $WORK__IDRP_SEARS_LOCATION_XREF_LOCATION

#delete the output HDFS directory
fn_delete_hadoop_directory_if_it_already_exist $WORK__IDRP_ITEM_ELIGIBILITY_SEARS_VENDOR_PACKAGE_TA_SHARED_LOCATION_LOCATION

fn_copy_from_local_file_to_hadoop_file $STAGING_DIR/TA.SHARED.LOCATION $WORK__IDRP_ITEM_ELIGIBILITY_SEARS_VENDOR_PACKAGE_TA_SHARED_LOCATION_LOCATION

#execute the pig script
fn_execute_pig -m ${PIG_SCHEMAS_DIR}/smith__idrp_eligible_loc.schema -m ${PIG_SCHEMAS_DIR}/smith__idrp_vend_pack_combined.schema  -m ${PIG_SCHEMAS_DIR}/smith__idrp_i2k_valid_rebuy_vendor_package_ids_current.schema -m ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sears_location_xref.schema -m ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_item_eligibility_sears_vendor_package_ta_shared_location.schema  -m ${PIG_SCHEMAS_DIR}/smith__idrp_i2k_sears_rebuy_vendor_package_current.schema -m ${PIG_SCHEMAS_DIR}/smith__idrp_batchdate.schema ${pig_script_file}

###############################################################################
#<>                                  END                                    <>#
###############################################################################
