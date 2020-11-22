#!/bin/bash
###########################################################################################
#       Script Name   : perform_ie_location_transformation.sh
#
#       Description   : This script is used to apply transformation logic in the location table
#       Input         : NA
#
#
#
#       Dependencies  : utility_functions.sh
#
#       Author        : Shalinee
#       Date created  : 18/07/2013
#       Restartable   : Yes
#       Usage         : <Shell script name>
#       Example       : perform_ie_location_transformation.sh
#
#       Modified on     Modified by           Description
#
#
#
#---------------------------------------------------------------------------------------------

PROJECT_ROOT_DIR=/appl/hdidrp
#Load all utility functions
. ${PROJECT_ROOT_DIR}/etc/utility_functions.sh

#call location table pig script here.
export PARAM_ROOT_DIR=/appl/hdidrp/pig/params/item_eligibility/location

export PIG_ROOT_DIR=/appl/hdidrp/pig/scripts/item_eligibility/loc

pig -logfile /logs/hdidrp/pig -param_file ${PARAM_ROOT_DIR}/perform_ie_location_params.param  ${PIG_ROOT_DIR}/perform_ie_location_transformation.pig

# Path for storing the data in hadoop
UDT_LOC_ORACLE_UNLOAD_FILE_WORK_LOCATION=/work/idrp/smith__idrp_eligible_loc

UDT_LOC_ORACLE_UNLOAD_FILE_SMITH_LOCATION=/smith/idrp/eligible_loc

error_code=$?

log ${error_code} "Pig scripts has failed to store data to location [${UDT_LOC_ORACLE_UNLOAD_FILE_WORK_LOCATION}" "Pig scripts has succeccfully stored data to location [${UDT_LOC_ORACLE_UNLOAD_FILE_WORK_LOCATION}]"

delete_hadoop_dir_if_exists "${UDT_LOC_ORACLE_UNLOAD_FILE_SMITH_LOCATION}"

hadoop fs -mv ${UDT_LOC_ORACLE_UNLOAD_FILE_WORK_LOCATION} ${UDT_LOC_ORACLE_UNLOAD_FILE_SMITH_LOCATION}

error_code=$?

log ${error_code} "Data stored by pig script has failed to move to the location [${UDT_LOC_ORACLE_UNLOAD_FILE_SMITH_LOCATION}]" "Data stored by pig script has successfully move to the location [${UDT_LOC_ORACLE_UNLOAD_FILE_SMITH_LOCATION}]"

