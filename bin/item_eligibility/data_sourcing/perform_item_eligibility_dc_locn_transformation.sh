#!/bin/bash
###########################################################################################
#       Script Name   : perform_ie_dc_locn_transformation.sh
#
#       Description   : This script is used to apply transformation logic in the dc_location table
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
#       Example       : perform_ie_dc_locn_transformation.sh
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
export PARAM_ROOT_DIR=/appl/hdidrp/pig/params/item_eligibility/data_sourcing

export PIG_ROOT_DIR=/appl/hdidrp/pig/scripts/item_eligibility/data_sourcing

pig -logfile /logs/hdidrp/pig -param_file ${PARAM_ROOT_DIR}/perform_ie_dc_locn_params.param  ${PIG_ROOT_DIR}/perform_ie_dc_locn_transformation.pig

# Path for storing the data in hadoop

DC_LOCN_DB2_FILE_WORK_LOCATION=/work/idrp/dc_locn

error_code=$?

log ${error_code} "Pig scripts has failed to store data to location [${DC_LOCN_DB2_FILE_WORK_LOCATION}]" "Pig scripts has succeccfully stored data to location [${DC_LOCN_DB2_FILE_WORK_LOCATION}]"

delete_hadoop_dir_if_exists "${DC_LOCN_DB2_FILE_WORK_LOCATION}"
