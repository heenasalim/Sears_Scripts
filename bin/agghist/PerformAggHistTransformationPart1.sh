#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         PerformAggHistTransformation.sh
# AUTHOR NAME:         Mayank Agarwal
# CREATION DATE:       Wed Dec 4, 2013
# CURRENT REVISION NO: 1
#
# DESCRIPTION: 
#
#
#
# DEPENDENCIES: USER.cfg,PROJECT.cfg
# RESTARTABLE:  
#
#
# REV LIST:
#      DATE         BY            	MODIFICATION
#	   03/04/2014   Mayank Agarwal  Changes made for aggregate history with zip code
#	   11/26/2014	Nava Jyothi		Changes made for CR #3388
#          11/18/2015   John Henschel   Added the second pig script to round outputs to 2 decimal places
###############################################################################
#<<                               INITIALIZE                                >>#
###############################################################################

#BATCH_ID=$1
#set project configuration directory
PROJECT_CONF_DIR=$2
INTERFACE=$3



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


module_relative_path=$INTERFACE

#this is the name of the script without extension
param_name=PerformAggHistTransformation
script_name=PerformAggHistTransformationPart1

#pig params file
pig_params_file=${PIG_PARAMS_DIR}/${module_relative_path}/${param_name}.param

#pig script file
pig_script_file=${PIG_SCRIPTS_DIR}/${module_relative_path}/${script_name}.pig


#load params file as shell environment variables file
#pig params file has same syntax as shell environment variables file.
. ${pig_params_file}

###############################################################################
#<>                                  BODY                                   <>#
###############################################################################



#delete existing output files

fn_delete_hadoop_directory_if_it_already_exist $udt_demand_unit_data_p1a
fn_delete_hadoop_directory_if_it_already_exist $item_location_ddc_source_p1b
fn_delete_hadoop_directory_if_it_already_exist $eligible_item_loc_data_p1c
fn_delete_hadoop_directory_if_it_already_exist $eligible_loc_data_c_p1d
fn_delete_hadoop_directory_if_it_already_exist $eligible_loc_data_d_p1e
fn_delete_hadoop_directory_if_it_already_exist $udt_dfu_view_data_p3a
fn_delete_hadoop_directory_if_it_already_exist $load_online_dfutosku_p3b
fn_delete_hadoop_directory_if_it_already_exist $srs_dc_zp_fac_data_p3c

#execute the pig script

fn_execute_pig \
-m ${pig_params_file} \
-m $PIG_SCHEMAS_DIR/smith__idrp_udt_demand_unit.schema \
-m $PIG_SCHEMAS_DIR/smith__idrp_eligible_item_location_current.schema \
-m $PIG_SCHEMAS_DIR/smith__idrp_eligible_loc.schema \
-m $PIG_SCHEMAS_DIR/smith__idrp_udt_dfu_view.schema \
-m $PIG_SCHEMAS_DIR/work__idrp_online_dfutosku_map.schema \
-m $PIG_SCHEMAS_DIR/gold__inventory_sears_dc_zip_facility_current.schema \
-m ${PIG_SCHEMAS_DIR}/agghist/PerformAggHistTransformationPart1.schema \
${pig_script_file}


###############################################################################
#<>                                  END                                    <>#
###############################################################################
