#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_obu_billable_weight_daily_format.sh
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Thu Mar 27 03:47:07 EDT 2014
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
# 7 Dec 2015	Pankaj gupta	Create 0 byte file in case of Source file not exists.
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
module_relative_path=item_eligibility/smith__idrp_obu_billable_weight_daily

#this is the name of the script without extension
script_name=perform_item_eligibility_smith__idrp_obu_billable_weight_daily_format

#pig schema file
pig_schema_file=${PIG_SCHEMAS_DIR}/smith__idrp_obu_billable_weight_daily.schema

#pig params file
pig_params_file=${PIG_PARAMS_DIR}/${module_relative_path}/${script_name}.param

#pig script file
pig_script_file=${PIG_SCRIPTS_DIR}/${module_relative_path}/${script_name}.pig

#load schema file as shell environment variables file
#pig schema file has same syntax as shell environment variables file.
. ${pig_schema_file}

#directory where the formatted data will be stored output will be stored
work_dir=${SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_WORK_LOCATION}

#smith location
output_dir=${SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_LOCATION}
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################

#create touchz file in case source file not exits

if [ -f $SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_STAGING_LOCATION/* ]
then
echo $(ls  $SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_STAGING_LOCATION/*) file exist
else
echo Source File not exists in path $SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_STAGING_LOCATION

touch $SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_STAGING_LOCATION/Billable_IDRP_DATA.txt

echo $(ls  $SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_STAGING_LOCATION/*) '0 bytes source file created'
fi;

#remove target directory if it already exists
fn_delete_hadoop_directory_if_it_already_exist $SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_INCOMING_LOCATION

#copy raw data from linux directory to hdfs
fn_parallel_copy_files_from_linux_to_hadoop $SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_STAGING_LOCATION $SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_INCOMING_LOCATION

fn_mkdir_hadoop_directory_if_it_does_not_exist $SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_LOCATION
fn_delete_hadoop_directory_if_it_already_exist $SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_WORK_LOCATION

#execute the pig script
fn_execute_pig -m ${pig_schema_file} -m ${pig_params_file} ${pig_script_file}

#remove smith directory if it already exists
fn_delete_hadoop_directory_if_it_already_exist "${SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_LOCATION}"

#copy work data to smith
fn_copy_hadoop_directory_if_target_location_does_not_exists "${SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_WORK_LOCATION}" "${SMITH__IDRP_OBU_BILLABLE_WEIGHT_DAILY_LOCATION}"

###############################################################################
#<>                                  END                                    <>#
###############################################################################
