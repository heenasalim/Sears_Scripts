#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_inforem_store_master_e2a.sh
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       Mon Oct 14 05:09:27 EDT 2013
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
#
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
module_relative_path=item_eligibility/work__idrp_inforem_store_master

#this is the name of the script without extension
script_name=perform_item_eligibility_work__idrp_inforem_store_master_e2a

#pig params file
pig_schema_file=${PIG_SCHEMAS_DIR}/item_eligibility/work__idrp_inforem_store_master.schema

#pig params file
pig_params_file=${PIG_PARAMS_DIR}/${module_relative_path}/perform_item_eligibility_work__idrp_inforem_store_master.param

#pig script file
pig_script_file=${PIG_SCRIPTS_DIR}/${module_relative_path}/${script_name}.pig

#load schema file as shell environment variables file
#pig schema file has same syntax as shell environment variables file.
. ${pig_schema_file}


#linux directory where the raw data will be downloaded
staging_dir=${WORK__IDRP_INFOREM_STORE_MASTER_STAGING_LOCATION}

#hdfs directory where the raw data will be stored
incoming_dir=${WORK__IDRP_INFOREM_STORE_MASTER_INCOMING_LOCATION}

#directory where the formatted data will be stored
work_converted_dir=${WORK__IDRP_INFOREM_STORE_MASTER_CONVERTED_WORK_LOCATION}

#smith location
smith_dir=${WORK__IDRP_INFOREM_STORE_MASTER_LOCATION}
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################

#remove target directory if it already exists
fn_delete_hadoop_directory_if_it_already_exist "${incoming_dir}"

#copy raw data from linux directory to hdfs
fn_parallel_copy_files_from_linux_to_hadoop "${staging_dir}" "${incoming_dir}"

#remove target directory if it already exists
fn_delete_hadoop_directory_if_it_already_exist "${work_converted_dir}"

#execute the pig script
fn_execute_pig -m ${pig_schema_file} -m ${pig_params_file} ${pig_script_file}


###############################################################################
#<>                                  END                                    <>#
###############################################################################
