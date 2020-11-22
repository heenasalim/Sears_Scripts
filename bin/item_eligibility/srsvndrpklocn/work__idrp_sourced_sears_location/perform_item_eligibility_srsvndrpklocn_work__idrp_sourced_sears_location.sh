#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################

# SCRIPT NAME:         perform_item_eligibility_srsvndrpklocn_work__idrp_sourced_sears_location.sh
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Thu Jul 24 06:23:19 EDT 2014
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
# DATE      	BY            		MODIFICATION
# 08/02/2018	Piyush Solanki		IPS-3142: Create CLONE for Item Elig Sears Vendor Pack Location for AMZ/SHO.
#                                             Added sourcing of work__idrp_3pl_ddc_xref.schema and
#                                             smith__idrp_ksn_attribute_current.schema
# 05/04/2020    Rajani Galpalli     IPS-4912: Added empty file check for few Hadoop files
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
module_relative_path=item_eligibility/srsvndrpklocn/work__idrp_sourced_sears_location

#this is the name of the script without extension
script_name=perform_item_eligibility_srsvndrpklocn_work__idrp_sourced_sears_location

#pig params file
pig_params_file=${PIG_PARAMS_DIR}/${module_relative_path}/${script_name}.param

#pig script file
pig_script_file=${PIG_SCRIPTS_DIR}/${module_relative_path}/${script_name}.pig

#load params file as shell environment variables file
#pig params file has same syntax as shell environment variables file.
. ${pig_params_file}

CURRENT_DATE=`date "+%y-%m-%d"`

###############################################################################
#<>                                  BODY                                   <>#
###############################################################################

#IPS-4912: Added empty file check
. ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sourced_sears_warehouse.schema 
. ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sourced_sears_import_center.schema 
. ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sourced_sears_store.schema 
. ${PIG_SCHEMAS_DIR}/smith__idrp_ksn_attribute_current.schema 
. ${PIG_SCHEMAS_DIR}/work__idrp_3pl_ddc_xref.schema 
  
fn_non_zero_file_check $module_relative_path "$WORK__IDRP_SOURCED_SEARS_WAREHOUSE_LOCATION $WORK__IDRP_SOURCED_SEARS_IMPORT_CENTER_LOCATION $WORK__IDRP_SOURCED_SEARS_STORE_LOCATION $SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_LOCATION $WORK__IDRP_3PL_DDC_XREF_LOCATION" $CURRENT_DATE

. ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sourced_sears_location.schema

fn_delete_hadoop_directory_if_it_already_exist $WORK__IDRP_SOURCED_SEARS_LOCATION_LOCATION


##### IPS-3142: commented below command and created new command with additional schema files
##### #execute the pig script
##### fn_execute_pig -m ${pig_params_file} -m ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sourced_sears_warehouse.schema -m ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sourced_sears_import_center.schema -m ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sourced_sears_store.schema -m ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sourced_sears_location.schema ${pig_script_file}

#execute the pig script
fn_execute_pig -m ${pig_params_file} -m ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sourced_sears_warehouse.schema -m ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sourced_sears_import_center.schema -m ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sourced_sears_store.schema -m ${PIG_SCHEMAS_DIR}/smith__idrp_ksn_attribute_current.schema -m ${PIG_SCHEMAS_DIR}/work__idrp_3pl_ddc_xref.schema -m ${PIG_SCHEMAS_DIR}/item_eligibility/srsvndrpklocn/work__idrp_sourced_sears_location.schema ${pig_script_file}

###############################################################################
#<>                                  END                                    <>#
###############################################################################
