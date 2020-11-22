#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################

# SCRIPT NAME:         perform_item_eligibility_work__idrp_kmart_vendor_package_location_store_level_part2.sh
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Wed Apr 23 02:52:16 EDT 2014
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
module_relative_path=item_eligibility/kmtvndrpklocn/work__idrp_kmart_vendor_package_location_store_level

#this is the name of the script without extension
script_name=perform_item_eligibility_work__idrp_kmart_vendor_package_location_store_level_part2
script_param_name=perform_item_eligibility_work__idrp_kmart_vendor_package_location_store_level

#pig params file
pig_params_file=${PIG_PARAMS_DIR}/${module_relative_path}/${script_param_name}.param

#pig script file
pig_script_file=${PIG_SCRIPTS_DIR}/${module_relative_path}/${script_name}.pig

#load params file as shell environment variables file
#pig params file has same syntax as shell environment variables file.
. ${pig_params_file}

###############################################################################
#<>                                  BODY                                   <>#
###############################################################################

. $PIG_SCHEMAS_DIR/item_eligibility/kmtvndrpklocn/work__idrp_kmart_vendor_package_location_store_level.schema

fn_delete_hadoop_directory_if_it_already_exist $WORK__IDRP_KMART_VENDOR_PACKAGE_LOCATION_STORE_LEVEL_LOCATION_PART1

#execute the pig script
fn_execute_pig -m ${pig_params_file} -m ${PIG_SCHEMAS_DIR}/item_eligibility/kmtvndrpklocn/work__idrp_store_level_vend_pack_loc_final.schema -m ${PIG_SCHEMAS_DIR}/item_eligibility/kmtvndrpklocn/work__idrp_kmart_vendor_package_location_store_level.schema -m ${PIG_SCHEMAS_DIR}/smith__idrp_vendor_package_store_driver.schema -m ${PIG_SCHEMAS_DIR}/gold__item_exploding_assortment.schema  -m ${PIG_SCHEMAS_DIR}/smith__idrp_item_eligibility_batchdate.schema -m ${PIG_SCHEMAS_DIR}/smith__idrp_vend_pack_combined.schema ${pig_script_file}


###############################################################################
#<>                                  END                                    <>#
###############################################################################
