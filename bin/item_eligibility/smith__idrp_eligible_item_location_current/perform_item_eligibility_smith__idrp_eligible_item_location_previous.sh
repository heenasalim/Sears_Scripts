#!/bin/bash
###############################################################################
#<>                           START HEADER DOCUMENT                         <>#
###############################################################################

# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_location_previous.sh
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Wed Jun 18 2014
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>
#
#
#
# DEPENDENCIES: None
# RESTARTABLE:  N/A
#
#
# REV LIST:
#      DATE         BY            MODIFICATION
#
#
#

###############################################################################
#<<               START COMMON HEADER CODE - DO NOT MANUALLY EDIT           >>#
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
#<<                         START CUSTOM HEADER CODE                        >>#
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
MODULE_RELATIVE_PATH=item_eligibility/smith__idrp_eligible_item_location_current

#this is the name of the script without extension
SCRIPT_NAME=perform_item_eligibility_smith__idrp_eligible_item_location_previous

#pig params file
PIG_PARAMS_FILE=${PIG_PARAMS_DIR}/${MODULE_RELATIVE_PATH}/${SCRIPT_NAME}.param

#pig script file
PIG_SCRIPT_FILE=${PIG_SCRIPTS_DIR}/${MODULE_RELATIVE_PATH}/${SCRIPT_NAME}.pig


#load param file as shell environment variables file
#pig sparams file has same syntax as shell environment variables file.
. ${PIG_PARAMS_FILE}

###############################################################################
#<<                START COMMON BODY CODE - DO NOT MANUALLY EDIT            >>#
###############################################################################


###############################################################################
#<<                          START CUSTOM BODY CODE                         >>#
###############################################################################

. ${PIG_SCHEMAS_DIR}/smith__idrp_eligible_item_location_current.schema

fn_mkdir_hadoop_directory_if_it_does_not_exist $SMITH__IDRP_ELIGIBLE_ITEM_LOCATION_PREVIOUS_LOCATION

fn_delete_hadoop_directory_if_it_already_exist $SMITH__IDRP_ELIGIBLE_ITEM_LOCATION_PREVIOUS_TODAY_LOCATION

for old_file in `hadoop fs -ls $SMITH__IDRP_ELIGIBLE_ITEM_LOCATION_PREVIOUS_LOCATION | grep -v "Found" | tr -s " " "|" | cut -d"|" -f 8 | sort -r | tail -n +7`; do
fn_delete_hadoop_directory_if_it_already_exist $old_file
done

fn_copy_hadoop_directory_if_target_location_does_not_exists $SMITH__IDRP_ELIGIBLE_ITEM_LOCATION_CURRENT_LOCATION $SMITH__IDRP_ELIGIBLE_ITEM_LOCATION_PREVIOUS_TODAY_LOCATION

#execute the pig script

###############################################################################
#<<              START COMMON FOOTER CODE - DO NOT MANUALLY EDIT            >>#
###############################################################################


###############################################################################
#<>                                 END                                     <>#
###############################################################################

