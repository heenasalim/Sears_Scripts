#!/bin/bash
###############################################################################
#<>                           START HEADER DOCUMENT                         <>#
###############################################################################

# SCRIPT NAME:         perform_item_eligibility_smith__idrp_time_phased_status_current_copy.sh
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
MODULE_RELATIVE_PATH=item_eligibility/smith__idrp_time_phased_status_current

#this is the name of the script without extension
SCRIPT_NAME=perform_item_eligibility_smith__idrp_time_phased_status_current

#pig params file
PIG_PARAMS_FILE=${PIG_PARAMS_DIR}/${MODULE_RELATIVE_PATH}/${SCRIPT_NAME}.param


#load param file as shell environment variables file
#pig sparams file has same syntax as shell environment variables file.
. ${PIG_PARAMS_FILE}

###############################################################################
#<<                START COMMON BODY CODE - DO NOT MANUALLY EDIT            >>#
###############################################################################


###############################################################################
#<<                          START CUSTOM BODY CODE                         >>#
###############################################################################

. ${PIG_SCHEMAS_DIR}/smith__idrp_time_phased_status_current.schema

fn_delete_hadoop_directory_if_it_already_exist $WORK__IDRP_TIME_PHASED_STATUS_CURRENT_INCOMING_LOCATION

ls -t $MULTI_INSTANCE_1_DIR/IE_TIME_PHASED_STATUS*.dat > /dev/null 2>&1
if [ $? -ne 0 ]; then
echo "INSTANCE_1_FILE MISSING"
exit 1
fi
INSTANCE_1_FILE=`ls -t $MULTI_INSTANCE_1_DIR/IE_TIME_PHASED_STATUS*.dat | head -1`

ls -t $MULTI_INSTANCE_2_DIR/IE_TIME_PHASED_STATUS*.dat > /dev/null 2>&1
if [ $? -ne 0 ]; then
echo "INSTANCE_2_FILE MISSING"
exit 1
fi
INSTANCE_2_FILE=`ls -t $MULTI_INSTANCE_2_DIR/IE_TIME_PHASED_STATUS*.dat | head -1`

ls -t $MULTI_INSTANCE_3_DIR/IE_TIME_PHASED_STATUS*.dat > /dev/null 2>&1
if [ $? -ne 0 ]; then
echo "INSTANCE_3_FILE MISSING"
exit 1
fi
INSTANCE_3_FILE=`ls -t $MULTI_INSTANCE_3_DIR/IE_TIME_PHASED_STATUS*.dat | head -1`

ls -t $MULTI_INSTANCE_4_DIR/IE_TIME_PHASED_STATUS*.dat > /dev/null 2>&1
if [ $? -ne 0 ]; then
echo "INSTANCE_4_FILE MISSING"
exit 1
fi
INSTANCE_4_FILE=`ls -t $MULTI_INSTANCE_4_DIR/IE_TIME_PHASED_STATUS*.dat | head -1`

sed '1d ;$d' $INSTANCE_1_FILE | hadoop fs -put - $WORK__IDRP_TIME_PHASED_STATUS_CURRENT_INCOMING_LOCATION/instance_1
sed '1d ;$d' $INSTANCE_2_FILE | hadoop fs -put - $WORK__IDRP_TIME_PHASED_STATUS_CURRENT_INCOMING_LOCATION/instance_2
sed '1d ;$d' $INSTANCE_3_FILE | hadoop fs -put - $WORK__IDRP_TIME_PHASED_STATUS_CURRENT_INCOMING_LOCATION/instance_3
sed '1d ;$d' $INSTANCE_4_FILE | hadoop fs -put - $WORK__IDRP_TIME_PHASED_STATUS_CURRENT_INCOMING_LOCATION/instance_4


###############################################################################
#<<              START COMMON FOOTER CODE - DO NOT MANUALLY EDIT            >>#
###############################################################################


###############################################################################
#<>                                 END                                     <>#
###############################################################################
