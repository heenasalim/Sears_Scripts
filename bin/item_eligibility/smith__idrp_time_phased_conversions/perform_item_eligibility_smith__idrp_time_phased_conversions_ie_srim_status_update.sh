#!/bin/bash
###############################################################################
#<>                           START HEADER DOCUMENT                         <>#
###############################################################################

# SCRIPT NAME:         perform_item_eligibility_smith__idrp_time_phased_conversions_ie_srim_status_update.sh
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
MODULE_RELATIVE_PATH=item_eligibility/smith__idrp_time_phased_conversions

#this is the name of the script without extension
SCRIPT_NAME=perform_item_eligibility_smith__idrp_time_phased_conversions_ie_srim_status_update


#pig params file
#PIG_PARAMS_FILE=${PIG_PARAMS_DIR}/${MODULE_RELATIVE_PATH}/${PARAM_FILE}.param

#pig script file
#PIG_SCRIPT_FILE=${PIG_SCRIPTS_DIR}/${MODULE_RELATIVE_PATH}/${SCRIPT_NAME}.pig


#load param file as shell environment variables file
#pig sparams file has same syntax as shell environment variables file.
#. ${PIG_PARAMS_FILE}

###############################################################################
#<<                START COMMON BODY CODE - DO NOT MANUALLY EDIT            >>#
###############################################################################


###############################################################################
#<<                          START CUSTOM BODY CODE                         >>#
###############################################################################


ls -t $MULTI_INSTANCE_1_DIR/IE_SRIM_STATUS_UPDATE.IETimePhased*.dat > /dev/null 2>&1
if [ $? -ne 0 ]; then
echo "INSTANCE_1_FILE MISSING"
exit 1
fi
INSTANCE_1_FILE=`ls -t $MULTI_INSTANCE_1_DIR/IE_SRIM_STATUS_UPDATE.IETimePhased*.dat | head -1`

ls -t $MULTI_INSTANCE_2_DIR/IE_SRIM_STATUS_UPDATE.IETimePhased*.dat > /dev/null 2>&1
if [ $? -ne 0 ]; then
echo "INSTANCE_2_FILE MISSING"
exit 1
fi
INSTANCE_2_FILE=`ls -t $MULTI_INSTANCE_2_DIR/IE_SRIM_STATUS_UPDATE.IETimePhased*.dat | head -1`

ls -t $MULTI_INSTANCE_3_DIR/IE_SRIM_STATUS_UPDATE.IETimePhased*.dat > /dev/null 2>&1
if [ $? -ne 0 ]; then
echo "INSTANCE_3_FILE MISSING"
exit 1
fi
INSTANCE_3_FILE=`ls -t $MULTI_INSTANCE_3_DIR/IE_SRIM_STATUS_UPDATE.IETimePhased*.dat | head -1`

ls -t $MULTI_INSTANCE_4_DIR/IE_SRIM_STATUS_UPDATE.IETimePhased*.dat > /dev/null 2>&1
if [ $? -ne 0 ]; then
echo "INSTANCE_4_FILE MISSING"
exit 1
fi
INSTANCE_4_FILE=`ls -t $MULTI_INSTANCE_4_DIR/IE_SRIM_STATUS_UPDATE.IETimePhased*.dat | head -1`

fn_delete_linux_file_if_exists $STAGING_DIR/DRPP.LTRANSAC.IESTATUS

cat $INSTANCE_1_FILE  >>  $STAGING_DIR/DRPP.LTRANSAC.IESTATUS
cat $INSTANCE_2_FILE  >>  $STAGING_DIR/DRPP.LTRANSAC.IESTATUS
cat $INSTANCE_3_FILE  >>  $STAGING_DIR/DRPP.LTRANSAC.IESTATUS
cat $INSTANCE_4_FILE  >>  $STAGING_DIR/DRPP.LTRANSAC.IESTATUS

###############################################################################
#<<              START COMMON FOOTER CODE - DO NOT MANUALLY EDIT            >>#
###############################################################################


###############################################################################
#<>                                 END                                     <>#
###############################################################################

