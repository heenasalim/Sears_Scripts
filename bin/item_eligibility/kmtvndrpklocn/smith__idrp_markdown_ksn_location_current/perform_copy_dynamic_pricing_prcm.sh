#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################

# SCRIPT NAME:         perform_copy_dynamic_pricing_prcm.sh
# AUTHOR NAME:         Pankaj gupta
# CREATION DATE:       
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>	Script copies the data from $WORK_DIR TO $PROJECT_WORK_DIR/dynamic_pricing/prcm/inforem/sears_kmart_distinct
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

#this is the name of the script without extension
script_name=perform_copy_dynamic_pricing_prcm

hadoop distcp $WORK_DIR/dynamic_pricing/prcm/inforem/sears_kmart_distinct $PROJECT_WORK_DIR/temp/dynamic_pricing/prcm/inforem/sears_kmart_distinct

if [ $? == 0 ]; then
DATESTAMP=$(date '+%Y%m%d%H%M%s')
hadoop fs -mkdir $PROJECT_WORK_DIR/archive/snapshot/dynamic_pricing/prcm/inforem/sears_kmart_distinct/$DATESTAMP
hadoop fs -mv $PROJECT_WORK_DIR/dynamic_pricing/prcm/inforem/sears_kmart_distinct $PROJECT_WORK_DIR/archive/snapshot/dynamic_pricing/prcm/inforem/sears_kmart_distinct/$DATESTAMP
hadoop fs -mv $PROJECT_WORK_DIR/temp/dynamic_pricing/prcm/inforem/sears_kmart_distinct $PROJECT_WORK_DIR/dynamic_pricing/prcm/inforem/sears_kmart_distinct

echo  "Data copied completed at location- " $PROJECT_WORK_DIR/dynamic_pricing/prcm/inforem/sears_kmart_distinct 
exit $?
else
echo  "ERROR: failed to copy $WORK_DIR/dynamic_pricing/prcm/inforem/sears_kmart_distinct."
exit 1
fi



###############################################################################
#<>                                  END                                    <>#
###############################################################################
