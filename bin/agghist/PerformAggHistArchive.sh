#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         PerformAggHistArchive.sh
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
#	   4/11/2014    Mayank Agarwal  Modifications made for Aggregate history 3.2
#
#
###############################################################################
#<<                               INITIALIZE                                >>#
###############################################################################

#set project configuration directory
export PROJECT_CONF_DIR=$1


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

DATESTAMP=$(date '+%Y%m%d%H%M%s')

###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################

INTERFACE=agghist

PREVDATADIR=$PROJECT_WORK_DIR/$INTERFACE/prev
BKUPDATADIR=$PROJECT_SMITH_DIR/archive/snapshot/$INTERFACE/
CURRDATADIR=$PROJECT_WORK_DIR/$INTERFACE/curr

###############################################################################
#<>                                  BODY                                   <>#
###############################################################################


fn_mk_hadoop_file_if_it_does_not_exist $PROJECT_WORK_DIR/$INTERFACE/prev/part

fn_mk_hadoop_file_if_it_does_not_exist $PROJECT_WORK_DIR/$INTERFACE/curr/part

fn_snapshotMaintenance $PREVDATADIR $BKUPDATADIR $CURRDATADIR


###############################################################################
#<>                                  END                                    <>#
###############################################################################