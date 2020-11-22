#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         PerformAggHistTransformationPart3a.sh
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
script_name=PerformAggHistTransformationPart3a

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

fn_delete_hadoop_directory_if_it_already_exist $union_sec9_p6a

#execute the pig script

fn_execute_pig \
-m ${pig_params_file} \
-m ${PIG_SCHEMAS_DIR}/agghist/PerformAggHistTransformationPart3a.schema \
${pig_script_file}



###############################################################################
#<>                                  END                                    <>#
###############################################################################

