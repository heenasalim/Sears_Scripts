#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_item_rpt_grp_sqoop.sh
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       Mon Oct 14 05:08:44 EDT 2013
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
module_relative_path=item_eligibility/work__idrp_item_rpt_grp

#this is the db2 table name
table_name=DB2.ITEM_RPT_GRP

#pig params file
pig_schema_file=${PIG_SCHEMAS_DIR}/item_eligibility/work__idrp_item_rpt_grp.schema

#load schema file as shell environment variables file
#pig schema file has same syntax as shell environment variables file.
. ${pig_schema_file}

#sql to trigger sqoop with
sql="SELECT ITEM_ID, RPT_GRP_ID, RPT_GRP_SEQ_NBR, CREAT_TS, LAST_CHG_USER_ID, LAST_CHG_TS, ITM_RPT_GRP_ALT_ID, DELT_DT FROM DB2.ITEM_RPT_GRP where \$CONDITIONS"

#database connnection properties
user=${DB2_USER}
password=${DB2_PASSWORD}
connection_uri=${DB2_JDBC_URL}

#general sqoop properties
field_delimiter=${DB2_FIELD_DELIMITER}
number_of_mappers=60
other_arguments="--fetch-size 3000 --split-by ITEM_ID"

#directory where the sqoop output will be stored
target_dir=${WORK__IDRP_ITEM_RPT_GRP_INCOMING_LOCATION}

###############################################################################
#<>                                  BODY                                   <>#
###############################################################################


#remove target directory if it already exists
fn_delete_hadoop_directory_if_it_already_exist "${target_dir}"

#execute the sqoop 
fn_execute_sqoop "${table_name}" "${user}" "${password}" "${connection_uri}" "${sql}" "${target_dir}" "${field_delimiter}" "${number_of_mappers}" "${other_arguments}"


###############################################################################
#<>                                  END                                    <>#
###############################################################################
