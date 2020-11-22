#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_dc_locn_sqoop.sh
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       Mon Oct 14 05:08:22 EDT 2013
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
module_relative_path=item_eligibility/work__idrp_dc_locn

#this is the db2 table name
table_name=DB2.DC_LOCN

#pig params file
pig_schema_file=${PIG_SCHEMAS_DIR}/item_eligibility/work__idrp_dc_locn.schema

#load schema file as shell environment variables file
#pig schema file has same syntax as shell environment variables file.
. ${pig_schema_file}

#sql to trigger sqoop with
sql="SELECT DC_LOCN_NBR,LAST_PO_NBR,LAST_IMPORT_PO_NBR,LAST_WRKSHT_NBR,FLOW_THRU_IND,ENABLE_JIF_DC_IND,DC_CD,DC_TYPE_CD,ORD_LOCN_IND,LOGISTICS_GROUP,DC_NM,DC_850_NM,HME_CTR_IND,JIT_IND,STK_IND,IMPORT_IND,RIM_IND,SEN_ITM_MAINT_IND,MDL_DC_LOCN_NBR,LST_CHG_DT,LST_CHG_USR_ID,SEND_ORD_MAINT_IND,PTC_IND FROM DB2.DC_LOCN WHERE \$CONDITIONS"

#database connnection properties
user=${DB2_USER}
password=${DB2_PASSWORD}
connection_uri=${DB2_JDBC_URL}

#general sqoop properties
field_delimiter=${DB2_FIELD_DELIMITER}
number_of_mappers=1
other_arguments=

#directory where the sqoop output will be stored
target_dir=${WORK__IDRP_DC_LOCN_INCOMING_LOCATION}

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
