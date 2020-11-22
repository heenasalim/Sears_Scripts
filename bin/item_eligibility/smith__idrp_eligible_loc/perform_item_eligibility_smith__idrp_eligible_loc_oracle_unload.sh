exit 0

#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_loc_oracle_unload.sh
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       Mon Oct 14 05:09:17 EDT 2013
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
module_relative_path=item_eligibility/smith__idrp_eligible_loc

#this is the db2 table name
table_name=STSC.UDT_LOC

#pig params file
pig_schema_file=${PIG_SCHEMAS_DIR}/smith__idrp_eligible_loc.schema

#load schema file as shell environment variables file
#pig schema file has same syntax as shell environment variables file.
. ${pig_schema_file}

#sql to trigger oracle unload with
sql="SELECT LOC, UDC_SRS_LOC, UDC_SHC_VNDR_NO, UDC_SRS_VNDR_NO, DESCR, UDC_LOC_OPN_DT, UDC_LOC_CLS_DT, UDC_LOC_TEMP_OPN_DT, UDC_LOC_TEMP_CLS_DT, UDC_LOC_LVL_CD, UDC_LOC_FMT_TYP_CD, UDC_FMT_TYP_CD, UDC_FMT_SUB_TYP_CD, UDC_FMT_MOD_CD, UDC_LOC_CTY, UDC_LOC_STE_CD, UDC_LOC_ZIP_CD, UDC_REGION_CD, UDC_REGION_NM, UDC_DISTRICT_CD, UDC_DISTRICT_NM, UDC_CLIMAZONE_CD, UDC_CLIMAZONE_NM, UDC_MERCH_AREA_CD, UDC_MERCH_AREA_NM, UDC_ELIG_USR_CD, UDC_ELIG_FNL_CD, UDC_DUNS_TYP_CD, UDC_DUNS_OWN_CD FROM STSC.UDT_LOC WHERE UDC_ELIG_FNL_CD != 'D'"

#database connnection properties
user=${ORACLE_USER}
password=${ORACLE_PASSWORD}
tns_name=${ORACLE_TNS_NAME}

#general oracle unload properties
field_delimiter=${ORACLE_FIELD_DELIMITER}
other_arguments=

#directory where the oracle unload output will be stored
staging_dir=${SMITH__IDRP_ELIGIBLE_LOC_STAGING_LOCATION}

#directory where the oracle unload output will be stored
target_dir=${SMITH__IDRP_ELIGIBLE_LOC_INCOMING_LOCATION}

###############################################################################
#<>                                  BODY                                   <>#
###############################################################################

#delete staging dir
fn_delete_linux_dir_if_exists "${staging_dir}"

#recreate staging dir
fn_mkdir_linux_dir_if_not_exists "${staging_dir}"

#perform oracle unload
fn_execute_oracle_unload "${table_name}" "${user}" "${password}" "${tns_name}" "${staging_dir}" "${field_delimiter}" "${sql}" "${other_arguments}"

#delete hadoop incoming dir
fn_delete_hadoop_directory_if_it_already_exist "${target_dir}"

#create target hadoop incoming dir
fn_mkdir_hadoop_directory_if_it_does_not_exist "${target_dir}"

#upload unloaded file to HDFS
fn_copy_from_local_dir_to_hadoop_dir "${staging_dir}" "${target_dir}"


###############################################################################
#<>                                  END                                    <>#
###############################################################################
