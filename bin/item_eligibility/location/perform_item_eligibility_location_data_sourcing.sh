#!/bin/bash
###########################################################################################
#       Script Name   : perform_ie_location_data_sourcing.sh
#
#       Description   : This script is used to unload the table from oracle
#       Input         : NA
#
#
#
#       Dependencies  : utility_functions.sh
#
#       Author        : Shalinee
#       Date created  : 18/07/2013
#       Restartable   : Yes
#       Usage         : <Shell script name>
#       Example       : perform_ie_location_data_sourcing.sh
#
#       Modified on     Modified by           Description
#
#
#
#---------------------------------------------------------------------------------------------

#TODO - verify all the following paths with the hlp of Arneb & Mudit
#IDRP Project Root Dir
export PROJECT_ROOT_DIR=/appl/hdidrp

typeset -i count
typeset -i x
typeset -i active_count

active_count=0

if [ "$(id -un)" != "hdidrp" ]; then
   echo "This script must be run as hdidrp" 1>&2
   exit 1
fi

export PROJECT_CONF_DIR=$2

. ${PROJECT_CONF_DIR}/HDIDRP.cfg
. ${PROJECT_ROOT_DIR}/etc/idrp_funcs

#Load all utility functions
. ${PROJECT_ROOT_DIR}/etc/utility_functions.sh

linux_output_dir="/staging/hdidrp/item_eligibility"
linux_output_path="${linux_output_dir}/udt_loc"
hdfs_output_path="/incoming/idrp/udt_loc"
#table_name=udt_loc
ORACLE_FIELD_DELIMITER='0x01'
oracle_unload_sql="SELECT LOC,UDC_LOC_CLS_DT ,UDC_LOC_CTY ,UDC_LOC_FMT_TYP_CD ,UDC_LOC_LVL_CD ,UDC_LOC_OPN_DT ,UDC_LOC_STE_CD,UDC_LOC_ZIP_CD ,UDC_SRS_LOC ,UDC_CLIMAZONE_CD ,UDC_CLIMAZONE_NM ,UDC_REGION_CD ,UDC_REGION_NM ,UDC_DISTRICT_CD ,UDC_DISTRICT_NM ,UDC_MERCH_AREA_CD,UDC_MERCH_AREA_NM ,UDC_FMT_TYP_CD ,UDC_FMT_SUB_TYP_CD ,UDC_FMT_MOD_CD,UDC_LOC_TEMP_CLS_DT,DESCR,UDC_ELIG_FNL_CD ,UDC_ELIG_USR_CD,UDC_LOC_TEMP_OPN_DT ,UDC_SHC_VNDR_NO ,UDC_SRS_VNDR_NO,UDC_DUNS_TYP_CD,UDC_DUNS_OWN_CD FROM STSC.UDT_LOC where UDC_ELIG_FNL_CD != 'D'"

#initialize the logging
log_base_dir=${PROJECT_ROOT_DIR}/log
log_file_name=udt_loc

init_logging ${log_base_dir} ${log_file_name}

fn_mkdir_linux_dir_if_not_exists ${linux_output_dir}

#Unload table from oracle
oracle_unload "${ORACLE_USER}" "${ORACLE_PASSWORD}" "${ORACLE_TNS_NAME}" "${linux_output_path}" "${ORACLE_FIELD_DELIMITER}" "${oracle_unload_sql}"
rc=$?

delete_hadoop_dir_if_exists "${hdfs_output_path}"

mkdir_hadoop_dir_if_not_exists "${hdfs_output_path}"

hadoop fs -put $linux_output_path ${hdfs_output_path}

error_code=$?
log ${error_code} "Data unloaded from oracle to linux location ${linux_output_path} has failed to copy to hadoop location ${hdfs_output_path}" "Data is successfully copy to hadoop location${hdfs_output_path}"
