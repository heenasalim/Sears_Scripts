#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################

# SCRIPT NAME:         perform_item_eligibility_work__idrp_kmart_vendor_package_location_store_level_part1.sh
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Wed Apr 23 02:52:16 EDT 2014
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
module_relative_path=item_eligibility/kmtvndrpklocn/work__idrp_kmart_vendor_package_location_store_level

#this is the name of the script without extension
script_name=perform_item_eligibility_work__idrp_kmart_vendor_package_location_store_level_part1
script_name_1b=perform_item_eligibility_work__idrp_kmart_vendor_package_location_store_level_part1_1b

script_param_name=perform_item_eligibility_work__idrp_kmart_vendor_package_location_store_level

#pig params file
pig_params_file=${PIG_PARAMS_DIR}/${module_relative_path}/${script_param_name}.param

#pig script file
pig_script_file=${PIG_SCRIPTS_DIR}/${module_relative_path}/${script_name}.pig
pig_script_file_1b=${PIG_SCRIPTS_DIR}/${module_relative_path}/${script_name_1b}.pig


#load params file as shell environment variables file
#pig params file has same syntax as shell environment variables file.
. ${pig_params_file}

#load schema file
. ${PIG_SCHEMAS_DIR}/item_eligibility/kmtvndrpklocn/work__idrp_store_level_vend_pack_loc_final.schema  


###############################################################################
#<>                                  BODY                                   <>#
###############################################################################

#IE_batchdate 
. ${PIG_SCHEMAS_DIR}/smith__idrp_item_eligibility_batchdate.schema

eval batch_dt_file=$SMITH__IDRP_ITEM_ELIGIBILITY_BATCHDATE_LOCATION
batch_dt=$(hdfs dfs -cat $batch_dt_file/part* | sed 's/\x01/~/g' | cut -d '~' -f2)
processing_ts=$(hdfs dfs -cat $batch_dt_file/part* | sed 's/\x01/~/g' | cut -d '~' -f3 | tr ' ' '~')

fn_delete_hadoop_directory_if_it_already_exist $WORK__IDRP_POST_KMART_MARKDOWN_PROCESS_ALLOC_REPLN_LOCATION
fn_delete_hadoop_directory_if_it_already_exist $WORK__IDRP_STORE_LEVEL_VEND_PACK_LOC_FINAL_LOCATION

#execute the pig script
fn_execute_pig -m ${pig_params_file} -m ${PIG_SCHEMAS_DIR}/smith__idrp_vend_pack_combined.schema -m $PIG_SCHEMAS_DIR/smith__idrp_ksn_attribute_current.schema -m ${PIG_SCHEMAS_DIR}/item_eligibility/work__idrp_vpstores_after_order_dotcom_edits.schema -m ${PIG_SCHEMAS_DIR}/gold__item_scan_based_trading_vendor_package_current.schema -m ${PIG_SCHEMAS_DIR}/gold__item_attribute_relate_current.schema -m ${PIG_SCHEMAS_DIR}/gold__geographic_network_store_dc.schema -m ${PIG_SCHEMAS_DIR}/smith__idrp_item_eligibility_batchdate.schema -m ${PIG_SCHEMAS_DIR}/gold__item_shc_hierarchy_current.schema -m ${PIG_SCHEMAS_DIR}/gold__item_exploding_assortment.schema -m ${PIG_SCHEMAS_DIR}/smith__idrp_collections_carton_pack_xref_current.schema -m ${PIG_SCHEMAS_DIR}/smith__idrp_vend_pack_dc_combined.schema -m ${PIG_SCHEMAS_DIR}/smith__idrp_dc_location_current.schema -m ${PIG_SCHEMAS_DIR}/smith__idrp_eligible_loc.schema -m ${PIG_SCHEMAS_DIR}/smith__idrp_markdown_ksn_location_current.schema -m ${PIG_SCHEMAS_DIR}/smith__idrp_vendor_package_store_driver.schema -m ${PIG_SCHEMAS_DIR}/item_eligibility/kmtvndrpklocn/work__idrp_store_level_vend_pack_loc_final.schema  -p batch_dt=$batch_dt -p processing_ts=$processing_ts   ${pig_script_file}

if [ "$?" == "0" ];
then
echo "Ready to execute script part1 1b"
else
echo Script $pig_script_file  execution failed.
exit 1
fi

fn_execute_pig -m ${pig_params_file} -m ${PIG_SCHEMAS_DIR}/gold__item_exploding_assortment.schema -m ${PIG_SCHEMAS_DIR}/item_eligibility/kmtvndrpklocn/work__idrp_store_level_vend_pack_loc_final.schema  ${pig_script_file_1b}

###############################################################################
#<>                                  END                                    <>#
###############################################################################
