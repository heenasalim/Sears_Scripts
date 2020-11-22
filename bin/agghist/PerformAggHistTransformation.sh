#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         PerformAggHistTransformation.sh
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
#	   03/04/2014   Mayank Agarwal  Changes made for aggregate history with zip code
#	   11/26/2014	Nava Jyothi		Changes made for CR #3388
#          11/18/2015   John Henschel   Added the second pig script to round outputs to 2 decimal places
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
script_name=PerformAggHistTransformation
script_name_2=PerformAggHistTransformationRound

#pig params file
pig_params_file=${PIG_PARAMS_DIR}/${module_relative_path}/${script_name}.param

#pig script file
pig_script_file=${PIG_SCRIPTS_DIR}/${module_relative_path}/${script_name}.pig
pig_script_file_2=${PIG_SCRIPTS_DIR}/${module_relative_path}/${script_name_2}.pig


#load params file as shell environment variables file
#pig params file has same syntax as shell environment variables file.
. ${pig_params_file}

###############################################################################
#<>                                  BODY                                   <>#
###############################################################################



#creating the temporary work location for sales history

hive -e "INSERT OVERWRITE DIRECTORY '$PROJECT_WORK_DIR/sales_history'
SELECT load_ts,ksn_id,sears_division_item_sku_desc,sears_ringing_facility_id_nbr,sears_delivery_facility_id_nbr,shc_ringing_location_nbr,shc_delivery_location_nbr,demand_group_cd,sales_transaction_type_cd,merchandise_status_cd,on_promotion_ind,scim_ind,customer_zip_cd,transaction_dt,actual_sale_qty,lift_sale_qty,lost_sale_qty,selling_amt,regular_price_amt,sales_data_source_cd,week_start_dt
FROM smith__idrp_sales_history;"

#delete existing output files

fn_delete_hadoop_directory_if_it_already_exist $output_hdfs_path

fn_delete_hadoop_directory_if_it_already_exist $smith_table_hdfs_path

fn_delete_hadoop_directory_if_it_already_exist $work_table_hdfs_path

#execute the pig script

fn_execute_pig \
-m ${pig_params_file} \
-m $PIG_SCHEMAS_DIR/work__idrp_sales_history.schema \
-m $PIG_SCHEMAS_DIR/smith__idrp_eligible_item_current.schema \
-m $PIG_SCHEMAS_DIR/smith__idrp_vend_pack_combined.schema \
-m $PIG_SCHEMAS_DIR/smith__idrp_udt_demand_unit.schema \
-m $PIG_SCHEMAS_DIR/gold__inventory_sears_dc_zip_facility_current.schema \
-m $PIG_SCHEMAS_DIR/smith__idrp_eligible_item_location_current.schema \
-m $PIG_SCHEMAS_DIR/smith__idrp_eligible_loc.schema \
-m $PIG_SCHEMAS_DIR/smith__idrp_udt_dfu_view.schema \
-m $PIG_SCHEMAS_DIR/work__idrp_online_dfutosku_map.schema \
${pig_script_file}

fn_execute_pig \
-m ${pig_params_file} \
-m $PIG_SCHEMAS_DIR/smith__idrp_dfutosku_sales_history.schema \
${pig_script_file_2}

###############################################################################
#<>                                  END                                    <>#
###############################################################################
