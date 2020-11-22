#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         PerformAggHistTransformationPart2.sh
# AUTHOR NAME:         Khim Mehta
# CREATION DATE:       Wed July 27, 2016
# CURRENT REVISION NO: 1
#
# DESCRIPTION: creating the temporary work location from  smith__idrp_sales_history hive table
#
# DEPENDENCIES: USER.cfg,PROJECT.cfg
# RESTARTABLE:  
# Jira Fixes : IPS-1125
#
###############################################################################
#<<                               INITIALIZE                                >>#
###############################################################################

#BATCH_ID=$1
#set project configuration directory
PROJECT_CONF_DIR=$1




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

PROG=`basename $0 .sh`
COMMAND_LINE=$0" "$@
export LOG_DATE=`date +%Y%m%d%H%M%S`
export LOG_FILE=$IDRP_LOG_DIR/$PROG.$LOG_DATE.log

###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
# IPS-1125
# IPS-2235
fn_delete_hadoop_directory_if_it_already_exist $PROJECT_WORK_DIR/agghist/agg_input_sales_history

#creating the temporary work location for sales history

hive -e "INSERT OVERWRITE DIRECTORY '$PROJECT_WORK_DIR/agghist/agg_input_sales_history'
SELECT load_ts,ksn_id,sears_division_item_sku_desc,sears_ringing_facility_id_nbr,sears_delivery_facility_id_nbr,shc_ringing_location_nbr,shc_delivery_location_nbr,demand_group_cd,sales_transaction_type_cd,merchandise_status_cd,on_promotion_ind,scim_ind,customer_zip_cd,transaction_dt,actual_sale_qty,lift_sale_qty,lost_sale_qty,selling_amt,regular_price_amt,sales_data_source_cd,week_start_dt
FROM smith__idrp_sales_history;" > $LOG_DIR/PerformAggHistTransformationPart2Pre$LOG_DATE.log 2>&1


###############################################################################
#<>                                  END                                    <>#
###############################################################################

