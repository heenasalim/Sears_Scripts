#!/bin/bash
###############################################################################
#<>                           START HEADER DOCUMENT                         <>#
###############################################################################

# SCRIPT NAME:         perform_get_eligible_loc_copy.sh
# AUTHOR NAME:         Neha Bisht
# CREATION DATE:       09-06-2014 04:32
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>
#
#
#
# DEPENDENCIES: None
# RESTARTABLE:  N/A
#
#
# REV LIST:
#      DATE         BY            MODIFICATION
#	04/22/2014	Pankaj Gupta		Added IDRP Header and Trailer logging mechanism.
#   10/03/2016	Bhagwan Soni	IPS-545 Switch batch archiving to use new archive directory.
#

###############################################################################
#<<               START COMMON HEADER CODE - DO NOT MANUALLY EDIT           >>#
###############################################################################

#Check for number of paramerets

if [ $# -ne 3 ]; then
	echo "Incorrect Usage: Requires 3 parameters: Syntax <script name> batch_id config_folder file_name(UDT_LOC here)"
	exit 1
fi

#set project configuration directory
export BATCH_ID=$1
export PROJECT_CONF_DIR=$2
export location_file=$3

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


# Set the environmental file/configuration file for IDRP Logging.
#base_dir=$(dirname $(readlink -f "$0"))
#. ${base_dir}/IDRP_Init.sh
. $PROJECT_BIN_DIR/IDRP_Init.sh

PROG=`basename $0 .sh`
COMMAND_LINE=$0" "$@
export LOG_DATE=`date +%Y%m%d%H%M%S`
export LOG_FILE=$IDRP_LOG_DIR/$PROG.$LOG_DATE.log

##Logging header before Job exeuction for Master Job.
write_log_header $LOG_FILE $PROG $COMMAND_LINE

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
#<<                         START CUSTOM HEADER CODE                        >>#
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
MODULE_RELATIVE_PATH=item_eligibility/smith__idrp_eligible_loc

#this is the name of the script without extension
SCRIPT_NAME=perform_smith__idrp_eligible_loc

#pig script file
PIG_SCRIPT_FILE=${PIG_SCRIPTS_DIR}/${MODULE_RELATIVE_PATH}/${SCRIPT_NAME}.pig

#pig schema file
PIG_SCHEMA_FILE=${PIG_SCHEMAS_DIR}/smith__idrp_eligible_loc.schema

#load param file as shell environment variables file
#pig sparams file has same syntax as shell environment variables file.

. ${PIG_SCHEMA_FILE}
###############################################################################
#<<                START COMMON BODY CODE - DO NOT MANUALLY EDIT            >>#
###############################################################################


###############################################################################
#<<                          START CUSTOM BODY CODE                         >>#
###############################################################################

fn_delete_hadoop_directory_if_it_already_exist $SMITH__IDRP_ELIGIBLE_LOC_INCOMING_LOCATION

fn_delete_hadoop_directory_if_it_already_exist $SMITH__IDRP_ELIGIBLE_LOC_WORK_LOCATION

ls -t $MULTI_INSTANCE_1_DIR/${location_file}*.dat > /dev/null 2>&1

if [ $? -ne 0 ]; then
echo "INSTANCE_1_FILE MISSING"
			STATUS=1
			##Logging Trailer #cd ${base_dir}
			cd $PROJECT_BIN_DIR
			write_log_trailer $LOG_FILE $PROG $STATUS $COMMAND_LINE
exit 1
fi

#get the latest file for location

INSTANCE_1_FILE=`ls -t $MULTI_INSTANCE_1_DIR/${location_file}*.dat | head -1`

echo "LOCATION FILE SELECTED = $INSTANCE_1_FILE"

sed '1d ;$d' $INSTANCE_1_FILE > ${SMITH__IDRP_ELIGIBLE_LOC_STAGING_LOCATION} 

testing=`cat ${SMITH__IDRP_ELIGIBLE_LOC_STAGING_LOCATION}|wc -l`

count=`cat ${INSTANCE_1_FILE} | tail -1 | cut -d '|' -f2`

echo "Actual Row Count is $testing and Trailier Record Count is $count"

if [ $testing -ne $count ]
then
echo "MISMATCH BETWEEN TRAILER RECORD COUNT AND ACTUAL ROW COUNT.."
			STATUS=1
			##Logging Trailer #cd ${base_dir}
			cd $PROJECT_BIN_DIR
			write_log_trailer $LOG_FILE $PROG $STATUS $COMMAND_LINE
exit 1
fi

cat ${SMITH__IDRP_ELIGIBLE_LOC_STAGING_LOCATION} | sed "s/|//g" | hadoop fs -put - $SMITH__IDRP_ELIGIBLE_LOC_INCOMING_LOCATION

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

#this is the name of the script without extension
script_name=perform_item_eligibility_smith__idrp_eligible_loc_format

#pig schema file
pig_schema_file=${PIG_SCHEMAS_DIR}/smith__idrp_eligible_loc.schema

#pig script file
pig_script_file=${PIG_SCRIPTS_DIR}/${module_relative_path}/${script_name}.pig

#load schema file as shell environment variables file
#pig schema file has same syntax as shell environment variables file.
. ${pig_schema_file}

#directory where the formatted data will be stored output will be stored
work_dir=${SMITH__IDRP_ELIGIBLE_LOC_WORK_LOCATION}

#smith location
output_dir=${SMITH__IDRP_ELIGIBLE_LOC_LOCATION}

#execute the pig script
fn_execute_pig -m ${pig_schema_file} ${pig_script_file}

#remove target directory if it already exists
fn_delete_hadoop_directory_if_it_already_exist "${output_dir}"

#copy work data to smith
fn_copy_hadoop_directory_if_target_location_does_not_exists "${work_dir}" "${output_dir}"

# Compress the load ready file and move to archive

gzip ${INSTANCE_1_FILE}
#Archiving change under the issue IPS-545
SHARD=$(echo $MULTI_INSTANCE_1_DIR | rev |cut -d'/' -f2 |rev)
#Commenting below statement and adding new one
#mv ${INSTANCE_1_FILE}.gz ${MULTI_INSTANCE_1_DIR}/archive/
mv ${INSTANCE_1_FILE}.gz $IDRP_ARCHIVE_DIR/$SHARD

	STATUS=$?
	##Logging Trailer 
	cd $PROJECT_BIN_DIR
	write_log_trailer $LOG_FILE $PROG $STATUS $COMMAND_LINE

###############################################################################
#<<              START COMMON FOOTER CODE - DO NOT MANUALLY EDIT            >>#
###############################################################################

###############################################################################
#<>                                 END                                     <>#
###############################################################################
