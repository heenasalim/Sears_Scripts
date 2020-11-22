#!/bin/bash
###############################################################################
#<>                                HEADER                                   <>#
###############################################################################
# SCRIPT NAME:         Perform_IE_Combined_Status_SQOOP.sh
# AUTHOR NAME:         Ajinkya Ragalwar
# CREATION DATE:       Tue Nov  6 09:05:33 EST 2018
# CURRENT REVISION NO: 1
#
# DESCRIPTION: IPS-3732 - Change AFT for Replen Status Job
#
#
#
# DEPENDENCIES: USER.cfg,PROJECT.cfg
# RESTARTABLE:
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
#set -x

export PROJECT_CONF_DIR=$1
export INTERFACE=$2
export TAB_NAME=$3
#export SQL_FILE=$4
export LOAD_MODE=$4


#check if the PROJECT_CONF_DIR is set to valid value

if [ -z "${PROJECT_CONF_DIR}" ]; then
  echo  "ERROR: PROJECT_CONF_DIR must be passed as second argument in the command line " \
    " But it is unset or set to the empty string."
  exit 1
fi

if [[ $LOAD_MODE == "" ]]; then
echo "ERROR: LOAD_MODE must be passed as fourth argument in the command line"\
     " But it is unset or set to the empty string."
exit 1
fi
###################################################################################

#load common configurations file
. ${PROJECT_CONF_DIR}/USER.cfg

#load common configurations file
. ${PROJECT_CONF_DIR}/PROJECT.cfg

#load common configurations file
#. ${PROJECT_CONF_DIR}/DPRP.cfg

###################################################################################

#Set the environmental file/configuration file for IDRP Logging.
#base_dir=$(dirname $(readlink -f "$0"))
#. ${base_dir}/IDRP_Init.sh
. $PROJECT_BIN_DIR/IDRP_Init.sh

PROG=`basename $0 .sh`
COMMAND_LINE=$0" "$@
export LOG_DATE=`date +%Y%m%d%H%M%S`
export LOG_FILE=$IDRP_LOG_DIR/$PROG.$LOG_DATE.log


##Logging header before Job exeuction for Master Job.
write_log_header $LOG_FILE $PROG $COMMAND_LINE

module_relative_path=$PROJECT_BIN/$INTERFACE
query_file=$PROJECT_SQL_DIR/$module_relative_path/Perform_IE_Combined_Status_MV_Creator.sql


######################################CREATING MATERIALIZED VIEW#############################################
#$PROJECT_BIN_DIR/DPRP_ORCLUtils -s -c $query_file -l $LOG_FILE
$PROJECT_BIN_DIR/DPRP_ORCLUtils -c $query_file -l $LOG_FILE -s STGMGR
ErrorCode=$?
if [ $ErrorCode -ne 0 ] ;
then
        echo "ERROR:${PROG}.sh script failed with a error code: $ErrorCode"
        exit $ErrorCode
else
        echo "Materialized View has been created successfully"
fi

write_log_trailer $LOG_FILE $PROGRAM_NAME $ErrorCode $COMMAND_LINE


###################################################################################

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

#Connection parameters for Integration/Production Environments
connection_string=jdbc:oracle:thin:@$PROXY_DB:$DB_PORT/$ORACLE_SID

username=$ORACLE_USER
password=$ORACLE_PASSWORD


###################################################################################

#Loading of query file, module path and target directory

module_relative_path=$PROJECT_BIN/$INTERFACE
query_file=$PROJECT_SQL_DIR/$module_relative_path/$SQL_FILE

table_name_01=${TAB_NAME}
target_dir_01=$PROJECT_SMITH_DIR/ie_combined_status_current
#query=`cat ${query_file}`
command_01="sqoop import"

###############################################################################
#Data file on Linux
###############################################################################
export DPRP_DATA_DIR
FILE_DATE=`date +%Y%m%d%H%M%S`
write_log_info $LOG_FILE "                              "
write_log_info $LOG_FILE "                              "
write_log_info $LOG_FILE "-----------------LOAD_MODE is: $LOAD_MODE-----------------"
write_log_info $LOG_FILE "                              "
write_log_info $LOG_FILE "                              "

if [[ $LOAD_MODE == "C" ]];
then
        FILE_NAME=IDRP_combined_ksn_loc_status_C_${FILE_DATE}.csv
else
        FILE_NAME=IDRP_combined_ksn_loc_status_F_${FILE_DATE}.csv
fi

PRE_DAY=`ls -rt ${MULTI_INSTANCE_SHARED_DIR}/IDRP_combined_ksn_loc_status_*.csv`
File_check=$?

###############################################################################
#Compress previous day file
###############################################################################
export DPRP_ARCHIVE_DIR=$DPRP_DATA_DIR/archive

if [ $File_check -eq 0 ]
then
        write_log_info $LOG_FILE "                              "
        write_log_info $LOG_FILE "                              "
        write_log_info $LOG_FILE "-----------------ARCHIVING PREVIOUS DAY'S FILE-----------------"
        write_log_info $LOG_FILE "                              "
        write_log_info $LOG_FILE "                              "
        #write_log_info $LOG_FILE "  Zipping File ${PRE_DAY} `gzip ${PRE_DAY}`"
        for file in $PRE_DAY
        do
                write_log_info $LOG_FILE " Moving $file to Archive directory $MULTI_INSTANCE_SHARED_ARCHIVE_DIR:`mv $file $MULTI_INSTANCE_SHARED_ARCHIVE_DIR`"
        done
else
        echo "Previous day's file not found!" $File_check
fi

A=`touch ${MULTI_INSTANCE_SHARED_DIR}/${FILE_NAME}`
write_log_info $LOG_FILE "                              "
write_log_info $LOG_FILE "                              "

###############################################################################
#<>                                  BODY                                   <>
###############################################################################

#delete already existing directory
fn_delete_hadoop_directory_if_it_already_exist "${target_dir_01}" &>> ${LOG_FILE}

#execute the sqoop script


write_log_info $LOG_FILE "                              "
write_log_info $LOG_FILE "                              "
write_log_info $LOG_FILE "-----------------IMPORTING DATA FROM TABLE ${TAB_NAME} ON ALL SHARDS -----------------"
write_log_info $LOG_FILE "                              "
write_log_info $LOG_FILE "                              "
#fn_execute_sqoop $table_name_01 $ORACLE_USER $ORACLE_PASSWORD $connection_string "$query" $target_dir_01 , 1 >> ${LOG_FILE}

$command_01 --connect $connection_string --username $ORACLE_USER --password $ORACLE_PASSWORD --table "$TAB_NAME"  --target-dir $target_dir_01 --fields-terminated-by , -m 16 --direct &>> $LOG_FILE

COM_STAT=$?

if [ $COM_STAT -ne 0 ]
then
        echo "SCRIPT COMPLETED WITH ERRORS. PLEASE CHECK ${LOG_FILE} FOR DETAILS"
        cd $PROJECT_BIN_DIR
        write_log_info $LOG_FILE "                              "
        write_log_info $LOG_FILE "                              "
        write_log_info $LOG_FILE "-----------------ERROR WHILE SQOOPING THE TABLE-----------------"
        write_log_info $LOG_FILE "                              "
        write_log_info $LOG_FILE "                              "
        write_log_trailer $LOG_FILE $PROG $COM_STAT $COMMAND_LINE
        exit 100
fi

##################################################################
#Copy Data from HDFS to Local
##################################################################

write_log_info $LOG_FILE "                              "
write_log_info $LOG_FILE "                              "
write_log_info $LOG_FILE "-----------------COPYING DATA FROM HDFS TO LINUX FILE-----------------"
write_log_info $LOG_FILE "                              "

hadoop fs -getmerge ${target_dir_01}/part-m* ${MULTI_INSTANCE_SHARED_DIR}/${FILE_NAME} &>> ${LOG_FILE}

write_log_info $LOG_FILE "                              "
write_log_info $LOG_FILE "New .csv file : "`ls -l ${MULTI_INSTANCE_SHARED_DIR}/${FILE_NAME}`
write_log_info $LOG_FILE "Records extracted in New file : "`wc -l ${MULTI_INSTANCE_SHARED_DIR}/${FILE_NAME} | awk '{print $1}'`
write_log_info $LOG_FILE "                              "

STATUS=$?
        ##Logging Trailer
        cd $PROJECT_BIN_DIR
        write_log_trailer $LOG_FILE $PROG $STATUS $COMMAND_LINE

echo "SCRIPT COMPLETED SUCCESSFULLY"

###############################################################################
#<>                                  END                                    <>#
###############################################################################
