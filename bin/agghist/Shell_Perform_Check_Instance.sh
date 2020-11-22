#!/bin/bash
###########################################################################################
#       Script Name   : Shell_Perform_Agghist_checkInstance_value.sh
#
#       Description   : This script is used to check number of records loaded into 4 Instances for AggHist 
#
#       Input         : Source Directory
#                       Idrp Batch Id
#
#
#
#       Author        : Eugene Levitan 
#       Date created  : 08/16/2016
#       Restartable   : Yes
#       Usage         : <Shell script name>
#       Example       : Shell_AggHist_Record_Counts_Check.sh 
#
#       Modified on    			 Modified by           Description
#	Mon Oct 10 05:31:58 EDT 2016	Pankaj Gupta	IPS-831 , Reduce abends to Agg Hist by changing threshold to look at D status only 
#
##############################################################################################
INTERFACE=$1
THRESHOLD_CHECK=$2
PROJECT_CONF_DIR=$3
INSTANCE1=$4
INSTANCE2=$5
INSTANCE3=$6
INSTANCE4=$7


echo "-----------------------------------------------------------------------------------------------------------------"
echo "Starting Processing of the Records counts validation for 'D' - Shell_Perform_Agghist_checkInstance_value.sh"
echo "-----------------------------------------------------------------------------------------------------------------"


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

#moving multiinstance path in one file
cd $LOG_DIR
rm agghist_multiinstance_path.txt

echo $INSTANCE1 > agghist_multiinstance_path.txt
echo $INSTANCE2 >> agghist_multiinstance_path.txt
echo $INSTANCE3 >> agghist_multiinstance_path.txt
echo $INSTANCE4 >> agghist_multiinstance_path.txt

FAILURE=1
SUCCESS=0


#FILE_NAME=`ls Hadoop_Jda_AggHistMaster_Part2*.log | tail -1`
#echo "Processing File: "   $FILE_NAME
#
#
##FILE_VALID=`wc -l agghist_records_check.txt | awk {'print $1'}`
#FILE_VALID=`grep "stored" $FILE_NAME | grep "multiinstance" | sed -ne "s/.*stored //;s/ records.*//p" | wc -l | awk {'print $1'}`
#
#echo "Number of read records: " $FILE_VALID
#
#i=1
#
#while [ $FILE_VALID -eq 0 ] && [ $i -le 5 ]
#  do 
#    echo "The file does not contain records processed entry, checking the previous file"
#
#    let i=i+1
#
#    FILE_NAME=`ls Hadoop_Jda_AggHistMaster_Part2* | tail -$i | head -1`
#    echo "Checking previous file " $FILE_NAME
#    FILE_VALID=`grep "stored" $FILE_NAME | grep "multiinstance" | sed -ne "s/.*stored //;s/ records.*//p" | wc -l | awk {'print $1'}`
#    echo "Number of read records: " $FILE_VALID
# done 
#
# if [ $FILE_VALID -eq 0 ]
#
#  then
#    echo "No valid files containing the number of processed records found, exiting the processing..."
#    exit 1
# fi
#
#
#grep "stored"  $FILE_NAME | grep "multiinstance" | sed -ne "s/.*stored //;s/ records.*//p" > agghist_records_check.txt
#

rm $LOG_DIR/agghist_multiinstance_count.txt
rm $LOG_DIR/agghist_records_count.txt


for j in $(cat $LOG_DIR/agghist_multiinstance_path.txt);
do

($((hadoop fs -cat $j/part* | awk -F"|" ' {if($35=="D") print $1;}' | wc -l ; echo $j) >> $LOG_DIR/agghist_multiinstance_count.txt ) >&2 & wait %1; echo $?) &

done | grep -qv 0 && echo "AWK Command for Deleted check failed:::" | grep failed && exit 1

echo "Record count against each instance" 

cat $LOG_DIR/agghist_multiinstance_count.txt

grep -v "multiinstance" $LOG_DIR/agghist_multiinstance_count.txt > $LOG_DIR/agghist_records_count.txt


echo THRESHOLD_CHECK is $THRESHOLD_CHECK

file_read=$LOG_DIR/agghist_records_count.txt

echo "Reading the " $file_read

while IFS= read line
do
 
 echo "Instance Count: " $line
 
 line_int=`echo $line | bc`
   
 if [ $THRESHOLD_CHECK -lt $line_int ]
  then
    echo " Count of delete records in multiinstance is greater than threshold value"
    echo "Exiting Processing with the error..." 
    exit 1 
 fi

done < $file_read


fn_log_info "$(basename $0) completed successfully: $(date)"

echo "-----------------------------------------------------------------------------------------------------------------"
echo "End of the Processing of the Records counts validation - Shell_Perform_Agghist_checkInstance_value.sh"
echo "-----------------------------------------------------------------------------------------------------------------"

exit 0

#-------------------------------END----------------------------------------------------------
