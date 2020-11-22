#-------------------BEGIN------------------------
#!/bin/bash
###########################################################################################
#       Script Name   : perform_ie_item_rpt_cost_data_sourcing.sh
#
#       Description   : This script is used to sqoop the table from DB2
#       Input         : NA
#
#
#
#       Dependencies  : HDIDRP.cfg
#
#       Author        : Shalinee
#       Date created  : 18/07/2013
#       Restartable   : Yes
#       Usage         : <Shell script name>
#       Example       : perform_ie_item_rpt_cost_data_sourcing.sh
#
#       Modified on     Modified by           Description
#
#
#
#---------------------------------------------------------------------------------------------
typeset -i count
typeset -i x
typeset -i active_count

active_count=0

if [ "$(id -un)" != "hdidrp" ]; then
   echo "This script must be run as hdidrp" 1>&2
   exit 1
fi

PROJECT_CONF_HOME=$2 

. ${PROJECT_CONF_HOME}/HDIDRP.cfg

. /appl/hdidrp/etc/idrp_funcs

#current_date='2013-05-19'
hdfs_output_path=/incoming/idrp
table_name=item_rpt_cost
hdsucessfile=$hdfs_output_path/$table_name/_SUCCESS
hdlogdir=$hdfs_output_path/$table_name/_logs
FIELD_DELIMITER="\001"
vbatchctlid="11"
MAPPERS="60"
FETCHSIZE="6000"
SPLIT_FIELD="ITEM_ID"
DB2_sql="select ITEM_ID,FISC_WK_END_DT,CORP_90DY_AVG_COST,PR_90DY_AVG_COST,VI_90DY_AVG_COST,GU_90DY_AVG_COST,HI_90DY_AVG_COST,STSD_90DY_AVG_COST,CORP_PTD_AVG_COST,PR_PTD_AVG_COST,VI_PTD_AVG_COST,GU_PTD_AVG_COST,HI_PTD_AVG_COST,STSD_PTD_AVG_COST from DB2.ITEM_RPT_COST where FISC_WK_END_DT > '2013-05-19' AND \$CONDITIONS" 

#------------------------------MAIN---------------------------------

# SQOOP script

echo 'Remove Target Hadoop directory '$hdfs_output_path/$table_name''

echo '---------------------------------------------------------------------------------------------------------------------------------'

hadoop fs -test -d "$hdfs_output_path/$table_name" > /dev/null 2>&1
hd=$?
if [ "$hd" -eq "0" ];
then
        hadoop fs -rmr $hdfs_output_path/$table_name
        check_for_errors $? "deleting Target Hadoop Directory $hdfs_output_path/$table_name"
else

echo "---------------------------------------------------------------------------------------------------------------------------------"

echo "Target Hadoop directory "$hdfs_output_path/$table_name" does not exist.  "

echo "---------------------------------------------------------------------------------------------------------------------------------"

fi

echo '---------------------------------------------------------------------------------------------------------------------------------'

echo 'Sqooping the data into '$hdfs_output_path/$table_name

echo '---------------------------------------------------------------------------------------------------------------------------------'

echo "Running sqoop importing data for ITEM_RPT_COST"

sqoop-import --connect jdbc:"${vKDB2Host}":"${vKDB2Port}"/"${vKDB2Database}" --username "${vKDB2UserName}" --password "${vKDB2Password}" --target-dir "${hdfs_output_path}/${table_name}" --fetch-size "${FETCHSIZE}" --split-by "${SPLIT_FIELD}" --fields-terminated-by "${FIELD_DELIMITER}" -e "${DB2_sql}" --m "${MAPPERS}"

check_for_errors $? "sqooping $table_name"

echo "---------------------------------------------------------------------------------------------------------------------------------"

echo "Sqoop Completed Successfully for table " $table_name
echo "Data loaded for table " $table_name
echo "Removing _SUCCESS File from " $hdfs_output_path/$table_name

echo "---------------------------------------------------------------------------------------------------------------------------------"

hadoop fs -test -e "$hdsucessfile" > /dev/null 2>&1
frc=$?
if [ "$frc" -eq "0" ];
then
        hadoop fs -rmr $hdsucessfile
        check_for_errors $? "deleting _SUCCESS file from the Hadoop directory.. $hdfs_output_path/$table_name"
else

echo "---------------------------------------------------------------------------------------------------------------------------------"

echo "_SUCCESS file does not exist in the Hadoop directory  "$hdfs_output_path/$table_name

echo "---------------------------------------------------------------------------------------------------------------------------------"
fi

echo '---------------------------------------------------------------------------------------------------------------------------------'

echo 'Removing Log directory '$hdlogdir 'if exists .. '

echo '---------------------------------------------------------------------------------------------------------------------------------'

hadoop fs -test -d "$hdlogdir" > /dev/null 2>&1
drc=$?
if [ "$drc" -eq "0" ];
then
        hadoop fs -rmr $hdlogdir
        check_for_errors $? "deleting Hadoop Directory $hdlogdir"
else

echo "---------------------------------------------------------------------------------------------------------------------------------"

echo "Hadoop directory "$hdlogdir" does not exist.  "

echo "---------------------------------------------------------------------------------------------------------------------------------"
fi

exit 0 
#-------------------------------END-----------------------------
