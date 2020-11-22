#-------------------BEGIN------------------------
#!/bin/bash
###########################################################################################
#       Script Name   : perform_sqoop_item_eligibility_extract_dc_locn
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
#       Example       : perform_sqoop_item_eligibility_extract_dc_locn
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

vbatchctlid=$1

PROJECT_CONF_HOME=$2 

. ${PROJECT_CONF_HOME}/HDIDRP.cfg
. /appl/hdidrp/etc/idrp_funcs

hdfs_output_path=/incoming/idrp
table_name=dc_locn
hdsucessfile=$hdfs_output_path/$table_name/_SUCCESS
hdlogdir=$hdfs_output_path/$table_name/_logs
FIELD_DELIMITER='\001'
vBatchctlId="11"
MAPPERS="1"
DB2_sql="select DC_LOCN_NBR,LAST_PO_NBR,LAST_IMPORT_PO_NBR,LAST_WRKSHT_NBR,FLOW_THRU_IND,ENABLE_JIF_DC_IND,DC_CD,DC_TYPE_CD,ORD_LOCN_IND,LOGISTICS_GROUP,DC_NM,DC_850_NM,HME_CTR_IND,JIT_IND,STK_IND,IMPORT_IND,RIM_IND,SEN_ITM_MAINT_IND,MDL_DC_LOCN_NBR,LST_CHG_DT,LST_CHG_USR_ID,SEND_ORD_MAINT_IND,PTC_IND from DB2.DC_LOCN where \$CONDITIONS"

#----------------MAIN---------------------------------

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

echo "Running sqoop importing data for DC_LOCN"

sqoop-import --connect jdbc:"${vKDB2Host}":"${vKDB2Port}"/"${vKDB2Database}" --username "${vKDB2UserName}" --password "${vKDB2Password}" --target-dir "${hdfs_output_path}/${table_name}" --fields-terminated-by "${FIELD_DELIMITER}" -e "${DB2_sql}" -m "${MAPPERS}"

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
