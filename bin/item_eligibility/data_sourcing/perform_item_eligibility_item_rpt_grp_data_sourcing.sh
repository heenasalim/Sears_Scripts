#-------------------BEGIN------------------------
#!/bin/ksh

typeset -i count
typeset -i x
typeset -i active_count

active_count=0
# IDRP_HOME=${HOME}/IDRP

# Check if desired user runs this script

if   [[ $(id -un) != "hdidrp" ]]; then
       echo "===> You must be logged in as hdidrp to run $(basename $0)!!!"
       exit 1
fi

vbatchctlid=$1

PROJECT_CONF_HOME=$2 

. ${PROJECT_CONF_HOME}/HDIDRP.cfg

# exec >> $IDRP_HOME/$(basename $0).log.$(date +%d) 2>&1

# SQOOP script

sqoopImport () {

HADOOPDATADIR=$1
SQL=$2
MAPPERS=$3
FETCHSIZE=$4
SPLIT_FIELD=$5
FIELD_DELIMITER='\001'

hadoop fs -rmr $HADOOPDATADIR.temp > /dev/null 2>&1

sqoop-import --connect jdbc:${vKDB2Host}:${vKDB2Port}/${vKDB2Database} --username ${vKDB2UserName} --password ${vKDB2Password} --target-dir $HADOOPDATADIR.temp --fetch-size ${FETCHSIZE} --split-by ${SPLIT_FIELD} --fields-terminated-by ${FIELD_DELIMITER} -e "$SQL" --m ${MAPPERS}
 
if [ $? -ne 0 ]; then
	echo "SQOOP command unsuccessful..."
	exit 1
else
	echo "SQOOP command successful..."
fi

hadoop fs -ls $HADOOPDATADIR.temp > /dev/null 2>&1 

if [ $? -ne 0 ]; then
	echo "SQOOP file not formed.."
	exit 1
else
	hadoop fs -rmr $HADOOPDATADIR > /dev/null 2>&1
	hadoop fs -mv $HADOOPDATADIR.temp $HADOOPDATADIR > /dev/null 2>&1
	echo "HDFS file $HADOOPDATADIR successfully created with new data..."
fi
}
hdfs_output_path="/incoming/idrp/item_rpt_grp"

echo "Running sqoop importing data for ITEM_RPT_GRP"

sqoopImport "${hdfs_output_path}" "select ITEM_ID,RPT_GRP_ID,RPT_GRP_SEQ_NBR,CREAT_TS,LAST_CHG_USER_ID,LAST_CHG_TS,ITM_RPT_GRP_ALT_ID,DELT_DT from DB2.ITEM_RPT_GRP where \$CONDITIONS" 60 3000 "ITEM_ID"

#-------------------------------END-----------------------------
