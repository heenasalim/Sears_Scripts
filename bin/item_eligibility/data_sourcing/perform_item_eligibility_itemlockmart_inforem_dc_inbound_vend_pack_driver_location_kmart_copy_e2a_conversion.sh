#!/bin/bash

JOB_NAME=INFOREM_DC_INBOUND_VEND_PACK_EBCIDIC_CONVERSION

linux_staging_path=/staging/hdidrp/mainframe/kmart/inforem/dc_inbound_vend_pack
hdfs_incoming_path=/incoming/idrp/mainframe/data/kmart/inforem/dc_inbound_vend_pack
hdfs_copybook_dir=/incoming/idrp/mainframe/copybook/kmart
linux_copybook_dir=/appl/hdidrp/mainframe/copybook
copybook_name=INFOREM-DC-INBOUND-VEND-PACK.cpbk

hadoop fs -mkdir ${hdfs_copybook_dir}

hadoop fs -copyFromLocal ${linux_copybook_dir}/${copybook_name}  ${hdfs_copybook_dir}/${copybook_name} 

hadoop fs -mkdir ${hdfs_incoming_path}

hadoop fs -rm ${hdfs_incoming_path}/*

echo "copying mainframe files from local path ${linux_staging_path} to hdfs path ${hdfs_incoming_path} "
hadoop fs -copyFromLocal ${linux_staging_path}/* ${hdfs_incoming_path}/
rc=$?

if [ $rc -ne 0 ];
then
	echo "Error $rc occured while copying files from ${linux_staging_path} to ${hdfs_incoming_path}"
	exit 1
fi   




pig -logfile /logs/hdidrp/pig -param_file /appl/hdidrp/pig/params/item_eligibility/data/ebcidic_INFOREM_DC_INBOUND_VEND_PACK.properties /appl/hdidrp/pig/scripts/item_eligibility/data_sourcing/ebsidic_2_ascii.pig

rc=$?
if [ $rc -ne 0 ];
then
        echo "Error $rc occured while executing $JOB_NAME job \n ABBENDING  $JOB_NAME job.."
        exit 1
fi
