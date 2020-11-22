#!/bin/bash

###########################################################################
#                                                                         #
# This script contains all utility functions generally required for       #
# data sourcing.                                                          #
#                                                                         #
###########################################################################

#Initializing logging
#This function takes two arguments
#1. Log base directory - Its location where the log files will get created.
#2. Name of the log file. - this function will append time in millis to avoid overriding of previous log files.
function init_logging(){
        log_base_dir=$1
        log_file_name=$2
        current_time_in_millis=`date +%Y%m%d%H%M%S`
        echo "initializing logging"
        if [ -d "${log_base_dir}" ]; then
                echo "Directory ${log_base_dir} exists"
        else
                mkdir -p ${log_base_dir}
        fi

        log_file="${log_base_dir}/${log_file_name}_${current_time_in_millis}.log"
        if [ -f "${log_file}" ]; then
                rm -f ${log_file}
        fi

#        exec 1> >(tee -a ${log_file}) 2>&1
}

#This function needs to be called after execution of some command to check the 
#exit code of the command and pring appropriate error or success message.
#This function accepts three argument. 
#1. Error code of command execution.
#2. Error message.
#3. Success message.
function log()
{
        error_code=$1
        error_message=$2
        success_message=$3

        if [[ $error_code != 0 ]]; then
            echo "ERROR: $error_code" $error_message
            exit $error_code
        else
            echo "SUCCESS: " $success_message
        fi
}

#This function deletes a directory on HDFS if it exists.
#It accepts one parameter which the full path to HDFS dir.
#It deletion fails then this method cause script to exit with error message.
function delete_hadoop_dir_if_exists()
{
        hadoop_dir=$1

        hadoop fs -test -e ${hadoop_dir}

        error_code=$?

        if [[ $error_code == 0 ]]; then
                hadoop fs -rmr ${hadoop_dir}
                error_code=$?
                log ${error_code} "failed to delete ${hadoop_dir}" "successfully deleted ${hadoop_dir}"
        fi
}

#This function creates a directory on HDFS if it does not already exists.
#It accepts one parameter which the full path of HDFS dir.
#It creation fails then this method causes script to exit with error message.
function mkdir_hadoop_dir_if_not_exists()
{
        hadoop_dir=$1

        hadoop fs -test -e ${hadoop_dir}

        error_code=$?

        if [[ $error_code == 1 ]]; then
                hadoop fs -mkdir ${hadoop_dir}
                error_code=$?
                log ${error_code} "failed to create ${hadoop_dir}" "successfully create ${hadoop_dir}"
        fi
}

#This function deletes a directory on linux if it exists.
#It accepts one parameter which the full path to linux dir.
#It deletion fails then this method cause script to exit with error message.
function delete_linux_dir_if_exists()
{
        linux_dir=$1
        if [ -d "${linux_dir}" ]; then
                rm -R ${linux_dir}
                error_code=$?
                log ${error_code} "failed to delete directory ${linux_dir}" "successfully deleted directory ${linux_dir}"
          fi
        
}

#This function creates a directory on linux if it does not already exists.
#It accepts one parameter which the full path of linux dir.
#It creation fails then this method causes script to exit with error message.
function mkdir_linux_dir_if_not_exists()
{
        linux_dir=$1
        if [ ! -d "${linux_dir}" ]; then
                mkdir -p ${linux_dir}
                error_code=$?
                log ${error_code} "failed to create directory ${linux_dir}" "successfully created directory ${linux_dir}"
        fi
}

#This function unloads a table from oracle.
#It requires 6 arguments as input
#1. Oracle user name
#2. Oracle user password
#3. Oracle TNS name - Before using this function, make sure this TNS name entry is made in tnsnames.ora file in case its not there already. It requires the help of Oracle DBA & Hadoop Infrastructure team.
#4. Linux output path - Its a location on the linux system where the downloaded table data should be stored.
#5. Field delimiter - Delimiter that needs to be added between fields in the reocrds in the downloaded table data. 
#6. SQL - sql to be fired to fetch the data
function oracle_unload()
{
        oracle_user=$1
        oracle_password=$2
        oracle_tns_name=$3
        linux_output_path=$4
        field_delimiter=$5
        sql=$6
        
        sqluldr2 user="${oracle_user}/${oracle_password}@${oracle_tns_name}" file="${linux_output_path}"  field="${field_delimiter}" query="${sql}"

        error_code=$?
        log ${error_code} "Oracle unload script has failed to download data to location [${linux_output_path}] using SQL [ ${sql} ]. Oracle user is [${oracle_user}]. TNS name is [${oracle_tns_name}]. Field delimiter is [${field_delimiter}]" "successfully deleted directory ${linux_dir}"        
        
}

#This function sqoops data from database table.
#It requires following 10 arguments as input.
#1. Database table name
#2. Database user
#3. Database password
#4. Database driver class name
#5. Database connection URI
#6. SQL to be fired 
#7. Target directory - Its location where the output of sqoop needs to be stored. This is HDFS location.
#8. Field delimiter - Delimiter that needs to be added between fields in the reocrds in the downloaded table data. 
#9. Number of mappers to be used to download the data in parallel fashion
function execute_sqoop()
{
        database_table_name=$1
        database_user=$2
        database_password=$3
         database_driver=$4
        database_connection_uri=$5
        db_query=$6
        target_dir=$7
        field_delimiter=$8
        log_base_dir=$9
        number_of_mappers=$10
       
        echo "sqooping table ${db_query} to target dir ${target_dir} using db query ${db_query}"

        delete_hadoop_dir_if_exists "${target_dir}"

        sqoop import \
              -D mapred.job.name="SQOOP_${database_table_name}" \
              --verbose \
              --username ${database_user} \
              --password ${database_password} \
              --driver ${database_driver} \
              --connect "${database_connection_uri}" \
              --query "${db_query}" \
              --target-dir "${target_dir}" \
              --fields-terminated-by "${field_delimiter}" \
              --outdir "${log_base_dir}" \
              -m ${number_of_mappers}

        error_code=$?
        log ${error_code} "failed to sqoop import table ${database_table_name}" "successfully sqoop imported table ${database_table_name}"

}
