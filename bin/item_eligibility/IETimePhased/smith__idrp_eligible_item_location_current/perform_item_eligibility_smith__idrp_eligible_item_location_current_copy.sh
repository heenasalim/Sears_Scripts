#!/bin/bash
###############################################################################
#<>                           START HEADER DOCUMENT                         <>#
###############################################################################

# SCRIPT NAME:         perform_item_eligibility_smith__idrp_eligible_item_location_current_copy.sh
# AUTHOR NAME:         Arjun Dabhade
# CREATION DATE:       Wed Jun 18 2014 
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
#	28-may-2015	Pankaj Gupta	Performance - File transfer from Linux to work dir running in background now.
#								
#	

###############################################################################
#<<               START COMMON HEADER CODE - DO NOT MANUALLY EDIT           >>#
###############################################################################


#set project configuration directory
export PROJECT_CONF_DIR=$2

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
MODULE_RELATIVE_PATH=item_eligibility/IETimePhased/smith__idrp_eligible_item_location_current

#this is the name of the script without extension
SCRIPT_NAME=perform_item_eligibility_smith__idrp_eligible_item_location_current

#pig params file
PIG_PARAMS_FILE=${PIG_PARAMS_DIR}/${MODULE_RELATIVE_PATH}/${SCRIPT_NAME}.param


#load param file as shell environment variables file
#pig sparams file has same syntax as shell environment variables file.
. ${PIG_PARAMS_FILE}

###############################################################################
#<<                START COMMON BODY CODE - DO NOT MANUALLY EDIT            >>#
###############################################################################


###############################################################################
#<<                          START CUSTOM BODY CODE                         >>#
###############################################################################

. ${PIG_SCHEMAS_DIR}/smith__idrp_eligible_item_location_current.schema

master_rec_count=0

file_data_validation()
{
FILE_NAME=$1

# File count and record count check
for j in $instances
do
	for count in $(seq -f %02g 1 32)
	do
		ls -t $MULTI_INSTANCE_SHARED_DIR/$j.IE_ITEM_LOC.$count.IETimePhased*.dat > /dev/null 2>&1
		#fn_log $? "File check for file $count in instance $j"
	done
	
	fn_log_info "All files present in instance $j"
			
	fn_assert_linux_file_exist $MULTI_INSTANCE_SHARED_DIR/$j.IE_ITEM_LOC.STATUS*
	
	inst_rec_count=$(tail -1 $MULTI_INSTANCE_SHARED_DIR/$j.IE_ITEM_LOC.STATUS* | grep -Po "(\d+)") 
	
	rec_count=$(cat $MULTI_INSTANCE_SHARED_DIR/$j.$FILE_NAME | wc -l)

	if [ $inst_rec_count -eq $rec_count ];
	then
		fn_log_info "Successful record count match from Instance $j [$inst_rec_count records]"
		master_rec_count=$(($master_rec_count + $rec_count))
	else
		fn_log_info "Record count mismatch from instance $j"
		fn_log_info "Status file count - $inst_rec_count"
		STATUS=1
		write_log_trailer $LOG_FILE $PROG $STATUS $COMMAND_LINE	
		fn_log 1 "Record Count - $rec_count"
		
	fi

done

### Writing master count to temp file, so it can be read in parent shell
echo $master_rec_count > $STAGING_DIR/master_rec_count_IE_ITEM_LOC_temp.txt

echo ${STAGING_DIR}/master_rec_count_IE_ITEM_LOC_temp.txt
echo "time finish fn data validation" `date`

}
############################end of function	


FILE_NAME='IE_ITEM_LOC.??.IETimePhased*'
pid_fn=""

#List out Instances
instances=$(ls -t $MULTI_INSTANCE_SHARED_DIR/*.IE_ITEM_LOC* | xargs -n1 basename | grep -Po "^(\w+)" | sort | uniq)

echo "List of instances = $instances"

# calling function in background 
file_data_validation $FILE_NAME &
pid_fn=$!

fn_delete_hadoop_directory_if_it_already_exist $WORK__IDRP_ELIGIBLE_ITEM_LOCATION_CURRENT_INCOMING_LOCATION
fn_mkdir_hadoop_directory_if_it_does_not_exist $WORK__IDRP_ELIGIBLE_ITEM_LOCATION_CURRENT_INCOMING_LOCATION

for j in $instances
do

($(hdfs dfs -put $MULTI_INSTANCE_SHARED_DIR/$j.IE_ITEM_LOC.??.IETimePhased*.dat $WORK__IDRP_ELIGIBLE_ITEM_LOCATION_CURRENT_INCOMING_LOCATION) >&2 & wait %1; echo $?) &

#pid_put=$!
#pid_put_list="${pid_put_list}  ${pid_put}"
done | grep -qv 0 && echo "Copying files from Linux to hadoop failed:::" | grep failed && exit 1

echo "Waiting for all background process to complete.-"

wait `echo "$pid_fn"`

wait_check=$?

if [ "${wait_check}" != "0" ];
then
echo "ERROR FOUND IN Validation Function - Process Id" $pid_fn
exit 1
fi

master_rec_count=$(cat $STAGING_DIR/master_rec_count_IE_ITEM_LOC_temp.txt)
TRAILER_COUNT=$(tail -1 $MULTI_INSTANCE_SHARED_DIR/IE_ITEM_LOC.STATUS* | grep -Po "(\d+)")	

echo "COUNT IN TRAILER 1 = "$TRAILER_COUNT

if [[ "$master_rec_count" -ne "$TRAILER_COUNT" ]]; then
	echo "COUNT OF ROWS DO NOT MATCH TRAILER COUNT"
	exit 1
else
	echo "ALL COUNTS MATCHED"
	echo Job Completed at `date`
	exit 0
fi


###############################################################################
#<<              START COMMON FOOTER CODE - DO NOT MANUALLY EDIT            >>#
###############################################################################


###############################################################################
#<>                                 END                                     <>#
###############################################################################
