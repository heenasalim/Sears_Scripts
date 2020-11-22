REGISTER $PIGGYBANK ;

DEFINE TRIM_STRING $TRIM_STRING ;

rmf $DC_LOCN_DB2_FILE_WORK_LOCATION;

dc_locn_db2_table = LOAD '$DC_LOCN_DB2_FILE_INCOMING_LOCATION' USING PigStorage('$FIELD_DELIMITER') AS ($DC_LOCN_DB2_FILE_SCHEMA) ;

dc_locn_db2_hadoop_table = FOREACH dc_locn_db2_table GENERATE $DC_LOCN_DB2_HADOOP_FILE_SCHEMA;

STORE dc_locn_db2_hadoop_table INTO '$DC_LOCN_DB2_FILE_WORK_LOCATION'  USING PigStorage('$FIELD_DELIMITER');
