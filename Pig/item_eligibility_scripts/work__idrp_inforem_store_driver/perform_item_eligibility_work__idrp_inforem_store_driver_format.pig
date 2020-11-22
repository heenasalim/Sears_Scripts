/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_inforem_store_driver_format.pig
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       Mon Oct 14 05:09:39 EDT 2013
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>
#
#
#
# DEPENDENCIES: <<TODO>>
#
#
# REV LIST:
#        DATE         BY            MODIFICATION
#
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/



/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/



--load existing data
existing_data = LOAD '$WORK__IDRP_INFOREM_STORE_DRIVER_CONVERTED_WORK_LOCATION' 
           USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
           AS ( 
                $WORK__IDRP_INFOREM_STORE_DRIVER_SCHEMA 
              );

--apply formatting to each field              
formatted_data = FOREACH existing_data
                 GENERATE 
                      TRIM(driver_item_id),
                      TRIM(driver_store_nbr),
                      TRIM(driver_dvsn_nbr),
                      TRIM(driver_catg_nbr),
                      TRIM(driver_ksn_id),
                      TRIM(driver_vend_pack_id),
                      TRIM(driver_reord_mthd_cd),
                      TRIM(driver_dtc_num),
                      TRIM(driver_dtce_num),
                      TRIM(driver_pog_face),
                      TRIM(driver_pog_pres),
                      TRIM(driver_pog_cap),
                      TRIM(driver_checkout_ind),
                      TRIM(driver_serv_dc),
                      TRIM(filler)
                 ;
			   
--store formatted data
STORE formatted_data 
INTO '$WORK__IDRP_INFOREM_STORE_DRIVER_FORMATTED_WORK_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
