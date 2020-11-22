/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_inforem_dc_inbound_vend_pack_format.pig
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       Mon Oct 14 05:10:24 EDT 2013
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
existing_data = LOAD '$WORK__IDRP_INFOREM_DC_INBOUND_VEND_PACK_CONVERTED_WORK_LOCATION' 
           USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
           AS ( 
                $WORK__IDRP_INFOREM_DC_INBOUND_VEND_PACK_SCHEMA 
              );

--apply formatting to each field              
formatted_data = FOREACH existing_data
                 GENERATE 
                      TRIM(inbnd_item_id),
                      TRIM(inbnd_dc_locn_nbr),
                      TRIM(inbnd_vend_pack_id),
                      TRIM(inbnd_ship_duns_nbr),
                      inbnd_reord_ins_date,
                      inbnd_store_count,
                      inbnd_orderable_ndc,
                      TRIM(filler)
                 ;
			   
--store formatted data
STORE formatted_data 
INTO '$WORK__IDRP_INFOREM_DC_INBOUND_VEND_PACK_FORMATTED_WORK_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
