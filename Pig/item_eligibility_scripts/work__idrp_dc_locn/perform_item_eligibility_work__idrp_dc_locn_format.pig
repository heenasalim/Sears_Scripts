/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_dc_locn_format.pig
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       Mon Oct 14 05:08:22 EDT 2013
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
existing_data = LOAD '$WORK__IDRP_DC_LOCN_INCOMING_LOCATION' 
           USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
           AS ( 
                $WORK__IDRP_DC_LOCN_SCHEMA 
              );

--apply formatting to each field              
formatted_data = FOREACH existing_data
                 GENERATE 
                      TRIM(dc_locn_nbr),
                      TRIM(last_po_nbr),
                      TRIM(last_import_po_nbr),
                      TRIM(last_wrksht_nbr),
                      TRIM(flow_thru_ind),
                      TRIM(enable_jif_dc_ind),
                      TRIM(dc_cd),
                      TRIM(dc_type_cd),
                      TRIM(ord_locn_ind),
                      TRIM(logistics_group),
                      TRIM(dc_nm),
                      TRIM(dc_850_nm),
                      TRIM(hme_ctr_ind),
                      TRIM(jit_ind),
                      TRIM(stk_ind),
                      TRIM(import_ind),
                      TRIM(rim_ind),
                      TRIM(sen_itm_maint_ind),
                      TRIM(mdl_dc_locn_nbr),
                      TRIM(lst_chg_dt),
                      TRIM(lst_chg_usr_id),
                      TRIM(send_ord_maint_ind),
                      TRIM(ptc_ind)
                 ;
               
--store formatted data
STORE formatted_data 
INTO '$WORK__IDRP_DC_LOCN_WORK_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
