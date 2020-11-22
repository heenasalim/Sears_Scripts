/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_work__idrp_inforem_store_master_format.pig
# AUTHOR NAME:         Abhijeet Shingate
# CREATION DATE:       Mon Oct 14 05:09:27 EDT 2013
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
existing_data = LOAD '$WORK__IDRP_INFOREM_STORE_MASTER_CONVERTED_WORK_LOCATION' 
           USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
           AS ( 
                $WORK__IDRP_INFOREM_STORE_MASTER_SCHEMA 
              );

--apply formatting to each field              
formatted_data = FOREACH existing_data
                 GENERATE 
                      TRIM(m_repl_item_id),
                      TRIM(m_repl_store),
                      TRIM(m_dvsn_nbr),
                      TRIM(m_catg_nbr),
                      TRIM(m_subcatg_nbr),
                      TRIM(m_merch_area),
                      TRIM(m_serv_dc),
                      TRIM(m_ord_duns_nbr),
                      TRIM(m_orderable_vend_pack_id),
                      TRIM(m_orderable_ksn_id),
                      TRIM(m_orderable_ksn_pack_id),
                      TRIM(m_dser),
                      TRIM(m_lt),
                      TRIM(m_madlt),
                      TRIM(m_ostrat),
                      TRIM(m_cost),
                      TRIM(m_packsz),
                      TRIM(m_imax),
                      TRIM(m_imin),
                      TRIM(m_usermax),
                      TRIM(m_usermin),
                      TRIM(m_ddrunsw),
                      TRIM(m_ucycle),
                      TRIM(m_bpn),
                      TRIM(m_dduwks),
                      TRIM(m_ddumos),
                      TRIM(m_aux_sysid),
                      TRIM(m_ft),
                      TRIM(m_dd),
                      TRIM(m_ddi),
                      TRIM(m_lpp),
                      TRIM(m_madss),
                      TRIM(m_fwmad),
                      TRIM(m_trndp),
                      TRIM(m_cumsls),
                      TRIM(m_cumpds),
                      TRIM(m_outstat),
                      TRIM(m_lrtfcst),
                      TRIM(m_ordstat),
                      TRIM(m_pt),
                      TRIM(m_ptag),
                      TRIM(m_rt),
                      TRIM(m_system_split_nbr),
                      TRIM(m_new_itmstr_ind),
                      TRIM(m_store_auth_ind),
                      m_last_auth_date,
                      TRIM(m_orderable_ind),
                      TRIM(m_orderable_cd),
                      TRIM(m_reord_mthd_cd),
                      TRIM(m_review_freq_cd),
                      TRIM(m_process_record_ind),
                      TRIM(m_store_supplier_cd),
                      TRIM(m_flowtype_cd),
                      TRIM(m_dc_hndl_cd),
                      TRIM(m_item_age_sw),
                      TRIM(m_store_age_sw),
                      m_first_auto_repl_date,
                      m_ptag_update_date,
                      TRIM(m_ptag_update_sess_id),
                      TRIM(m_calc_cstock),
                      TRIM(m_pog_face),
                      TRIM(m_pog_pres),
                      TRIM(m_pog_cap),
                      TRIM(m_pog_checkout_ind),
                      m_dtc_date,
                      m_dtce_date,
                      TRIM(m_pkg_weight),
                      TRIM(m_pkg_weight_cd),
                      TRIM(m_unit_sell_price),
                      TRIM(m_locn_division_cd),
                      TRIM(m_locn_format_type),
                      TRIM(m_locn_format_sub_type),
                      m_locn_open_date,
                      TRIM(m_rate_of_sale_ind),
                      TRIM(m_network_distrb_cd),
                      TRIM(m_season_code),
                      TRIM(m_top_item_store_cd),
                      TRIM(m_corporate_dser),
                      m_deciles_rank_nbr,
                      TRIM(m_rate_of_sale_cd),
                      TRIM(m_import_cd),
                      TRIM(filler)
                 ;
			   
--store formatted data
STORE formatted_data 
INTO '$WORK__IDRP_INFOREM_STORE_MASTER_FORMATTED_WORK_LOCATION' 
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
