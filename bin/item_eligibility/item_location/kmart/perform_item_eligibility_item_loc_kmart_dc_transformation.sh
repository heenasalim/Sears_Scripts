pig -x mapreduce -logfile /logs/hdidrp/pig -param_file /appl/hdidrp/pig/params/item_eligibility/item_loc/kmart/kmart_dc.param -m /appl/hdidrp/pig/schema/item_eligibility/item_loc/kmart/join_loc_gen_join_item_dc_locn_ksn_pk_aprk_eql.param /appl/hdidrp/pig/scripts/item_eligibility/item_loc/kmart/perform_item_eligibility_item_loc_kmart_dc_transformation.pig

