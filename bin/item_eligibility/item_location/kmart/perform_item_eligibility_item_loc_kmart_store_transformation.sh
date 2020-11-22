
pig -x mapreduce -logfile /logs/hdidrp/pig -param_file /appl/hdidrp/pig/params/item_eligibility/item_loc/kmart/kmart_store.param -m /appl/hdidrp/pig/schema/item_eligibility/item_loc/kmart/smith__idrp_eligible_item_loc_final_tbl_gen.param /appl/hdidrp/pig/scripts/item_eligibility/item_loc/kmart/perform_item_eligibility_item_loc_kmart_stores_transformation.pig
