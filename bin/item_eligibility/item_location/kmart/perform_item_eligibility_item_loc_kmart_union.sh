
pig -x mapreduce -logfile /logs/hdidrp/pig -param_file /appl/hdidrp/pig/schema/item_eligibility/item_loc/kmart/smith__idrp_eligible_item_loc.param -m /appl/hdidrp/pig/params/item_eligibility/item_loc/kmart/union_kmart.param /appl/hdidrp/pig/scripts/item_eligibility/item_loc/kmart/perform_item_eligibility_item_loc_kmart_union.pig
