
pig -x mapreduce -logfile /logs/hdidrp/pig -param_file /appl/hdidrp/pig/params/item_eligibility/item_loc/union_kmart_sears.param -m /appl/hdidrp/pig/schema/item_eligibility/item_loc/smith__idrp_eligible_item_loc.param /appl/hdidrp/pig/scripts/item_eligibility/item_loc/perform_item_eligibility_kmart_and_sears_output_union.pig
