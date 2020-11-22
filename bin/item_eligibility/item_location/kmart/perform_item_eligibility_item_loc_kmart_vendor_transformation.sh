
pig -x mapreduce -logfile /logs/hdidrp/pig -param_file /appl/hdidrp/pig/params/item_eligibility/item_loc/kmart/kmart_vendor.param -m /appl/hdidrp/pig/schema/item_eligibility/item_loc/kmart/join_ie_item_ie_loc.param /appl/hdidrp/pig/scripts/item_eligibility/item_loc/kmart/perform_item_eligibility_item_loc_kmart_vendor_transformation.pig
