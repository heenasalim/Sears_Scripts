hadoop fs -mkdir /smith/idrp/eligible_item_loc

pig -logfile /logs/hdidrp/pig -m "/appl/hdidrp/pig/schema/smith__idrp_eligible_item_loc.param" -m "/appl/hdidrp/pig/params/item_eligibility/item_loc/perform_item_eligibility_item_loc_comparision.param" "/appl/hdidrp/pig/scripts/item_eligibility/item_loc/perform_item_eligibility_item_loc_comparision.pig"

