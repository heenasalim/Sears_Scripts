hadoop fs -mkdir /smith/idrp/eligible_item

pig -logfile /logs/hdidrp/pig -m "/appl/hdidrp/pig/schema/item_eligibility/item/FINAL_JOIN.param" -m "/appl/hdidrp/pig/params/item_eligibility/item/perform_item_eligibility_item_comparision.param" "/appl/hdidrp/pig/scripts/item_eligibility/item/perform_item_eligibility_item_comparision.pig"

