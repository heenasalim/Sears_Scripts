hadoop fs -mkdir /smith/idrp/eligible_item

PKG_NAME=com.searshc.supplychain.idrp.udf

pig -Dudf.import.list=$PKG_NAME -x mapreduce -logfile /logs/hdidrp/pig -param_file /appl/hdidrp/pig/params/item_eligibility/item/perform_item_eligibility_item_schema1.param -m /appl/hdidrp/pig/schema/item_eligibility/item/GEN_LAST_JOIN.param /appl/hdidrp/pig/scripts/item_eligibility/item/perform_item_eligibility_item_transformations2.pig

