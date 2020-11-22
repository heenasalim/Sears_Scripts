/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform_item_eligibility_smith__idrp_space_planning_authorized_vendor_package_stores.pig
# AUTHOR NAME:         Onkar Malewadikar
# CREATION DATE:       Mon May 26 06:16:37 EDT 2014
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
#	19 oct 2016		Pankaj			IPS-942 performance.
#	19 feb 2019     Heena Shaikh    IPS-3325 - Optimize Hadoop job IE Master DP-00-IE-DETER_AUTHO_KMART_MSTR
#                                              inorder to achieve the performance and speed
#
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/
REGISTER $UDF_JAR;
DEFINE AddDays com.searshc.supplychain.idrp.udf.AddOrRemoveDaysToDate();
set default_parallel 300;
--set io.sort.mb 50
/*IPS-3325 :- Adding Compression Technique and adjusting the mappers*/
set io.compression.codec.lzo.class com.hadoop.compression.lzo.LzoCodec
set pig.tmpfilecompression true
set pig.tmpfilecompression.codec lzo
set mapred.child.java.opts -Xmx4096m
set mapred.compress.map.output true
set mapred.min.split.size 524288;
/*IPS-3325 :- Removed following part as it is not required
set pig.cachedbag.memusage 0.15
set io.sort.factor 10
set opt.multiquery false */
SET mapred.max.split.size 134217728
SET pig.maxCombinedSplitSize 4000000
SET mapreduce.map.java.opts: -Xmx3072m
SET mapreduce.reduce.java.opts: -Xmx6144m
/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/


/****************************** SPACE PLANNING LOGIC**************/


/******* Logic to get current date + 1 *******/

%declare date_plus_1 `date -d "+1 days" +%Y-%m-%d`
rev_dtc_file = LOAD '$SPACE_PLANNING_SG_GDTU100_DTCITEM_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS
                        (
                                REV_ITEM: chararray,
                                REV_STORE: chararray,
                                REV_DTC_NUM: chararray,
                                REV_DTCE_NUM: chararray,
                                REV_TOTPLANS: chararray,
                                REV_CUR_FACE: chararray,
                                REV_CUR_PRES: chararray,
                                REV_CUR_FILL: chararray,
                                REV_CUR_CAP: chararray,
                                REV_CHKOUT: chararray,
                                REV_REC_CRDTE: chararray,
                                REV_REC_LUDTE: chararray,
                                REV_PLNBUS: chararray,
                                REV_CUR_DISP_FACE: chararray
                        );


smith__idrp_vend_pack_combined_data =
    LOAD '$SMITH__IDRP_VEND_PACK_COMBINED_LOCATION'
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
    AS ($SMITH__IDRP_VEND_PACK_COMBINED_SCHEMA);

/*IPS-3325 :- Filtering the file after loading immidiately to improve the speed*/
filtered_vend_packs = FILTER smith__idrp_vend_pack_combined_data  by (TRIM(ksn_purchase_status_cd) != 'U') ;

/********* LOADING HIER_ITEM table *********/

smith__idrp_shc_item_combined_data =
    LOAD '$SMITH__IDRP_SHC_ITEM_COMBINED_LOCATION'
    USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
    AS ($SMITH__IDRP_SHC_ITEM_COMBINED_SCHEMA);


smith__idrp_ksn_attribute_current = LOAD '$SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_KSN_ATTRIBUTE_CURRENT_SCHEMA);

hier_item_data_required = FOREACH smith__idrp_shc_item_combined_data GENERATE
                        shc_item_id AS H_ITEM_ID,
                        shc_division_nbr AS H_DIV_NBR,
                        shc_category_nbr AS H_CATG_NBR,
                        shc_department_nbr AS H_DEPT_NBR ;

/*IPS-3325 :- Removed skewed join as data is not skewed and changed the sequence of the tables to get good performance benifit */
rev_dtc_file11 = JOIN hier_item_data_required BY H_ITEM_ID, rev_dtc_file BY REV_ITEM;

rev_dtc_file12 = FOREACH rev_dtc_file11 GENERATE
                rev_dtc_file::REV_ITEM AS REV_ITEM:chararray,
                                rev_dtc_file::REV_STORE AS REV_STORE:chararray,
                                rev_dtc_file::REV_DTC_NUM AS REV_DTC_NUM:chararray,
                                rev_dtc_file::REV_DTCE_NUM AS REV_DTCE_NUM:chararray,
                                rev_dtc_file::REV_TOTPLANS AS REV_TOTPLANS:chararray,
                                rev_dtc_file::REV_CUR_FACE AS REV_CUR_FACE:chararray,
                                rev_dtc_file::REV_CUR_PRES AS REV_CUR_PRES:chararray,
                                rev_dtc_file::REV_CUR_FILL AS REV_CUR_FILL:chararray,
                                rev_dtc_file::REV_CUR_CAP AS REV_CUR_CAP:chararray,
                                rev_dtc_file::REV_CHKOUT AS REV_CHKOUT:chararray,
                                hier_item_data_required::H_DIV_NBR AS REV_DEPT:chararray,
                                hier_item_data_required::H_CATG_NBR AS REV_CATG:chararray,
                                rev_dtc_file::REV_REC_CRDTE AS REV_REC_CRDTE:chararray,
                                rev_dtc_file::REV_REC_LUDTE AS REV_REC_LUDTE:chararray,
                                rev_dtc_file::REV_PLNBUS AS REV_PLNBUS:chararray,
                                rev_dtc_file::REV_CUR_DISP_FACE AS REV_CUR_DISP_FACE:chararray;

/*rev_dtc_file = DISTINCT rev_dtc_file12;*/


/******* Loading REPLITEM file *******/

replitems_rec_file = LOAD '$WORK__IDRP_ITEMS_VEND_PACKS_CAN_CARRY_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($WORK__IDRP_ITEMS_VEND_PACKS_CAN_CARRY_SCHEMA);

replitems_rec_file = 
    FOREACH replitems_rec_file 
	GENERATE
	    (chararray)shc_item_id AS ITEM;

replitems_rec_file = 
    DISTINCT replitems_rec_file ;


/******* Loading SBT_VEND_PACKS file ********/

SBT_VEND_PACKS = LOAD '$SPACE_PLANNING_SBT_VEND_PACKS_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS
            (
                VEND_PACK_ID:chararray,
                FLOWTYPE_CD:chararray,
                KSN_ID:chararray,
                ITEM_ID:chararray,
                DUNS_NBR:chararray
            );
/******* Loading SBT_ORD_PT file ********/

SBT_ORD_PT = LOAD '$SPACE_PLANNING_SBT_ORD_PT_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS
            (
                ORD_DUNS_NBR:chararray,
                LAUNCH_ID:chararray,
                VEND_ID:chararray,
                MULT_DISTRB_IND:chararray,
                DC_VEND_IND:chararray,
                EDI_852_IND:chararray,
                EDI_861_IND:chararray,
                ROLL_VEND:chararray,
                CLOSE_LOCN_EXCPN:chararray,
                VEND_STAT:chararray,
                D2S_VEND_IND:chararray
            );
/******* Loading SBT_ORD_PT file ********/

gold_geographic_model_str_data = LOAD '$GOLD__GEOGRAPHIC_MODEL_STORE_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($GOLD__GEOGRAPHIC_MODEL_STORE_SCHEMA);

MDL_STR = FOREACH gold_geographic_model_str_data GENERATE model_nbr AS MDL_NBR,location_nbr AS LOCN_NBR;


/******* Loading SBT_ORD_PT file ********/

smith__idrp_eligible_loc_data = LOAD '$SMITH__IDRP_ELIGIBLE_LOC_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS ($SMITH__IDRP_ELIGIBLE_LOC_SCHEMA);


MSTR_LOCN_filtered = FILTER smith__idrp_eligible_loc_data by (int)loc<=9999;
store_stat = FOREACH MSTR_LOCN_filtered GENERATE
                                                loc AS LOCN_NBR,
                                                TRIM(loc_ste_cd) AS M_L_ST_CD:chararray,
						loc_fmt_typ_cd;

op1_req_mstr = JOIN rev_dtc_file12 BY REV_STORE, store_stat BY LOCN_NBR using 'replicated';

op1_req_mstr1 = FOREACH op1_req_mstr GENERATE
                        rev_dtc_file12::REV_ITEM AS REV_ITEM:chararray,
                                        rev_dtc_file12::REV_STORE AS REV_STORE:chararray,
                                        rev_dtc_file12::REV_DTC_NUM AS REV_DTC_NUM:chararray,
                                        rev_dtc_file12::REV_DTCE_NUM AS REV_DTCE_NUM:chararray,
                                        rev_dtc_file12::REV_TOTPLANS AS REV_TOTPLANS:chararray,
                                        rev_dtc_file12::REV_CUR_FACE AS REV_CUR_FACE:chararray,
                                        rev_dtc_file12::REV_CUR_PRES AS REV_CUR_PRES:chararray,
                                        rev_dtc_file12::REV_CUR_FILL AS REV_CUR_FILL:chararray,
                                        rev_dtc_file12::REV_CUR_CAP AS REV_CUR_CAP:chararray,
                                        rev_dtc_file12::REV_CHKOUT AS REV_CHKOUT:chararray,
                                        rev_dtc_file12::REV_DEPT AS REV_DEPT:chararray,
                                        rev_dtc_file12::REV_CATG AS REV_CATG:chararray,
                                        rev_dtc_file12::REV_REC_CRDTE AS REV_REC_CRDTE:chararray,
                                        rev_dtc_file12::REV_REC_LUDTE AS REV_REC_LUDTE:chararray,
                                        rev_dtc_file12::REV_PLNBUS AS REV_PLNBUS:chararray,
                                        rev_dtc_file12::REV_CUR_DISP_FACE AS REV_CUR_DISP_FACE:chararray,
                                        store_stat::M_L_ST_CD AS REV_ST_CD:chararray,
					store_stat::loc_fmt_typ_cd AS loc_fmt_typ_cd;

/*rev_dtc_file = DISTINCT op1_req_mstr1;*/


/******* Loading SBT_ORD_PT file ********/

SBT_LAUNCH_STR = LOAD '$SPACE_PLANNING_SBT_LAUNCH_STR_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A') AS
            (
                LAUNCH_ID:chararray,
                LOCN_NBR:chararray,
                EFF_DT:chararray,
                TST_PROD_CD:chararray
            );

locn_filtered = FILTER SBT_LAUNCH_STR by EFF_DT <= 'date_plus_1';
/********* LOGIC 1 *********/

SPLIT op1_req_mstr1 INTO
        dept_match   IF (
                (REV_DEPT == '57')
               ),
        dept_unmatch IF (
                (REV_DEPT != '57')
               );

splited_rep_join = JOIN dept_unmatch by (REV_ITEM) , replitems_rec_file by (ITEM) using 'replicated';

to_do_union = FOREACH splited_rep_join GENERATE
                dept_unmatch::REV_ITEM AS REV_ITEM:chararray,
                dept_unmatch::REV_STORE AS REV_STORE:chararray,
                dept_unmatch::REV_DTC_NUM AS REV_DTC_NUM:chararray,
                dept_unmatch::REV_DTCE_NUM AS REV_DTCE_NUM:chararray,
                dept_unmatch::REV_TOTPLANS AS REV_TOTPLANS:chararray,
                dept_unmatch::REV_CUR_FACE AS REV_CUR_FACE:chararray,
                dept_unmatch::REV_CUR_PRES AS REV_CUR_PRES:chararray,
                dept_unmatch::REV_CUR_FILL AS REV_CUR_FILL:chararray,
                dept_unmatch::REV_CUR_CAP AS REV_CUR_CAP:chararray,
                dept_unmatch::REV_CHKOUT AS REV_CHKOUT:chararray,
                dept_unmatch::REV_DEPT AS REV_DEPT:chararray,
                dept_unmatch::REV_CATG AS REV_CATG:chararray,
                dept_unmatch::REV_REC_CRDTE AS REV_REC_CRDTE:chararray,
                dept_unmatch::REV_REC_LUDTE AS REV_REC_LUDTE:chararray,
                dept_unmatch::REV_PLNBUS AS REV_PLNBUS:chararray,
                dept_unmatch::REV_CUR_DISP_FACE AS REV_CUR_DISP_FACE:chararray,
                dept_unmatch::REV_ST_CD AS REV_ST_CD:chararray,
		dept_unmatch::loc_fmt_typ_cd AS loc_fmt_typ_cd;

union_data = UNION dept_match,to_do_union;

store_auth = FOREACH union_data GENERATE
                    REV_ITEM,
                    REV_STORE,
                    REV_REC_CRDTE,
                    REV_REC_LUDTE,
                    REV_DTC_NUM,
                    REV_DTCE_NUM,
                    REV_TOTPLANS,
                    REV_CUR_FACE,
                    REV_CUR_PRES,
                    REV_CUR_FILL,
                    REV_CUR_CAP,
                    REV_CHKOUT,
                    REV_DEPT,
                    REV_CATG,
                    REV_ST_CD,
		    loc_fmt_typ_cd;

/*store_auth = DISTINCT store_auth;*/


/********* LOGIC 2 *********/

/*join1 = DISTINCT smith__idrp_vend_pack_combined_data;*/

join2 = JOIN smith__idrp_vend_pack_combined_data BY shc_item_id, store_auth BY REV_ITEM;
join3 = JOIN join2 BY smith__idrp_vend_pack_combined_data::vendor_package_id LEFT, SBT_VEND_PACKS BY VEND_PACK_ID using 'replicated';

join4 = JOIN join3 BY SBT_VEND_PACKS::DUNS_NBR LEFT, SBT_ORD_PT BY ORD_DUNS_NBR using 'replicated';



input_req_col = FOREACH join4 GENERATE
                    join3::join2::smith__idrp_vend_pack_combined_data::shc_item_id AS ITEM_ID,
                                        join3::join2::store_auth::REV_STORE AS REV_STORE,
                                        join3::join2::smith__idrp_vend_pack_combined_data::ksn_id AS KSN_NBR,
                                        join3::join2::smith__idrp_vend_pack_combined_data::dotcom_allocation_ind AS KSN_DTCOM_ORDER_IND,
                                        join3::join2::smith__idrp_vend_pack_combined_data::ksn_purchase_status_cd AS KSN_PURCH_STAT_CD,
                                        join3::join2::smith__idrp_vend_pack_combined_data::vendor_package_id AS VEND_PACK_NBR,
                                        ((IsNull(join3::join2::smith__idrp_vend_pack_combined_data::service_area_restriction_model_id,'') == '')? '0' : join3::join2::smith__idrp_vend_pack_combined_data::service_area_restriction_model_id) AS SARM_NBR,
                                        ((SBT_ORD_PT::LAUNCH_ID is NULL)? '0' : SBT_ORD_PT::LAUNCH_ID) AS SBT_LAUNCH_ID,
                    			join3::join2::store_auth::REV_REC_CRDTE AS REV_REC_CRDTE,
                                        join3::join2::store_auth::REV_REC_LUDTE AS REV_REC_LUDTE,
                                        join3::join2::store_auth::REV_DTC_NUM AS REV_DTC_NUM,
                                        join3::join2::store_auth::REV_DTCE_NUM AS REV_DTCE_NUM,
                                        join3::join2::store_auth::REV_TOTPLANS AS REV_TOTPLANS,
                                        join3::join2::store_auth::REV_CUR_FACE AS REV_CUR_FACE,
                                        join3::join2::store_auth::REV_CUR_PRES AS REV_CUR_PRES,
                                        join3::join2::store_auth::REV_CUR_FILL AS REV_CUR_FILL,
                                        join3::join2::store_auth::REV_CUR_CAP AS REV_CUR_CAP,
                                        join3::join2::store_auth::REV_CHKOUT AS REV_CHKOUT,
                                        join3::join2::store_auth::REV_DEPT AS REV_DEPT,
                                        join3::join2::store_auth::REV_CATG AS REV_CATG,
                                        REV_ST_CD,
					loc_fmt_typ_cd,
					join3::join2::smith__idrp_vend_pack_combined_data::purchase_status_cd AS PURCH_STAT_CD;

/*input_req_col = DISTINCT input_req_col;*/


/********* LOGIC 3*********/

SPLIT input_req_col INTO
            input_req_fltr IF ((int)SBT_LAUNCH_ID > 0),
            input_req_un_fltr IF (SBT_LAUNCH_ID == '0');

input_req_gen = FOREACH input_req_fltr GENERATE
                        ITEM_ID,
                        REV_STORE,
                        KSN_NBR,
                        KSN_DTCOM_ORDER_IND,
                        KSN_PURCH_STAT_CD,
                        VEND_PACK_NBR,
                        SARM_NBR,
                        SBT_LAUNCH_ID,
                        REV_REC_CRDTE,
                        REV_REC_LUDTE,
                        REV_DTC_NUM,
                        REV_DTCE_NUM,
                        REV_TOTPLANS,
                        REV_CUR_FACE,
                        REV_CUR_PRES,
                        REV_CUR_FILL,
                        REV_CUR_CAP,
                        REV_CHKOUT,
                        REV_DEPT,
                        REV_CATG,
                        REV_ST_CD,
			loc_fmt_typ_cd,
			PURCH_STAT_CD;

/*input_req_gen = DISTINCT input_req_gen;*/


sbt_vend_join1 = JOIN filtered_vend_packs BY vendor_package_id, SBT_VEND_PACKS BY VEND_PACK_ID using 'replicated';

sbt_ord_join1 = JOIN sbt_vend_join1 BY SBT_VEND_PACKS::DUNS_NBR, SBT_ORD_PT BY ORD_DUNS_NBR using 'replicated';

/*IPS-3325 :- Modified the join to replicated as data is replicated */
input_join = JOIN input_req_gen BY ITEM_ID, sbt_ord_join1 BY sbt_vend_join1::filtered_vend_packs::shc_item_id using 'replicated';


input_req = FOREACH input_join GENERATE
				        sbt_ord_join1::SBT_ORD_PT::LAUNCH_ID AS SBT_LAUNCH_ID_1,
                    			input_req_gen::ITEM_ID AS ITEM_ID,
                    			input_req_gen::REV_STORE AS REV_STORE,
                    			input_req_gen::KSN_NBR AS KSN_NBR,
                    			input_req_gen::KSN_DTCOM_ORDER_IND AS KSN_DTCOM_ORDER_IND,
                    			input_req_gen::KSN_PURCH_STAT_CD AS KSN_PURCH_STAT_CD,
                    			input_req_gen::VEND_PACK_NBR AS VEND_PACK_NBR,
                    			input_req_gen::SARM_NBR AS SARM_NBR,
                    			input_req_gen::SBT_LAUNCH_ID AS SBT_LAUNCH_ID,
                    			input_req_gen::REV_REC_CRDTE AS REV_REC_CRDTE,
                    			input_req_gen::REV_REC_LUDTE AS REV_REC_LUDTE,
                    			input_req_gen::REV_DTC_NUM AS REV_DTC_NUM,
                    			input_req_gen::REV_DTCE_NUM AS REV_DTCE_NUM,
                                        input_req_gen::REV_TOTPLANS AS REV_TOTPLANS,
                                        input_req_gen::REV_CUR_FACE AS REV_CUR_FACE,
                                        input_req_gen::REV_CUR_PRES AS REV_CUR_PRES,
                                        input_req_gen::REV_CUR_FILL AS REV_CUR_FILL,
                                        input_req_gen::REV_CUR_CAP AS REV_CUR_CAP,
                                        input_req_gen::REV_CHKOUT AS REV_CHKOUT,
                                        input_req_gen::REV_DEPT AS REV_DEPT,
                                        input_req_gen::REV_CATG AS REV_CATG,
                                        input_req_gen::join3::join2::store_auth::REV_ST_CD AS REV_ST_CD,
					loc_fmt_typ_cd as loc_fmt_typ_cd;
					
input_sbt_join = JOIN input_req BY (SBT_LAUNCH_ID_1,REV_STORE), locn_filtered BY (LAUNCH_ID,LOCN_NBR) using 'replicated';

op1_req_tested = FOREACH input_sbt_join GENERATE
                    			input_req::ITEM_ID AS ITEM_ID,
                    			input_req::REV_STORE AS REV_STORE,
                    			input_req::KSN_NBR AS KSN_NBR,
                    			input_req::KSN_DTCOM_ORDER_IND AS KSN_DTCOM_ORDER_IND,
                    			input_req::KSN_PURCH_STAT_CD AS KSN_PURCH_STAT_CD,
                    			input_req::VEND_PACK_NBR AS VEND_PACK_NBR,
                    			input_req::SARM_NBR AS SARM_NBR,
                    			input_req::SBT_LAUNCH_ID AS SBT_LAUNCH_ID,
                    			input_req::REV_REC_CRDTE AS REV_REC_CRDTE,
                                        input_req::REV_REC_LUDTE AS REV_REC_LUDTE,
                                        input_req::REV_DTC_NUM AS REV_DTC_NUM,
                                        input_req::REV_DTCE_NUM AS REV_DTCE_NUM,
                                        input_req::REV_TOTPLANS AS REV_TOTPLANS,
                                        input_req::REV_CUR_FACE AS REV_CUR_FACE,
                                        input_req::REV_CUR_PRES AS REV_CUR_PRES,
                                        input_req::REV_CUR_FILL AS REV_CUR_FILL,
                                        input_req::REV_CUR_CAP AS REV_CUR_CAP,
                                        input_req::REV_CHKOUT AS REV_CHKOUT,
                                        input_req::REV_DEPT AS REV_DEPT,
                                        input_req::REV_CATG AS REV_CATG,
                                        input_req::REV_ST_CD AS REV_ST_CD,
					input_req::loc_fmt_typ_cd as loc_fmt_typ_cd;

op1_req_new_765 = DISTINCT op1_req_tested;


input_req_un_fltr = FOREACH input_req_un_fltr GENERATE
			                         ITEM_ID,
                                                REV_STORE,
                                                KSN_NBR,
                                                KSN_DTCOM_ORDER_IND,
                                                KSN_PURCH_STAT_CD,
                                                VEND_PACK_NBR,
                                                SARM_NBR,
                                                SBT_LAUNCH_ID,
                                                REV_REC_CRDTE,
                                                REV_REC_LUDTE,
                                                REV_DTC_NUM,
                                                REV_DTCE_NUM,
                                                REV_TOTPLANS,
                                                REV_CUR_FACE,
                                                REV_CUR_PRES,
                                                REV_CUR_FILL,
                                                REV_CUR_CAP,
                                                REV_CHKOUT,
                                                REV_DEPT,
                                                REV_CATG,
                        	                REV_ST_CD,
						loc_fmt_typ_cd;

op1_req_prev = UNION op1_req_new_765, input_req_un_fltr;

/********* LOGIC 4*********/


SPLIT op1_req_prev INTO
            grp_11 IF ((int)SBT_LAUNCH_ID > 0),
            grp_22 IF (SBT_LAUNCH_ID == '0');

op1_req_new_123 = JOIN grp_11 BY (ITEM_ID,REV_STORE) FULL OUTER, grp_22 BY (ITEM_ID,REV_STORE);

SPLIT op1_req_new_123 INTO
        t_data_2 IF ((grp_11::ITEM_ID is null and grp_11::REV_STORE is null) and (grp_22::ITEM_ID is not null and grp_22::REV_STORE is not null)),
        t_data_1 IF ((grp_11::ITEM_ID is not null and grp_11::REV_STORE is not null) and (grp_22::ITEM_ID is null and grp_22::REV_STORE is  null)),
        t_data_3 IF ((grp_11::ITEM_ID == grp_22::ITEM_ID and grp_11::REV_STORE == grp_22::REV_STORE) and (grp_11::SBT_LAUNCH_ID != grp_22::SBT_LAUNCH_ID));

t_data_1 = FOREACH t_data_1 GENERATE
			                        grp_11::ITEM_ID AS ITEM_ID,
                                                grp_11::REV_STORE AS REV_STORE,
                                                grp_11::KSN_NBR AS KSN_NBR,
                                                grp_11::KSN_DTCOM_ORDER_IND AS KSN_DTCOM_ORDER_IND,
                                                grp_11::KSN_PURCH_STAT_CD AS KSN_PURCH_STAT_CD,
                                                grp_11::VEND_PACK_NBR AS VEND_PACK_NBR,
                                                grp_11::SARM_NBR AS SARM_NBR,
                                                grp_11::SBT_LAUNCH_ID AS SBT_LAUNCH_ID,
                                                grp_11::REV_REC_CRDTE AS REV_REC_CRDTE,
                                                grp_11::REV_REC_LUDTE AS REV_REC_LUDTE,
                                                grp_11::REV_DTC_NUM AS REV_DTC_NUM,
                                                grp_11::REV_DTCE_NUM AS REV_DTCE_NUM,
                                                grp_11::REV_TOTPLANS AS REV_TOTPLANS,
                                                grp_11::REV_CUR_FACE AS REV_CUR_FACE,
                                                grp_11::REV_CUR_PRES AS REV_CUR_PRES,
                                                grp_11::REV_CUR_FILL AS REV_CUR_FILL,
                                                grp_11::REV_CUR_CAP AS REV_CUR_CAP,
                                                grp_11::REV_CHKOUT AS REV_CHKOUT,
                                                grp_11::REV_DEPT AS REV_DEPT,
                                                grp_11::REV_CATG AS REV_CATG,
                                                grp_11::REV_ST_CD AS REV_ST_CD,
						grp_11::loc_fmt_typ_cd as loc_fmt_typ_cd;
t_data_2 = FOREACH t_data_2 GENERATE
                                                grp_22::ITEM_ID AS ITEM_ID,
                                                grp_22::REV_STORE AS REV_STORE,
                                                grp_22::KSN_NBR AS KSN_NBR,
                                                grp_22::KSN_DTCOM_ORDER_IND AS KSN_DTCOM_ORDER_IND,
                                                grp_22::KSN_PURCH_STAT_CD AS KSN_PURCH_STAT_CD,
                                                grp_22::VEND_PACK_NBR AS VEND_PACK_NBR,
                                                grp_22::SARM_NBR AS SARM_NBR,
                                                grp_22::SBT_LAUNCH_ID AS SBT_LAUNCH_ID,
                                                grp_22::REV_REC_CRDTE AS REV_REC_CRDTE,
                                                grp_22::REV_REC_LUDTE AS REV_REC_LUDTE,
                                                grp_22::REV_DTC_NUM AS REV_DTC_NUM,
                                                grp_22::REV_DTCE_NUM AS REV_DTCE_NUM,
                                                grp_22::REV_TOTPLANS AS REV_TOTPLANS,
                                                grp_22::REV_CUR_FACE AS REV_CUR_FACE,
                                                grp_22::REV_CUR_PRES AS REV_CUR_PRES,
                                                grp_22::REV_CUR_FILL AS REV_CUR_FILL,
                                                grp_22::REV_CUR_CAP AS REV_CUR_CAP,
                                                grp_22::REV_CHKOUT AS REV_CHKOUT,
                                                grp_22::REV_DEPT AS REV_DEPT,
                                                grp_22::REV_CATG AS REV_CATG,
                                                grp_22::REV_ST_CD AS REV_ST_CD,
						 grp_22::loc_fmt_typ_cd as loc_fmt_typ_cd;
t_data_3 = FOREACH t_data_3 GENERATE
                        			grp_11::ITEM_ID AS ITEM_ID,
                                                grp_11::REV_STORE AS REV_STORE,
                                                grp_11::KSN_NBR AS KSN_NBR,
                                                grp_11::KSN_DTCOM_ORDER_IND AS KSN_DTCOM_ORDER_IND,
                                                grp_11::KSN_PURCH_STAT_CD AS KSN_PURCH_STAT_CD,
                                                grp_11::VEND_PACK_NBR AS VEND_PACK_NBR,
                                                grp_11::SARM_NBR AS SARM_NBR,
                                                grp_11::SBT_LAUNCH_ID AS SBT_LAUNCH_ID,
                                                grp_11::REV_REC_CRDTE AS REV_REC_CRDTE,
                                                grp_11::REV_REC_LUDTE AS REV_REC_LUDTE,
                                                grp_11::REV_DTC_NUM AS REV_DTC_NUM,
                                                grp_11::REV_DTCE_NUM AS REV_DTCE_NUM,
                                                grp_11::REV_TOTPLANS AS REV_TOTPLANS,
                                                grp_11::REV_CUR_FACE AS REV_CUR_FACE,
                                                grp_11::REV_CUR_PRES AS REV_CUR_PRES,
                                                grp_11::REV_CUR_FILL AS REV_CUR_FILL,
                                                grp_11::REV_CUR_CAP AS REV_CUR_CAP,
                                                grp_11::REV_CHKOUT AS REV_CHKOUT,
                                                grp_11::REV_DEPT AS REV_DEPT,
                                                grp_11::REV_CATG AS REV_CATG,
                                                grp_11::REV_ST_CD AS REV_ST_CD,
						grp_11::loc_fmt_typ_cd as loc_fmt_typ_cd;

/*IPS-3325 Applied the reducers to Union in order to acheive the speed */
test_data_1234 = UNION t_data_1, t_data_2, t_data_3 PARALLEL 500;

op1_req_new_here = FOREACH test_data_1234 GENERATE
			                        ITEM_ID,
                                                REV_STORE,
                                                KSN_NBR,
                                                KSN_DTCOM_ORDER_IND,
                                                KSN_PURCH_STAT_CD,
                                                VEND_PACK_NBR,
                                                SARM_NBR,
                                                SBT_LAUNCH_ID,
                                                REV_REC_CRDTE,
                                                REV_REC_LUDTE,
                                                REV_DTC_NUM,
                                                REV_DTCE_NUM,
                                                REV_TOTPLANS,
                                                REV_CUR_FACE,
                                                REV_CUR_PRES,
                                                REV_CUR_FILL,
                                                REV_CUR_CAP,
                                                REV_CHKOUT,
                                                REV_DEPT,
                                                REV_CATG,
                                                REV_ST_CD,
						loc_fmt_typ_cd;

op1_req_new_here = DISTINCT op1_req_new_here;

/********* LOGIC 5*********/

SPLIT op1_req_new_here INTO
                        sarm_gtzero IF ((int)SARM_NBR > 0),
                        sarm_eqzero IF (SARM_NBR == '0');

store_num_join = JOIN sarm_gtzero by (SARM_NBR,REV_STORE) , MDL_STR by (MDL_NBR,LOCN_NBR);

mdl_sarm_found_1 = FOREACH store_num_join GENERATE
                                        sarm_gtzero::ITEM_ID AS ITEM_ID_1,
                                        sarm_gtzero::REV_STORE AS REV_STORE_1,
                                        sarm_gtzero::KSN_NBR AS KSN_NBR_1,
                                        sarm_gtzero::KSN_DTCOM_ORDER_IND AS KSN_DTCOM_ORDER_IND_1,
                                        sarm_gtzero::KSN_PURCH_STAT_CD AS KSN_PURCH_STAT_CD_1,
                                        sarm_gtzero::VEND_PACK_NBR AS VEND_PACK_NBR_1,
                                        sarm_gtzero::SARM_NBR AS SARM_NBR_1,
                                        sarm_gtzero::SBT_LAUNCH_ID AS SBT_LAUNCH_ID_1,
                                        sarm_gtzero::REV_REC_CRDTE  AS REV_REC_CRDTE_1,
                                        sarm_gtzero::REV_REC_LUDTE AS REV_REC_LUDTE_1,
                                        sarm_gtzero::REV_DTC_NUM AS REV_DTC_NUM_1,
                                        sarm_gtzero::REV_DTCE_NUM AS REV_DTCE_NUM_1,
                                        sarm_gtzero::REV_TOTPLANS AS REV_TOTPLANS_1,
                                        sarm_gtzero::REV_CUR_FACE AS REV_CUR_FACE_1,
                                        sarm_gtzero::REV_CUR_PRES AS REV_CUR_PRES_1,
                                        sarm_gtzero::REV_CUR_FILL AS REV_CUR_FILL_1,
                                        sarm_gtzero::REV_CUR_CAP AS REV_CUR_CAP_1,
                                        sarm_gtzero::REV_CHKOUT AS REV_CHKOUT_1,
                                        sarm_gtzero::REV_DEPT AS REV_DEPT_1,
                                        sarm_gtzero::REV_CATG AS REV_CATG_1,
                                        sarm_gtzero::REV_ST_CD AS REV_ST_CD_1,
					sarm_gtzero::loc_fmt_typ_cd as loc_fmt_typ_cd;

sarm_eqzero_gen1 = FOREACH sarm_eqzero GENERATE
                                        ITEM_ID AS ITEM_ID_1,
                                        REV_STORE AS REV_STORE_1,
                                        KSN_NBR AS KSN_NBR_1,
                                        KSN_DTCOM_ORDER_IND AS KSN_DTCOM_ORDER_IND_1,
                                        KSN_PURCH_STAT_CD AS KSN_PURCH_STAT_CD_1,
                                        VEND_PACK_NBR AS VEND_PACK_NBR_1,
                                        SARM_NBR AS SARM_NBR_1,
                                        SBT_LAUNCH_ID AS SBT_LAUNCH_ID_1,
                                        REV_REC_CRDTE AS REV_REC_CRDTE_1,
                                        REV_REC_LUDTE AS REV_REC_LUDTE_1,
                                        REV_DTC_NUM AS REV_DTC_NUM_1,
                                        REV_DTCE_NUM AS REV_DTCE_NUM_1,
                                        REV_TOTPLANS AS REV_TOTPLANS_1,
                                        REV_CUR_FACE AS REV_CUR_FACE_1,
                                        REV_CUR_PRES AS REV_CUR_PRES_1,
                                        REV_CUR_FILL AS REV_CUR_FILL_1,
                                        REV_CUR_CAP AS REV_CUR_CAP_1,
                                        REV_CHKOUT AS REV_CHKOUT_1,
                                        REV_DEPT AS REV_DEPT_1,
			                REV_CATG AS REV_CATG_1,
                                        REV_ST_CD AS REV_ST_CD_1,
					loc_fmt_typ_cd as loc_fmt_typ_cd;

mdl_op1 = UNION mdl_sarm_found_1,sarm_eqzero_gen1;

/*mdl_op1 = DISTINCT mdl_op1;*/



/********* LOGIC 6 *********/
/*
SPLIT mdl_op1 INTO
                        fltr_needed IF (REV_STORE_1 != 7840),
                        fltr_nt_needed IF (REV_STORE_1 == 7840 AND 
					((KSN_DTCOM_ORDER_IND_1 == 'K') OR (KSN_DTCOM_ORDER_IND_1 == 'B')));
*/

mdl_op1_kns_attr_join = JOIN mdl_op1 BY KSN_NBR_1 full outer, smith__idrp_ksn_attribute_current by ksn_id;


SPLIT mdl_op1_kns_attr_join INTO
  fltr_needed IF ( loc_fmt_typ_cd != 'KINT' AND loc_fmt_typ_cd != 'SINT'),
  fltr_nt_needed_kmart IF ((loc_fmt_typ_cd == 'KINT') AND  ((smith__idrp_ksn_attribute_current::warehouse_sizing_attribute_cd == 'WG8801') OR (smith__idrp_ksn_attribute_current::warehouse_sizing_attribute_cd == 'WG8809'))),
  fltr_nt_needed_sears IF  ((loc_fmt_typ_cd == 'SINT') AND  (smith__idrp_ksn_attribute_current::warehouse_sizing_attribute_cd == 'WG8801'));

/*IPS-3325 :- Applied the reducers to union in order to acheive the speed*/
store_sarm_nbr_union_final = UNION fltr_needed,fltr_nt_needed_kmart,fltr_nt_needed_sears PARALLEL 1000;

/*store_sarm_nbr_union_distinct = DISTINCT store_sarm_nbr_union_final;*/

/* 
----IPS-942 removing unnecessary generate
store_prev = FOREACH store_sarm_nbr_union_final GENERATE
                                        ITEM_ID_1,
                                        REV_STORE_1,
                                        KSN_NBR_1,
                                        KSN_DTCOM_ORDER_IND_1,
                                        KSN_PURCH_STAT_CD_1,
                                        VEND_PACK_NBR_1,
                                        SARM_NBR_1,
                                        SBT_LAUNCH_ID_1,
                                        REV_REC_CRDTE_1,
                                        REV_REC_LUDTE_1,
                                        REV_DTC_NUM_1,
                                        REV_DTCE_NUM_1,
                                        REV_TOTPLANS_1,
                                        REV_CUR_FACE_1,
                                        REV_CUR_PRES_1,
                                        REV_CUR_FILL_1,
                                        REV_CUR_CAP_1,
                                        REV_CHKOUT_1,
                                        REV_DEPT_1,
                                        REV_CATG_1,
                                        REV_ST_CD_1;
*/
										

store_sarm_nbr_union = FOREACH store_sarm_nbr_union_final GENERATE
                                                        '$CURRENT_TIMESTAMP'	AS	load_ts	,
														VEND_PACK_NBR_1 AS VEND_PACK_NBR,
                                                        REV_STORE_1 AS REV_STORE,
                                                        REV_DTC_NUM_1 AS REV_DTC_NUM,
                                                        REV_DTCE_NUM_1 AS REV_DTCE_NUM,
                                                        REV_TOTPLANS_1 AS REV_TOTPLANS,
                                                        REV_CUR_FACE_1 AS REV_CUR_FACE,
                                                        REV_CUR_PRES_1 AS REV_CUR_PRES,
                                                        REV_CUR_FILL_1 AS REV_CUR_FILL,
                                                        REV_CUR_CAP_1 AS REV_CUR_CAP,
                                                        REV_CHKOUT_1 AS REV_CHKOUT,
                                                        REV_DEPT_1 AS REV_DEPT,
                                                        REV_CATG_1 AS REV_CATG,
                                                        REV_REC_CRDTE_1 AS REV_REC_CRDTE,
                                                        REV_REC_LUDTE_1 AS REV_REC_LUDTE,
                                                        '0' AS REV_DTC_VP_PLANBUS_1,
                                                        '0' AS REV_DTC_VP_KCODE_1,
                                                        ITEM_ID_1 AS ITEM_ID,
                                                        ' ' AS KSN_STAT_1:chararray;
STORE store_sarm_nbr_union INTO '$SMITH__IDRP_SPACE_PLANNING_AUTHORIZED_VENDOR_PACKAGE_STORES_LOCATION' USING PigStorage('$FIELD_DELIMITER_CONTROL_A');





/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/

