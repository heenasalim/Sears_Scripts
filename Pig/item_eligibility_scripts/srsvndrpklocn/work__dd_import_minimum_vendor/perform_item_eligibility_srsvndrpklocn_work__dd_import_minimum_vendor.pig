/*
###############################################################################
#<>                                   HEADER                                <>#
###############################################################################
# SCRIPT NAME:         perform__item_eligibility_srsvndrpklocn_work__dd_import_minimum_vendor.pig
# AUTHOR NAME:         Neera Singh
# CREATION DATE:       Fri Jul 11 09:37:58 EST 2014
# CURRENT REVISION NO: 1
#
# DESCRIPTION: <<TODO>>
#
#
# DEPENDENCIES: <<TODO>>
#
#
# REV LIST:
#        DATE   BY            		MODIFICATION
#  10/10/2014   Arjun Dabhade    	Added TrimLeadingZeros as per Spira#3138
#  27/10/2014	Priyanka Gurjar	  	change the code to add the spilt for location_format_type_cd=='CDFC' and removed the naming convention'TW' per the SPIRA #3208
#  08/12/2014	Siddhivinayak Karpe	Code change for CDFC_warehouse records. Columns srim_source_location_id as candidate_source_location_id AND srim_source_nbr as candidate_vendor_nbr generated. CR#3208
#  17/12/2014	Siddhivinayak Karpe	CR#3457 Type Casting Column candidate_vendor_package_qty to Integer
#  12/01/2015   Sushauvik Deb   	CR#3517 modify minimum vendor and warehouse sourcing to use item type to determine whether to use DOS or SRIM source.
#  07/01/2015   Khim           		CR#4743 Import Minimum vendor should include all DC types not just RRC, DDC and CFDC. in project "IDRP Accelerated" has been changed
#  01/30/2019   Piyush Solanki		IPS-3749: Update po_vnd_no for Rapid items for MDO locations
#
###############################################################################
#<<                                DECLARE                                  >>#
###############################################################################
*/

SET default_parallel 99;
SET job.priority 'very_high';

--register the jar containing all PIG UDFs
REGISTER $UDF_JAR;

--trim spaces around string
DEFINE TRIM_STRING $TRIM_STRING ;

--trim leading zeros
DEFINE TRIM_INTEGER $TRIM_INTEGER ;

--trim leading and trailing zeros
DEFINE TRIM_DECIMAL $TRIM_DECIMAL ;


DEFINE TrimLeadingZeros com.searshc.supplychain.idrp.udf.TrimLeadingZeros();

/*
###############################################################################
#<>                                  BODY                                   <>#
###############################################################################
*/

/*********Loading work__idrp_candidate_sears_warehouse************************************************************************/

work__idrp_candidate_sears_warehouse = 
        LOAD '$WORK__IDRP_CANDIDATE_SEARS_WAREHOUSE_LOCATION'
        USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
        $WORK__IDRP_CANDIDATE_SEARS_WAREHOUSE_SCHEMA
       );

gen_work__idrp_candidate_sears_warehouse = 
	FOREACH
	work__idrp_candidate_sears_warehouse
	GENERATE
		sears_division_nbr,
		sears_item_nbr,
		sears_sku_nbr,
		idrp_item_type_desc,
		dos_source_location_level_cd,
		location_format_type_cd,
		srim_source_location_level_cd,
		(int)srim_source_package_qty AS srim_source_package_qty,
		dos_source_location_id,
		(int)dos_source_nbr AS dos_source_nbr,					--IPS-3749: convert to (int)
        (int)dos_source_package_qty AS dos_source_package_qty,	--IPS-3749: convert to (int)
		(int)srim_source_nbr as srim_source_nbr,				--IPS-3749: convert to (int)
		srim_source_location_id as srim_source_location_id,
        active_ind AS active_ind;


/*********Loading work__idrp_candidate_sears_store**************************************************************************/

work__idrp_candidate_sears_store = 
	LOAD '$WORK__IDRP_CANDIDATE_SEARS_STORE_LOCATION'
        USING PigStorage('$FIELD_DELIMITER_CONTROL_A')
        AS (
        $WORK__IDRP_CANDIDATE_SEARS_STORE_SCHEMA
       );

gen_work__idrp_candidate_sears_store = 
	FOREACH
	work__idrp_candidate_sears_store
	GENERATE
	sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        idrp_item_type_desc,
        source_location_level_cd,
        location_format_type_cd,
        (int)source_package_qty AS candidate_vendor_package_qty,
        source_location_id as candidate_source_location_id,
        rim_source_nbr as candidate_vendor_nbr,
        active_ind AS active_ind;




/******split work__idrp_candidate_sears_warehouse****************************************************************************/

SPLIT gen_work__idrp_candidate_sears_warehouse 
INTO rec_RRC_warehouse 
IF((TRIM(idrp_item_type_desc)!='IMPORT' AND (TRIM(idrp_item_type_desc)!='RAPID'))
   AND (srim_source_location_level_cd IS NOT NULL AND TRIM(srim_source_location_level_cd)=='VENDOR' 
   AND (TRIM(location_format_type_cd)=='RRC') AND active_ind =='Y')),
rec_RAPID_rrc_warehouse
IF((TRIM(idrp_item_type_desc)=='RAPID')
   AND (dos_source_location_level_cd IS NOT NULL AND TRIM(dos_source_location_level_cd)=='VENDOR')
   AND (TRIM(location_format_type_cd)=='RRC')),                           --IPS-3749: removed condition for RRC: AND active_ind=='Y'
rec_DDC_MDO_warehouse 
IF(TRIM(idrp_item_type_desc)!='IMPORT' AND TRIM(idrp_item_type_desc)!='RAPID'
   AND (srim_source_location_level_cd IS NOT NULL AND TRIM(srim_source_location_level_cd)=='VENDOR'
   AND (TRIM(location_format_type_cd)=='DDC' AND active_ind =='Y'))),
rec_RAPID_ddc_warehouse
IF((TRIM(idrp_item_type_desc)=='RAPID')
  AND (dos_source_location_level_cd IS NOT NULL AND TRIM(dos_source_location_level_cd)=='VENDOR')
  AND (TRIM(location_format_type_cd)=='DDC')),                            --IPS-3749: removed condition for DDC: AND active_ind=='Y'
import_rec_RRC_warehouse
IF(TRIM(idrp_item_type_desc)=='IMPORT'
   AND (TRIM(dos_source_location_level_cd)=='VENDOR'
   AND (TRIM(location_format_type_cd)=='RRC') AND active_ind =='Y')),
import_rec_DDC_MDO_warehouse
IF(TRIM(idrp_item_type_desc)=='IMPORT'
   AND (TRIM(dos_source_location_level_cd)=='VENDOR'
   AND (TRIM(location_format_type_cd)=='DDC') AND active_ind =='Y')),
import_rec_CDFC_warehouse
IF(TRIM(idrp_item_type_desc)=='IMPORT'
   AND (TRIM(srim_source_location_level_cd)=='VENDOR'
   AND (TRIM(location_format_type_cd)=='CDFC') AND active_ind =='Y')),
import_rec_Non_CDFC_warehouse
IF(TRIM(idrp_item_type_desc)=='IMPORT'
   AND (srim_source_location_level_cd IS NOT NULL AND TRIM(srim_source_location_level_cd)=='VENDOR')
   AND (TRIM(location_format_type_cd)!='CDFC' AND  TRIM(location_format_type_cd)!='RRC' AND TRIM(location_format_type_cd)!='DDC') AND active_ind =='Y');


/******split work__idrp_candidate_sears_store******************************************************************************/

SPLIT gen_work__idrp_candidate_sears_store
INTO store_split1
IF(TRIM(idrp_item_type_desc)=='IMPORT'
   AND (TRIM(source_location_level_cd)=='VENDOR') AND active_ind =='Y'),
store_split_others
IF(NOT(TRIM(idrp_item_type_desc)=='IMPORT'
   AND (TRIM(source_location_level_cd)=='VENDOR')));
   
gen_store_split1 = foreach store_split1 generate
                   sears_division_nbr,
                   sears_item_nbr,
                   sears_sku_nbr,
		   candidate_vendor_package_qty,
		   candidate_source_location_id,
		   candidate_vendor_nbr;
		


/*********Union store_split1 and rec_RRC_warehouse***********************************************************************/

gen_rec_RRC_warehouse = 
	FOREACH
	rec_RRC_warehouse
	GENERATE
	sears_division_nbr,
    sears_item_nbr,
    sears_sku_nbr,
	srim_source_package_qty AS candidate_vendor_package_qty,
	srim_source_location_id AS candidate_source_location_id,
	srim_source_nbr AS candidate_vendor_nbr;

gen_rec_RAPID_rrc_warehouse =
        FOREACH
        rec_RAPID_rrc_warehouse
        GENERATE
        sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        dos_source_package_qty AS candidate_vendor_package_qty,
        dos_source_location_id AS candidate_source_location_id,
        dos_source_nbr AS candidate_vendor_nbr,
		active_ind AS active_ind;        				--IPS-3749: added column active_ind for RRC

gen_rec_DDC_MDO_warehouse = 
	FOREACH
	rec_DDC_MDO_warehouse
	GENERATE
	sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        srim_source_package_qty AS candidate_vendor_package_qty,
        srim_source_location_id AS candidate_source_location_id,
        srim_source_nbr AS candidate_vendor_nbr;

gen_rec_RAPID_ddc_warehouse =
        FOREACH
        rec_RAPID_ddc_warehouse
        GENERATE
        sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        dos_source_package_qty AS candidate_vendor_package_qty,
        dos_source_location_id AS candidate_source_location_id,
        dos_source_nbr AS candidate_vendor_nbr,
		active_ind AS active_ind;	        			--IPS-3749: added column active_ind for DDC

grp_gen_rec_RRC_warehouse = 
	GROUP
	gen_rec_RRC_warehouse
	BY (sears_division_nbr, sears_item_nbr, sears_sku_nbr);

unq_grp_gen_rec_RRC_warehouse = 
	FOREACH
	grp_gen_rec_RRC_warehouse{
		sorted = ORDER gen_rec_RRC_warehouse BY candidate_vendor_nbr,candidate_vendor_package_qty;
		unq = LIMIT sorted 1;
		GENERATE FLATTEN(unq);
				};

work__rrc_minimum_vendor = 
	FOREACH
	unq_grp_gen_rec_RRC_warehouse
	GENERATE
	unq::sears_division_nbr AS sears_division_nbr,
	unq::sears_item_nbr AS sears_item_nbr,
	unq::sears_sku_nbr AS sears_sku_nbr,
	unq::candidate_vendor_nbr AS min_vendor_nbr,
	unq::candidate_vendor_package_qty AS min_vendor_package_qty,
	unq::candidate_source_location_id AS min_vendor_location_id;


grp_rec_RAPID_rrc_warehouse =
        GROUP
        gen_rec_RAPID_rrc_warehouse
        BY(sears_division_nbr, sears_item_nbr, sears_sku_nbr);

unq_grp_rec_RAPID_rrc_warehouse =
        FOREACH
        grp_rec_RAPID_rrc_warehouse{
                sorted = ORDER gen_rec_RAPID_rrc_warehouse BY active_ind, candidate_vendor_nbr;     --IPS-3749: added column active_ind for RRC
                unq = LIMIT sorted 1;
                GENERATE FLATTEN(unq);
                                   };


work__rec_RAPID_rrc_warehouse =
         FOREACH
         unq_grp_rec_RAPID_rrc_warehouse
         GENERATE
         unq::sears_division_nbr AS sears_division_nbr,
         unq::sears_item_nbr AS sears_item_nbr,
         unq::sears_sku_nbr AS sears_sku_nbr,
         unq::candidate_vendor_nbr AS min_vendor_nbr,
         (int)unq::candidate_vendor_package_qty AS min_vendor_package_qty,
         unq::candidate_source_location_id AS min_vendor_location_id;



union_rrc_data = UNION
                 work__rec_RAPID_rrc_warehouse,
                 work__rrc_minimum_vendor;

STORE union_rrc_data
INTO '$WORK__RRC_MINIMUM_VENDOR_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

grp_gen_rec_DDC_MDO_warehouse = 
        GROUP
        gen_rec_DDC_MDO_warehouse
        BY (sears_division_nbr, sears_item_nbr, sears_sku_nbr);

unq_grp_gen_rec_DDC_MDO_warehouse = 
        FOREACH
        grp_gen_rec_DDC_MDO_warehouse{
                sorted = ORDER gen_rec_DDC_MDO_warehouse BY candidate_vendor_nbr,candidate_vendor_package_qty;
                unq = LIMIT sorted 1;
                GENERATE FLATTEN(unq);
                                };

work__ddc_minimum_vendor = 
        FOREACH
        unq_grp_gen_rec_DDC_MDO_warehouse
        GENERATE
        unq::sears_division_nbr AS sears_division_nbr,
        unq::sears_item_nbr AS sears_item_nbr,
        unq::sears_sku_nbr AS sears_sku_nbr,
        unq::candidate_vendor_nbr AS min_vendor_nbr,
        unq::candidate_vendor_package_qty AS min_vendor_package_qty,
        unq::candidate_source_location_id AS min_vendor_location_id;


grp_rec_RAPID_ddc_warehouse =
        GROUP
        gen_rec_RAPID_ddc_warehouse
        BY (sears_division_nbr, sears_item_nbr, sears_sku_nbr);

unq_grp_rec_RAPID_ddc_warehouse =
        FOREACH
        grp_rec_RAPID_ddc_warehouse{
                sorted = ORDER gen_rec_RAPID_ddc_warehouse BY active_ind, candidate_vendor_nbr;     --IPS-3749: added column active_ind for DDC
                unq = LIMIT sorted 1;
                GENERATE FLATTEN(unq);
                                };

work__rec_RAPID_ddc_warehouse =
        FOREACH
        unq_grp_rec_RAPID_ddc_warehouse
        GENERATE
        unq::sears_division_nbr AS sears_division_nbr,
        unq::sears_item_nbr AS sears_item_nbr,
        unq::sears_sku_nbr AS sears_sku_nbr,
        unq::candidate_vendor_nbr AS min_vendor_nbr,
        (int)unq::candidate_vendor_package_qty AS min_vendor_package_qty,
        unq::candidate_source_location_id AS min_vendor_location_id;

union_ddc_minimum_vendor = UNION
                           work__rec_RAPID_ddc_warehouse,
                           work__ddc_minimum_vendor;

STORE union_ddc_minimum_vendor
INTO '$WORK__DDC_MINIMUM_VENDOR_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');

gen_import_rec_RRC_warehouse = foreach import_rec_RRC_warehouse generate
sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        (int)dos_source_package_qty AS candidate_vendor_package_qty,
        dos_source_location_id AS candidate_source_location_id,
        dos_source_nbr AS candidate_vendor_nbr;
		
gen_import_rec_DDC_MDO_warehouse = foreach import_rec_DDC_MDO_warehouse generate
sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        (int)dos_source_package_qty AS candidate_vendor_package_qty,
        dos_source_location_id AS candidate_source_location_id,
        dos_source_nbr AS candidate_vendor_nbr;

gen_import_rec_CDFC_warehouse = foreach import_rec_CDFC_warehouse generate
sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        srim_source_package_qty AS candidate_vendor_package_qty,
        srim_source_location_id as candidate_source_location_id,
        srim_source_nbr as candidate_vendor_nbr;		

gen_import_rec_Non_CDFC_warehouse = foreach import_rec_Non_CDFC_warehouse generate
sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        srim_source_package_qty AS candidate_vendor_package_qty,
        srim_source_location_id as candidate_source_location_id,
        srim_source_nbr as candidate_vendor_nbr;
        
un_import_rec_warehouse = 
	UNION
	gen_store_split1,
	gen_import_rec_DDC_MDO_warehouse,
	gen_import_rec_CDFC_warehouse,
	gen_import_rec_RRC_warehouse,
	gen_import_rec_Non_CDFC_warehouse;
	
gen_import_rec_warehouse = 
        FOREACH
        un_import_rec_warehouse
        GENERATE
        sears_division_nbr,
        sears_item_nbr,
        sears_sku_nbr,
        candidate_vendor_package_qty,
        candidate_source_location_id,
        candidate_vendor_nbr;

grp_gen_import_rec_warehouse = 
        GROUP
        gen_import_rec_warehouse
        BY (sears_division_nbr,TrimLeadingZeros(sears_item_nbr), sears_sku_nbr);

unq_grp_gen_import_rec_warehouse = 
        FOREACH
        grp_gen_import_rec_warehouse{
                sorted = ORDER gen_import_rec_warehouse BY candidate_vendor_nbr,candidate_vendor_package_qty;
                unq = LIMIT sorted 1;
                GENERATE FLATTEN(unq);
                                };

work__import_minimum_vendor = 
	FOREACH
        unq_grp_gen_import_rec_warehouse
        GENERATE
        unq::sears_division_nbr AS sears_division_nbr,
        TrimLeadingZeros(unq::sears_item_nbr) AS sears_item_nbr,
        unq::sears_sku_nbr AS sears_sku_nbr,
        unq::candidate_vendor_nbr AS min_vendor_nbr,
        unq::candidate_vendor_package_qty AS min_vendor_package_qty,
        unq::candidate_source_location_id AS min_vendor_location_id;

STORE work__import_minimum_vendor
INTO '$WORK__IMPORT_MINIMUM_VENDOR_LOCATION'
USING PigStorage('$FIELD_DELIMITER_CONTROL_A');


/*
###############################################################################
#<>                                 END                                     <>#
###############################################################################
*/
