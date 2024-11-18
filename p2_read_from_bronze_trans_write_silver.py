################################################################################################
"""
file_name - p2_read_from_bronze_trans_write_silver.py
desc - Main script for reading data from bronze layer ,apply transformation and
write clean data in silver layer(GCS)in parquet fromat
       This script performs the following operations:
       - Configures logging for tracking pipeline execution.
       - Initializes a Spark session with necessary configurations.
       - Creates an instance of the Utils class
       - Parses command-line arguments for API URL and pipeline name.
       - Executes a series of functions to:
         1. Read the Parquet file from GCS.
         2. Extract and transform the required data by flattening it.
         3. Write the cleaned data to a silver location in GCS in Parquet format.
start_date - 2024-10-21
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, explode, struct, to_timestamp, from_unixtime, expr
from datetime import datetime
import argparse
import requests
import logging
import  json
from utility import Utils
import config as cnf

def read_from_bronze_trans_write_silver():
    """
    read data from bronze layer ,apply transformation and
    write clean data in silver layer(GCS) in parquet format.

    Steps:

    1. Reads the Parquet file from GCS.
    2. Flattens and transforms the data.
    3. Writes cleaned data to GCS (silver location).

    Command-line arguments:
    - pipeline_nm: Name of the pipeline.

    Logs the start and end times, status, and processed records for each step.
    Raises exceptions on failure.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Initialize Spark session and configs
    spark = SparkSession.builder.master("local[*]").appName("extract_the_data_from_API").getOrCreate()
    spark.conf.set("temporaryGcsBucket", cnf.temp_dataproc_bucket)

    ## create Util classs obj
    util_obj = Utils()

    # Parse command-line arguments
    parser = argparse.ArgumentParser() ## argumentparser  collects run time inputs which we pass
    parser.add_argument('-pipeline_nm', '--pipeline_nm', required=True, help='Pipeline name')
    args = parser.parse_args()

    pipeline_name = args.pipeline_nm

    job_id = cnf.cur_timestamp
    # Function 3: Read parquest file from GCS (landing location)
    try:
        function_name = "3_readParquetFilefromGCS"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        ### 3 : function call #####
        raw_data = util_obj.readParquetFilefromGCS(spark, cnf.eq_landing_gcs_loc)
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "successful"
        process_record = raw_data.count()
        error_msg = None
    except Exception as e:
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "fail"
        process_record = 0
        error_msg = e
        logging.error(f"Error in {function_name}: {e}")
    util_obj.log_audit(spark, job_id, pipeline_name, function_name, start_time, end_time, status, process_record,
                       cnf.eq_audit_tbl_loc, error_msg)

    # Function 4: extract the required data and flatten it as well as apply transformation
    try:
        function_name = "4_extractReqDataFlattenApplyTrans"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        ### 4 : function call #####
        clean_data = util_obj.extractReqDataFlattenApplyTrans(raw_data)
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "successful"
        process_record = clean_data.count()
        error_msg = None
    except Exception as e:
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "fail"
        process_record = 0
        error_msg = e
        logging.error(f"Error in {function_name}: {e}")
    util_obj.log_audit(spark, job_id, pipeline_name, function_name, start_time, end_time, status, process_record,
                       cnf.eq_audit_tbl_loc, error_msg)

    # Function 5: write clean data into gcs in parquet format (silver location)
    try:
        function_name = "5_writeParquetDataintoGCS"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        ### 5 : function call #####
        util_obj.writeParquetDataintoGCS(spark, clean_data, cnf.eq_silver_gcs_loc)
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "successful"
        process_record = clean_data.count()
        error_msg = None
    except Exception as e:
        end_time, status = datetime.now().strftime('%Y%m%d_%H%M%S'), "fail"
        process_record = 0
        error_msg = e
        logging.error(f"Error in {function_name}: {e}")
    util_obj.log_audit(spark, job_id, pipeline_name, function_name, start_time, end_time, status, process_record,
                       cnf.eq_audit_tbl_loc, error_msg)

if __name__ == '__main__':
    read_from_bronze_trans_write_silver()