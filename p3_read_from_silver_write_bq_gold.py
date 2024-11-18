################################################################################################
"""
file_name - p3_read_from_silver_write_bq_gold.py
desc - Main script for read data from silver(GCS) layer location and write in golden layer
       This script performs the following operations:
       - Configures logging for tracking pipeline execution.
       - Initializes a Spark session with necessary configurations.
       - Creates an instance of the Utils class
       - Parses command-line arguments for API URL and pipeline name.
       - Executes a series of functions to:
         1. Reads the Parquet file from silver GCS.
         2. Loads the cleaned data into a BigQuery table.
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

def read_from_silver_write_bq_gold():
    """
    read data from silver layer (GCS) and write in bigquery golden layer

    Steps:

    1. Reads the Parquet file from silver GCS.
    2. Loads the cleaned data into a BigQuery table.

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
    # Function 3: Read parquest file from GCS (silver location)
    try:
        function_name = "6_readParquetFilefromsilverGCS"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        ### 3 : function call #####
        clean_df_parq_schema= util_obj.clean_df_parq_schema()
        # logging.info(clean_df_parq_schema)
        clean_data = util_obj.readParquetFilefromGCS(spark, cnf.eq_silver_gcs_loc ,clean_df_parq_schema)
        clean_data.printSchema()
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

    # Function 6: write clean data into bigquery table
    try:
        function_name = "7_writeDataintoBigquery"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        ### 6: function call #####
        util_obj.writeDataintoBigquery(cnf.eq_bigquery_tbl_loc, clean_data)
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
    read_from_silver_write_bq_gold()