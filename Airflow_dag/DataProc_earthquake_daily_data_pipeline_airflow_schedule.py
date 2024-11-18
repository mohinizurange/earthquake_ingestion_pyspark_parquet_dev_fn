from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'M_Airflow',
    'retrives': 2,
    'depends_on_past': False,# Tasks do not depend on previous runs
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 11, 10),
}

# Define DAG
with DAG(
        'DataProc_Earthquake_daily_dataload_schedule',
        default_args=default_args,
        schedule_interval='0 10 * * *',  # This sets the DAG to run daily at 10 AM
        catchup=False, # Do not run past schedules if the DAG is missed
) as dag:
    # Step 1: Create Dataproc Cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id="spark-learning-43150",
        region="us-central1",
        cluster_name="dataproc-cluster",
        cluster_config={
            "config_bucket": "earthquake-dp_temp_bk",
            "gce_cluster_config": {
                "zone_uri": "us-central1-a",
                "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
                "tags": ["pyspark"]
            },
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_size_gb": 100}
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_size_gb": 100}
            },
            "software_config": {
                "image_version": "2.0-debian10",
                "optional_components": ["JUPYTER"]
            },
            "endpoint_config": {
                "enable_http_port_access": True  # Allow access to the Dataproc UI via HTTP
            }
        },
        use_if_exists=True,# Use the cluster if it already exists
        delete_on_error=True # Delete the cluster if an error occurs
    )

    # Step 2: Submit PySpark Job for extract and ingect data to bronze
    job_id='extract_and_ingect_data_bronze_job_daily'+ datetime.now().strftime('%Y%m%d_%H%M%S') #dataproc cluster wont allow duplicate job id so
    submit_pyspark_job_p1 = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job_p1",
        project_id="spark-learning-43150",
        region="us-central1",
        job={
            "reference": {"job_id": job_id},
            "placement": {"cluster_name": "dataproc-cluster"},
            "pyspark_job": {
                "main_python_file_uri": "gs://earthquake_analysis_buck/pysaprk/pyspark_code/p1_extract_and_ingect_data_bronze.py",
                "args": [
                    "--api_url", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
                    "--pipeline_nm", "daily"
                ],
                "python_file_uris": [
                    "gs://earthquake_analysis_buck/pysaprk/pyspark_code/utility.py",
                    "gs://earthquake_analysis_buck/pysaprk/pyspark_code/config.py"
                ]
            }
        },
        gcp_conn_id="gcp_connection",
    )

    # step 3
    job_id = 'read_from_bronze_trans_write_silver_job_daily' + datetime.now().strftime('%Y%m%d_%H%M%S')  # dataproc cluster wont allow duplicate job id so
    submit_pyspark_job_p2 = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job_p2",
        project_id="spark-learning-43150",
        region="us-central1",
        job={
            "reference": {"job_id": job_id},
            "placement": {"cluster_name": "dataproc-cluster"},
            "pyspark_job": {
                "main_python_file_uri": "gs://earthquake_analysis_buck/pysaprk/pyspark_code/p2_read_from_bronze_trans_write_silver.py",
                "args": [
                    "--pipeline_nm", "daily"
                ],
                "python_file_uris": [
                    "gs://earthquake_analysis_buck/pysaprk/pyspark_code/utility.py",
                    "gs://earthquake_analysis_buck/pysaprk/pyspark_code/config.py"
                ]
            }
        },
        gcp_conn_id="gcp_connection",
    )

    ## step 4
    job_id = 'read_from_silver_write_bq_gold_job_daily' + datetime.now().strftime('%Y%m%d_%H%M%S')  # dataproc cluster wont allow duplicate job id so
    submit_pyspark_job_p3 = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job_p3",
        project_id="spark-learning-43150",
        region="us-central1",
        job={
            "reference": {"job_id": job_id},
            "placement": {"cluster_name": "dataproc-cluster"},
            "pyspark_job": {
                "main_python_file_uri": "gs://earthquake_analysis_buck/pysaprk/pyspark_code/p3_read_from_silver_write_bq_gold.py",
                "args": [
                    "--pipeline_nm", "daily"
                ],
                "python_file_uris": [
                    "gs://earthquake_analysis_buck/pysaprk/pyspark_code/utility.py",
                    "gs://earthquake_analysis_buck/pysaprk/pyspark_code/config.py"
                ],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.21.0.jar"
                ]
            }
        },
        gcp_conn_id="gcp_connection",
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id="spark-learning-43150",
        cluster_name='dataproc-cluster',
        region='us-central1',
        trigger_rule='all_done',  ###the task will wait for all upstream tasks to finish before it runs, regardless of whether they succeed, fail, or are skipped
        gcp_conn_id='gcp_connection',
    )
    # Define task dependencies
    create_cluster >> submit_pyspark_job_p1>> submit_pyspark_job_p2 >> submit_pyspark_job_p3 >> delete_cluster
