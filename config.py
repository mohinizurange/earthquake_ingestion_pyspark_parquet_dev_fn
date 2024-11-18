from datetime import datetime


## paths
cur_timestamp = datetime.now().strftime('%Y%m%d')
eq_landing_gcs_loc = f"gs://earthquake_analysis_buck/pysaprk/landing/{cur_timestamp}/"
eq_silver_gcs_loc =f'gs://earthquake_analysis_buck/pysaprk/silver/{cur_timestamp}/'
project_id = 'spark-learning-43150'
temp_dataproc_bucket = "earthquake-dp_temp_bk"
eq_bigquery_tbl_loc =f'{project_id}.earthquake_db.earthquake_data_m'
eq_audit_tbl_loc = f'{project_id}.earthquake_db.earthquake_audit_tbl'

# api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
