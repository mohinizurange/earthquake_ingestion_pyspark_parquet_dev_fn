from utility import Utils
import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestApp").getOrCreate()

@pytest.fixture
def raw_data_df(spark):
    # Sample raw JSON data as a string
    json_data = '''{
        "type": "FeatureCollection",
        "metadata": {
            "generated": 1731468455000,
            "url": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",
            "title": "USGS All Earthquakes, Past Month",
            "status": 200,
            "api": "1.14.1",
            "count": 7784
        },
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "mag": 1,
                    "place": "2 km N of The Geysers, CA",
                    "time": 1731467499040,
                    "updated": 1731467597506,
                    "tz": null,
                    "url": "https://earthquake.usgs.gov/earthquakes/eventpage/nc75085856",
                    "detail": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/nc75085856.geojson",
                    "felt": null,
                    "cdi": null,
                    "mmi": null,
                    "alert": null,
                    "status": "automatic",
                    "tsunami": 0,
                    "sig": 15,
                    "net": "nc",
                    "code": "75085856",
                    "ids": ",nc75085856,",
                    "sources": ",nc,",
                    "types": ",nearby-cities,origin,phase-data,",
                    "nst": 15,
                    "dmin": 0.00612,
                    "rms": 0.02,
                    "gap": 72,
                    "magType": "md",
                    "type": "earthquake",
                    "title": "M 1.0 - 2 km N of The Geysers, CA"
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-122.759498596191, 38.7944984436035, 0.519999980926514]
                },
                "id": "nc75085856"
            }
        ]
    }'''

    # Convert the JSON string to a DataFrame by reading it as a JSON from a list
    rdd = spark.sparkContext.parallelize([json_data])
    return spark.read.json(rdd)


def test_extractReqDataFlattenApplyTrans(spark, raw_data_df):
    # Initialize your class with the function to test
    u_obj = Utils()

    # raw_data_df.printSchema()
    # Call the transformation function
    transformed_df = u_obj.extractReqDataFlattenApplyTrans(raw_data_df)

    # Define expected columns
    expected_columns = ["mag", "place", "time", "updated", "tz", "url", "detail", "felt",
                        "cdi", "mmi", "alert", "status", "tsunami", "sig", "net", "code",
                        "ids", "sources", "types", "nst", "dmin", "rms", "gap", "magType",
                        "type", "title", "area", "geometry", "insert_date"]

    # Perform column validation
    assert transformed_df.columns == expected_columns, "Column validation failed: Expected columns do not match."
    # print("Column validation passed: All expected columns are present.")

    # Test if 'time' and 'updated' columns are converted to timestamp type
    time_column_type = dict(transformed_df.dtypes)['time']  ### [('mag', 'double'), ('place', 'string'), ('time', 'timestamp'), ('updated', 'timestamp'), ...] -> {'mag': 'double', 'place': 'string', 'time': 'timestamp', 'updated': 'timestamp', ...}
    updated_column_type = dict(transformed_df.dtypes)['updated'] ### [('mag', 'double'), ('place', 'string'), ('time', 'timestamp'), ('updated', 'timestamp'), ...] -> {'mag': 'double', 'place': 'string', 'time': 'timestamp', 'updated': 'timestamp', ...}

    assert time_column_type == 'timestamp', f"Expected 'timestamp' for 'time' column, but got {time_column_type}"
    assert updated_column_type == 'timestamp', f"Expected 'timestamp' for 'updated' column, but got {updated_column_type}"
    # print("Timestamp conversion validation passed for 'time' and 'updated' columns.")

    # Test if 'area' column is correctly extracted from 'place' field
    area_value = transformed_df.select("area").collect()[0][0]
    assert area_value == "The Geysers, CA", f"Expected 'The Geysers, CA' in 'area' column, but got {area_value}"
    # print("Area extraction validation passed.")

    # Check geometry structure: longitude, latitude, depth
    geometry = transformed_df.select("geometry").collect()[0][0]
    assert geometry.longitude == -122.759498596191, f"Expected longitude -122.759498596191, but got {geometry.longitude}"
    assert geometry.latitude == 38.7944984436035, f"Expected latitude 38.7944984436035, but got {geometry.latitude}"
    assert geometry.depth == 0.519999980926514, f"Expected depth 0.519999980926514, but got {geometry.depth}"
    # print("Geometry field validation passed.")

    # Check if 'insert_date' is present and of type timestamp
    insert_date_column_type = dict(transformed_df.dtypes)['insert_date']
    assert insert_date_column_type == 'timestamp', f"Expected 'timestamp' for 'insert_date' column, but got {insert_date_column_type}"
    # print("Insert date column validation passed.")
