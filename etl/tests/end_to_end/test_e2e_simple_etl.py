import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col
from unittest.mock import patch
from etl.simple_etl import run_code

# Create a DataFrame to be returned by the mock
@pytest.fixture
def fake_data(spark):
    data = [
        Row(user_id="1", first_time_sold_timestamp="2021-01-01 00:00:00"),
        Row(user_id="2", first_time_sold_timestamp="2021-02-01 00:00:00"),
        Row(user_id="1", first_time_sold_timestamp="2021-01-02 00:00:00"),
        Row(user_id="2", first_time_sold_timestamp="2021-02-02 00:00:00"),
        Row(user_id="3", first_time_sold_timestamp="2021-03-01 00:00:00"),
    ]
    return spark.createDataFrame(data)

# Test function to check if run_code works as expected
def test_run_code(spark, fake_data):
    # Mock the get_upstream_table function to return the fake_data
    with patch('etl.simple_etl.get_upstream_table', return_value=fake_data):
        # Because collect() is called in load_seller_data, we check its output
        expected_data = [
            Row(user_id='1', first_time_sold='2021-01-01 00:00:00'),
            Row(user_id='2', first_time_sold='2021-02-01 00:00:00'),
            Row(user_id='3', first_time_sold='2021-03-01 00:00:00'),
        ]
        
        # Run the code end to end
        result_data = run_code(spark)
        
        # Convert result_data to a DataFrame to make it comparable
        result_df = spark.createDataFrame(result_data)
        
        # Sort both dataframes by user_id before comparison to avoid order issues
        expected_df = spark.createDataFrame(expected_data).orderBy("user_id")
        result_sorted = result_df.orderBy("user_id")

        # Assert that the transformed data is as expected
        assert result_sorted.collect() == expected_df.collect(), "The transformed data does not match expected output"

