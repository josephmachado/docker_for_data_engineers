import pytest
from pyspark.sql import functions as F
from pyspark.sql import Row
from etl.simple_etl import transform_seller_data, load_seller_data


def test_transform_and_load_seller_data(spark):
    # Create a sample DataFrame with fake seller data
    raw_seller_data = spark.createDataFrame([
        Row(user_id=1, first_time_sold_timestamp="2022-01-01 10:00:00"),
        Row(user_id=1, first_time_sold_timestamp="2021-12-31 09:00:00"),
        Row(user_id=2, first_time_sold_timestamp="2023-01-01 12:00:00"),
        Row(user_id=3, first_time_sold_timestamp="2021-11-11 08:00:00")
    ])

    # Process the data through transform function
    transformed_data = transform_seller_data(raw_seller_data)

    # Mock the load operation to capture output
    result = load_seller_data(transformed_data)

    # Define expected results
    expected_results = [
        {'user_id': 1, 'first_time_sold': "2021-12-31 09:00:00"},
        {'user_id': 2, 'first_time_sold': "2023-01-01 12:00:00"},
        {'user_id': 3, 'first_time_sold': "2021-11-11 08:00:00"}
    ]

    # Convert result to list of dicts for easier comparison
    result_dicts = [row.asDict() for row in result]

    # Check if the results match the expected results
    assert sorted(result_dicts, key=lambda x: x['user_id']) == sorted(expected_results, key=lambda x: x['user_id'])
