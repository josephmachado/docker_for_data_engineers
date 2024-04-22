import pytest
from pyspark.sql import functions as F
from pyspark.sql import Row
from etl.simple_etl import transform_seller_data


def test_transform_seller_data(spark):
    # Create a sample DataFrame with fake seller data
    raw_seller_data = spark.createDataFrame([
        Row(user_id=1, first_time_sold_timestamp="2022-01-01 10:00:00"),
        Row(user_id=1, first_time_sold_timestamp="2021-12-31 09:00:00"),
        Row(user_id=2, first_time_sold_timestamp="2023-01-01 12:00:00"),
        Row(user_id=3, first_time_sold_timestamp="2021-11-11 08:00:00")
    ])

    # Apply the transform_seller_data function
    clean_seller_data = transform_seller_data(raw_seller_data)

    # Expected result is the minimum timestamp for each user_id
    expected_data = spark.createDataFrame([
        Row(user_id=1, first_time_sold_timestamp="2021-12-31 09:00:00"),
        Row(user_id=2, first_time_sold_timestamp="2023-01-01 12:00:00"),
        Row(user_id=3, first_time_sold_timestamp="2021-11-11 08:00:00")
    ])

    # Collect data for assertion (convert to set for unordered comparison)
    clean_seller_set = set(clean_seller_data.collect())
    expected_set = set(expected_data.collect())

    # Assert that the transformed data matches the expected data
    assert clean_seller_set == expected_set
