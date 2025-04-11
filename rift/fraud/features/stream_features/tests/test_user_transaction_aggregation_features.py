import os
import pytest
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from fraud.features.stream_features.user_transaction_aggregation_features import user_transaction_aggregation_features

@pytest.mark.skipif(
    "TECTON_TEST_SPARK" not in os.environ,
    reason="Skipping test because TECTON_TEST_SPARK is not set",
)
def test_user_transaction_aggregation_features():
    spark = SparkSession.builder.getOrCreate()

    start_time = datetime(2024, 1, 1, 0, 0, 0)
    data = [
        # user_1 transactions
        (start_time, "user_1", 100.0, "merchant_A"),  # First transaction
        (start_time + timedelta(minutes=15), "user_1", 200.0, "merchant_B"),
        (start_time + timedelta(minutes=30), "user_1", 300.0, "merchant_A"),
        (start_time + timedelta(minutes=45), "user_1", 400.0, "merchant_C"),
        
        # user_2 transactions
        (start_time + timedelta(minutes=10), "user_2", 150.0, "merchant_D"),
        (start_time + timedelta(minutes=20), "user_2", 250.0, "merchant_E"),
        (start_time + timedelta(minutes=40), "user_2", 350.0, "merchant_D")
    ]

    schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("user_id", StringType(), False),
        StructField("amt", DoubleType(), False),
        StructField("merchant", StringType(), False)
    ])

    df = spark.createDataFrame(data, schema)

    # Get features in range directly from the feature view
    output = user_transaction_aggregation_features.get_features_in_range(
        df,
        start_time=start_time,
        end_time=start_time + timedelta(hours=2)
    )

    output_pd = output.toPandas()
    print("\nOutput DataFrame:")
    print(output_pd)

    # Sort by user_id and _valid_from to ensure consistent order
    output_pd = output_pd.sort_values(['user_id', '_valid_from'])

    # Check that the output has the expected columns
    expected_columns = {
        'user_id',
        '_valid_from',
        '_valid_to',
        'amt_sum_1h_continuous',
        'amt_mean_1h_continuous',
        'amt_count_1h_continuous',
        'amt_min_1h_continuous',
        'amt_max_1h_continuous',
        'amt_stddev_1h_continuous',
        'amt_approx_percentile_1h_continuous',
        'amt_first_1h_continuous',
        'amt_last_1h_continuous',
        'amt_first_distinct_1h_continuous',
        'amt_last_distinct_1h_continuous',
        'merchant_first_1h_continuous',
        'merchant_last_1h_continuous',
        'merchant_first_distinct_1h_continuous',
        'merchant_last_distinct_1h_continuous',
        'merchant_last_2_1h_continuous'
    }
    assert set(output_pd.columns) == expected_columns, f"Expected columns {expected_columns}, got {set(output_pd.columns)}"

    # Check that we have data for both users
    assert set(output_pd['user_id'].unique()) == {'user_1', 'user_2'}, f"Expected users ['user_1', 'user_2'], got {output_pd['user_id'].unique()}"

    print("\nAll rows:")
    print(output_pd)

    # Check specific aggregations for user_1's last window
    user1_data = output_pd[output_pd['user_id'] == 'user_1'].sort_values('_valid_from')
    user1_last = user1_data[user1_data['_valid_from'] == start_time + timedelta(minutes=45)].iloc[0]
    print("\nUser 1's last row:")
    print(user1_last)

    assert user1_last['amt_sum_1h_continuous'] == 1000.0, f"Expected sum of 1000.0 for user_1's last hour (all four transactions), got {user1_last['amt_sum_1h_continuous']}"
    assert user1_last['amt_mean_1h_continuous'] == 250.0, f"Expected mean of 250.0 for user_1's last hour (average of all four transactions), got {user1_last['amt_mean_1h_continuous']}"
    assert user1_last['amt_count_1h_continuous'] == 4, f"Expected count of 4 for user_1's last hour, got {user1_last['amt_count_1h_continuous']}"
    assert user1_last['amt_min_1h_continuous'] == 100.0, f"Expected min of 100.0 for user_1's last hour, got {user1_last['amt_min_1h_continuous']}"
    assert user1_last['amt_max_1h_continuous'] == 400.0, f"Expected max of 400.0 for user_1's last hour, got {user1_last['amt_max_1h_continuous']}"
    assert user1_last['amt_stddev_1h_continuous'] == pytest.approx(129.099, rel=1e-3), f"Expected stddev of ~129.099 for user_1's last hour, got {user1_last['amt_stddev_1h_continuous']}"
    assert user1_last['amt_approx_percentile_1h_continuous'] == pytest.approx(300.0, rel=1e-3), f"Expected 50th percentile of ~300.0 for user_1's last hour, got {user1_last['amt_approx_percentile_1h_continuous']}"
    assert user1_last['amt_first_1h_continuous'] == 100.0, f"Expected first amount of 100.0 for user_1's last hour, got {user1_last['amt_first_1h_continuous']}"
    assert user1_last['amt_last_1h_continuous'] == 400.0, f"Expected last amount of 400.0 for user_1's last hour, got {user1_last['amt_last_1h_continuous']}"
    assert user1_last['amt_first_distinct_1h_continuous'] == 100.0, f"Expected first distinct amount of 100.0 for user_1's last hour, got {user1_last['amt_first_distinct_1h_continuous']}"
    assert user1_last['amt_last_distinct_1h_continuous'] == 400.0, f"Expected last distinct amount of 400.0 for user_1's last hour, got {user1_last['amt_last_distinct_1h_continuous']}"
    assert user1_last['merchant_first_1h_continuous'] == 'merchant_A', f"Expected first merchant of 'merchant_A' for user_1's last hour, got {user1_last['merchant_first_1h_continuous']}"
    assert user1_last['merchant_last_1h_continuous'] == 'merchant_C', f"Expected last merchant of 'merchant_C' for user_1's last hour, got {user1_last['merchant_last_1h_continuous']}"
    assert user1_last['merchant_first_distinct_1h_continuous'] == 'merchant_A', f"Expected first distinct merchant of 'merchant_A' for user_1's last hour, got {user1_last['merchant_first_distinct_1h_continuous']}"
    assert user1_last['merchant_last_distinct_1h_continuous'] == 'merchant_C', f"Expected last distinct merchant of 'merchant_C' for user_1's last hour, got {user1_last['merchant_last_distinct_1h_continuous']}"
    assert set(user1_last['merchant_last_2_1h_continuous']) == {'merchant_C', 'merchant_A'}, f"Expected last 2 distinct merchants of ['merchant_C', 'merchant_A'] for user_1's last hour, got {user1_last['merchant_last_2_1h_continuous']}"

    # Check specific aggregations for user_2's last window
    user2_data = output_pd[output_pd['user_id'] == 'user_2'].sort_values('_valid_from')
    user2_last = user2_data[user2_data['_valid_from'] == start_time + timedelta(minutes=40)].iloc[0]
    print("\nUser 2's last row:")
    print(user2_last)

    assert user2_last['amt_sum_1h_continuous'] == 750.0, f"Expected sum of 750.0 for user_2's last hour (all three transactions), got {user2_last['amt_sum_1h_continuous']}"
    assert user2_last['amt_mean_1h_continuous'] == 250.0, f"Expected mean of 250.0 for user_2's last hour (average of all three transactions), got {user2_last['amt_mean_1h_continuous']}"
    assert user2_last['amt_count_1h_continuous'] == 3, f"Expected count of 3 for user_2's last hour, got {user2_last['amt_count_1h_continuous']}"
    assert user2_last['amt_min_1h_continuous'] == 150.0, f"Expected min of 150.0 for user_2's last hour, got {user2_last['amt_min_1h_continuous']}"
    assert user2_last['amt_max_1h_continuous'] == 350.0, f"Expected max of 350.0 for user_2's last hour, got {user2_last['amt_max_1h_continuous']}"
    assert user2_last['amt_stddev_1h_continuous'] == pytest.approx(100.0, rel=1e-3), f"Expected stddev of ~100.0 for user_2's last hour, got {user2_last['amt_stddev_1h_continuous']}"
    assert user2_last['amt_approx_percentile_1h_continuous'] == pytest.approx(250.0, rel=1e-3), f"Expected 50th percentile of ~250.0 for user_2's last hour, got {user2_last['amt_approx_percentile_1h_continuous']}"
    assert user2_last['amt_first_1h_continuous'] == 150.0, f"Expected first amount of 150.0 for user_2's last hour, got {user2_last['amt_first_1h_continuous']}"
    assert user2_last['amt_last_1h_continuous'] == 350.0, f"Expected last amount of 350.0 for user_2's last hour, got {user2_last['amt_last_1h_continuous']}"
    assert user2_last['amt_first_distinct_1h_continuous'] == 150.0, f"Expected first distinct amount of 150.0 for user_2's last hour, got {user2_last['amt_first_distinct_1h_continuous']}"
    assert user2_last['amt_last_distinct_1h_continuous'] == 350.0, f"Expected last distinct amount of 350.0 for user_2's last hour, got {user2_last['amt_last_distinct_1h_continuous']}"
    assert user2_last['merchant_first_1h_continuous'] == 'merchant_D', f"Expected first merchant of 'merchant_D' for user_2's last hour, got {user2_last['merchant_first_1h_continuous']}"
    assert user2_last['merchant_last_1h_continuous'] == 'merchant_D', f"Expected last merchant of 'merchant_D' for user_2's last hour, got {user2_last['merchant_last_1h_continuous']}"
    assert user2_last['merchant_first_distinct_1h_continuous'] == 'merchant_D', f"Expected first distinct merchant of 'merchant_D' for user_2's last hour, got {user2_last['merchant_first_distinct_1h_continuous']}"
    assert user2_last['merchant_last_distinct_1h_continuous'] == 'merchant_D', f"Expected last distinct merchant of 'merchant_D' for user_2's last hour, got {user2_last['merchant_last_distinct_1h_continuous']}"
    assert set(user2_last['merchant_last_2_1h_continuous']) == {'merchant_D', 'merchant_E'}, f"Expected last 2 distinct merchants of ['merchant_D', 'merchant_E'] for user_2's last hour, got {user2_last['merchant_last_2_1h_continuous']}" 