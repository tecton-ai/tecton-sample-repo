import os
import pytest
from datetime import datetime, timedelta
import pandas as pd

from fraud.features.stream_features.last_transaction_amount_pyspark import last_transaction_amount_pyspark

@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_last_transaction_amount_pyspark(tecton_pytest_spark_session):
    # Create test data with transactions over time
    start_time = datetime(2022, 5, 1).replace(tzinfo=None)
    end_time = start_time + timedelta(hours=2)
    
    # Create transactions for two users:
    # user_1: 3 transactions with increasing amounts
    # user_2: 2 transactions with different amounts
    data = {
        "timestamp": [
            start_time,
            start_time + timedelta(minutes=30),
            start_time + timedelta(hours=1),
            start_time + timedelta(minutes=45),
            start_time + timedelta(hours=1, minutes=30)
        ],
        "user_id": ["user_1", "user_1", "user_1", "user_2", "user_2"],
        "amt": [100.0, 200.0, 300.0, 150.0, 250.0]
    }
    input_pd = pd.DataFrame(data)
    
    # Convert to Spark DataFrame
    input_spark_df = tecton_pytest_spark_session.createDataFrame(input_pd)

    # Run the transformation
    output = last_transaction_amount_pyspark.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={"transactions": input_spark_df}
    )

    # Convert to pandas for easier assertion
    output_pd = output.to_pandas()

    # Create expected output with the same schema as the feature view output
    expected_data = {
        "user_id": ["user_1", "user_2"],
        "amt": [300.0, 250.0],  # Last amounts for each user
        "timestamp": [
            start_time + timedelta(hours=1),  # Last timestamp for user_1
            start_time + timedelta(hours=1, minutes=30)  # Last timestamp for user_2
        ]
    }
    expected_pd = pd.DataFrame(expected_data)

    # Sort both dataframes by user_id to ensure consistent order
    output_pd = output_pd.sort_values("user_id").reset_index(drop=True)
    expected_pd = expected_pd.sort_values("user_id").reset_index(drop=True)

    # Assert the dataframes are equal, ignoring dtype
    pd.testing.assert_frame_equal(
        output_pd,
        expected_pd,
        check_dtype=False
    ) 