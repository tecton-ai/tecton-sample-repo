import pytest
import pandas as pd
from datetime import datetime, timezone
from fraud.features.batch_features.merchant_fraud_rate import merchant_fraud_rate

def test_merchant_fraud_rate():
    # Create test data with transactions from different merchants and fraud status
    timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),   # Merchant1 - Fraud
        datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),   # Merchant1 - Not Fraud
        datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),   # Merchant2 - Fraud
        datetime(2024, 1, 3, 10, 0, tzinfo=timezone.utc),   # Merchant2 - Not Fraud
        datetime(2024, 1, 4, 10, 0, tzinfo=timezone.utc),   # Merchant3 - Fraud
    ]

    test_data = pd.DataFrame({
        'merchant': ['merchant1', 'merchant1', 'merchant2', 'merchant2', 'merchant3'],
        'is_fraud': [1, 0, 1, 0, 1],
        'timestamp': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 5, 0, 0, tzinfo=timezone.utc)

    # Run the transformation
    result = merchant_fraud_rate.run_transformation(start_time, end_time, mock_inputs={'transactions_batch': test_data})
    result_df = result.to_pandas()

    # Verify output schema
    expected_columns = ['merchant', 'is_fraud', 'timestamp']
    assert set(result_df.columns) == set(expected_columns)

    # Verify the data is correctly prepared for aggregation
    assert len(result_df) == len(test_data)  # No rows should be lost

    # Verify merchant-fraud combinations are preserved
    for _, row in test_data.iterrows():
        merchant = row['merchant']
        is_fraud = row['is_fraud']
        timestamp = row['timestamp']
        
        # Find the corresponding row in the result
        matching_rows = result_df[
            (result_df['merchant'] == merchant) & 
            (result_df['is_fraud'] == is_fraud) &
            (result_df['timestamp'] == timestamp)
        ]
        
        # Verify we found exactly one matching row
        assert len(matching_rows) == 1

    # Verify timestamps are preserved
    pd.testing.assert_series_equal(
        result_df['timestamp'].sort_values().reset_index(drop=True),
        test_data['timestamp'].sort_values().reset_index(drop=True),
        check_names=False
    ) 