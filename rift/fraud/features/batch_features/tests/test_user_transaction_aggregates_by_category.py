import pandas as pd
from datetime import datetime, timezone
from fraud.features.batch_features.user_transaction_aggregates_by_category import user_transaction_aggregates_by_category, CATEGORIES

def test_user_transaction_aggregates_by_category():
    # Create test data with transactions in different categories
    timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 3, 12, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 4, 13, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 5, 14, 0, tzinfo=timezone.utc)
    ]

    test_data = pd.DataFrame({
        'user_id': ['user1', 'user1', 'user1', 'user2', 'user2'],
        'category': ['gas_transport', 'food_dining', 'shopping_pos', 'grocery_pos', 'entertainment'],
        'amt': [100.0, 200.0, 300.0, 400.0, 500.0],
        'timestamp': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 6, 0, 0, tzinfo=timezone.utc)

    # Run the transformation
    result = user_transaction_aggregates_by_category.run_transformation(start_time, end_time, mock_inputs={'transactions_df': test_data})
    result_df = result.to_pandas()

    # Verify the output schema
    expected_columns = ['user_id', 'timestamp'] + [f'{cat}_amt' for cat in CATEGORIES]
    assert set(result_df.columns) == set(expected_columns)

    # Verify the data transformation
    assert len(result_df) == 5  # Should have same number of rows as input

    # Verify category amounts are correctly assigned
    for _, row in test_data.iterrows():
        category = row['category']
        amount = row['amt']
        user_id = row['user_id']
        timestamp = row['timestamp']
        
        # Find the corresponding row in the result
        result_row = result_df[
            (result_df['user_id'] == user_id) & 
            (result_df['timestamp'] == timestamp)
        ].iloc[0]
        
        # Verify the amount is assigned to the correct category column
        assert result_row[f'{category}_amt'] == amount
        
        # Verify other category columns are None
        other_categories = [cat for cat in CATEGORIES if cat != category]
        for other_cat in other_categories:
            assert pd.isna(result_row[f'{other_cat}_amt']), f"Expected None for {other_cat} but got {result_row[f'{other_cat}_amt']}"

    # Verify timestamps are preserved
    pd.testing.assert_series_equal(
        result_df['timestamp'].sort_values().reset_index(drop=True),
        test_data['timestamp'].sort_values().reset_index(drop=True),
        check_names=False
    )