import pandas as pd
from datetime import datetime, timezone
from recsys.features.batch_features.article_features import article_interactions

def test_article_interactions():
    # Create test data with different types of interactions (clicks, carts, orders)
    # across different articles and timestamps
    timestamps = [
        # Article 1 interactions
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),   # click
        datetime(2024, 1, 1, 11, 0, tzinfo=timezone.utc),   # cart
        datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),   # order
        datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),  # click
        # Article 2 interactions
        datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),   # click
        datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc),   # click
        datetime(2024, 3, 1, 10, 0, tzinfo=timezone.utc),   # click (outside 30d window)
    ]

    test_data = pd.DataFrame({
        'aid': pd.Series([1, 1, 1, 1, 2, 2, 2], dtype='int32'),  # Explicitly set to int32
        'type': ['click', 'cart', 'order', 'click', 'click', 'click', 'click'],
        'ts': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[ns]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 3, 2, 0, 0, tzinfo=timezone.utc)

    # Run the transformation
    result = article_interactions.run_transformation(
        start_time,
        end_time,
        mock_inputs={'sessions_batch': test_data}
    )

    # Convert TectonDataFrame to pandas DataFrame for easier testing
    result_df = result.to_pandas()

    # Verify the output schema
    assert set(result_df.columns) == {'aid', 'ts', 'type', 'interaction'}

    # Verify no rows are lost in the transformation
    assert len(result_df) == len(test_data)

    # Verify interaction counts are correct
    # For Article 1:
    article1_interactions = result_df[result_df['aid'] == 1]
    assert len(article1_interactions[article1_interactions['type'] == 'click']) == 2  # 2 clicks
    assert len(article1_interactions[article1_interactions['type'] == 'cart']) == 1   # 1 cart
    assert len(article1_interactions[article1_interactions['type'] == 'order']) == 1  # 1 order

    # For Article 2:
    article2_interactions = result_df[result_df['aid'] == 2]
    assert len(article2_interactions[article2_interactions['type'] == 'click']) == 3  # 3 clicks
    assert len(article2_interactions[article2_interactions['type'] == 'cart']) == 0   # 0 carts
    assert len(article2_interactions[article2_interactions['type'] == 'order']) == 0  # 0 orders

    # Verify all interaction values are 1
    assert (result_df['interaction'] == 1).all()

    # Verify timestamps are preserved
    result_df['ts'] = result_df['ts'].astype('datetime64[ns, UTC]')
    pd.testing.assert_series_equal(
        result_df['ts'].sort_values().reset_index(drop=True),
        test_data['ts'].sort_values().reset_index(drop=True),
        check_names=False
    ) 