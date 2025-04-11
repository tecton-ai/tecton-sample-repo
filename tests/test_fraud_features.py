import pytest
from tecton import Workspace

# Initialize the workspace
workspace = Workspace(workspace="kf_test")

def test_offline_store():
    """Test offline store functionality."""
    # Get the feature view
    feature_view = workspace.get_feature_view("user_transaction_amount_metrics")
    
    # Get the transformation function
    transform_func = feature_view.transformation()
    
    # Create a sample DataFrame with the expected schema
    df = spark.createDataFrame([
        (1, 100.0, "2024-01-01 00:00:00"),
        (1, 200.0, "2024-01-01 01:00:00"),
        (2, 300.0, "2024-01-01 00:00:00")
    ], ["user_id", "amount", "timestamp"])
    
    # Apply the transformation
    transformed_df = transform_func(df)
    
    # Verify the schema
    expected_columns = {
        "user_id",
        "amount",
        "timestamp",
        "user_transaction_amount_metrics__avg",
        "user_transaction_amount_metrics__max",
        "user_transaction_amount_metrics__min",
        "user_transaction_amount_metrics__sum",
        "user_transaction_amount_metrics__count"
    }
    assert set(transformed_df.columns) == expected_columns
    
    # Verify the data
    user1_data = transformed_df.filter("user_id = 1").collect()
    assert len(user1_data) == 2
    assert user1_data[0]["user_transaction_amount_metrics__avg"] == 150.0
    assert user1_data[0]["user_transaction_amount_metrics__max"] == 200.0
    assert user1_data[0]["user_transaction_amount_metrics__min"] == 100.0
    assert user1_data[0]["user_transaction_amount_metrics__sum"] == 300.0
    assert user1_data[0]["user_transaction_amount_metrics__count"] == 2

def test_feature_attributes():
    """Test feature attributes."""
    # Get the feature view
    feature_view = workspace.get_feature_view("user_transaction_amount_metrics")
    
    # Get the features
    features = feature_view.features()
    
    # Verify feature attributes
    assert len(features) == 6  # sum and mean over 1h, 1d, and 3d windows
    
    # Expected feature names and windows
    expected_features = [
        ('sum', '1h'), ('sum', '1d'), ('sum', '3d'),
        ('mean', '1h'), ('mean', '1d'), ('mean', '3d')
    ]
    
    # Check each feature's attributes
    for feature, (func, window) in zip(features, expected_features):
        expected_name = f"user_transaction_amount_metrics__{func}_{window}"
        assert feature.name == expected_name
        assert feature.dtype == "float64"  # Float64 type from feature view
        assert feature.description is not None
        assert feature.tags == {'release': 'production'} 