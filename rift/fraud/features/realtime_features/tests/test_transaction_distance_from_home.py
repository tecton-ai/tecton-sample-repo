import pytest
from fraud.features.realtime_features.transaction_distance_from_home import transaction_distance_from_home


def test_transaction_distance_from_home():
    # Test normal case - transaction far from home
    request = {'lat': 37.7749, 'long': -122.4194}  # San Francisco
    user_home = {'lat': 40.7128, 'long': -74.0060}  # New York
    result = transaction_distance_from_home.run_transformation({'request': request, 'user_home_location': user_home})
    assert 'dist_km' in result
    assert isinstance(result['dist_km'], float)
    assert result['dist_km'] > 0  # Distance should be positive

    # Test edge case - transaction at home
    home_location = {'lat': 37.7749, 'long': -122.4194}
    result = transaction_distance_from_home.run_transformation({'request': home_location, 'user_home_location': home_location})
    assert result['dist_km'] == 0.0  # Distance should be 0 for same location

    # Test edge case - transaction on opposite side of the world
    home = {'lat': 0, 'long': 0}
    transaction = {'lat': 0, 'long': 180}
    result = transaction_distance_from_home.run_transformation({'request': transaction, 'user_home_location': home})
    assert result['dist_km'] > 0  # Distance should be positive 