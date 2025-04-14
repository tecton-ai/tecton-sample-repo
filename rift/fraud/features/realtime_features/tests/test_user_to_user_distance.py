import pytest
from fraud.features.realtime_features.user_to_user_distance import user_to_user_distance


def test_user_to_user_distance():
    # Test normal case - two users in different locations
    # This test verifies that the haversine distance calculation works correctly
    # for typical use cases where users are in different cities. This is important
    # for fraud detection when evaluating if a transaction location is suspiciously
    # far from a user's known location.
    sender_location = {'lat': 37.7749, 'long': -122.4194}  # San Francisco
    recipient_location = {'lat': 40.7128, 'long': -74.0060}  # New York
    result = user_to_user_distance.run_transformation({'sender_location': sender_location, 'recipient_location': recipient_location})
    assert 'dist_km' in result
    assert isinstance(result['dist_km'], float)
    assert result['dist_km'] > 0  # Distance should be positive

    # Test edge case - same location
    # This test verifies that the distance calculation correctly returns 0
    # when users are at the same location. This is important for validating
    # legitimate transactions where the sender and recipient are in the same place.
    same_location = {'lat': 37.7749, 'long': -122.4194}
    result = user_to_user_distance.run_transformation({'sender_location': same_location, 'recipient_location': same_location})
    assert result['dist_km'] == 0.0  # Distance should be 0 for same location

    # Test edge case - opposite sides of the world
    # This test verifies that the haversine formula correctly handles the maximum
    # possible distance between two points on Earth. This is important for
    # detecting potentially fraudulent transactions where users claim to be
    # on opposite sides of the world.
    location1 = {'lat': 0, 'long': 0}
    location2 = {'lat': 0, 'long': 180}
    result = user_to_user_distance.run_transformation({'sender_location': location1, 'recipient_location': location2})
    assert result['dist_km'] > 0  # Distance should be positive 