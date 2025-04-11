from fraud.features.realtime_features.user_to_user_distance import user_to_user_distance
import pytest


@pytest.mark.parametrize(
    "sender_location,recipient_location,expected_distance",
    [
        # Same location
        (
            {'lat': 37.7749, 'long': -122.4194},
            {'lat': 37.7749, 'long': -122.4194},
            0.0
        ),
        # Different locations in San Francisco
        (
            {'lat': 37.7749, 'long': -122.4194},  # Downtown SF
            {'lat': 37.7833, 'long': -122.4167},  # Union Square
            0.95  # Approximately 0.95 km
        ),
        # Cross-country distance
        (
            {'lat': 37.7749, 'long': -122.4194},  # San Francisco
            {'lat': 40.7128, 'long': -74.0060},   # New York
            4129.0  # Approximately 4129 km
        ),
    ],
)
def test_user_to_user_distance(sender_location, recipient_location, expected_distance):
    tolerance = 0.1
    
    actual = user_to_user_distance.run_transformation(
        input_data={
            "sender_location": sender_location,
            "recipient_location": recipient_location
        }
    )
    
    assert abs(actual['dist_km'] - expected_distance) < tolerance 