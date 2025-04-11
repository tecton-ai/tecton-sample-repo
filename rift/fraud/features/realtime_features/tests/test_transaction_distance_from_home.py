from fraud.features.realtime_features.transaction_distance_from_home import transaction_distance_from_home
import pytest


@pytest.mark.parametrize(
    "transaction_location,user_home_location,expected_distance",
    [
        # Transaction at home
        (
            {'lat': 37.7749, 'long': -122.4194},
            {'lat': 37.7749, 'long': -122.4194},
            0.0
        ),
        # Transaction nearby
        (
            {'lat': 37.7833, 'long': -122.4167},  # Union Square
            {'lat': 37.7749, 'long': -122.4194},  # Downtown SF
            0.95  # Approximately 0.95 km
        ),
        # Transaction far away
        (
            {'lat': 40.7128, 'long': -74.0060},   # New York
            {'lat': 37.7749, 'long': -122.4194},  # San Francisco
            4129.0  # Approximately 4129 km
        ),
    ],
)
def test_transaction_distance_from_home(transaction_location, user_home_location, expected_distance):
    tolerance = 0.1
    
    actual = transaction_distance_from_home.run_transformation(
        input_data={
            "request": transaction_location,
            "user_home_location": user_home_location
        }
    )
    
    assert abs(actual['dist_km'] - expected_distance) < tolerance 