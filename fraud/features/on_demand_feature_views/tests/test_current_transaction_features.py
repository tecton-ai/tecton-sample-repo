from fraud.features.on_demand_feature_views.current_transaction_features import transaction_amount_features
from fraud.features.on_demand_feature_views.current_transaction_features import transaction_user_features
import pytest


@pytest.mark.parametrize(
    "daily_mean,amount,expected",
    [
        (100, 200, [True, True]),
        (100, 10, [False, False]),
        (75, 100, [False, True]),
    ],
)
def test_transaction_amount_features(daily_mean, amount, expected):
    user_transaction_amount_metrics = {'amt_mean_1d_10m': daily_mean}
    transaction_request = {'amt': amount}

    actual = transaction_amount_features.test_run(
        transaction_request=transaction_request,
        user_transaction_amount_metrics=user_transaction_amount_metrics)

    expected = {'transaction_amount_is_high': expected[0], 'transaction_amount_is_higher_than_avg': expected[1]}
    assert expected == actual


def test_transaction_user_features():
    user_date_of_birth = {'USER_DATE_OF_BIRTH': '1992-12-05'}
    user_home_location = {'lat': 45.7597, 'long': 4.8422}
    lat = 48.8567
    long = 2.3508
    request = {'timestamp': '2021-05-14T00:00:00.000+00:00', 'lat': lat, 'long': long, 'amt': 40}

    actual = transaction_user_features.test_run(transaction_request=request, user_date_of_birth=user_date_of_birth, user_home_location=user_home_location)
    expected = {'user_age': 10387, 'dist_km': 392.2172595594006}
    assert actual == expected