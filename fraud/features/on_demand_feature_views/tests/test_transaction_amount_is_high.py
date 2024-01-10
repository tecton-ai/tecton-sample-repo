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


# Testing the 'user_age' feature which takes in request data ('timestamp')
# and a precomputed feature ('USER_DATE_OF_BIRTH') as inputs
# def test_user_age():
#     user_date_of_birth = {'USER_DATE_OF_BIRTH': '1992-12-05'}
#     request = {'timestamp': '2021-05-14T00:00:00.000+00:00'}

#     actual = transaction_user_features.test_run(request=request, user_date_of_birth=user_date_of_birth)
#     expected = {'user_age': 10387}
#     assert actual == expected