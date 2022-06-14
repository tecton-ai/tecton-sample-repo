from fraud.features.on_demand_feature_views.transaction_amount_is_higher_than_average import transaction_amount_is_higher_than_average
import pytest

# Testing the 'transaction_amount_is_higher_than_average' feature which takes in request data ('amt')
# and a precomputed feature ('amt_mean_1d_10m') as inputs
@pytest.mark.parametrize(
    "daily_mean,amount,expected",
    [
        (100, 200, True),
        (100, 10, False),
        (100, 100, False),
    ],
)
def test_transaction_amount_is_higher_than_average(daily_mean, amount, expected):
    user_transaction_amount_metrics = {'amt_mean_1d_10m': daily_mean}
    transaction_request = {'amt': amount}

    actual = transaction_amount_is_higher_than_average.run(
        transaction_request=transaction_request,
        user_transaction_amount_metrics=user_transaction_amount_metrics)

    expected = {'transaction_amount_is_higher_than_average': expected}
    assert expected == actual
