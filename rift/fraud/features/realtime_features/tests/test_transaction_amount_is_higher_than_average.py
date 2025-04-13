from fraud.features.realtime_features.transaction_amount_is_higher_than_average import transaction_amount_is_higher_than_average
import pytest


# Testing the 'transaction_amount_is_higher_than_average' feature which takes in request data ('amt')
# and a precomputed feature ('amt_mean_1d_continuous') as inputs
@pytest.mark.parametrize(
    "daily_mean,amount,expected",
    [
        (100, 200, True),
        (100, 10, False),
        (100, 100, False),
    ],
)
def test_transaction_amount_is_higher_than_average(daily_mean, amount, expected):
    # Prepare input data
    input_data = {
        'transaction_request': {'amt': amount},
        'user_transaction_amount_metrics': {'amt_mean_1d_continuous': daily_mean}
    }

    actual = transaction_amount_is_higher_than_average.run_transformation(input_data=input_data)

    expected = {'transaction_amount_is_higher_than_average': expected}
    assert expected == actual
