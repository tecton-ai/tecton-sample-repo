from fraud.features.realtime_features.transaction_amount_is_high import transaction_amount_is_high
import pytest


@pytest.mark.parametrize(
    "amount,expected",
    [
        (90, False),
        (100, False),
        (110, True),
    ],
)
def test_transaction_amount_is_high(amount, expected):
    transaction_request = {'amt': amount}
    expected = {'transaction_amount_is_high': expected}

    actual = transaction_amount_is_high.run_transformation(input_data={"transaction_request": transaction_request})
    assert expected == actual
