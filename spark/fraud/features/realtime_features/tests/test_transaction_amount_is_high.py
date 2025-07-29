from fraud.features.realtime_features.transaction_amount_is_high import transaction_amount_is_high
import pytest
import pandas as pd


# Testing the 'transaction_amount_is_high' feature which depends on request data ('amt') as input
# To test Realtime Feature Views with Calculations, we use the get_features_for_events method,
# which evaluates the Calculation expressions on the input data.
@pytest.mark.parametrize(
    "amount,expected",
    [
        (90.0, False),
        (100.0, False),
        (110.0, True),
    ],
)
def test_transaction_amount_is_high(amount, expected):
    input_df = pd.DataFrame({
        'amt': [amount]
    })
    
    expected_df = pd.DataFrame({
        'amt': [amount],
        'transaction_amount_is_high__transaction_amount_is_high': [expected]
    })
    
    actual_df = transaction_amount_is_high.get_features_for_events(input_df).to_pandas()
    
    pd.testing.assert_frame_equal(actual_df, expected_df)
