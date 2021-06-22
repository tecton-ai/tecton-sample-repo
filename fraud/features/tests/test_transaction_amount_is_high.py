from fraud.features.on_demand_feature_views.transaction_amount_is_high import transaction_amount_is_high
import pandas as pd
from pandas.testing import assert_frame_equal

# Testing the 'transaction_amount_is_high' feature which depends on request data ('amount') as input
def test_transaction_amount_is_high():
    transaction_request = pd.DataFrame({'amount': [124, 10001, 34235436234]})

    actual = transaction_amount_is_high.run(transaction_request=transaction_request)
    expected = pd.DataFrame({'transaction_amount_is_high': [0, 1, 1]})

    assert_frame_equal(actual, expected)
