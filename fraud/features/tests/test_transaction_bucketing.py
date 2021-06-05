from fraud.features.transaction_bucketing import transaction_bucketing
import pandas as pd
from pandas.testing import assert_frame_equal

# Testing the 'transaction_bucketing' feature which depends on request data as input
def test_transaction_bucketing():
    transaction_request = pd.DataFrame([
        [15000, 0, 0, 0, 0, 1],
        [5000, 1, 0, 0, 0, 0],
        [40000, 0, 1, 0, 0, 0]
    ], columns=['amount', 'type_CASH_IN', 'type_CASH_OUT', 'type_DEBIT', 'type_PAYMENT', 'type_TRANSFER'])
    expected = pd.DataFrame([
        [2, 'transfer'],
        [0, 'credit'],
        [4, 'debit']
    ], columns=['amount_bucket', 'type_bucket'])

    actual = transaction_bucketing.run(transaction_request=transaction_request)
    assert_frame_equal(actual, expected)
