from fraud.features.transaction_bucketing import transaction_bucketing
import pandas as pd
from pandas.testing import assert_frame_equal

# # Testing the 'transaction_amount_is_high' feature which depends on request data ('amount') as input
# def test_transaction_amount_is_high():
#     transaction_request = pd.DataFrame({'amount': [124, 10001, 34235436234]})

#     actual = transaction_amount_is_high.run(transaction_request=transaction_request)
#     expected = pd.DataFrame({'transaction_amount_is_high': [0, 1, 1]})

#     assert_frame_equal(actual, expected)

# def test_transaction_bucketing():
#     transaction_request = pd.DataFrame([
#         [15000, 0, 0, 0, 0, 1]
#     ], columns=['amount', 'type_CASH_IN', 'type_CASH_OUT', 'type_DEBIT', 'type_PAYMENT', 'type_TRANSFER'])

#     print(transaction_bucketing.inputs.keys())
#     actual = transaction_bucketing.run(transaction_request=transaction_request)
#     expected = pd.DataFrame({'transaction_bucketing': [2, 'transfer']})

#     print(actual)

#     assert_frame_equal(actual, expected)