from fraud.features.on_demand_feature_views.transaction_amount_is_higher_than_average import transaction_amount_is_higher_than_average
import pandas as pd
from pandas.testing import assert_frame_equal

# Testing the 'transaction_amount_is_higher_than_average' feature which takes in request data ('amount')
# and a precomputed feature ('amount_mean_24h_coninuous') as inputs
def test_transaction_amount_is_higher_than_average():
    transaction_request = pd.DataFrame({'amount': [124, 10001, 34235436234]})
    user_transaction_amount_metrics = pd.DataFrame({'amount_mean_24h_10m': [42, 10002, 3435]})

    actual = transaction_amount_is_higher_than_average.run(transaction_request=transaction_request, user_transaction_amount_metrics=user_transaction_amount_metrics)
    expected = pd.DataFrame({'transaction_amount_is_higher_than_average': [True, False, True]})

    assert_frame_equal(actual, expected)
