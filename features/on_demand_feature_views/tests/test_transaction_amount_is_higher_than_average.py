from features.on_demand_feature_views.transaction_amount_is_higher_than_average import transaction_amount_is_higher_than_average
import pandas as pd
from pandas.testing import assert_frame_equal

# Testing the 'transaction_amount_is_higher_than_average' feature which takes in request data ('amount')
# and a precomputed feature ('amount_mean_24h_coninuous') as inputs
def test_transaction_amount_is_higher_than_average():
    test_amounts = [124, 10001, 10000000, 0]
    test_transaction_amount_metrics = [42, 10002, 3435, None]
    expected_results = [True, False, True, False]
    for test_amount,test_transaction_amount_metric,expected_result in zip(test_amounts,test_transaction_amount_metrics,expected_results):
        transaction_request = {'amount': test_amount}
        user_transaction_amount_metrics = {'amount_mean_24h_10m': test_transaction_amount_metric}
        expected = {'transaction_amount_is_higher_than_average': expected_result}

        actual = transaction_amount_is_higher_than_average.run(transaction_request=transaction_request, user_transaction_amount_metrics=user_transaction_amount_metrics)
        assert expected == actual
