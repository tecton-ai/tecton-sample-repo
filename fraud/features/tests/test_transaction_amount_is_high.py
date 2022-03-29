from fraud.features.on_demand_feature_views.transaction_amount_is_high import transaction_amount_is_high

# Testing the 'transaction_amount_is_high' feature which depends on request data ('amount') as input
def test_transaction_amount_is_high():
    test_amounts = [124, 10001, 10000000]
    expected_results = [0, 1, 1]
    for test_amount,expected_result in zip(test_amounts,expected_results):
        transaction_request = {'amount': test_amount}
        expected = {'transaction_amount_is_high': expected_result}

        actual = transaction_amount_is_high.run(transaction_request=transaction_request)
        assert expected == actual
