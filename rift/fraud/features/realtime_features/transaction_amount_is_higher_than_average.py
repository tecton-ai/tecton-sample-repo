from tecton import RequestSource, realtime_feature_view, Attribute
from tecton.types import String, Timestamp, Float64, Field, Bool
from fraud.features.stream_features.user_transaction_amount_metrics import user_transaction_amount_metrics

request_schema = [Field('amt', Float64)]
transaction_request = RequestSource(schema=request_schema)
features = [Attribute('transaction_amount_is_higher_than_average', Bool)]

@realtime_feature_view(
    sources=[transaction_request, user_transaction_amount_metrics],
    mode='python',
    features=features,
    description='The transaction amount is higher than the 1 day average.'
)
def transaction_amount_is_higher_than_average(transaction_request, user_transaction_amount_metrics):
    amount_mean = 0 if user_transaction_amount_metrics['amt_mean_1d_continuous'] is None else user_transaction_amount_metrics['amt_mean_1d_continuous']
    return {'transaction_amount_is_higher_than_average': transaction_request['amt'] > amount_mean}
