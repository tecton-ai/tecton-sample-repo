from tecton import RequestSource, RealtimeFeatureView, Calculation
from tecton.types import String, Timestamp, Float64, Field, Bool
from fraud.features.stream_features.user_transaction_amount_metrics import user_transaction_amount_metrics

request_schema = [Field('amt', Float64)]
transaction_request = RequestSource(name='transaction_request', schema=request_schema)

transaction_amount_is_higher_than_average = RealtimeFeatureView(
    name='transaction_amount_is_higher_than_average',
    sources=[transaction_request, user_transaction_amount_metrics],
    features=[
        Calculation(
            name='transaction_amount_is_higher_than_average',
            expr='transaction_request.amt > COALESCE(user_transaction_amount_metrics.amt_mean_1d_10m, 0.0)',
        )
    ],
    description='The transaction amount is higher than the 1 day average.'
)
