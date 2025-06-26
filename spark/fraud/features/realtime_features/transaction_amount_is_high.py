from tecton import RequestSource, RealtimeFeatureView, Calculation
from tecton.types import Float64, Field, Bool

request_schema = [Field('amt', Float64)]
transaction_request = RequestSource(name='transaction_request', schema=request_schema)

transaction_amount_is_high = RealtimeFeatureView(
    name='transaction_amount_is_high',
    sources=[transaction_request],
    features=[
        Calculation(
            name='transaction_amount_is_high',
            expr='transaction_request.amt > 100',
        )
    ],
    description='The transaction amount is higher than $100.'
)
