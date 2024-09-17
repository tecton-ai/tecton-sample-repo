from tecton import RequestSource, on_demand_feature_view, Attribute
from tecton.types import Float64, Field, Bool

request_schema = [Field('amt', Float64)]
transaction_request = RequestSource(schema=request_schema)
features = [Attribute('transaction_amount_is_high', Bool)]

# An example of an on-demand feature view that depends only on a request source.
@on_demand_feature_view(
    sources=[transaction_request],
    mode='python',
    features=features,
    description='The transaction amount is higher than $100.'
)
def transaction_amount_is_high(transaction_request):
    return {'transaction_amount_is_high': transaction_request['amt'] > 100}
