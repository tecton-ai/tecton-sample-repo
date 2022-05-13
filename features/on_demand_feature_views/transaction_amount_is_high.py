from tecton import on_demand_feature_view, RequestSource
from tecton.types import Int64, Bool, Field


transaction_request = RequestSource(schema=[Field('transaction_amount_is_high', Int64)])

@on_demand_feature_view(
    sources=[transaction_request],
    mode='python',
    schema=[Field('transaction_amount_is_high', Bool)],
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description='Whether the transaction amount is considered high (over $10000)'
)
def transaction_amount_is_high(transaction_request):
    return {'transaction_amount_is_high': transaction_request['amount'] >= 10000}
