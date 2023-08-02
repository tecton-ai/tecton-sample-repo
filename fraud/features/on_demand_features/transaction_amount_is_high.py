from tecton import RequestSource, on_demand_feature_view
from tecton.types import Field, Float64, Int64

transaction_request = RequestSource(schema=[Field("amt", Float64)])

output_schema = [Field("transaction_amount_is_high", Int64)]


@on_demand_feature_view(
    sources=[transaction_request],
    mode="python",
    schema=output_schema,
    description="Whether the transaction amount is considered high (over $100)",
)
def transaction_amount_is_high(transaction_request):
    result = {}
    result["transaction_amount_is_high"] = int(transaction_request["amt"] >= 100)
    return result
