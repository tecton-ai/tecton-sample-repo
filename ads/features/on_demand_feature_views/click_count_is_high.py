from tecton import on_demand_feature_view, RequestSource
from tecton.types import Field, Int64, String, Bool

request_schema = [Field("click_count", Int64)]
click_count_request = RequestSource(schema=request_schema)
output_schema = [Field('click_count_is_high', Bool)]

1
@on_demand_feature_view(
    sources=[click_count_request],
    mode='python',
    schema=output_schema,
    owner='pooja@tecton.ai',
    tags={'release': 'production'},
)
def click_count_is_high(request):
    result = {'click_count_is_high': request["click_count"] > 100}
    return result
