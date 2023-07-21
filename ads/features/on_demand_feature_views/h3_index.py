from tecton import on_demand_feature_view, RequestSource
from tecton.types import Field, Int64, String, Float64

request_schema = [Field("latitude", Float64), Field("longitude", Float64), Field("resolution", Int64)]
h3_index_request = RequestSource(schema=request_schema)
output_schema = [Field('h3_index', String)]


@on_demand_feature_view(
    sources=[h3_index_request],
    mode='python',
    schema=output_schema,
    environments=['tecton-python-extended:0.1'],
    owner='pooja@tecton.ai',
    tags={'release': 'production'},
)
def h3_index(request):
    import h3
    h3_index = h3.geo_to_h3(request["latitude"], request["longitude"], request["resolution"])
    result = {'h3_index': h3_index}
    return result
