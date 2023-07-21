from tecton import on_demand_feature_view, RequestSource
from tecton.types import Field, Int64, String, Float64

request_schema = [Field("n", Int64), Field("k", Int64)]
permutation_request = RequestSource(schema=request_schema)
output_schema = [Field("permutation", Float64), Field("combination", Int64)]


@on_demand_feature_view(
    sources=[permutation_request],
    mode='python',
    schema=output_schema,
    environments=['tecton-python-extended:0.1'],
    owner='pooja@tecton.ai',
    tags={'release': 'production'},
)
def permutations_combinations(request):
    from scipy.special import comb
    from scipy.special import perm
    combinations = comb(request['n'], request['k'], exact=False, repetition=True)
    permutations = perm(request['n'], request['k'], exact=True)
    result = {'permutation': combinations, 'combination': permutations}
    return result
