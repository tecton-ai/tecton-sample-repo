from tecton import RequestSource, on_demand_feature_view
from tecton.types import Field, Array, Float64
from tecton import FeatureService


request_schema = [Field('query_embedding', Array(Float64))]
request = RequestSource(schema=request_schema)

output_schema = [Field('mean', Float64)]


@on_demand_feature_view(
    sources=[request],
    mode='python',
    schema=output_schema,
    owner='achal@tecton.ai',
    tags={'release': 'production'},
    description="Computes the cosine similarity between a query embedding and a precomputed user embedding."
)
def user_query_embedding_similarity(request):
    import numpy as np

    result = np.mean(request)

    return result


ad_ctr_feature_service = FeatureService(
    name='odfv_fs',
    description='A FeatureService providing features for a model that predicts if a user will click an ad.',
    tags={'release': 'production'},
    owner='t-rex@tecton.ai',
    online_serving_enabled=True,
    features=[
        user_query_embedding_similarity
    ],
)
