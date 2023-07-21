from tecton import FeatureService
from ads.features.on_demand_feature_views.similarity import fuzzy_similarity
from ads.features.on_demand_feature_views.h3_index import h3_index

h3_feature_service = FeatureService(
    name='h3_feature_service',
    tags={'release': 'production'},
    owner='pooja@tecton.ai',
    on_demand_environment="tecton-python-extended:0.1",
    online_serving_enabled=True,
    features=[
        fuzzy_similarity,
        h3_index
    ],
)
