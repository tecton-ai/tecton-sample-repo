from tecton import FeatureService
from ads.features.on_demand_feature_views.similarity import fuzzy_similarity
from ads.features.on_demand_feature_views.permutations import permutations_combinations
from ads.features.on_demand_feature_views.click_count_is_high import click_count_is_high


similarity_feature_service = FeatureService(
    name='similarity_feature_service',
    tags={'release': 'production'},
    owner='pooja@tecton.ai',
    on_demand_environment="tecton-python-extended:0.1",
    online_serving_enabled=True,
    features=[
       fuzzy_similarity
    ],
)
