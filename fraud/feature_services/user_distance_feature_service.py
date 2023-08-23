from tecton import FeatureService
from fraud.features.on_demand_feature_views.user_to_user_distance import user_to_user_distance
from fraud.features.on_demand_feature_views.transaction_distance_from_home import transaction_distance_from_home

user_distance_feature_service = FeatureService(
    name='user_distance_feature_service',
    description='Feature Service for computing distance related features based on user location.',
    features=[user_to_user_distance, transaction_distance_from_home],
    on_demand_environment='tecton-python-extended:0.2',
    online_serving_enabled=True
)