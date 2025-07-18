from tecton import FeatureService
from fraud.features.realtime_features.user_to_user_distance import user_to_user_distance
from fraud.features.realtime_features.transaction_distance_from_home import transaction_distance_from_home

user_distance_feature_service = FeatureService(
    name='user_distance_feature_service',
    description='Feature Service for computing distance related features based on user location.',
    features=[user_to_user_distance, transaction_distance_from_home],
    realtime_environment='tecton-core-1.1.0',
    online_serving_enabled=True
)