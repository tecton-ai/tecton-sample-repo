from tecton.v09_compat import FeatureService
from recsys.features.batch_features.article_features import article_sessions
from recsys.features.batch_features.session_features import session_approx_count_articles

recommender_feature_service = FeatureService(
    name='recommender_feature_service',
    description='Feature Service for computing article and session features for making recommendations',
    features=[article_sessions, session_approx_count_articles],
    online_serving_enabled=False
)