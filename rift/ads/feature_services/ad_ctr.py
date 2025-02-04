from tecton import FeatureService
from ads.features.stream_features.user_ad_impression_counts import user_ad_impression_counts
from ads.features.stream_features.user_click_counts import user_click_counts
from ads.features.stream_features.user_impression_counts import user_impression_counts
from ads.features.batch_features.user_distinct_ad_count_7d import user_distinct_ad_count_7d


ad_ctr_feature_service = FeatureService(
    name='ad_ctr_feature_service',
    description='A FeatureService providing features for a model that predicts if a user will click an ad.',
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    online_serving_enabled=False,
    features=[
        user_click_counts,
        user_impression_counts,
        user_ad_impression_counts,
        user_distinct_ad_count_7d
    ],
)
