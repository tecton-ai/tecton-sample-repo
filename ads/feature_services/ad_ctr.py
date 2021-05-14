from tecton import FeatureService
from ads.features.user_ad_impression_counts import user_ad_impression_counts

ad_ctr_feature_service = FeatureService(
    name='ad_ctr_feature_service',
    description='A FeatureService providing features for a model that predicts if a user will click an ad.',
    family='ads',
    tags={'release': 'production'},
    owner="matt@tecton.ai",
    features=[
        user_ad_impression_counts,
    ],
)
