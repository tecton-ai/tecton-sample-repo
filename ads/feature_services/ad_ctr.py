from tecton import FeatureService
from ads.features.user_ad_impression_counts import user_ad_impression_counts
from ads.features.user_click_counts import user_click_counts
from ads.features.user_ctr_7d import user_ctr_7d
from ads.features.user_impression_counts import user_impression_counts
from core.features.user_date_of_birth import user_date_of_birth
from core.features.user_age import user_age

ad_ctr_feature_service = FeatureService(
    name='ad_ctr_feature_service',
    description='A FeatureService providing features for a model that predicts if a user will click an ad.',
    family='ads',
    tags={'release': 'production'},
    owner="matt@tecton.ai",
    features=[
        user_ctr_7d,
        user_age,
        user_date_of_birth,
        user_click_counts,
        user_impression_counts,
        user_ad_impression_counts,
    ],
)
