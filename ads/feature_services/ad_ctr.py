from tecton import FeatureService

from ads.features.stream_features.user_push_dynamo import user_push_dynamo_no_transform
from ads.features.stream_features.keyword_push_dynamo import keyword_push_dynamo_wafv


ad_ctr_dynamo_feature_service = FeatureService(
    name='ad_ctr_dynamo_feature_service',
    description='A FeatureService providing features for a model that predicts if a user will click an ad.',
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    online_serving_enabled=True,
    features=[
       user_push_dynamo_no_transform,
       keyword_push_dynamo_wafv
    ],
)
