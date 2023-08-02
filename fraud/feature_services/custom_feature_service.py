from tecton import FeatureService

from features.on_demand_features.struct_field import my_odfv
from features.on_demand_features.nested_array import my_odfv2
from features.on_demand_features.array_struct import my_odfv3

fraud_detection_feature_service = FeatureService(
    name="custom_feature_service",
    online_serving_enabled=True,
    features=[
        my_odfv,
        my_odfv2,
        my_odfv3
    ],
)
