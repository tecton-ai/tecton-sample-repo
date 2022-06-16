from tecton import RequestSource, on_demand_feature_view
from tecton.types import String, Timestamp, Float64, Field
from fraud.features.batch_features.user_home_location import user_home_location

# This on-demand feature uses the same user feature view as two separate inputs. The join keys
# for the on-demand feature view are a "sender_id" and "recipient_id".
@on_demand_feature_view(
    sources=[
        user_home_location.with_join_key_map({"user_id": "sender_id"}),
        user_home_location.with_join_key_map({"user_id": "recipient_id"}),
    ],
    mode='python',
    schema=[Field('dist_km', Float64)],
    description="How far apart two users' home locations are."
)
def user_to_user_distance(sender_location, recipient_location):
    from math import sin, cos, sqrt, atan2, radians

    sender_lat = radians(sender_location['lat'])
    sender_long = radians(sender_location['long'])
    recipient_lat = radians(recipient_location['lat'])
    recipient_long = radians(recipient_location['long'])

    # approximate radius of earth in km
    R = 6373.0

    dlon = recipient_long - sender_long
    dlat = recipient_lat - sender_lat

    a = sin(dlat / 2)**2 + cos(recipient_lat) * cos(sender_lat) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return {'dist_km': distance}
