from tecton import RequestSource, realtime_feature_view, Attribute
from tecton.types import String, Timestamp, Float64, Field
from fraud.features.batch_features.user_home_location import user_home_location

# This on-demand feature uses the same user feature view as two separate inputs. The join keys
# for the on-demand feature view are a "sender_id" and "recipient_id".
@realtime_feature_view(
    sources=[
        user_home_location,
        user_home_location.with_join_key_map({"user_id": "recipient_id"}),
    ],
    mode='python',
    features=[Attribute('dist_km', Float64)],
    description="How far apart two users' home locations are.",
    environments=['tecton-core-1.2.1']
)
def user_to_user_distance(sender_location, recipient_location):
    from haversine import haversine

    sender = (sender_location['lat'], sender_location['long'])
    recipient = (recipient_location['lat'], recipient_location['long'])
    distance = haversine(sender, recipient) # In kilometers

    return {'dist_km': distance}
