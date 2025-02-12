from tecton import RequestSource, realtime_feature_view, Attribute
from tecton.types import String, Timestamp, Float64, Field
from fraud.features.batch_features.user_home_location import user_home_location

request_schema = [
    Field('lat', Float64),
    Field('long', Float64),
]
request = RequestSource(schema=request_schema)
features = [Attribute('dist_km', Float64)]

@realtime_feature_view(
    sources=[request, user_home_location],
    mode='python',
    features=features,
    description="How far a transaction is from the user's home",
    environments=['tecton-core-1.1.0']
)
def transaction_distance_from_home(request, user_home_location):
    from haversine import haversine

    user = (user_home_location['lat'], user_home_location['long'])
    transaction = (request['lat'],request['long'])
    distance = haversine(user, transaction) # In kilometers

    return {'dist_km': distance}
