from tecton import Attribute, realtime_feature_view
from tecton.v09_compat import RequestSource, on_demand_feature_view
from tecton.types import String, Timestamp, Float64, Field
from fraud.features.batch_features.user_home_location import user_home_location

request_schema = [
    Field('lat', Float64),
    Field('long', Float64),
]
request = RequestSource(schema=request_schema)
output_schema = [Field('dist_km', Float64)]

@realtime_feature_view(
    sources=[request, user_home_location],
    mode='python',
    features=[Attribute("dist_km", dtype=Float64)],
    description="How far a transaction is from the user's home",
    environments=['tecton-python-extended:0.1', 'tecton-python-extended:0.2']
)
def transaction_distance_from_home(request, user_home_location):
    from haversine import haversine

    user = (user_home_location['lat'], user_home_location['long'])
    transaction = (request['lat'],request['long'])
    distance = haversine(user, transaction) # In kilometers

    return {'dist_km': distance}
