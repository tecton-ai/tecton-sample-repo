from tecton import RequestSource, on_demand_feature_view
from tecton.types import String, Timestamp, Float64, Field
from fraud.features.batch_features.user_home_location import user_home_location

# On-Demand Feature Views require enabling Snowpark.
# Contact Tecton for assistance in enabling this feature.


request_schema = [
    Field('lat', Float64),
    Field('long', Float64),
]
request = RequestSource(schema=request_schema)
output_schema = [Field('dist_km', Float64)]

@on_demand_feature_view(
    sources=[request, user_home_location],
    mode='python',
    schema=output_schema,
    description="How far a transaction is from the user's home"
)
def transaction_distance_from_home(request, user_home_location):
    from math import sin, cos, sqrt, atan2, radians

    user_lat = user_home_location['lat']
    user_long = user_home_location['long']

    transaction_lat = request['lat']
    transaction_long = request['long']

    # approximate radius of earth in km
    R = 6373.0

    user_lat = radians(52.2296756)
    user_long = radians(21.0122287)
    transaction_lat = radians(52.406374)
    transaction_long = radians(16.9251681)

    dlon = transaction_long - user_long
    dlat = transaction_lat - user_lat

    a = sin(dlat / 2)**2 + cos(user_lat) * cos(transaction_lat) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return {'dist_km': distance}
