from tecton import RequestSource, on_demand_feature_view
from tecton.types import String, Timestamp, Float64, Field
from features.batch_features.user_home_location import user_home_location

request_schema = [
    Field("merch_lat", Float64),
    Field("merch_long", Float64),
]
request = RequestSource(schema=request_schema)
output_schema = [Field("dist_km", Float64)]


@on_demand_feature_view(
    sources=[request, user_home_location],
    mode="python",
    schema=output_schema,
    description="How far a transaction is from the user's home",
)
def transaction_distance_from_home(request, user_home_location):
    from math import sin, cos, sqrt, atan2, radians

    user_lat = radians(user_home_location["lat"])
    user_long = radians(user_home_location["long"])
    transaction_lat = radians(request["merch_lat"])
    transaction_long = radians(request["merch_long"])

    # approximate radius of earth in km
    R = 6373.0

    dlon = transaction_long - user_long
    dlat = transaction_lat - user_lat

    a = sin(dlat / 2) ** 2 + cos(user_lat) * cos(transaction_lat) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return {"dist_km": distance}
