from tecton import RequestSource, on_demand_feature_view
from tecton.types import Float64, Field, Bool, String, Int64
from fraud.features.stream_features.user_transaction_amount_metrics import user_transaction_amount_metrics
from fraud.features.batch_features.user_home_location import user_home_location
from fraud.features.batch_features.user_date_of_birth import user_date_of_birth

request_schema = [
    Field('amt', Float64),
    Field('lat', Float64),
    Field('long', Float64),
    Field('timestamp', String)
]
transaction_request = RequestSource(schema=request_schema)

output_schema_transaction_amount_features = [
    Field('transaction_amount_is_high', Bool),
    Field('transaction_amount_is_higher_than_avg', Bool)
]

# An example of an on-demand feature view that depends on a request source and a stream feature view.
@on_demand_feature_view(
    sources=[transaction_request, user_transaction_amount_metrics],
    mode='python',
    schema=output_schema_transaction_amount_features,
    description='The transaction amount is higher than $100 and is higher than the mean.'
)
def transaction_amount_features(transaction_request, user_transaction_amount_metrics):
    amount_mean = 0 if user_transaction_amount_metrics['amt_mean_1d_10m'] is None else user_transaction_amount_metrics['amt_mean_1d_10m']
    return {
        'transaction_amount_is_high': transaction_request['amt'] > 100,
        'transaction_amount_is_higher_than_avg': transaction_request['amt'] > amount_mean 
    }


output_schema_transaction_user_features = [Field('dist_km', Float64), Field('user_age', Int64)]

# An example of an on-demand feature view using a custom environment
@on_demand_feature_view(
    sources=[transaction_request, user_home_location, user_date_of_birth],
    mode='python',
    schema=output_schema_transaction_user_features,
    description="How far a transaction is from the user's home and user's age at time of transaction",
    environments=['tecton-python-extended:0.2']
)
def transaction_user_features(transaction_request, user_home_location, user_date_of_birth):
    from haversine import haversine
    from datetime import datetime, date

    user = (user_home_location['lat'], user_home_location['long'])
    transaction = (transaction_request['lat'], transaction_request['long'])
    distance = haversine(user, transaction) # In kilometers

    request_datetime = datetime.fromisoformat(transaction_request['timestamp']).replace(tzinfo=None)
    dob_datetime = datetime.fromisoformat(user_date_of_birth['USER_DATE_OF_BIRTH'])

    td = request_datetime - dob_datetime

    return {'dist_km': distance, 'user_age': td.days}




