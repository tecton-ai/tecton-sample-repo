from tecton import RequestSource, realtime_feature_view, Attribute
from tecton.types import String, Int64, Field
from fraud.features.batch_features.user_date_of_birth import user_date_of_birth


request_schema = [Field('timestamp', String)]
request = RequestSource(schema=request_schema)
features = [Attribute('user_age', Int64)]

@realtime_feature_view(
    sources=[request, user_date_of_birth],
    mode='python',
    features=features,
    description="The user's age in days."
)
def user_age(request, user_date_of_birth):
    from datetime import datetime, date

    request_datetime = datetime.fromisoformat(request['timestamp']).replace(tzinfo=None)
    dob_datetime = datetime.fromisoformat(user_date_of_birth['user_date_of_birth'])

    td = request_datetime - dob_datetime

    return {'user_age': td.days}
