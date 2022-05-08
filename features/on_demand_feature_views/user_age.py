from tecton import on_demand_feature_view, RequestSource, Field
from tecton.types import String, StructType, Int64
from features.batch_feature_views.user_date_of_birth import user_date_of_birth


request = RequestSource(schema=[Field('timestamp', String)])

@on_demand_feature_view(
    sources=[request, user_date_of_birth],
    mode='python',
    schema=[Field('user_age', Int64)],
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description="The user's age in days."
)
def user_age(request, user_date_of_birth):
    from datetime import datetime

    request_datetime = datetime.fromisoformat(request['timestamp']).replace(tzinfo=None)
    dob_datetime = datetime.fromisoformat(user_date_of_birth['user_date_of_birth'])
    time_diff = request_datetime - dob_datetime

    return {'user_age': time_diff.days}
