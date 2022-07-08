from tecton.compat import RequestDataSource, Input, on_demand_feature_view
from pyspark.sql.types import StringType, StructType, StructField, LongType
from fraud.features.batch_feature_views.user_date_of_birth import user_date_of_birth

request_schema = StructType([
    StructField('timestamp', StringType())
])
request = RequestDataSource(request_schema=request_schema)

output_schema = StructType([
    StructField('user_age', LongType())
])


@on_demand_feature_view(
    inputs={
        'request': Input(request),
        'user_date_of_birth': Input(user_date_of_birth)
    },
    mode='python',
    output_schema=output_schema,
    family='fraud',
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description="The user's age in days."
)
def user_age(request, user_date_of_birth):
    from datetime import datetime, date

    request_datetime = datetime.fromisoformat(request['timestamp']).replace(tzinfo=None)
    dob_datetime = datetime.fromisoformat(user_date_of_birth['user_date_of_birth'])

    td = request_datetime - dob_datetime

    result = {}
    result['user_age'] = td.days
    return result
