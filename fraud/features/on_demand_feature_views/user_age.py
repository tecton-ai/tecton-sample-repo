from tecton import RequestDataSource, Input, on_demand_feature_view
from pyspark.sql.types import StringType, StructType, StructField, LongType
from fraud.features.batch_feature_views.user_date_of_birth import user_date_of_birth
import pandas


request_schema = StructType()
request_schema.add(StructField('timestamp', StringType()))
request = RequestDataSource(request_schema=request_schema)

output_schema = StructType()
output_schema.add(StructField('user_age', LongType()))


@on_demand_feature_view(
    inputs={
        'request': Input(request),
        'user_date_of_birth': Input(user_date_of_birth)
    },
    mode='pandas',
    output_schema=output_schema,
    family='fraud',
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description="The user's age in days."
)
def user_age(request: pandas.DataFrame, user_date_of_birth: pandas.DataFrame):
    import pandas as pd

    request['timestamp'] = pd.to_datetime(request['timestamp'], utc=True)
    user_date_of_birth['user_date_of_birth'] = pd.to_datetime(user_date_of_birth['user_date_of_birth'], utc=True)

    df = pd.DataFrame()
    df['user_age'] = (request['timestamp'] - user_date_of_birth['user_date_of_birth']).dt.days.astype('int64')
    return df
