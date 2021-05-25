from tecton import RequestDataSource
from tecton.feature_views import on_demand_feature_view
from tecton.feature_views.feature_view import Input
from pyspark.sql.types import StringType, StructType, StructField, LongType
from core.features.user_date_of_birth import user_date_of_birth
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
    family='core',
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description="The user's age in days."
)
def user_age(request: pandas.DataFrame, user_date_of_birth: pandas.DataFrame):
    import pandas as pd

    request['timestamp'] = pd.to_datetime(request['timestamp'], utc=True)
    user_date_of_birth['user_date_of_birth'] = pd.to_datetime(user_date_of_birth['user_date_of_birth'], utc=True)

    df = pd.DataFrame()
    df['user_age'] = (request['timestamp'] - user_date_of_birth['user_date_of_birth']).dt.days
    return df
