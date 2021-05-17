from tecton import RequestDataSource
from tecton.feature_views import on_demand_feature_view
from tecton.feature_views.feature_view import Input
from pyspark.sql.types import DoubleType, StructType, StructField
from ads.features.user_click_counts import user_click_counts
from ads.features.user_impression_counts import user_impression_counts
import pandas


output_schema = StructType()
output_schema.add(StructField("user_ctr_7d", DoubleType()))


@on_demand_feature_view(
    inputs={
        "user_click_counts": Input(user_click_counts),
        "user_impression_counts": Input(user_impression_counts)
    },
    mode="pandas",
    output_schema=output_schema,
    family="ads",
    owner="matt@tecton.ai",
    tags={"release": "production"},
    description="The user's click through rate over the last 7 days."
)
def user_ctr_7d(user_click_counts: pandas.DataFrame, user_impression_counts: pandas.DataFrame):
    import pandas as pd

    df = pd.DataFrame()
    df['user_age'] = user_click_counts['clicked_sum_168h_1h'] / user_impression_counts['impression_count_168h_1h']
    return df
