from tecton import on_demand_feature_view
from tecton import Input
from pyspark.sql.types import DoubleType, StructType, StructField
from ads.features.stream_window_aggregate_feature_views.user_click_counts import user_click_counts
from ads.features.stream_window_aggregate_feature_views.user_impression_counts import user_impression_counts
import pandas


output_schema = StructType([
    StructField('user_ctr_7d', DoubleType()),
    StructField('user_ctr_7d_scaled', DoubleType())
])


@on_demand_feature_view(
    inputs={
        'user_click_counts': Input(user_click_counts),
        'user_impression_counts': Input(user_impression_counts)
    },
    mode='pandas',
    output_schema=output_schema,
    family='ads',
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description="The user's click through rate over the last 7 days."
)
def user_ctr_7d(user_click_counts: pandas.DataFrame, user_impression_counts: pandas.DataFrame):
    import pandas as pd

    df = pd.DataFrame()
    df['user_ctr_7d'] = user_click_counts['clicked_sum_168h_1h'] / user_impression_counts['impression_count_168h_1h']
    df['user_ctr_7d_scaled'] = (user_click_counts['clicked_sum_168h_1h'] / user_impression_counts['impression_count_168h_1h']) * 100

    return df

@on_demand_feature_view(
    inputs={
        'user_click_counts': Input(user_click_counts),
        'user_impression_counts': Input(user_impression_counts)
    },
    mode='pandas',
    output_schema=output_schema,
    family='ads',
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description="The user's click through rate over the last 7 days."
)
def user_ctr_7d_2(user_click_counts: pandas.DataFrame, user_impression_counts: pandas.DataFrame):
    import pandas as pd

    df = pd.DataFrame()
    df['user_ctr_7d'] = user_click_counts['clicked_sum_168h_1h'] / user_impression_counts['impression_count_168h_1h']
    df['user_ctr_7d_scaled'] = (user_click_counts['clicked_sum_168h_1h'] / user_impression_counts['impression_count_168h_1h']) * 100

    return df

