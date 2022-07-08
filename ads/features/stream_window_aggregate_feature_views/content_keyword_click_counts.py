from tecton.compat import stream_window_aggregate_feature_view
from tecton.compat import Input
from tecton.compat import FeatureAggregation, DatabricksClusterConfig
from ads.entities import content_keyword
from ads.data_sources.ad_impressions import ad_impressions_stream
from datetime import datetime

cluster_config = DatabricksClusterConfig(
    instance_type='m4.4xlarge',
    number_of_workers=4,
    extra_pip_dependencies=["tensorflow==2.2.0"],
)

@stream_window_aggregate_feature_view(
    inputs={'ad_impressions': Input(ad_impressions_stream)},
    entities=[content_keyword],
    mode='pyspark',
    aggregation_slide_period='continuous', # enable low latency streaming
    aggregations=[FeatureAggregation(column='clicked', function='sum', time_windows=['1min', '5min'])],
    batch_cluster_config=cluster_config,
    stream_cluster_config=cluster_config,
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 6, 1),
    family='ads',
    tags={'release': 'production'},
    owner='ross@tecton.ai',
    description='The count of ad impressions for a content_keyword'
)
def content_keyword_click_counts(ad_impressions):
    from pyspark.sql import functions as F
    from pyspark.sql.functions import lit
    from pyspark.sql.types import IntegerType

    def absolute(x):
        import tensorflow as tf
        return int(tf.math.abs(float(x)).numpy())

    abs_udf = F.udf(absolute, IntegerType())
    return ad_impressions.select('content_keyword', abs_udf('clicked').alias('clicked'), 'timestamp')
