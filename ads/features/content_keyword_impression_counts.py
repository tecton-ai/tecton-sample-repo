from tecton.feature_views import stream_window_aggregate_feature_view
from tecton.feature_views.feature_view import Input
from tecton import FeatureAggregation, NewDatabricksClusterConfig
from ads.entities import content_keyword
from ads.data_sources.ad_impressions_stream import ad_impressions_stream
from datetime import datetime

cluster_config = NewDatabricksClusterConfig(
    instance_type='m4.xlarge',
    number_of_workers=4,
    extra_pip_dependencies=["tensorflow==2.2.0"],
)

@stream_window_aggregate_feature_view(
    inputs={'ad_impressions': Input(ad_impressions_stream)},
    entities=[content_keyword],
    mode='pyspark',
    aggregation_slide_period='continuous', # enable low latency streaming
    aggregations=[FeatureAggregation(column='impression', function='count', time_windows=['1h', '12h', '24h','72h','168h'])],
    batch_materialization=cluster_config,
    streaming_materialization=cluster_config,
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 1, 1),
    family='ads',
    tags={'release': 'production'},
    owner='ross@tecton.ai',
    description='The count of ad impressions for a content_keyword'
)
def content_keyword_impression_counts(ad_impressions):
    from pyspark.sql import functions as F
    from pyspark.sql.functions import lit
    from pyspark.sql.types import StringType

    def lowercase(s):
        import tensorflow as tf
        return tf.strings.lower(s).numpy().decode('UTF-8')

    lowercase_udf = F.udf(lowercase, StringType())
    return ad_impressions.select(lowercase_udf('content_keyword').alias('content_keyword'), 'timestamp').withColumn('impression', lit(1))
