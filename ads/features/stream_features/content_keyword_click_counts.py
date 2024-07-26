from tecton import stream_feature_view, FilteredSource, DatabricksClusterConfig, StreamProcessingMode, Aggregate
from tecton.types import Field, Bool

from ads.entities import content_keyword
from ads.data_sources.ad_impressions import ad_impressions_stream
from datetime import datetime, timedelta

cluster_config = DatabricksClusterConfig(
    instance_type='m4.4xlarge',
    number_of_workers=4,
    extra_pip_dependencies=["tensorflow==2.2.0"],
)

@stream_feature_view(
    source=FilteredSource(ad_impressions_stream),
    entities=[content_keyword],
    mode='pyspark',
    stream_processing_mode=StreamProcessingMode.CONTINUOUS, # enable low latency streaming
    features=[
        Aggregate(input_column=Field('column', Bool), function='count', time_window=timedelta(minutes=1)),
        Aggregate(input_column=Field('column', Bool), function='count', time_window=timedelta(minutes=5)),
    ],
    timestamp_field='timestamp',
    batch_compute=cluster_config,
    stream_compute=cluster_config,
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
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
