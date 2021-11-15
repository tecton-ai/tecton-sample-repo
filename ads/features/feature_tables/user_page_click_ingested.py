from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType
from tecton.feature_table.feature_table import FeatureTable
from ads.entities import content

# Schema that will be used for ingesting every row
# in the feature table.
schema = StructType([
    StructField("content_id", StringType()),
    StructField("page_id", StringType()),
    StructField("clicked", LongType()),
    StructField("timestamp", TimestampType()),
])


# Feature Table Definition.
# Online and Offline Materialization is enabled
# Entity is content id and this will be used to retrieve feature values
# Serving TTL is 30d so any rows older than 30d will not be used in serving.
ft = FeatureTable(
    name="user_page_click_feature_table",
    entities=[content],
    schema=schema,
    online=False,
    offline=True,
    ttl="30d",
    description="Test features",
    owner="rohit@tecton.ai",
    family='ads',
    tags={'release': 'production'},
)
