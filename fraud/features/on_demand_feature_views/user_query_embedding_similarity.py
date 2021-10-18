from tecton import RequestDataSource, Input, on_demand_feature_view
from pyspark.sql.types import StructType, StructField, DoubleType, ArrayType
from fraud.features.feature_tables.user_embeddings import user_embeddings
import pandas


request_schema = StructType()
request_schema.add(StructField('query_embedding', ArrayType(DoubleType())))
request = RequestDataSource(request_schema=request_schema)

output_schema = StructType()
output_schema.add(StructField('cosine_similarity', DoubleType()))


@on_demand_feature_view(
    inputs={
        'request': Input(request),
        'user_embedding': Input(user_embeddings)
    },
    mode='pandas',
    output_schema=output_schema,
    family='fraud',
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description="Computes the cosine similarity between a query embedding and a precomputed user embedding."
)
def user_query_embedding_similarity(request: pandas.DataFrame, user_embedding: pandas.DataFrame):
    import pandas as pd
    import numpy as np
    from numpy.linalg import norm

    @np.vectorize
    def cosine_similarity(a, b):
        # Handle the case where there is no precomputed user embedding.
        if a is None or b is None:
            return -1.0

        return np.dot(a, b)/(norm(a)*norm(b))

    df = pd.DataFrame()
    df["cosine_similarity"] = cosine_similarity(user_embedding["user_embedding"], request["query_embedding"])

    return df
