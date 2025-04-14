from tecton import batch_feature_view, Attribute
from tecton.types import String
import pandas as pd
from datetime import datetime, timedelta

from fraud.entities import user
from fraud.data_sources.fraud_users import fraud_users_batch


@batch_feature_view(
    sources=[fraud_users_batch.unfiltered()],
    entities=[user],
    mode='pandas',
    online=False,
    offline=False,
    # Note the timestamp is the signup date, hence the old start_time.
    feature_start_time=datetime(2017,1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=3650),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='User date of birth, entered at signup.',
    features=[
        Attribute('user_date_of_birth', String)
    ],
    timestamp_field='timestamp'
)
def user_date_of_birth(fraud_users_batch):
    import pandas as pd  # Import pandas inside the function to ensure it's available in the execution context
    
    # Ensure we have a pandas DataFrame
    if not isinstance(fraud_users_batch, pd.DataFrame):
        fraud_users_batch = pd.DataFrame(fraud_users_batch)
    
    df = fraud_users_batch.copy()
    
    # Try different date formats
    date_formats = ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y']
    
    def parse_date(date_str):
        for fmt in date_formats:
            try:
                return pd.to_datetime(date_str, format=fmt).strftime('%Y-%m-%d')
            except:
                continue
        return None
    
    # Apply date parsing with multiple formats
    df['user_date_of_birth'] = df['dob'].apply(parse_date)
    
    # Drop rows with invalid dates
    df = df.dropna(subset=['user_date_of_birth'])
    
    # Rename signup_timestamp to timestamp
    df = df.rename(columns={'signup_timestamp': 'timestamp'})
    
    # Select required columns
    return df[['user_id', 'user_date_of_birth', 'timestamp']]
