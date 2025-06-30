from awsglue.utils import getResolvedOptions
import sys
import boto3
import pandas as pd
import io


args = getResolvedOptions(sys.argv, ['INPUT_BUCKET', 'INPUT_KEY', 'OUTPUT_BUCKET'])

INPUT_BUCKET = args['INPUT_BUCKET'] 
INPUT_KEY = args['INPUT_KEY']        
OUTPUT_BUCKET = args['OUTPUT_BUCKET']
OUTPUT_KEY = INPUT_KEY.replace(".csv", "_cleaned.csv")


s3 = boto3.client('s3')
response = s3.get_object(Bucket=INPUT_BUCKET, Key=INPUT_KEY)
df = pd.read_csv(io.BytesIO(response['Body'].read()))


df.drop(columns=['contact', 'poutcome'], inplace=True)
df.replace("unknown", pd.NA, inplace=True)
df.dropna(subset=['job', 'education'], inplace=True)

age_bins = [0, 29, 60, 100]
age_labels = ['Under_30', '30_to_60', 'Above_60']
df['age_group'] = pd.cut(df['age'], bins=age_bins, labels=age_labels)

df['has_previous_contact'] = df['previous'] > 0
df['last_contact_date'] = df['day'].astype(str) + '-' + df['month'].str.title()
df['is_high_balance'] = df['balance'] > 1000


output_buffer = io.BytesIO()
df.to_csv(output_buffer, index=False)

s3.put_object(
    Bucket=OUTPUT_BUCKET,
    Key=OUTPUT_KEY,
    Body=output_buffer.getvalue(),
    ContentType='text/csv'
)

print(f" Processed {INPUT_KEY} → Saved {OUTPUT_KEY}")
