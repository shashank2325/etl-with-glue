import boto3
from botocore.exceptions import ClientError

s3 = boto3.client('s3')


bucket_name = 'bank-output-data-trigger'
object_key = 'bank_part3_cleaned.csv' 
local_file_path = 'bank_part3_cleaned.csv'


try:
    s3.download_file(bucket_name, object_key, local_file_path)
    print(f"Downloaded s3://{bucket_name}/{object_key} to {local_file_path}")

except ClientError as e:
    if e.response['Error']['Code'] == '404':
        print(f"File not found: {object_key}")
    else:
        print(f"AWS Error: {e}")
