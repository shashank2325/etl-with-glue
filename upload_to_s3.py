import boto3

s3 = boto3.client('s3')


local_file_path = 'bank_part2.csv'
bucket_name = 'bank-input-data-trigger'
object_key = 'bank_part2.csv'


s3.upload_file(local_file_path, bucket_name, object_key)

print(f"Uploaded {local_file_path} to s3://{bucket_name}/{object_key}")
