import boto3
import urllib.parse
import json

def lambda_handler(event, context):
    print("🔍 Event received:")
    print(json.dumps(event, indent=4))

    glue = boto3.client('glue')

    record = event['Records'][0]
    bucket_name = record['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(record['s3']['object']['key'])

    cleaned_key = object_key.replace('.csv', '_cleaned.csv')

    response = glue.start_job_run(
        JobName='cleaning-bank-data-trigger',
        Arguments={
            '--INPUT_BUCKET': bucket_name,
            '--INPUT_KEY': object_key,
            '--OUTPUT_BUCKET': 'bank-output-data-trigger',
            '--OUTPUT_KEY': cleaned_key
        }
    )

    print(f"Started Glue job {response['JobRunId']} for {object_key}")

    return {
        'statusCode': 200,
        'body': f"Glue job triggered for {object_key}"
    }
