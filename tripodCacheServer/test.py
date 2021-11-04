import boto3
region = 'us-east-1' # define your region here
bucketname = 'test'  # define bucket
key = 'objkey' # s3 file
Bytes_range = 'bytes=73-1024'
client = boto3.client('s3',region_name = region)
resp = client.get_object(Bucket=bucketname,Key=key,Range=Bytes_range)
data = resp['Body'].read()