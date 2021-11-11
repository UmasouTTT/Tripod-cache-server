import boto3
from botocore.client import Config
import sys
import time
access_key = "4OJ6S588FKUF9X55IEX4"
secret_key = "CU3NQAPy8qhe12z3TPPHzHwpcXH39xw635ukS0Lk"

endpoint_url = "http://10.10.1.1:8080"
is_secure = False
#boto3.set_stream_logger(name='botocore')
client = boto3.client(service_name='s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                    endpoint_url=endpoint_url,
                    verify=False, use_ssl=False)


obj_name = sys.argv[2]
bucket_name = "inputdata"

if sys.argv[1] == "put":
        f = open(obj_name, 'r')
        data = f.read()
        print ("Putting Object")
        client.create_bucket(Bucket=bucket_name)
        res = client.put_object(Bucket=bucket_name, Key=obj_name, Body=data)
        print (res)


if sys.argv[1] == "get":
        print ("Getting Object")
        t1 = time.time()
        res = client.get_object(Bucket=bucket_name, Key=obj_name)
        print ("len of response:" +str(len(res['Body'].read())))
        print ("It took:", time.time()-t1)

if sys.argv[1] == "range_get":
        print ("Getting Object")
        t1 = time.time()
        res = client.get_object(Bucket=bucket_name, Key=obj_name, Range="bytes=40399536128-40433090560")
        print ("len of response:" +str(len(res['Body'].read())))
        print ("It took:", time.time()-t1)

if sys.argv[1] == "prefetch":
        print ("Getting Object")
        header = {'prefetch': 'value'}
        set_header = (lambda **kwargs: kwargs['params']['headers'].update(header))
        client.meta.events.register('before-call.s3.GetObject', set_header)
        res = client.get_object(Bucket=bucket_name, Key=obj_name)
        print (res)
        #print (res['Body'].read())
        print ("len of response:" +str(len(res['Body'].read())))



