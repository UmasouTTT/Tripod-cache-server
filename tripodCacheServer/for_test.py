import time

import boto3
import fcntl
from time import *

access_key = "8LDFY2MPBS20395TS9LE"
secret_key = "c03vMjGOJTJ8CmcorvWCQ7Dz4IiYCEQtFl54Pznv"

endpoint_url = "http://127.0.0.1:8080"

bucket_name = 'inputdata'

Query_type = 1

client = boto3.client(service_name='s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                          endpoint_url=endpoint_url,
                          verify=False, use_ssl=False)

stage_cached_partition = []
prefetch_plan = None




# prefetch_files: [[filename, stage_id, [range1, partition_id], range2 ...]...]
def init(prefetch_files, num_of_stages):
    for i in range(num_of_stages):
        stage_cached_partition.append([i])
    prefetch_plan = prefetch_files



def load_file(file_path):
    with open(file_path, 'w+') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        for stage_inf in stage_cached_partition:
            if 1 == len(stage_inf):
                continue
            else:
                cached_partitions = str(stage_inf[0]) + ":" + str(stage_inf[1])
                for partition in stage_inf[2:]:
                    cached_partitions += ","
                    cached_partitions += str(partition)
                cached_partitions += "\n"
                f.write(cached_partitions)
        fcntl.flock(f, fcntl.LOCK_UN)
    f.close()

def load_partial_object(key, range, stage_id, partition_id):
    import time
    # range: 1-100
    # start_time = time()
    bytes_range = 'bytes=' + str(range)
    print("Start access object: {} _ {} for stage {} 's partition {}".format(key, range, stage_id, partition_id))
    time.sleep(0.8)
    stage_cached_partition[stage_id].append(partition_id)
   # print("Access result: {}, use time : {}".format(req, end_time - start_time))


def start(prefetch_files, num_of_stages, file_path):
    init(prefetch_files, num_of_stages)
    for prefetch_plan in prefetch_files:
        file_name = prefetch_plan[0]
        stage_id = prefetch_plan[1]
        for range_inf in prefetch_plan[2:]:
            range_name = range_inf[0]
            partition_id = range_inf[1]
            load_partial_object(file_name, range_name, stage_id, partition_id)
            load_file(file_path)

if Query_type == 1:
    # prefetch_files: [[filename, stage_id, [range1, partition_id], range2...]...]
    file_path = "/Users/umasou/workSpace/tmp/TripodConfig/stagesCachedCondition.txt"
    num_of_stages = 4
    prefetch_files = []
    prefetch_file_1 = ["lineitem", 0, [22, 22], [21, 21], [20, 20], [19, 19], [18, 18], [17, 17], [16, 16], [15, 15],
                       [14, 14], [13, 13], [12, 12], [11, 11], [10, 10], [9, 9], [8, 8], [7, 7], [6, 6], [5, 5], [4, 4]]
    prefetch_files.append(prefetch_file_1)
    start(prefetch_files, num_of_stages, file_path)


