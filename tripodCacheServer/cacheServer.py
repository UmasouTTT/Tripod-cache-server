import boto3
import fcntl
from time import *

access_key = "EAXK482V9VW8E2EEQID0"
secret_key = "5w28NWrKVTQsuCEc0d3jq5iWeZ9H3LMTh3abx1Bv"

endpoint_url = "http://10.10.1.1:8080"

bucket_name = 'inputdata'

Query_type = 1

client = boto3.client(service_name='s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                          endpoint_url=endpoint_url,
                          verify=False, use_ssl=False)

stage_cached_partition = []
prefetch_plan = None




# prefetch_files: [[filename, stage_id, range, partition_id]...]
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

    # range: 1-100
    start_time = time()
    bytes_range = 'bytes=' + range
    print("Start access object: {} _ {}".format(key, range))
    req = client.get_object(Bucket=bucket_name, Key=key, Range=bytes_range)
    end_time = time()
    stage_cached_partition[stage_id].append(partition_id)
    print("Access result: {}, use time : {}".format(req, end_time - start_time))


def start(prefetch_files, num_of_stages):
    init(prefetch_files, num_of_stages)
    for prefetch_plan in prefetch_files:
        file_name = prefetch_plan[0]
        stage_id = prefetch_plan[1]
        partition_id = prefetch_plan[3]
        range_name = prefetch_plan[2]
        load_partial_object(file_name, range_name, stage_id, partition_id)

def get_prefetch_files(file_path):
    f = open(file_path, "r+", encoding="utf8")
    prefetch_files = []
    for line in f:
        content = line.split(":")
        file_name = content[0]
        partition_id = content[1]
        range = content[2]
        start = int(range.split("-")[0])
        length = int(range.split("-")[1])
        end = start + length
        prefetch_files.append([file_name, 0, "{}-{}".format(start, end), partition_id])
    f.close()
    return prefetch_files

if Query_type == 1:
    # # prefetch_files: [[filename, stage_id, range, partition_id]...]
    num_of_stages = 4
    file_path = "./datas/1/partition_inf"
    prefetch_files = get_prefetch_files(file_path)
    start(prefetch_files, 4)
    print(stage_cached_partition)













