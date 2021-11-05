import math
from time import *
import boto3
import fcntl
from time import *
from metadata import *

objectStore = ObjectStore()
access_key = "4OJ6S588FKUF9X55IEX4"
secret_key = "CU3NQAPy8qhe12z3TPPHzHwpcXH39xw635ukS0Lk"
endpoint_url = "http://10.10.1.1:8080"

bucket_name = 'inputdata'

Query_type = 1

client = boto3.client(service_name='s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                          endpoint_url=endpoint_url,
                          verify=False, use_ssl=False)

stage_cached_partition = []
prefetch_plan = None

piece_of_file = 4194304


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

def load_partial_object(key, range_start, end, belong_infs):
    print("Start access object: {} _ {}-{}".format(key, range_start, end))
    bytes_range = 'byte=' + str(range_start) + "-" + str(end)
    #client.get_object(Bucket=bucket_name, Key=key, Range=bytes_range)
    objectStore.fetch_object_partial(bucket_name, key, int(range_start), int(end))
    for belong_inf in belong_infs:
        stage_cached_partition[int(belong_inf[0])].append(int(belong_inf[1]))
    # print("Access result: {}, use time : {}".format(req, end_time - start_time))


def start(prefetch_files, num_of_stages):
    init(prefetch_files, num_of_stages)
    for prefetch_plan in prefetch_files:
        file_name = prefetch_plan[0]
        start = prefetch_plan[1]
        end = prefetch_plan[2]
        belong_infs = prefetch_plan[3]
        load_partial_object(file_name, start, end, belong_infs)
        load_file("/proj/ccjs-PG0/tpch-spark/TripodConfig/stagesCachedCondition.txt")

def get_prefetch_files(file_path):
    f = open(file_path, "r+", encoding="utf8")
    prefetch_files = []
    for line in f:
        content = line.strip().split(",")
        file_name = content[0].split("/")[-1]

        range = content[1]
        start = int(range.split("-")[0])
        end = int(range.split("-")[1])

        belong_infs = []
        belong_inf = content[-1]
        belong_inf = belong_inf.strip().split("|")
        for inf in belong_inf:
            stage_partition = inf.split("-")
            belong_infs.append([stage_partition[0], stage_partition[1]])

        prefetch_files.append([file_name, start, end, belong_infs])
    f.close()
    return prefetch_files

if Query_type == 1:
    # # prefetch_files: [[filename, stage_id, start, end, partition_id]...]
    num_of_stages = 4
    file_path = "../generate_simulate_data/datas/1/cache_plan"
    start_time = time.time()
    prefetch_files = get_prefetch_files(file_path)
    start(prefetch_files, 4)
    end_time = time.time()
    print(end_time - start_time)
    print(stage_cached_partition)













