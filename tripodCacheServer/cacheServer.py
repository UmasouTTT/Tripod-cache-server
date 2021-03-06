import math
import sys
from time import *
import boto3
import fcntl
from time import *
from metadata import *

objectStore = ObjectStore()
access_key = "4OJ6S588FKUF9X55IEX4"
secret_key = "CU3NQAPy8qhe12z3TPPHzHwpcXH39xw635ukS0Lk"
endpoint_url = "http://10.10.1.1:8080"

bucket_name = 'testuserinputdata'

Query_type = 1

num_of_unit_slices = 512 / 32

# client = boto3.client(service_name='s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
#                           endpoint_url=endpoint_url,
#                           verify=False, use_ssl=False)

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
                cached_partitions = str(stage_inf[0]) + ":\n"
                f.write(cached_partitions)
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
    slices = []
    for prefetch_plan in prefetch_files:

        if 0 != len(slices):
            # 512m or diff file
            if num_of_unit_slices == len(slices) or prefetch_plan[0] != slices[0][0]:
                # union files
                file = slices[0][0]
                start = slices[-1][1]
                end = slices[0][2]
                belong_infs = []
                for slice in slices:
                    belong_infs.extend(slice[3])
                try:
                    load_partial_object(file, start, end, belong_infs)
                    load_file("/proj/ccjs-PG0/tpch-spark/TripodConfig/stagesCachedCondition.txt")
                except:
                    pass
                # update
                slices.clear()

        slices.append(prefetch_plan)

    if 0 != len(slices):
        # union files
        file = slices[0][0]
        start = slices[-1][1]
        end = slices[0][2]
        belong_infs = []
        for slice in slices:
            belong_infs.extend(slice[3])
        load_partial_object(file, start, end, belong_infs)
        load_file("/proj/ccjs-PG0/tpch-spark/TripodConfig/stagesCachedCondition.txt")
        # update
        slices.clear()



def get_prefetch_files(file_path):
    f = open(file_path, "r+", encoding="utf8")
    prefetch_files = []
    for line in f:
        content = line.strip().split(",")
        file_name = content[0]

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

# param
Query_idx = 8

# # prefetch_files: [[filename, stage_id, start, end, partition_id]...]
num_of_stages = 16
file_path = "../generate_simulate_data/datas/{}/cache_plan.txt".format(Query_idx)
start_time = time.time()
prefetch_files = get_prefetch_files(file_path)
start(prefetch_files, num_of_stages)
end_time = time.time()
print(end_time - start_time)
print(stage_cached_partition)













