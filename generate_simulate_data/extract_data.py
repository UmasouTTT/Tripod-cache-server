import os
from makeTripodInput import *

def extractPartialFiles(file_path):
    f = open(file_path, "r+", encoding="utf-8")
    partial_files = {}
    for line in f:
        if "Tripod Input split" in line:
            content = line.split(" ")
            partition_id = int(content[7])
            file_inf = content[-1]
            file_content = file_inf.split(":")
            file_name = file_content[1]
            file_range = file_content[2]
            start_range = file_range.split("+")[0]
            end_range = file_range.split("+")[1]
            range = start_range + "-" + end_range
            if file_name not in partial_files:
                partial_files[file_name] = {}
            assert partition_id not in partial_files[file_name]
            partial_files[file_name][partition_id] = range
    f.close()

    # record
    f = open(partition_inf_path, "w+", encoding="utf-8")
    for file_name in partial_files.keys():
        for partition_id in partial_files[file_name].keys():
            f.write("{}:{}:{}".format(file_name, partition_id, partial_files[file_name][partition_id]))
    f.close()

def trans_time(time_str):
    content = time_str.split(":")
    return int(content[0]) * 3600 + int(content[1]) * 60 + int(content[2])

def extract_task_duration(file_path, out_path, is_remote):
    condition = "cached"
    if is_remote:
        condition = "remote"
    # 21/11/04 09:48:02 INFO TaskSetManager: Starting task 4.0 in stage 0.0 (TID 0, localhost, executor driver, partition 4, PROCESS_LOCAL, 7957 bytes)
    # 21/11/04 09:48:03 INFO Executor: Finished task 4.0 in stage 0.0 (TID 0). 2259 bytes result sent to driver

    # {stage_id:{task_id:{"start":, "end":, "partition_id":}}}
    tasks_durations = {}
    f = open(file_path, "r+", encoding="utf-8")
    for line in f:
        if "Starting task" in line:
            content = line.split(" ")
            start_time = trans_time(content[1])
            task_id = content[6]
            stage_id = content[9]
            partition_id = content[16]
            if stage_id not in tasks_durations:
                tasks_durations[stage_id] = {}
            assert task_id not in tasks_durations[stage_id]
            tasks_durations[stage_id][task_id] = {}
            tasks_durations[stage_id][task_id]["start"] = start_time
            tasks_durations[stage_id][task_id]["end"] = None
            tasks_durations[stage_id][task_id]["use_time"] = None
            tasks_durations[stage_id][task_id]["partition_id"] = partition_id
        if "Finished task" in line:
            content = line.split(" ")
            end_time = trans_time(content[1])
            task_id = content[6]
            stage_id = content[9]
            use_time = int(content[13])
            assert stage_id in tasks_durations
            assert task_id in tasks_durations[stage_id]
            tasks_durations[stage_id][task_id]["end"] = end_time
            tasks_durations[stage_id][task_id]["use_time"] = use_time
    f.close()

    # show avg time
    for stage in tasks_durations:
        whole_time = 0
        for task in tasks_durations[stage]:
            whole_time += tasks_durations[stage][task]["use_time"]
        print("Stage {} avg task time is {}".format(stage, whole_time / len(tasks_durations[stage])))

    # record
    f = open(out_path, "w+", encoding="utf-8")
    for stage in tasks_durations:
        for task in tasks_durations[stage]:
            f.write("{}:{}:{}:{}:{}\n".format(int(float(stage)), int(float(task)), tasks_durations[stage][task]["partition_id"],
                                         tasks_durations[stage][task]["use_time"],
                                            condition))
    f.close()


query_index = 8
# {filename:{partition_id:range}}
partition_inf_path = "datas/{}/partition_inf".format(query_index)
remote_duration_path = "datas/{}/tasks_duration".format(query_index)
cached_duration_path = "datas/{}/cached_tasks_duration".format(query_index)
extractPartialFiles("datas/{}/remote.log".format(query_index))
extract_task_duration("datas/{}/remote.log".format(query_index), remote_duration_path, True)
extract_task_duration("datas/{}/cached.log".format(query_index), cached_duration_path, False)
make_Tripod_input(query_index)
