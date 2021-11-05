num_of_querys = 2
import sys



def make_Tripod_input(i):
    partition_inf_path = "datas/{}/partition_inf".format(i)
    remote_duration_inf_path = "datas/{}/tasks_duration".format(i)
    cached_duration_inf_path = "datas/{}/cached_tasks_duration".format(i)
    stage_input_inf = "datas/{}/stage_file_inf".format(i)
    all_stages = set()
    result = "datas/{}/{}".format(i, i)

    partition_inf = {}
    duration_inf = {}
    stage_input = {}
    stages_has_input = set()
    # partition_inf
    f = open(partition_inf_path, "r+", encoding="utf-8")
    for line in f:
        content = line.strip().split(":")
        file_name = content[0]
        _range = content[-1]
        _start = int(_range.split("-")[0])
        task_size = int(_range.split("-")[1])
        _end = _start + int(_range.split("-")[1])
        if file_name not in partition_inf:
            partition_inf[file_name] = {}
            partition_inf[file_name]["start"] = sys.maxsize
            partition_inf[file_name]["end"] = 0
            partition_inf[file_name]["task_size"] = 0
        partition_inf[file_name]["start"] = min(partition_inf[file_name]["start"], _start)
        partition_inf[file_name]["end"] = max(partition_inf[file_name]["end"], _end)
        partition_inf[file_name]["task_size"] = max(partition_inf[file_name]["task_size"], task_size)
    f.close()
    for file in partition_inf:
        partition_inf[file]["size"] = partition_inf[file]["end"] - partition_inf[file]["start"]

    # duration_inf
    f = open(remote_duration_inf_path, "r+", encoding="utf-8")
    for line in f:
        content = line.strip().split(":")
        stage = content[0]
        all_stages.add(stage)
        task_id = content[1]
        duration = int(content[3])
        condition = content[4]
        if stage not in duration_inf:
            duration_inf[stage] = {}
            duration_inf[stage]["remote_durations"] = []
            duration_inf[stage]["cached_durations"] = []
        if condition == "remote":
            duration_inf[stage]["remote_durations"].append(duration)
        else:
            duration_inf[stage]["cached_durations"].append(duration)
    f.close()

    f = open(cached_duration_inf_path, "r+", encoding="utf-8")
    for line in f:
        content = line.strip().split(":")
        stage = content[0]
        task_id = content[1]
        duration = int(content[3])
        condition = content[4]
        if stage not in duration_inf:
            duration_inf[stage] = {}
            duration_inf[stage]["remote_durations"] = []
            duration_inf[stage]["cached_durations"] = []
        if condition == "remote":
            duration_inf[stage]["remote_durations"].append(duration)
        else:
            duration_inf[stage]["cached_durations"].append(duration)
    f.close()
    for stage in duration_inf:
        duration_inf[stage]["num_of_tasks"] = len(duration_inf[stage]["remote_durations"])
        if len(duration_inf[stage]["remote_durations"]) > 0:
            assert len(duration_inf[stage]["remote_durations"]) == duration_inf[stage]["num_of_tasks"]
            duration_inf[stage]["avg_remote_duration"] = int(sum(duration_inf[stage]["remote_durations"]) / len(duration_inf[stage]["remote_durations"]))
        if len(duration_inf[stage]["remote_durations"]) > 0:
            assert len(duration_inf[stage]["remote_durations"]) == duration_inf[stage]["num_of_tasks"]
            duration_inf[stage]["avg_cached_duration"] = int(sum(
                duration_inf[stage]["cached_durations"]) / len(duration_inf[stage]["cached_durations"]))

    # stage input inf
    f = open(stage_input_inf, "r+", encoding="utf-8")
    for line in f:
        content = line.strip().split(":")
        stage_id = content[0]
        files = content[1].split(",")
        if stage_id not in stage_input:
            stage_input[stage_id] = []
        for file in files:
            stage_input[stage_id].append(file)
        stages_has_input.add(stage_id)
    f.close()

    # record
    f = open(result, "w+", encoding="utf-8")
    f.write("stage_id,num_of_tasks,task_remote_duration,task_cached_duration,input_file,input_size, task_size\n")
    for stage in all_stages:
        num_of_tasks = duration_inf[stage]["num_of_tasks"]
        task_remote_duration = duration_inf[stage]["avg_remote_duration"]
        task_cached_duration = duration_inf[stage]["avg_cached_duration"]
        input_file = ""
        input_size = 0
        start_range = 0
        task_size = 0
        if stage in stages_has_input:
            assert 1 == len(stage_input[stage])
            input_file = stage_input[stage][0]
            input_size = partition_inf[input_file]["size"]
            start_range = partition_inf[input_file]["start"]
            task_size = partition_inf[input_file]["task_size"]
        f.write("{},{},{},{},{},{},{},{}\n".format(stage, num_of_tasks, task_remote_duration, task_cached_duration, input_file,
                                           start_range, input_size, task_size))