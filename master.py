# -*- coding: utf-8 -*-

from concurrent import futures
import argparse
import random
import master_pb2
import master_pb2_grpc
import grpc
import time
import collections
import ast
from multiprocessing import Process
from configuration import (
    inverted_index_path,
    word_count_path,
    # word_count_map,

#     word_count_reducer,
#     inverted_index_map,
#     inverted_index_reducer,
    mapper_tasks_path,
#     reducer_task_path,
#     configuration_path,
)
import store_pb2
import store_pb2_grpc
import worker_pb2
import worker_pb2_grpc
import os
import pickle
from constants import (
    INITIAL_STAGE,
    INTERMEDIATE_STAGE,
    FINAL_STAGE,
    TASK_INVERTED_INDEX,
    TASK_WORD_COUNT,
    DATA_STORE_PORT,
    WORKER_PORTS
)

from dependency_manager import Dependencies


class MasterServicer(master_pb2_grpc.MasterServicer):

    def ping_master(self, request, context):
        response = master_pb2.ping_response()
        response.data = f"Yes I am listening on port {request.data}"
        return response

    def run_map_reduce(self, request, context):
        log.write("STARTING MAP REDUCE AT MASTER")
        # print(request.workers)
        # print('store', request.store)
        response = master_pb2.final_response()
        response.data = "1"
        print(request)
        run(request.workers, request.store, request.map_f, request.reduce_f)
        return response

log = Dependencies.log()

clusters = collections.defaultdict(list)
processes = collections.defaultdict(list)
worker_stubs = collections.defaultdict(list)
store_stub = None
worker_stubs = []
store_stub = None


def connect_datastore():
    global store_stub
    channel = grpc.insecure_channel(f'127.0.0.1:{DATA_STORE_PORT}')
    store_stub =  store_pb2_grpc.GetSetStub(channel)
    log.write('Master channel established with the store', 'debug')


def connect_worker(ip, port):
    global worker_stubs
    channel = grpc.insecure_channel(f'{ip}:{port}')
    worker_stub =  worker_pb2_grpc.WorkerStub(channel)
    worker_stubs.append(worker_stub)
    log.write(f'Master connected to worker at {ip}:{port}', 'debug')
    
def command_to_store(value, stage = INITIAL_STAGE):
    log.write('Making RPC call to store')
    global store_stub
    request = store_pb2.Request()
    request.operation = value
    request.stage = stage 
    response = store_stub.operation(request)
    log.write('Returning from command_to_store')
    return response.data

def serve(port):

    # global clusters, servers
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    master_pb2_grpc.add_MasterServicer_to_server(MasterServicer(), server)

    failed = server.add_insecure_port(f"[::]:{port}")
    if failed != 0:
        server.start()
        log.write(f"Started master server. Listening on {port}.", "debug")
    # else:
    #     if cluster_id in clusters:
    #         del clusters[cluster_id]
    #     log.write(f"Failed to start server at port: {port}", "error")
    #     log.write(
    #         f"Make sure it is not already running, use destroy command to destory the cluster",
    #         "critical",
    #     )

    # servers.append(server)
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


def ping_server(port, ip="127.0.0.1"):
    try:
        with grpc.insecure_channel(f"{ip}:{port}") as channel:
            stub = worker_pb2_grpc.WorkerStub(channel)
            request = worker_pb2.ping_request(data=port)
            response = stub.ping(request)
            log.write(response.data)
    except:
        log.write(f"Unable to ping to {ip}:{port}")

def save_initial_data(key, data):
    log.write(f"SAVE INITIAL DATA: Trying to store initial data from mapper {key}")

    command_to_store(f"set {key} {data}", INITIAL_STAGE)


def save_intermediate_data(key, data):
    log.write(f"SAVE INTERMEDIATE DATA: Trying to store initial data from mapper {key}")
    command_to_store(f"set {key} {data}", INTERMEDIATE_STAGE)


def save_final_data(key, data):
    log.write(f"SAVE FINAL DATA: Trying to store initial data from mapper {key}")
    command_to_store(f"set {key} {data}", FINAL_STAGE)


def combined_for_reducer(data, cluster_id, task_type):
    task_type =10
    log.write("Combining the data - START")
    combined = []
    unique_keys = set()
    for l in data:
        for item in l:
            unique_keys.add(item.key)
            combined.append((item.key, item.value))

    combined.sort(key=lambda x: x[0])
    unique_keys = list(unique_keys)

    for index, key in enumerate(unique_keys):
        key_wise = []
        for item in combined:
            if item[0] == key:
                key_wise.append((key, item[1]))

        key = f"{cluster_id}:combiner:{task_type}:{index}"
        command_to_store(f'set {key} {key_wise}', INTERMEDIATE_STAGE)

    log.write("Combining the data - END")
    return index


def convert_to_proto_format(list_of_tuples):

    response_list = []
    tup = worker_pb2.tuple()
    print(list_of_tuples)
    for key,value in list_of_tuples:
        tup = worker_pb2.tuple()
        tup.key = key
        tup.value = value
        response_list.append(tup)
    return response_list


def run_map_red(cluster_id, task_type, map_func, reduce_func):
    log.write("Map reduce started")
    global stubs
    # stub_list = stubs.get(int(cluster_id), 0)
    task_count = run_map_chunks(cluster_id, map_func, reduce_func, task_type )
    # reducer_task_count = combined_for_reducer(data)
    # request = worker_pb2.reducer_request()

    # reduce_func_path = (
    #     word_count_reducer if task_type == TASK_WORD_COUNT else inverted_index_reducer
    # )
    # with open(reduce_func_path, "rb") as f:
    #     request.reducer_function = f.read()
    # log.write("Reducing - STARTED")
    # for i in range(reducer_task_count):
    #     selected_reducer = (i + 1) % len(worker_list)
    #     with open(f"{reducer_task_path}task{i}", "rb") as task_file:
    #         current_task = pickle.load(task_file)
    #         #            log.write('current_task for the reducer', current_task)
    #         request.result.extend(convert_to_proto_format(current_task[1]))
    #         result = stub_list[selected_reducer].worker_reducer(request)

    # log.write(
    #     "=====================FINAL RESULT OF MAP REDUCE========================",
    #     "debug",
    # )
    # log.write(result, "debug")
    # log.write("Reducing - END")
    # for tup in result.result:
    #     save_final_data(f"{tup.key}", f"{tup.value}")

    # log.write("=======================================================================")


def create_mapper_data(path, task, cluster_id=0):

    log.write(f'Reading from path {path}', 'infl')
    if not os.path.isfile(path) and task == TASK_WORD_COUNT:
        log.write("The provided path is not a file", "critical")
        return -1
    elif not os.path.isdir(path) and task == TASK_INVERTED_INDEX:
        log.write("The provided path is not a directory", "critical")
        return -1
    else:
        files = (
            [path]
            if task == TASK_WORD_COUNT
            else [f"{path}/{file_name}" for file_name in os.listdir(path)]
        )
        task_count = 0
        for file in files:
            with open(file, "r") as f:
                not_done = True
                while not_done:
                    line = f.readlines(10)
                    if len(line) > 0:
                        with open(f"{mapper_tasks_path}task{task_count}", "wb") as task_file:
                            save_initial_data(f'{cluster_id}:mapper:{task}:{task_count}', (file, line))
                        task_count += 1
                    else:
                        not_done = False
        return task_count

def run_reduce(cluster_id, task_type, task_count, map_func, reduce_func):
    global worker_stubs
    for i in range(task_count):
        key = f'{cluster_id}:combiner:{task_type}:{i}'
        print('gettign data for the key', key)
        get_data = ast.literal_eval(command_to_store(f'get {key}', INTERMEDIATE_STAGE))
        # reduce_func_path = word_count_reducer if task_type == TASK_WORD_COUNT else inverted_index_reducer
        # print(get_data)
                # selected_reducer = (i + 1) % len(worker_list)
        
            #            log.write('current_task for the reducer', current_task)
        request = worker_pb2.reducer_request()
        request.reducer_function = reduce_func
        # with open(reduce_func_path, "rb") as f:
        #     request.reducer_function = f.read()
        request.result.extend(convert_to_proto_format(get_data))
        result = worker_stubs[0].worker_reducer(request)
        print(result)



def run_map_chunks(cluster_id, map_func, reduce_func, task_type):
    global worker_stubs
    log.write("Dividing data in chunks- START")
    path = word_count_path if task_type == TASK_WORD_COUNT else inverted_index_path
    tasks_count = create_mapper_data(path, task_type, cluster_id)
    # print(map_func)
    
    data_list = []
    for i in range(tasks_count):
        task_key = f'{cluster_id}:mapper:{task_type}:{i}'

        file_name, data = ast.literal_eval(command_to_store(f'get {task_key}'))
        request = worker_pb2.mapper_request()
        request.file_name = file_name
        request.map_function = map_func
        request.lines.extend(data)


        # func_path = (word_count_map if task_type == TASK_WORD_COUNT else inverted_index_map)
        # print(worker_stubs[0].worker_map)
        # with open(func_path, "rb") as f:
        #     request.map_function = f.read()
                #                log.write('Making request to the mapper')
        random_worker = random.randint(0,1)
        
        response = worker_stubs[random_worker].worker_map(request)
        
        result = list(response.result)
        command_to_store(f'set {cluster_id}:reducer:{task_type}:{i} {result}', INTERMEDIATE_STAGE)
        data_list.append(result)
    
    task_count = combined_for_reducer(data_list, cluster_id, task_type)
    
    run_reduce(cluster_id, task_type, task_count, map_func, reduce_func)
    
    



        
    # stub_list = stubs.get(int(cluster_id), 0)
    # mapper_data = []
    # for i in range(tasks_count):
    #     with open(f"{mapper_tasks_path}task{i}", "rb") as task_file:
    #         current_task = pickle.load(task_file)
    #         selected_mapper = (i + 1) % len(worker_list[cluster_id])
    #         if stub_list and len(stub_list) > 1:
    #             stub = stub_list[selected_mapper]
    #             request = worker_pb2.mapper_request()
    #             #                log.write(f'Sending mapper task to node {selected_mapper}')
    #             request.file_name = current_task[0]
    #             func_path = (
    #                 word_count_map
    #                 if task_type == TASK_WORD_COUNT
    #                 else inverted_index_map
    #             )
    #             # Reading the map function for the given task
    #             with open(func_path, "rb") as f:
    #                 request.map_function = f.read()
    #             request.lines.extend(current_task[1])
    #             #                log.write('Making request to the mapper')
    #             response = stub.worker_map(request)
    #             result = list(response.result)
    #             mapper_data.append(result)
    # #    save_initial_data(f'{cluster_id}:{task_type}', mapper_data)
    # log.write("Dividing data in chunks- END")
    # return mapper_data


def print_running_cluster():
    global clusters
    log.write(f"Running clusters", "error")
    for cluster in clusters:
        if len(clusters[cluster]) != 0:
            log.write(f"Cluster ID:{cluster}", "error")


def run(worker_addresses, store_address, map_func, reduce_func):
    
    global store_stub
    connect_datastore()
    for item in worker_addresses:
        connect_worker(item.ip, item.port)
    run_map_red(0, TASK_INVERTED_INDEX, map_func, reduce_func )



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Worker for the map reduce')
    parser.add_argument("port", help="port for the worker", type=int)
    args = parser.parse_args()
    serve(args.port)

