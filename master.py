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
    word_count_path
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
        response = master_pb2.final_response()
        response.data = "1"
        run(request.workers, request.store, request.map_f, request.reduce_f, request.path)
        return response

log = Dependencies.log()

clusters = collections.defaultdict(list)
processes = collections.defaultdict(list)
worker_stubs = collections.defaultdict(list)
store_stub = None
worker_stubs = []
store_stub = None


def connect_datastore(address):
    global store_stub
    channel = grpc.insecure_channel(f'{address.ip}:{address.port}')
    store_stub =  store_pb2_grpc.GetSetStub(channel)
    log.write(f'Master channel established with the store at {address.ip}:{address.port}', 'debug')


def connect_worker(ip, port, store_address):
    global worker_stubs
    channel = grpc.insecure_channel(f'{ip}:{port}')
    worker_stub =  worker_pb2_grpc.WorkerStub(channel)
    worker_stub.connect_to_store(store_address)
    worker_stubs.append(worker_stub)
    log.write(f'Master connected to worker at {ip}:{port}', 'debug')
    
def command_to_store(value, stage = INITIAL_STAGE):
    # log.write('Making RPC call to store')
    global store_stub
    request = store_pb2.Request()
    request.operation = value
    request.stage = stage 
    response = store_stub.operation(request)
    # log.write('Returning from command_to_store')
    return response.data

def serve(port):
    time.sleep(20)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    master_pb2_grpc.add_MasterServicer_to_server(MasterServicer(), server)

    failed = server.add_insecure_port(f"[::]:{port}")
    if failed != 0:
        server.start()
        log.write(f"Started master server. Listening on {port}.", "debug")
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
    # log.write(f"SAVE INITIAL DATA: Trying to store initial data from mapper {key}")

    command_to_store(f"set {key} {data}", INITIAL_STAGE)


def save_intermediate_data(key, data):
    log.write(f"SAVE INTERMEDIATE DATA: Trying to store initial data from mapper {key}")
    command_to_store(f"set {key} {data}", INTERMEDIATE_STAGE)


def save_final_data(key, data):
    log.write(f"SAVE FINAL DATA: Trying to store initial data from mapper {key}")
    command_to_store(f"set {key} {data}", FINAL_STAGE)


def combined_for_reducer(data, cluster_id):
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

        key = f"{cluster_id}:combiner:{index}"
        command_to_store(f'set {key} {key_wise}', INTERMEDIATE_STAGE)

    log.write("Combining the data - END")
    return index


def convert_to_proto_format(list_of_tuples):

    response_list = []
    tup = worker_pb2.tuple()
    for key,value in list_of_tuples:
        tup = worker_pb2.tuple()
        tup.key = key
        tup.value = value
        response_list.append(tup)
    return response_list


def run_map_red(cluster_id, map_func, reduce_func, path, worker_addresses):
    log.write("Map reduce started")
    run_map_chunks(cluster_id, map_func, reduce_func, path, worker_addresses )


def create_mapper_data(path, cluster_id=0):

    log.write(f'Reading from path {path}', 'info')
    if os.path.isfile(path):
        files = [path]
    else:
        files =  [f"{path}/{file_name}" for file_name in os.listdir(path)]
    task_count = 0
    
    for file in files:
        with open(file, "r") as f:
            not_done = True
            while not_done:
                line = f.readlines(10)
                if len(line) > 0:
                    save_initial_data(f'{cluster_id}:mapper:{task_count}', (file, line))
                    
                    task_count += 1
                else:
                    not_done = False
        return task_count

    # log.write('MAP TASK COMLETE2', 'critical')

def run_reduce(cluster_id, task_count, map_func, reduce_func):
    global worker_stubs
    for i in range(task_count):
        key = f'{cluster_id}:combiner:{i}'
        print('gettign data for the key', key)
        get_data = ast.literal_eval(command_to_store(f'get {key}', INTERMEDIATE_STAGE))

        request = worker_pb2.reducer_request()
        request.reducer_function = reduce_func

        request.result.extend(convert_to_proto_format(get_data))
        result = worker_stubs[0].worker_reducer(request)
        key = f'{cluster_id}:final:{i}'
        save_final_data(key, result.result)
        print(result)

def run_map_chunks(cluster_id, map_func, reduce_func, path, worker_addresses):
    global worker_stubs
    log.write("Dividing data in chunks- START") 
    tasks_count = create_mapper_data(path, cluster_id)
    data_list = []
    
    for i in range(tasks_count):
        task_key = f'{cluster_id}:mapper:{i}'

        file_name, data = ast.literal_eval(command_to_store(f'get {task_key}'))
        

        request = worker_pb2.mapper_request()
        request.file_name = file_name
        request.map_function = map_func
        
        request.lines.extend(data)
        worker_count = len(worker_addresses)
        random_worker = random.randint(0,worker_count)
        response = worker_stubs[random_worker].worker_map(request)
        result = list(response.result)
        command_to_store(f'set {cluster_id}:reducer:{i} {result}', INTERMEDIATE_STAGE)
        data_list.append(result)
    task_count = combined_for_reducer(data_list, cluster_id)
    
    run_reduce(cluster_id, task_count, map_func, reduce_func)


def run(worker_addresses, store_address, map_func, reduce_func, path):
    
    global store_stub
    connect_datastore(store_address)
    for item in worker_addresses:
        connect_worker(item.ip, item.port, store_address)

    print('here', 'critical')
    run_map_red(0, map_func, reduce_func, path, worker_addresses)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Worker for the map reduce')
    parser.add_argument("port", help="port for the worker", type=int)
    args = parser.parse_args()
    serve(args.port)

