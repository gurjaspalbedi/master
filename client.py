# -*- coding: utf-8 -*-

import grpc
import worker_pb2_grpc

def run(ip, port):
    channel = grpc.insecure_channel(f'{ip}:{port}')
    stub = worker_pb2_grpc.WorkerStub(channel)
    return stub


if __name__ == '__main__':
    run()
