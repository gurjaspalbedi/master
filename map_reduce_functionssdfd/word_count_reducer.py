# -*- coding: utf-8 -*-


def reduce_func(reducer_list):
    py_counter = {}
    for tup in reducer_list:
        py_counter[tup.key] = py_counter.get(tup.key,0) +  1
    return py_counter