# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-

def reduce_func(reducer_list):
    py_counter = {}
    for tup in reducer_list:
        key = tup.key
        value_list = tup.value
        value = py_counter.get(key,[]) + [value_list]
        py_counter[key] = list(set(value))
    return py_counter