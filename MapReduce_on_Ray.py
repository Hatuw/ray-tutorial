#!/usr/bin/env python
# coding=utf-8
import numpy as np
import ray
import time

# this comand will print `node_ip_address`, `redis_address`...
#   after ray.init()
ray.init(num_cpus=4, include_webui=False, ignore_reinit_error=True)


# define map function
def map_serial(function, xs):
    return [function(x) for x in xs]


"""
Exercise 1
"""
# parallel map
def map_parallel(function, xs):
    """Apply a remote function to each element of a list."""
    if not isinstance(xs, list):
        raise ValueError('The xs argument must be a list.')

    if not hasattr(function, 'remote'):
        raise ValueError('The function argument must be a remote function.')

    # return [function(x) for x in xs]
    return [function.remote(x) for x in xs]


def increment_regular(x):
    return x + 1


@ray.remote
def increment_remote(x):
    return x + 1

xs = [i for i in range(1, 6)]  # 1, 2, ..., 5
result_ids = map_parallel(increment_remote, xs)

assert isinstance(result_ids, list), \
    'The output of "map_parallel" must be a list.'

assert all([isinstance(x, ray.ObjectID) for x in result_ids]), \
    'The output of map_parallel must be a list of ObjectIDs.'

assert ray.get(result_ids) == map_serial(increment_regular, xs)

print(xs)
print(ray.get(result_ids))

print('Congratulations, the test passed!')


"""
Exercise 2
"""
def sleep_regular(x):
    time.sleep(1)
    return x + 1


@ray.remote
def sleep_remote(x):
    time.sleep(1)
    return x + 1

# Regular sleep should take 4 seconds.
print('map_serial:')
results_serial = map_serial(sleep_regular, [1, 2, 3, 4])

# Initiaing the map_parallel should be instantaneous.
print('\ncalling map_parallel:')
result_ids = map_parallel(sleep_remote, [1, 2, 3, 4])

print('\ngetting results from map_parallel:')
results_parallel = ray.get(result_ids)

assert results_parallel == results_serial


def reduce_serial(function, xs):
    if len(xs) == 1:
        return xs[0]

    result = xs[0]
    for i in range(1, len(xs)):
        result = function(result, xs[i])

    return result


def add_regular(x, y):
    time.sleep(0.3)
    return x + y

assert reduce_serial(add_regular, [1, 2, 3, 4, 5, 6, 7, 8]) == 36

"""
Exercise 3
"""
def reduce_parallel(function, xs):
    if not isinstance(xs, list):
        raise ValueError('The xs argument must be as list.')
    
    if not hasattr(function, 'remote'):
        raise ValueError('The function argument must be a remote function.')

    if len(xs) == 1:
        return xs[0]
    
    result = xs[0]
    for i in range(1, len(xs)):
        result = function(result, xs[i])

    return result


@ray.remote
def add_remote(x, y):
    time.sleep(0.3)
    return x + y

xs = [1, 2, 3, 4, 5, 6, 7, 8]
result_id = reduce_parallel(add_remote, xs)
assert ray.get(result_id) == reduce_serial(add_regular, xs)
print('Congratulations, the test passed!')


"""
Exercise 4
"""
def reduce_parallel_tree(function, xs):
    if not isinstance(xs, list):
        raise ValueError('The xs argument must be a list.')

    if not hasattr(function, 'remote'):
        raise ValueError('The function argument must be a remote fcuntion.')

    raise NotImplementedError

xs = np.arange(1, 9)
print(xs)
result_id = reduce_parallel_tree(add_remote, xs)
assert ray.get(result_id) == reduce_serial(add_regular, xs)

"""
Exercise 5
"""
print('reduce_serial:')
results_serial = reduce_serial(add_regular, [1, 2, 3, 4, 5, 6, 7, 8])

print('\ncalling reduce_parallel:')
result_ids = reduce_parallel(add_remote, [1, 2, 3, 4, 5, 6, 7, 8])

print('\ngetting results from reduce_parallel:')
results_parallel = ray.get(result_ids)

assert results_parallel == results_serial

print('\ncalling reduce_parallel_tree')
result_tree_ids = reduce_parallel_tree(add_remote, [1, 2, 3, 4, 5, 6, 7, 8])

print('\ngetting results from reduce_parallel_tree:')
results_parallel_tree = ray.get(result_tree_ids)

assert results_parallel_tree == results_serial
