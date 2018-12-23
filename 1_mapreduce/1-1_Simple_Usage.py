#!/usr/bin/env python
# coding=utf-8
from __future__ import print_function
import sys
import ray
import time

py_version = sys.version[0]
assert py_version in ['2', '3'], "Your Python version is not support."


# put and get Ray's vars
def put_and_get():
    x = "Hello ray"
    x_id = ray.put(x)
    print("\nObjectId: {}\n".format(x_id))
    # >> Output: ObjectId(xxxx)

    x_res = ray.get(x_id)
    print("Object Value: {}\n".format(x_res))
    # >> Output: Hello ray


# compare serial with parallel
def serial_and_parallel():
    # sleep 5 sec and return
    def simple_func():
        time.sleep(5)
        return 0

    # sleep 5 sec and return,
    # (decorate with ray.remote)
    @ray.remote
    def simple_func_on_ray():
        time.sleep(5)
        return 0

    # serial:
    tic = time.time()
    [simple_func() for _ in range(4)]
    toc = time.time()
    print("serial exec time: {}\n".format(toc - tic))
    # >> Output: 20.014071226119995

    # parallel:
    tic = time.time()
    object_ids = [simple_func_on_ray.remote() for _ in range(4)]
    ray.get(object_ids)
    toc = time.time()
    print("parallel exec time: {}\n".format(toc - tic))
    # >> Output: 5.0089240074157715

    # print ObjectIds
    print("ObjectIds:\n {}\n".format(object_ids))
    """ Output:
    ObjectID(0100000069672b193fb42e9ead39fb73e8a656ba)
    ObjectID(010000002c31d522f2ce9969946e38d2c928160d)
    ObjectID(010000005a0910e5c35bd4e5d5447b880f34172a)
    ObjectID(01000000e9e04c5ea8b146c730f77fb706528d1d)
    """

# Map: x -> x^2
# Reduce: sum of Map's output
def my_MapReduce():
    global py_version

    # map serial
    # [0, 1, 2, 3, 4] --> [0, 1, 4, 9, 16]
    def square_local(x):
        time.sleep(1)
        return x ** 2
    # map_serial_res = map(lambda x: x ** 2, range(5))
    map_serial_res = map(square_local, range(5))

    # In Python3.x, map func will return a iterator
    if py_version == '3':
        map_serial_res = [i for i in map_serial_res]

    print("Serial Map result: {}".format(map_serial_res))


    # reduce serial
    # import reduce from functools if py's version is 3.x
    if py_version == '3':
        from functools import reduce
    else:
        global reduce

    def sum_local(x, y):
        time.sleep(1)
        return x + y

    # reduce_serial_res = reduce(lambda x, y: x + y, map_serial_res)
    reduce_serial_res = reduce(sum_local, map_serial_res)

    print("Serial Reduce result: {}\n".format(reduce_serial_res))

    # ========================================

    # map parallel
    map_parallel = lambda func, xs: [func.remote(x) for x in xs]
    
    @ray.remote
    def square_remote(x):
        time.sleep(1)
        return x ** 2

    map_parallel_res = ray.get(map_parallel(square_remote, range(5)))
    print("Parallel Map result: {}".format(map_parallel_res))


    def reduce_parallel(func, xs):
        len_xs = len(xs)
        if len_xs == 1:
            return xs[0] 
        elif len_xs == 2:
            return ray.get(func.remote(xs[0], xs[1]))

        x_left = xs[:(len_xs // 2)]
        x_right = xs[(len_xs // 2):]
        
        result = reduce_parallel(func, x_left) + reduce_parallel(func, x_right)
        return result

    @ray.remote
    def sum_remote(x, y):
        time.sleep(1)
        return x + y

    reduce_parallel_res = reduce_parallel(sum_remote, map_parallel_res)
    print("Parallel Reduce result: {}".format(reduce_parallel_res))
    


def main():
    # put and get vars
    # put_and_get()

    # compate serial with parallel
    # serial_and_parallel()

    # simple MapReduce
    my_MapReduce()



if __name__ == '__main__':
    ray.init(
        num_cpus = 4,
        include_webui=False,
        ignore_reinit_error=True
        )
    main()

