import multiprocessing
import os

import ips.steps.shift_weight as shift_weight
import ips.utils.common_functions as cf
from functools import partial


def parallelise_calculation(func_list, run_id):
    num_partitions = len(func_list)
    pool = multiprocessing.Pool(num_partitions)

    for func in func_list:
        f = partial(func, run_id)
        pool.apply(f)

    pool.close()
    pool.join()


def step_1(run_id):
    shift_weight.shift_weight_step(run_id, cf.get_sql_connection())
    print(f"shift_weight calculation, process id: {os.getpid()}")


def step_2(run_id):
    print(f"Calculation 2, process id: {os.getpid()}")


def step_3(run_id):
    print(f"Calculation 3, process id: {os.getpid()}")


def step_4(run_id):
    print(f"Calculation 4, process id: {os.getpid()}")


def step_5(run_id):
    print(f"Calculation 5, process id: {os.getpid()}")


def step_6(run_id):
    print(f"Calculation 6, process id: {os.getpid()}")


def step_7(run_id):
    print(f"Calculation 7, process id: {os.getpid()}")


def step_8(run_id):
    print(f"Calculation 8, process id: {os.getpid()}")


def step_9(run_id):
    print(f"Calculation 9, process id: {os.getpid()}")


def step_10(run_id):
    print(f"Calculation 10, process id: {os.getpid()}")


def step_11(run_id):
    print(f"Calculation 11, process id: {os.getpid()}")


def step_12(run_id):
    print(f"Calculation 12, process id: {os.getpid()}")


def step_13(run_id):
    print(f"Calculation 13, process id: {os.getpid()}")


def step_14(run_id):
    print(f"Calculation 14, process id: {os.getpid()}")


dag_list = {
    1: [step_1, step_8, step_9, step_14],
    2: [step_2, step_3, step_10],
    3: [step_4],
    4: [step_5],
    5: [step_6],
    6: [step_7],
    7: [step_11],
    8: [step_13],
    9: [step_12]
}


def run_calculations(run_id):
    for x in dag_list:
        lst = dag_list[x]
        print(f"--> Start Step: {x}")
        parallelise_calculation(lst, run_id)
        print(f"--> End Step: {x}\n")
