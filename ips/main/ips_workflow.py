import multiprocessing
import os
from functools import partial

import ips.steps.air_miles as airmiles
import ips.steps.fares_imputation as fares_imputation
import ips.steps.minimums_weight as minimums_weight
import ips.steps.non_response_weight as non_response_weight
import ips.steps.shift_weight as shift_weight
import ips.steps.spend_imputation as spend_imputation
import ips.steps.stay_imputation as stay_imputation
import ips.steps.traffic_weight as traffic_weight


def parallelise_calculation(func_list, run_id):
    num_partitions = len(func_list)
    pool = multiprocessing.Pool(num_partitions)

    for func in func_list:
        f = partial(func, run_id)
        pool.apply(f)
        # func(run_id)

    pool.close()
    pool.join()


def step_1(run_id):
    print(f"shift_weight calculation, process id: {os.getpid()}")
    shift_weight.shift_weight_step(run_id)


def step_2(run_id):
    print(
        f"Calculation 2, non_response_weight calculation, non_response_weight.non_response_weight_step, process id: {os.getpid()}")
    non_response_weight.non_response_weight_step(run_id)


def step_3(run_id):
    print(f"Calculation 3, minimums_weight.minimums_weight_step, process id: {os.getpid()}")
    minimums_weight.minimums_weight_step(run_id)


def step_4(run_id):
    print(f"Calculation 4, process id: {os.getpid()}")
    traffic_weight.traffic_weight_step(run_id)


def step_5(run_id):
    print(f"Calculation 5, process id: {os.getpid()}")


def step_6(run_id):
    print(f"Calculation 6, process id: {os.getpid()}")


def step_7(run_id):
    print(f"Calculation 7, non_response_weight.non_response_weight_step, process id: {os.getpid()}")
    non_response_weight.non_response_weight_step(run_id)


def step_8(run_id):
    print(f"Calculation 8, stay_imputation.stay_imputation_step, process id: {os.getpid()}")
    stay_imputation.stay_imputation_step(run_id)


def step_9(run_id):
    print(f"Calculation 9, fares_imputation.fares_imputation_step, process id: {os.getpid()}")
    fares_imputation.fares_imputation_step(run_id)


def step_10(run_id):
    print(f"Calculation 10, spend_imputation.spend_imputation_step, process id: {os.getpid()}")
    spend_imputation.spend_imputation_step(run_id)


def step_11(run_id):
    print(f"Calculation 11, process id: {os.getpid()}")


def step_12(run_id):
    print(f"Calculation 12, process id: {os.getpid()}")


def step_13(run_id):
    print(f"Calculation 13, process id: {os.getpid()}")


def step_14(run_id):
    print(f"Calculation 14, airmiles.airmiles_step, process id: {os.getpid()}")
    airmiles.airmiles_step(run_id)


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
