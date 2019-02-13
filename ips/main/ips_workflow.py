import multiprocessing
import os
import uuid
import ips.utils.common_functions as cf
import ips.steps.shift_weight as shift_weight
import ips.db.import_survey_data as survey
from ips.dataimport.import_csv import import_csv, CSVType

run_id = str(uuid.uuid4())


def parallelise_calculation(func_list):
    num_partitions = len(func_list)
    pool = multiprocessing.Pool(num_partitions)

    for func in func_list:
        pool.apply(func)

    pool.close()
    pool.join()


def step_1():
    shift_weight.shift_weight_step(run_id, cf.get_sql_connection())
    print(f"shift_weight calculation, process id: {os.getpid()}")


def step_2():
    print(f"Calculation 2, process id: {os.getpid()}")


def step_3():
    print(f"Calculation 3, process id: {os.getpid()}")


def step_4():
    print(f"Calculation 4, process id: {os.getpid()}")


def step_5():
    print(f"Calculation 5, process id: {os.getpid()}")


def step_6():
    print(f"Calculation 6, process id: {os.getpid()}")


def step_7():
    print(f"Calculation 7, process id: {os.getpid()}")


def step_8():
    print(f"Calculation 8, process id: {os.getpid()}")


def step_9():
    print(f"Calculation 9, process id: {os.getpid()}")


def step_10():
    print(f"Calculation 10, process id: {os.getpid()}")


def step_11():
    print(f"Calculation 11, process id: {os.getpid()}")


def step_12():
    print(f"Calculation 12, process id: {os.getpid()}")


def step_13():
    print(f"Calculation 13, process id: {os.getpid()}")


def step_14():
    print(f"Calculation 14, process id: {os.getpid()}")


dag_adjacency_list = {
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

reference_data = {
    "Sea": "../tests/data/import_data/dec/Sea Traffic Dec 2017.csv",
    "Air": "../tests/data/import_data/dec/Air Sheet Dec 2017 VBA.csv",
    "Tunnel": "../tests/data/import_data/dec/Tunnel Traffic Dec 2017.csv",
    "Shift": "../tests/data/import_data/dec/Poss shifts Dec 2017.csv",
    "Non Response": "../tests/data/import_data/dec/Dec17_NR.csv",
    "Unsampled": "../tests/data/import_data/dec/Unsampled Traffic Dec 2017.csv"
}

survey_data = "../tests/data/import_data/dec/survey_data_in_actual.csv"


def run_calculations():
    setup_calculations()

    print("calculations setup done")

    for x in dag_adjacency_list:
        lst = dag_adjacency_list[x]
        print(f"--> Start Step: {x}")
        parallelise_calculation(lst)
        print(f"--> End Step: {x}\n")


def setup_calculations():
    import_reference_data()
    return survey.import_survey_data(survey_data, run_id)


def import_reference_data():
    import_sea = import_csv(file_type=CSVType.Sea, run_id=run_id, file_name=reference_data['Sea'])
    import_air = import_csv(file_type=CSVType.Air, run_id=run_id, file_name=reference_data['Air'])
    import_tunnel = import_csv(file_type=CSVType.Tunnel, run_id=run_id, file_name=reference_data['Tunnel'])
    import_shift = import_csv(file_type=CSVType.Shift, run_id=run_id, file_name=reference_data['Shift'])
    import_non_response = import_csv(file_type=CSVType.NonResponse,
                                     run_id=run_id,
                                     file_name=reference_data['Non Response'])
    import_unsampled = import_csv(file_type=CSVType.Unsampled,
                                  run_id=run_id,
                                  file_name=reference_data['Unsampled'])

    import_sea()
    import_air()
    import_tunnel()
    import_shift()
    import_non_response()
    import_unsampled()


if __name__ == '__main__':
    run_calculations()
