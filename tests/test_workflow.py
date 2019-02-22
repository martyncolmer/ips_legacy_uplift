import time
import uuid

from ips.dataimport.import_csv import import_csv, CSVType
from ips.db import data_management as idm
from ips.main.ips_workflow import run_calculations
from ips.utils import common_functions as cf

reference_data = {
    "Sea": "../tests/data/import_data/dec/Sea Traffic Dec 2017.csv",
    "Air": "../tests/data/import_data/dec/Air Sheet Dec 2017 VBA.csv",
    "Tunnel": "../tests/data/import_data/dec/Tunnel Traffic Dec 2017.csv",
    "Shift": "../tests/data/import_data/dec/Poss shifts Dec 2017.csv",
    "Non Response": "../tests/data/import_data/dec/Dec17_NR.csv",
    "Unsampled": "../tests/data/import_data/dec/Unsampled Traffic Dec 2017.csv"
}

survey_data = "../tests/data/import_data/dec/survey_data_in_actual.csv"

run_id = str(uuid.uuid4())
start_time = time.time()
print("Module level start time: {}".format(start_time))


def setup_module(module):
    """ setup any state specific to the execution of the given module."""

    import_reference_data()
    setup_pv()


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

    import_survey = import_csv(file_type=CSVType.Survey, run_id=run_id, file_name=survey_data)

    import_sea()
    import_air()
    import_tunnel()
    import_shift()
    import_non_response()
    import_unsampled()
    import_survey()


def setup_pv():
    df = cf.select_data('*', "PROCESS_VARIABLE_PY", 'RUN_ID', 'TEMPLATE')
    df['RUN_ID'] = run_id
    cf.insert_dataframe_into_table('PROCESS_VARIABLE_PY', df)


def teardown_module(module):
    """ teardown any state that was previously setup with a setup_module
        method.
    """
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', run_id)

    # List of tables to cleanse where [RUN_ID] = RUN_ID
    tables_to_cleanse = ['PROCESS_VARIABLE_PY',
                         'PROCESS_VARIABLE_TESTING',
                         'TRAFFIC_DATA',
                         'SHIFT_DATA',
                         'NON_RESPONSE_DATA',
                         'UNSAMPLED_OOH_DATA',
                         idm.SURVEY_SUBSAMPLE_TABLE]

    # Try to delete from each table in list where condition.  If exception occurs,
    # assume table is already empty, and continue deleting from tables in list.
    for table in tables_to_cleanse:
        try:
            cf.delete_from_table(table, 'RUN_ID', '=', run_id)
        except Exception:
            continue

    print("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))


def test_workflow():
    run_calculations(run_id)
