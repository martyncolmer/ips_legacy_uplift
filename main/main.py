"""
Created on 26 April 2018
@author: Thomas Mahoney

Refactored on 2 October 2018
@author: Elinor Thorne
"""

from utils import common_functions as cf
from typing import Optional
import pyodbc
import sys

from steps.air_miles import airmiles_step
from steps.fares_imputation import fares_imputation_step
from steps.final_weight import final_weight_step
from steps.imbalance_weight import imbalance_weight_step
from steps.minimums_weight import minimums_weight_step
from steps.non_response_weight import non_response_weight_step
from steps.rail_imputation import rail_imputation_step
from steps.regional_weights import regional_weights_step
from steps.shift_weight import shift_weight_step
from steps.spend_imputation import spend_imputation_step
from steps.stay_imputation import stay_imputation_step
from steps.town_stay_expenditure import town_stay_expenditure_imputation_step
from steps.traffic_weight import traffic_weight_step
from steps.unsampled_weight import unsampled_weight_step


def run_ips(run_id, steps_to_run):
    # Connection to the SQL server database
    connection: Optional[pyodbc.Connection] = cf.get_sql_connection()

    if connection is None:
        print("Cannot connect to database")
        sys.exit(1)

    if 1 in steps_to_run:
        try:
            shift_weight_step(run_id, connection)
        except Exception as err:
            print(err)

    if 2 in steps_to_run:
        try:
            non_response_weight_step(run_id, connection)
        except Exception as err:
            print(err)

    if 3 in steps_to_run:
        try:
            minimums_weight_step(run_id, connection)
        except Exception as err:
            print(err)

    if 4 in steps_to_run:
        try:
            traffic_weight_step(run_id, connection)
        except Exception as err:
            print(err)

    if 5 in steps_to_run:
        try:
            unsampled_weight_step(run_id, connection)
        except Exception as err:
            print(err)

    if 6 in steps_to_run:
        try:
            imbalance_weight_step(run_id, connection)
        except Exception as err:
            print(err)

    if 7 in steps_to_run:
        try:
            final_weight_step(run_id, connection)
        except Exception as err:
            print(err)

    if 8 in steps_to_run:
        try:
            stay_imputation_step(run_id, connection)
        except Exception as err:
            print(err)

    if 9 in steps_to_run:
        try:
            fares_imputation_step(run_id, connection)
        except Exception as err:
            print(err)

    if 10 in steps_to_run:
        try:
            spend_imputation_step(run_id, connection)
        except Exception as err:
            print(err)

    if 11 in steps_to_run:
        try:
            rail_imputation_step(run_id, connection)
        except Exception as err:
            print(err)

    if 12 in steps_to_run:
        try:
            regional_weights_step(run_id, connection)
        except Exception as err:
            print(err)

    if 13 in steps_to_run:
        try:
            town_stay_expenditure_imputation_step(run_id, connection)
        except Exception as err:
            print(err)

    if 14 in steps_to_run:
        try:
            airmiles_step(run_id, connection)
        except Exception as err:
            print(err)

    # Run Id (this will be generated automatically and will be unique)
    run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'


if __name__ == '__main__':
    run_ips(0, "")
