import os
import pytest
import pandas as pd

from pandas.util.testing import assert_frame_equal


@pytest.mark.parametrize('step_name, step_df',[
    ("SHIFT_WEIGHT", 'True'),
    ("NON_RESPONSE", 'True'),
    ("MINIMUMS_WEIGHT", 'False'),
    ("TRAFFIC_WEIGHT", 'True'),
    ("UNSAMPLED_WEIGHT", 'True'),
    ("IMBALANCE_WEIGHT", 'False'),
    ("FINAL_WEIGHT", 'False'),
    ("STAY_IMPUTATION", 'False'),
    ("FARES_IMPUTATION", 'False'),
    ("SPEND_IMPUTATION", 'False'),
    ("RAIL_IMPUTATION", 'False'),
    ("REGIONAL_WEIGHTS", 'False'),
    ("TOWN_AND_STAY_EXPENDITURE", 'False'),
    ("AIR_MILES", 'False'),
])
def test_dtypes(step_name, step_df):
    dir = r'S:\CASPA\IPS\Testing\scratch\integration dtypes'
    full_dir = os.path.join(dir, step_name)
    integration_file_name = 'survey_input_dtypes_integration.csv'
    xml_file_name = 'survey_input_dtypes_xml.csv'

    integration_survey_dtypes = pd.read_csv(os.path.join(full_dir, integration_file_name), engine='python')
    xml_survey_dtypes = pd.read_csv(os.path.join(full_dir, xml_file_name), engine='python')

    assert_frame_equal(integration_survey_dtypes, xml_survey_dtypes)

    if step_df == 'True':
        integration_file_name = 'step_input_dtypes_integration.csv'
        xml_file_name = 'step_input_dtypes_xml.csv'

        integration_step_dtypes = pd.read_csv(os.path.join(full_dir, integration_file_name), engine='python')
        xml_step_dtypes = pd.read_csv(os.path.join(full_dir, xml_file_name), engine='python')

        assert_frame_equal(integration_step_dtypes, xml_step_dtypes)
