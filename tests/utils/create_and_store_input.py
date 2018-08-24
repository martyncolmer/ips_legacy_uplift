from sas7bdat import SAS7BDAT
import pandas

in_path = r'C:\Users\thorne1\PycharmProjects\IPS_Legacy_Uplift\tests\data\ips_data_management\final_weight_integration\surveydata.sas7bdat'
out_path = r'C:\Users\thorne1\PycharmProjects\IPS_Legacy_Uplift\tests\data\ips_data_management\final_weight_integration\surveydata.csv'

# Create and store df ready for testing
df = SAS7BDAT(in_path).to_data_frame()
df.columns = df.columns.str.upper()
# df.sort_values(by=["UNSAMP_REGION_GRP_PV", "ARRIVEDEPART"])
# df.index = range(0, len(df))
# df = df.head(1)
# df['RUN_ID'] = 'test-idm-integration-final-wt'
df.to_csv(out_path, index=False, encoding='utf-8')

# df.drop(["SHIFT_FACTOR", "CROSSINGS_FACTOR"], axis=1, inplace=True)
# df.sort_values(by=["SERIAL"])









