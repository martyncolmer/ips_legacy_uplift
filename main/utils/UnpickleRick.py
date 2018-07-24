"""
Unpickles file to CSV in same location with same filename
"""
import pandas

# Fill in dir and filename WITHOUT .pkl
dir = r"C:\Git_projects\IPS_Legacy_Uplift\tests\data\ips_data_management\shift_weight"
file_name = r"\update_survey_data_pvs_result"

# Just in case you didn't read my previous comment
if file_name[-4:] == '.pkl':
    file_name = file_name[:-4]

# Takes pickled file and duplicates filename with .csv
in_file = "\{}.pkl".format(file_name)
out_file = "{}.csv".format(file_name)

# Read pickle in as df
df = pandas.read_pickle(dir+in_file)

# Send to CSV
df.to_csv(r"{}\{}".format(dir, out_file))



