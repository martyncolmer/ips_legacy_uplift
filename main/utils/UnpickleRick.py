"""
Unpickles file to CSV in same location with same filename
"""
import pandas

# Fill in dir and filename WITHOUT .pkl
dir = r"C:\Users\thorne1\PycharmProjects\IPS_Legacy_Uplift\tests\data\calculations\october_2017\imbalance_weight_new"
file_name = r"\imb_weight_surveydata_output"

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



