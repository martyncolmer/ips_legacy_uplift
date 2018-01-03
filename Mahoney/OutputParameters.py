'''
Created on 27 Dec 2017

@author: mahont1
'''
from IPSTransformation.CommonFunctions import get_oracle_connection
from IPSTransformation import CommonFunctions as cf
import pandas as pd

import sys

params = cf.unload_parameters(59)

for x in params:
    print(x + " - " + str(params[x]))
          
