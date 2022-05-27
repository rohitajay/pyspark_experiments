import pandas as pd
from itertools import groupby
from collections import OrderedDict
import json


df = pd.read_csv('/home/rohitgupta/BigData_projects/pyspark_experiments/data/small_data.csv', dtype={
            "zipcode" : str,
            "date" : str,
            "state" : str,
            "val1" : str,
            "val2" : str,
            "val3" : str,
            "val4" : str,
            "val5" : str
        })

results = []

for (zipcode, state) , bag in df.groupby(["zipcode","state"]):
    contents_df = bag.drop(["zipcode","state"], axis=1)
    subset = [OrderedDict(row) for i, row in contents_df.iterrows()]
    results.append(OrderedDict([("zipcode",zipcode), ("state", state), ("subset", subset)]))

    print(json.dumps(results[0], indent=4))