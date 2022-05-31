from pyspark.sql import SparkSession

import pandas as pd

def dictify(s):
  if '_' not in s:
    return s
  key, rest = s.split('_', 1)
  return {key: dictify(rest)}


# print(dictify("calendarReference_schoolYear"))

agp = "/home/rohitgupta/BigData_projects/pyspark_experiments/data/ssa/v3_ssa_agp.csv"
df_agp_dict = pd.read_csv(agp).to_dict(orient="records")[0]



