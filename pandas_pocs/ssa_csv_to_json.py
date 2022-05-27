import pandas as pd

base = "/home/rohitgupta/BigData_projects/pyspark_experiments/data/ssa/v3_ssa_base.csv"

agp = "/home/rohitgupta/BigData_projects/pyspark_experiments/data/ssa/v3_ssa_agp.csv"

edu_plan = "/home/rohitgupta/BigData_projects/pyspark_experiments/data/ssa/v3_ssa_educationplan.csv"

df_base = pd.read_csv(base)
df_agp = pd.read_csv(agp)
df_edu_plan = pd.read_csv(edu_plan)

base_dict = df_base.to_dict(orient="records")