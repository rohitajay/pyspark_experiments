import pandas as pd

def dict_nest(df_dict):
    di = {}
    # print(type(df_dict))
    for k,v in df_dict.items():
        if "_" not in k or 'tx_' in k:
          di[k] = v
        elif "_" in k and "tx_" not in k:
            k_split = k.split('_')
            di.setdefault(k_split[0],{})[k_split[1]] = v
            # di[k_split[0]] = {k_split[1]:v}
    return di


base = "/home/rohitgupta/BigData_projects/pyspark_experiments/data/ssa/v3_ssa_base.csv"
agp = "/home/rohitgupta/BigData_projects/pyspark_experiments/data/ssa/v3_ssa_agp.csv"

edu_plan = "/home/rohitgupta/BigData_projects/pyspark_experiments/data/ssa/v3_ssa_educationplan.csv"

df_base = pd.read_csv(base)
# print(df_base.to_dict(orient= "records")[0])
df_agp = pd.read_csv(agp)
df_edu_plan = pd.read_csv(edu_plan)

list_common_cols =  ['entryDate','schoolReference_schoolId','studentReference_studentUniqueId']
df_list = [df_base, df_agp, df_edu_plan]

def merge_dfs(df_list, list_common_cols):
    from functools import reduce
    new_data = reduce(lambda x,y: pd.merge(x,y,  on=list_common_cols, how='outer'), df_list )
    return new_data


merged_df = merge_dfs(df_list=df_list,list_common_cols=list_common_cols)

merged_df  = merged_df.to_dict(orient= "records")

nests = [dict_nest(i) for i in merged_df][0]

print(nests)





# print(dict3)

# print(nests)

# for i in df_agp_dict:
#     print(type(i))
#     print(dict_nest(i))
















# dict1 = df_base[0]
# print(dict1)
# dict_new = {}
# for key,value in dict1.items():
#     if "_" in key and "tx_" not in key:
#         print(key)
#         sp_key = key.split('_')
#         # print(key.split('_'))
#         # print(key)
#         new_kv = {sp_key[0]:{sp_key[1]: value}}
#         dict_new.add(new_kv)
#     else:
#         dict_new.add()
#



