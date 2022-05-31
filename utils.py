

def repo_path(relative_path):
    import os
    current_dir = os.path.dirname(__file__)
    # relative_path = ".././data/spark_in_action_data/books.csv"
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path

def merge_dfs(df_list, list_common_cols):
    from functools import reduce
    import pandas as pd
    new_data = reduce(lambda x,y: pd.merge(x,y,  on=list_common_cols, how='outer'), df_list )
    return new_data


def dict_nest(df_dict):
    """used for work"""
    di = {}
    # print(type(df_dict))
    for k,v in df_dict.items():
        if "_" not in k or 'tx_' in k:
          di[k] = v
        elif "_" in k and "tx_" not in k:
            k_split = k.split('_')
            # di.setdefault(k_split[0],{})[k_split[1]] = v
            # for i in k_split:
                # di.setdefault(k_split[i],{})[]
            di.setdefault(k_split[0],{}).setdefault(k_split[1],{})[k_split[2]] = v
            # di[k_split[0]] = {k_split[1]:v}
    return di

import collections.abc

def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d

def nested_set(dic, keys, value, create_missing=True):
    d = dic
    for key in keys[:-1]:
        if key in d:
            d = d[key]
        elif create_missing:
            d = d.setdefault(key, {})
        else:
            return dic
    if keys[-1] in d or create_missing:
        d[keys[-1]] = value
    return dic

di = {}
keys = ["a", "b", "c"]
value = "abc"

dictt = nested_set(di,keys,value)
print(dictt)
def dict_nest_1(df_dict):
    """used for work"""
    from collections import defaultdict
    # def rec_dd():
    #     return defaultdict(rec_dd)
    #
    # di = rec_dd()
    di = {}
    # print(type(df_dict))
    for k, v in df_dict.items():
        if "_" not in k or 'tx_' in k:
            di[k] = v
        elif "_" in k and "tx_" not in k:
            k_split = k.split('_')
            # print(k_split)
            d = {}
            new_dict = nested_set(d, k_split, value=v)
            print(new_dict)
            di.update(new_dict)
    return di



if __name__ == "__main__":
    import json
    val = {"alternativeGraduationPlans_alternativeGraduationPlanReference_educationOrganizationId" : 185,
           "alternativeGraduationPlans_alternativeGraduationPlanReference_graduationSchoolYear" : 2015,}
    print(dict_nest_1(val))
    # print(json.dumps(dict_nest_1(val)))
    pass
