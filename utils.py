

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
            # print(new_dict)
            di.update(new_dict)
    return di
def d1_d2_common(d1,d2):
    keys = d1.keys() | d2.keys()
    # print(keys)print(d1.get(k, {}
    res = {k: {**d1.get(k, {}), **d2.get(k, {})} for k in keys}
    return res

def split_dict_equally(input_dict, chunks=2):
    "Splits dict by keys. Returns a list of dictionaries."
    # prep with empty dicts
    return_list = [dict() for idx in range(chunks)]
    idx = 0
    for k,v in input_dict.items():
        return_list[idx][k] = v
        if idx < chunks-1:  # indexes start at 0
            idx += 1
        else:
            idx = 0
    return return_list

import itertools
def group(_input):
  d = list(itertools.chain(*list(map(lambda x:list(x.items()), _input))))
  _s = [[a, [c for _, c in b]] for a, b in itertools.groupby(sorted(d, key=lambda x:x[0]), key=lambda x:x[0])]
  return {a:group(b) if all(isinstance(i, dict) for i in b) else list(itertools.chain(*b)) for a, b in _s}

# print(group([a, b, c]))

if __name__ == "__main__":
    import json
    val = {"alternativeGraduationPlans_alternativeGraduationPlanReference_educationOrganizationId" : 185,
           "alternativeGraduationPlans_alternativeGraduationPlanReference_graduationSchoolYear" : 2015,}
    # print(len(val))

    d1 = split_dict_equally(val, chunks=2)
    # print(d1)
    dits = [dict_nest_1(i) for i in d1]
    # print(group(dits))
    # z = {key:[dits[0][key],dits[1][key]] for key in dits[0]}
    keys = dits[0].keys() | dits[1].keys()
    z = {key: {dits[0].get(key,{}), dits[1].get(key,{})} for key in keys}
    # z = {k: {dits[0][key], dits[1][key] }}
    print(z)
    # print(dits[0])
    # print(d1_d2_common(dits[0],dits[1]))
    pass
