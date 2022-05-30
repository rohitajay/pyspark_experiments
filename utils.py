

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
            di.setdefault(k_split[0],{})[k_split[1]] = v
            # di[k_split[0]] = {k_split[1]:v}
    return di

