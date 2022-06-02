import pandas as pd
import json





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


def dict_nest_1(df_dict):
    """used for work"""
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

            di.update(new_dict)
    return di


def merge_dfs(df_list, list_common_cols):
    from functools import reduce
    new_data = reduce(lambda x,y: pd.merge(x,y,  on=list_common_cols, how='outer'), df_list)
    return new_data



if __name__ == "__main__":
    base = "/home/rohitgupta/BigData_projects/pyspark_experiments/data/ssa/v3_ssa_base.csv"
    agp = "/home/rohitgupta/BigData_projects/pyspark_experiments/data/ssa/v3_ssa_agp.csv"
    edu_plan = "/home/rohitgupta/BigData_projects/pyspark_experiments/data/ssa/v3_ssa_educationplan.csv"
    df_base = pd.read_csv(base)
    df_agp = pd.read_csv(agp)
    df_edu_plan = pd.read_csv(edu_plan)

    df_agp = df_agp.rename(columns=
                           {
                               "alternativeGraduationPlanReference_educationOrganizationId": "alternativeGraduationPlans_alternativeGraduationPlanReference_educationOrganizationId",
                               "alternativeGraduationPlanReference_graduationSchoolYear": "alternativeGraduationPlans_alternativeGraduationPlanReference_graduationSchoolYear",
                               "alternativeGraduationPlanReference_graduationPlanTypeDescriptor": "alternativeGraduationPlans_alternativeGraduationPlanReference_graduationPlanTypeDescriptor"})

    list_common_cols = ['entryDate', 'schoolReference_schoolId', 'studentReference_studentUniqueId']
    df_list = [df_base, df_agp, df_edu_plan]
    merged_df = merge_dfs(df_list=df_list, list_common_cols=list_common_cols)
    merged_df = merged_df.to_dict(orient="records")
    nests = [dict_nest_1(i) for i in merged_df]
    print(json.dumps(nests[0]))