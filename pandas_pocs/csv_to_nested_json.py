import pandas as pd




if __name__ == "__main__":
    df = pd.read_csv('/home/rohitgupta/BigData_projects/pyspark_experiments/data/small_data_2.csv')
    df['Purchase'] = df[['b','c','d']].to_dict('records')
    df['Sales'] = df[['d','e']].to_dict('records')
    out = df[['a', 'Purchase', 'Sales']].to_json(orient='records', indent=4)
    print(out)