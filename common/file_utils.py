import pandas as pd

def read_csv_from_path(path):
    return pd.read_csv(path)

def write_parquet(df, path):
    df.to_paquet(path, index=False)
