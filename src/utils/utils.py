import pandas as pd

def csv2other(file, format='json', skiprows=None):
    filename = file.split('.')[0]
    df = pd.read_csv(file, skiprows=skiprows)
    if format=='format':
        df.to_json(f"{filename}.json")
    elif format=='parquet':
        df.to_parquet(f"{filename}.parquet", engine="auto")
    else:
        ValueError("Invalid format specified: 'json' or 'parquet' only")

    print(f"Saved {file} as {filename}.{format}")
    return df
