import pandas as pd

def csv_to_json(file, skiprows=None):
    filename = file.split('.')[0]
    df = pd.read_csv(file, skiprows=skiprows)
    df.to_json(f"{filename}.json")
    print(f"Saved {file} as {filename}.json")
    del df
