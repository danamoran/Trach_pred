import pandas as pd
from os import walk, path
from functools import reduce

SOURCE_DIR = r"C:\Users\Dana\Documents\Final_Project\Data\Data"


def create_temp_df(fname):
    temp_path = path.join(SOURCE_DIR, fname)
    temp_raw_df = pd.read_csv(temp_path)
    temp_raw_df["Time"] = pd.to_datetime(temp_raw_df.Time)
    base = temp_raw_df.loc[temp_raw_df.index[0], 'Time'].minute
    df = temp_raw_df.groupby(['Key', pd.Grouper(freq='1T', key='Time', base=base)]).mean()
    df = pd.DataFrame(df.to_records())
    name = fname.split("_")[0]
    df = df.rename(columns={"Value": name})
    return df


if __name__ == "__main__":
    all_tables = []
    for root, dirs, files in walk(SOURCE_DIR, topdown=False):
        for name in files:
            temp_df = create_temp_df(name)
            all_tables.append(temp_df)
    raw_data = reduce(lambda x, y: pd.merge(x, y, on=['Key', 'Time'], how='outer'), all_tables)
    raw_data.to_csv("raw_data.csv")
