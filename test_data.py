import dask.dataframe as dd
import pandas as pd
#from glob import glob
#from datetime import datetime
#from dask import delayed, compute
#from time import time

sfile = "test_data.csv"
sfile = "trip_data/trip_data_*.csv"
dfile = "test_data2.csv"

def convert_mins(secs):
    if secs >= 60:
        return secs / 60
    return 0

# Modify trip_time_in_secs and convert to minutes if greater than 60, store in new column
def transform(df):
    #df["trip_time_in_mins"] = df.trip_time_in_secs.map(convert_mins)

    #return df.assign(trip_time_in_mins=df.trip_time_in_secs / 60)
    df["trip_time_in_secs"] = df.trip_time_in_secs.map(convert_mins)
    return df

# Read in file
df = dd.read_csv(sfile)
print(type(df))
print(df.trip_time_in_secs.head())
result = df.map_partitions(transform, meta=df)
print(type(result))
df = result.compute()
new_file = df.to_csv(dfile, single_file=True, compute=True)
print(new_file)

