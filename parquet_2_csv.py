import pandas as pd
import os

source = "parquet_data_source"
target = "csv_data_source"
for file in os.listdir(source):
    print(file)
    df = pd.read_parquet(os.path.join(source, file))
    df.to_csv(os.path.join(target, file.replace(".parquet", ".csv")), index=False)
