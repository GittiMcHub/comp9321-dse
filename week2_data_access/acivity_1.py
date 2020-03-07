import pandas as pd
import numpy as np

# Read the CSV file and put the samples into a pandas' dataframe.
df = pd.read_csv('input.csv')

# Programmatically print the columns of the dataframe
# print(df.columns)

# Programmatically print the rows of the dataframe
for idx, row in df.iterrows():
    for col in row:
        print(row[col])

# Save the dataframe as a CSV file

