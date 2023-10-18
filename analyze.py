

# Analyse the data to see if there is easy ways to visualise this 

import  pandas as pd
from pandas.plotting import parallel_coordinates
import re
import matplotlib.pyplot as plt

data_dir = "data/"

df = pd.read_parquet(data_dir)

# Fix the column names 
# Rplace the special characters with _
renameer = lambda x :  re.sub("_$", "", re.sub("[/(),]{1}|\\s+", "_", x.lower().strip()))
df.rename(renameer, axis=1, inplace=True)


# Just get sum summary stats for now   
print(df.describe())

# Info 
df.info()


# Check the split columns
df['split'].value_counts()


df['flag'] = df['split'].str.contains('\\*')

df['split'] = df['split'].str.replace('*','').str.strip()

# Check the split columns
df['split'].value_counts()

# Check the flagged  records 
df[df['flag'] == True]

# Parallel coordinates plot
tmp_df = df.head(100)

tmp_df.columns

tmp_df[['name', 'split', 'time', 'flag']]

parallel_coordinates(tmp_df, 'Split', color=('#556270', '#4ECDC4', '#C7F464'))
plt.show()
