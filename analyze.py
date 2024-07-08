# Analyse the data to see if there is easy ways to visualise this

import pandas as pd
from pandas.plotting import parallel_coordinates
import re
import matplotlib.pyplot as plt


data_dir = "data/"

df = pd.read_parquet(data_dir)

# Fix the column names
# Rplace the special characters with _
renameer = lambda x: re.sub("_$", "", re.sub("[/(),]{1}|\\s+", "_", x.lower().strip()))
df.rename(renameer, axis=1, inplace=True)

df.rename({"name__ctz": "name_"}, axis=1, inplace=True)


# Just get sum summary stats for now
print(df.describe())

# Info
df.info()


# Check the split columns
df["split"].value_counts()


df["flag"] = df["split"].str.contains("\\*")

df["split"] = df["split"].str.replace("*", "").str.strip()

# Check the split columns
df["split"].value_counts()

# Check the flagged  records
df[df["flag"] == True]


df["min_km"] = (
    pd.to_timedelta("00:" + df["min_km"], errors="coerce").dt.total_seconds() / 60
)

df["time"] = pd.to_timedelta(df["time"], errors="coerce")

# Get aggregates and summaries per categor
(
    df.groupby(["age_group", "gender", "split"])
    .agg({"min_km": ["mean"], "time": ["mean"]})
    .unstack()
    .reset_index()
)


# Parallel coordinates plot
tmp_df = df
tmp_df = tmp_df[tmp_df["split"] != "HALF"]
# Convert min_km to numeric
t = (
    tmp_df[["name_", "split", "min_km"]]
    .dropna()
    .pivot(index="name_", columns="split", values="min_km")
    .reset_index()
)
# Plot rows of the data frame as lines
# Make alpha lower to see individual lines to see the spread
ax = parallel_coordinates(t, "name_")
ax.set_alpha(0.4)
# Remove the legend
ax.get_legend().remove()
ax.set_xlabel("Split Distance")
ax.set_ylabel("Time (mins)")
ax.set_title("Chicago Marathon Split Times")
plt.show()

# Do boxplot per split
t.boxplot()
plt.show()
