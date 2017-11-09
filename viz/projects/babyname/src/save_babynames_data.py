# fetch

import requests
import zipfile
import StringIO
import pandas as pd
import numpy as np
import os

# result = requests.get("http://www.ssa.gov/oact/babynames/state/namesbystate.zip")

print("Downloading babynames data (takes about 30 seconds)...")

result = requests.get("http://www.ssa.gov/oact/babynames/names.zip")
zf = zipfile.ZipFile(StringIO.StringIO(result.content))

def process_sex(df):
  df = df.sort_values(by='births', ascending=False)
  df['rank'] = np.arange(1, df.shape[0] + 1)
  df['frac'] = df.births / df.births.sum()
  return df

birthnames = pd.DataFrame()
for filename in zf.namelist():
  if filename.endswith('txt'):
    output = zf.read(filename)
    year = int(filename[3:7])
    df = pd.read_csv(StringIO.StringIO(output), names=['name', 'sex', 'births'])
    df['year'] = year
    men = process_sex(df[df.sex=='M'])
    women = process_sex(df[df.sex=='F'])
    birthnames = birthnames.append(women).append(men)

# save CSV in same directory
destination = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data/birthnames.csv")
birthnames.to_csv(destination)

# get top 100 names
popularity = pd.DataFrame(birthnames.groupby(['sex', 'name'])['births'].sum())
popularity = popularity.sort_values(by='births', ascending=False)
top100_males = popularity.ix['M'][:100]
top100_females = popularity.ix['F'][:100]
birthnames = birthnames.reset_index().set_index(['sex', 'name'])
top_records = birthnames.ix['M'].ix[top100_males.index].append(birthnames.ix['F'].ix[top100_females.index])

# save records to file
destination = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data/birthnames_top_100.csv")
top_records.reset_index().to_csv(destination)

print("DONE.")
