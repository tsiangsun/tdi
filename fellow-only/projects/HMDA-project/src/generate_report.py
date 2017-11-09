#!/usr/bin/python
import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
%matplotlib inline
%config InlineBackend.figure_format = 'svg'

%cd ~/datacourse/Week1/HMDA-project/

filename="data/fha_by_tract.csv"
names =["Census_Tract_Number", "NUM_ALL", "NUM_FHA", "PCT_NUM_FHA", "AMT_ALL", "AMT_FHA", "PCT_AMT_FHA"]
df=pd.read_csv(filename,names=names)
df['Census_Tract_ID'] = df['Census_Tract_Number']*100

df[df['AMT_ALL']>5000]['PCT_AMT_FHA'].hist(bins=50)

print df['NUM_ALL'].describe()
df['NUM_ALL'].apply(np.log).hist(bins=50)


print df['AMT_ALL'].describe()
df['AMT_ALL'].apply(np.log).hist(bins=50)


df_geo = pd.read_table("data/raw/2013_Gaz_tracts_national.tsv")
df_geo.columns = [u'USPS', u'GEOID', u'ALAND', u'AWATER', u'ALAND_SQMI', u'AWATER_SQMI', u'INTPTLAT', u'INTPTLONG'] # Fix a newline in the lasat one!
df_geo['Census_Tract_ID'] = df_geo['GEOID'].apply(lambda x : x % 10**6) 

df_with_geo = df.merge(df_geo, on=['Census_Tract_ID'])
df_with_geo[df_with_geo['AMT_ALL']>5000]

df_tmp = df_with_geo[
          (df_with_geo['AMT_ALL']>5000) 
        & (df_with_geo['INTPTLONG']<-50) 
        & (df_with_geo['INTPTLONG']>-130)
        & (df_with_geo['USPS'] != 'PR')]

area = df_tmp['ALAND_SQMI'].sum()
constant = 2.4 * 10**4
#plt.scatter(x=df_tmp['INTPTLONG'], y=df_tmp['INTPTLAT'], c=df_tmp['PCT_AMT_FHA'], s=df_tmp['ALAND_SQMI'] * constant/ area, alpha=0.5, linewidths=(0.0,))

def map_state(state):
    df_tmp = df_with_geo[
          (df_with_geo['AMT_ALL']>5000) 
        & (df_with_geo['INTPTLONG']<-50) 
        & (df_with_geo['INTPTLONG']>-130)
        & (df_with_geo['USPS'] == state)]
    area = df_tmp['ALAND_SQMI'].sum()
    constant = 2.4 * 10**4
    return plt.scatter(x=df_tmp['INTPTLONG'], y=df_tmp['INTPTLAT'], c=df_tmp['PCT_AMT_FHA'], s=df_tmp['ALAND_SQMI'] * constant / area, alpha=0.5, linewidths=(0.0,))

map_state('NY')
