import pandas as pd

sf = '10000'

df = pd.read_csv(f'/export/scratch2/dljtw/ldbc_snb_bi/parameters/parameters-sf{sf}/bi-15a.csv', sep="|")

df = df.drop(columns=['startDate:DATE', 'endDate:DATE'])

df.to_csv(f'/export/scratch2/dljtw/ldbc_snb_bi/parameters/parameters-sf{sf}/interactive-13.csv', index=False, sep="|")
