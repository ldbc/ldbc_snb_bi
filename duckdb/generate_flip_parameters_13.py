import pandas as pd

sf = '3'

df = pd.read_csv(f'/home/daniel/Documents/ldbc_snb_bi/parameters/parameters-sf{sf}/bi-15a.csv', sep="|")

df = df.drop(columns=['startDate:DATE', 'endDate:DATE'])
df.rename(columns={'person1Id:ID': 'person2Id:ID', 'person2Id:ID': 'person1Id:ID'}, inplace=True)
# df.rename(columns={'person2id:ID_': 'person2id:ID'}, inplace=True)
df = df[df.columns[::-1]]

df.to_csv(f'/home/daniel/Documents/ldbc_snb_bi/parameters/parameters-sf{sf}/interactive-13.csv', index=False, sep="|")