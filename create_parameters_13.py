import pandas as pd



sfs = ['1', '3', '10', '30', '100', '300', '1000']

for sf in sfs: 

    df = pd.read_csv(f'/export/scratch2/dljtw/ldbc_snb_bi/parameters/parameters-sf{sf}/bi-15a.csv', sep="|")

    df = df.drop(columns=['startDate:DATE', 'endDate:DATE'])
    df.rename(columns={'person1Id:ID': 'person2Id:ID', 'person2Id:ID': 'person1Id:ID'}, inplace=True)
    # df.rename(columns={'person2id:ID_': 'person2id:ID'}, inplace=True)
    df = df[df.columns[::-1]]
    df.to_csv(f'/export/scratch2/dljtw/ldbc_snb_bi/parameters/parameters-sf{sf}/interactive-13.csv', index=False, sep="|")
