import pandas as pd


df = pd.read_csv('bank.csv')


df = df.sample(frac=1).reset_index(drop=True)

n = len(df)
df1 = df.iloc[:n//3]
df2 = df.iloc[n//3:2*n//3]
df3 = df.iloc[2*n//3:]


df1.to_csv('bank_part1.csv', index=False)
df2.to_csv('bank_part2.csv', index=False)
df3.to_csv('bank_part3.csv', index=False)
