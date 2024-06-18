import pandas as pd

df = pd.read_csv('test/car_urls_L.csv')
print(df)

for a in df.values:
    print(a[0])