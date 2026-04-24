import pandas as pd

df = pd.read_csv('Data/Clean/Cleaned_restaurant_inspection.csv', encoding="latin1")

print(df.isna().sum())