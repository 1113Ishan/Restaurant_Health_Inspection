import pandas as pd


df = pd.read_csv('Data/Raw/DOHMH_New_York_City_Restaurant_Inspection_Results_20250919.csv')


# Cleaning the INSPECTION DATE column

df['INSPECTION DATE'] = pd.to_datetime(df['INSPECTION DATE'], errors = "coerce")

# Remove all invalid date formats (no dates, 1/1/1900)

df = df[df['INSPECTION DATE'].notna()]

# Remove the 1/1/1900 dates

df = df[df['INSPECTION DATE'] > '1900-01-01']

# Handeling missing values in CUISINE DESCRIPTION

df["CUISINE DESCRIPTION"] = df['CUISINE DESCRIPTION'].fillna("Unknown")

# Handle missing grades
df["GRADE"] = df["GRADE"].fillna("Not Graded")

# Handle CRITICAL FLAG's "Not applicable" values
df["CRITICAL FLAG"] = df["CRITICAL FLAG"].replace("Not Applicable", pd.NA)

no_violation = df["VIOLATION DESCRIPTION"].isna()

df.loc[no_violation, "VIOLATION DESCRIPTION"] = pd.NA
df.loc[no_violation, "CRITICAL FLAG"] = pd.NA

df.to_csv("Data/Clean/Cleaned_restaurant_inspection.csv", index=False)
