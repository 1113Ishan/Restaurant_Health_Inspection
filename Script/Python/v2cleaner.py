import pandas as pd
import numpy as np
import re

# -----------------------------
# 1. LOAD CLEAN V1 FILE
# -----------------------------
df = pd.read_csv(
    "Data/Clean/Cleaned_restaurant_inspection.csv",
    encoding="latin1"
)

# -----------------------------
# 2. NORMALIZE MISSING VALUES
# -----------------------------
df = df.replace([None, "", " ", "  "], np.nan)

# -----------------------------
# 3. STRONG UNDERSCORE CLEANING (MODERN SAFE VERSION)
# -----------------------------
def clean_value(value):
    if pd.isna(value):
        return np.nan
    if isinstance(value, str):
        value = value.strip()

        # catch underscore-only or mixed junk
        if re.fullmatch(r"_+|\s*_+\s*", value):
            return np.nan

    return value

# APPLY COLUMN-WISE (compatible with newer pandas)
df = df.apply(lambda col: col.map(clean_value))

# -----------------------------
# 4. SAFE NUMERIC CONVERSION
# -----------------------------
numeric_cols = [
    "SCORE",
    "Community Board",
    "Council District",
    "Census Tract",
    "BIN",
    "BBL"
]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# -----------------------------
# 5. SAFE DATE CONVERSION
# -----------------------------
date_cols = [
    "INSPECTION DATE",
    "GRADE DATE",
    "RECORD DATE"
]

for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

# Remove known bad date
df["INSPECTION DATE"] = df["INSPECTION DATE"].replace(pd.Timestamp("1900-01-01"), pd.NaT)

# -----------------------------
# 6. CLEAN TEXT COLUMNS
# -----------------------------
text_cols = [
    "CRITICAL FLAG",
    "GRADE",
    "CUISINE DESCRIPTION",
    "DBA",
    "BORO",
    "STREET",
    "ACTION",
    "VIOLATION DESCRIPTION",
    "VIOLATION CODE",
    "INSPECTION TYPE",
    "NTA"
]

for col in text_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace(["nan", "None", ""], np.nan)

# -----------------------------
# 7. VALIDATION CHECK (SAFE)
# -----------------------------
print("NULL counts after cleaning:\n")
print(df.isna().sum())

underscore_count = df.astype(str).apply(lambda x: x.str.contains(r"_+", na=False)).sum().sum()
print(f"\nRemaining underscore-like patterns: {underscore_count}")

# -----------------------------
# 8. EXPORT V2
# -----------------------------
output_path = "Data/Clean/Cleaned_restaurant_inspection_v2.csv"
df.to_csv(output_path, index=False)

print(f"\nClean V2 file created at: {output_path}")