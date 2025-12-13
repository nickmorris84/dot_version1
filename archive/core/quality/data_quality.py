# define optional DQ steps
def drop_empty_journey(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["journey_id"].astype(str).str.strip() != ""]

def trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.select_dtypes(include="object"):
        df[c] = df[c].astype(str).str.strip()
    return df

