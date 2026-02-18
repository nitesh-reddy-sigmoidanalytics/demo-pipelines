def validate_not_null(df, column):
    if df[column].isnull().any():
        raise ValueError(f"Null values found in {column}")

def validate_positive(df, column):
    if (df[column] < 0).any():
        raise ValueError(f"Negative values in {column}")
