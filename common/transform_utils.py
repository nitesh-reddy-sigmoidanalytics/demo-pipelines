from datetime import datetime

def standardize_columns(df):
    df.columns = [c.lower().strip() for c in df.columns]
    return df

def add_ingestion_timestamp(df):
    df["ingested_at"] = datetime.utcnow()
    return df
