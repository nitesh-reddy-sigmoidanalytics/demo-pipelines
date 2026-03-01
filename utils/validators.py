# dags/utils/validators.py
"""
Common data quality validators used across all DAGs.
"""

import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def validate_required_columns(df: pd.DataFrame, required: list[str], context_name: str = '') -> None:
    """Raise ValueError if any required columns are missing."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[{context_name}] Missing required columns: {missing}")
    logger.info(f"[{context_name}] All required columns present: {required}")


def validate_null_threshold(df: pd.DataFrame, threshold: float = 0.1, context_name: str = '') -> None:
    """Raise ValueError if any column exceeds null % threshold."""
    null_pct = df.isnull().mean()
    bad_cols = null_pct[null_pct > threshold].to_dict()
    if bad_cols:
        raise ValueError(f"[{context_name}] Columns exceed {threshold*100}% nulls: {bad_cols}")
    logger.info(f"[{context_name}] Null check passed (threshold={threshold})")


def validate_no_negatives(df: pd.DataFrame, columns: list[str], context_name: str = '') -> None:
    """Warn if any numeric column contains negative values."""
    for col in columns:
        if col in df.columns:
            neg_count = (df[col] < 0).sum()
            if neg_count > 0:
                logger.warning(f"[{context_name}] Column '{col}' has {neg_count} negative values")


def validate_row_count(df: pd.DataFrame, min_rows: int = 1, context_name: str = '') -> None:
    """Raise ValueError if DataFrame has fewer rows than expected."""
    if len(df) < min_rows:
        raise ValueError(f"[{context_name}] Expected >= {min_rows} rows, got {len(df)}")
    logger.info(f"[{context_name}] Row count OK: {len(df)} rows")


def validate_date_range(
    df: pd.DataFrame,
    date_col: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    context_name: str = ''
) -> None:
    """Warn if dates fall outside expected range."""
    dates = pd.to_datetime(df[date_col], errors='coerce')
    if start and (dates < pd.Timestamp(start)).any():
        logger.warning(f"[{context_name}] '{date_col}' has values before {start}")
    if end and (dates > pd.Timestamp(end)).any():
        logger.warning(f"[{context_name}] '{date_col}' has values after {end}")
