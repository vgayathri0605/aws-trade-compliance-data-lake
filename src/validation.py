#Extract → Validate → Transform → Load


import pandas as pd
from .config import VALID_CURRENCIES

EXPECTED_COLUMNS = [
    "trade_id",
    "trade_date",
    "country",
    "amount",
    "currency",
    "status"
]

def validate_schema(df: pd.DataFrame):
    """
    Validates that incoming dataframe has expected columns.
    Raises error if schema mismatch.
    """
    if list(df.columns) != EXPECTED_COLUMNS:
        raise ValueError(
            f"Schema mismatch. Expected {EXPECTED_COLUMNS} but got {list(df.columns)}"
        )

def validate_nulls(df: pd.DataFrame):
    """
    Ensure no critical column contains null values.
    """
    if df[EXPECTED_COLUMNS].isnull().any().any():
        raise ValueError("Null values detected in critical columns.")
    
def validate_business_rules(df):

    if (df["amount"] < 0).any():
        raise ValueError("Negative trade amount detected")

    if df["trade_id"].duplicated().any():
        raise ValueError("Duplicate trade_id detected")
    
def validate_business_rules(df):
    """
    Validate business rules for trade data
    """

    errors = []

    # Check negative trade amounts
    if (df["amount"] < 0).any():
        errors.append("Negative trade amount detected")

    # Check duplicate trade IDs
    if df["trade_id"].duplicated().any():
        errors.append("Duplicate trade_id detected")

    # Standardize currency values
    df["currency"] = df["currency"].str.strip().str.upper()

    # Check invalid currency codes
    invalid_currency = df[~df["currency"].isin(VALID_CURRENCIES)]

    if not invalid_currency.empty:
        errors.append(
            f"Invalid currency codes found: {invalid_currency['currency'].unique()}"
        )

    if errors:
        raise ValueError(errors)