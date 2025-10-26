"""Pure functions for detecting dataset updates based on frequency cadence."""

from datetime import datetime
import pandas as pd


def parse_frequency_to_days(frequency: str) -> int:
    """Convert StatsCan frequency string to days between updates.

    Args:
        frequency: Frequency string from StatsCan API
            (e.g., "Monthly", "Quarterly", "Annual")

    Returns:
        Number of days to wait before checking for updates
    """
    mapping = {
        "Daily": 1,
        "Weekly": 7,
        "Bi-weekly": 14,
        "Monthly": 30,
        "Quarterly": 90,
        "Semi-annual": 180,
        "Annual": 365,
        "Occasional": 180,
    }
    return mapping.get(frequency, 180)  # Default: 6 months for unknown


def should_check_for_update(
    frequency: str,
    last_ingestion_date: datetime,
    current_date: datetime
) -> bool:
    """Determine if enough time has passed to check for dataset update.

    Args:
        frequency: Dataset frequency (e.g., "Monthly")
        last_ingestion_date: When dataset was last ingested
        current_date: Current date/time

    Returns:
        True if dataset should be checked for updates
    """
    interval_days = parse_frequency_to_days(frequency)
    days_since_ingestion = (current_date - last_ingestion_date).days
    return days_since_ingestion >= interval_days


def identify_datasets_for_processing(
    fresh_catalog_df: pd.DataFrame,
    existing_catalog_df: pd.DataFrame,
    current_date: datetime
) -> pd.DataFrame:
    """Identify datasets needing processing (new or due for update).

    Args:
        fresh_catalog_df: Latest catalog from StatsCan API
            Required columns: productId, title, frequency
        existing_catalog_df: Current catalog from S3
            Required columns: productId, last_ingestion_date
            Can be empty DataFrame for first run
        current_date: Current date for update detection

    Returns:
        DataFrame with columns: productId, title, frequency, reason
        where reason is either "new" or "update_due"
    """
    fresh_ids = set(fresh_catalog_df['productId'])
    existing_ids = set(existing_catalog_df['productId']) if len(existing_catalog_df) > 0 else set()

    results = []

    # NEW datasets (never ingested before)
    for pid in (fresh_ids - existing_ids):
        row = fresh_catalog_df[fresh_catalog_df['productId'] == pid].iloc[0]
        results.append({
            'productId': pid,
            'title': row['title'],
            'frequency': row['frequency'],
            'reason': 'new'
        })

    # EXISTING datasets - check if update is due
    for pid in (fresh_ids & existing_ids):
        fresh_row = fresh_catalog_df[fresh_catalog_df['productId'] == pid].iloc[0]
        existing_row = existing_catalog_df[existing_catalog_df['productId'] == pid].iloc[0]

        # Skip if last_ingestion_date is null/NaT
        if pd.isna(existing_row['last_ingestion_date']):
            results.append({
                'productId': pid,
                'title': fresh_row['title'],
                'frequency': fresh_row['frequency'],
                'reason': 'new'  # Treat as new if never properly ingested
            })
            continue

        if should_check_for_update(
            fresh_row['frequency'],
            existing_row['last_ingestion_date'],
            current_date
        ):
            results.append({
                'productId': pid,
                'title': fresh_row['title'],
                'frequency': fresh_row['frequency'],
                'reason': 'update_due'
            })

    return pd.DataFrame(results)


def apply_limit_to_new_datasets(datasets_df: pd.DataFrame, limit: int | None) -> pd.DataFrame:
    """Apply limit to new datasets only, preserve all updates.

    Args:
        datasets_df: DataFrame with columns: productId, title, frequency, reason
        limit: Maximum number of new datasets to process, or None for no limit

    Returns:
        DataFrame with limited new datasets + all update_due datasets
    """
    if limit is None:
        return datasets_df

    new_datasets = datasets_df[datasets_df['reason'] == 'new']
    update_datasets = datasets_df[datasets_df['reason'] == 'update_due']

    limited_new = new_datasets.head(limit)
    return pd.concat([limited_new, update_datasets], ignore_index=True)
