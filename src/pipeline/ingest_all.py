#!/usr/bin/env python3
"""Ingest multiple StatsCan datasets with size constraints."""

import pandas as pd
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import requests
import zipfile
import tempfile
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from . import utils


API_BASE = "https://www150.statcan.gc.ca/t1/wds/rest"
MAX_TOTAL_GB = 10  # Just immigration data
MAX_DOWNLOAD_MB = 100  # Skip ZIP files larger than this (heuristic to save bandwidth)
MAX_UNCOMPRESSED_MB = 200  # Skip uncompressed CSVs larger than this (accurate check after download)
NUM_WORKERS = 1  # Sequential processing to avoid memory exhaustion


# === Functional Core (Pure Functions - No I/O) ===

def sanitize_column_names(columns):
    """Replace spaces, slashes, and hyphens with underscores for parquet compatibility.

    Args:
        columns: List of column names

    Returns:
        List of sanitized column names
    """
    return [col.replace(' ', '_').replace('/', '_').replace('-', '_') for col in columns]


def create_folder_name(product_id, title):
    """Generate clean folder name from productId and title.

    Args:
        product_id: Dataset product ID (int)
        title: Dataset title (str)

    Returns:
        Folder name like '12100163-international-trade'
    """
    clean_title = "".join(c if c.isalnum() or c in (' ', '-') else '' for c in title)
    clean_title = "-".join(clean_title.lower().split())
    return f"{product_id}-{clean_title}"


def should_download(file_size_bytes, max_mb):
    """Check if file is within download size limit.

    Args:
        file_size_bytes: File size in bytes
        max_mb: Maximum allowed size in MB

    Returns:
        True if file should be downloaded, False otherwise
    """
    return file_size_bytes <= max_mb * 1e6


def should_process_csv(uncompressed_size_bytes, max_mb):
    """Check if uncompressed CSV is within size limit.

    Args:
        uncompressed_size_bytes: Uncompressed CSV size in bytes
        max_mb: Maximum allowed size in MB

    Returns:
        True if CSV should be processed, False otherwise
    """
    return uncompressed_size_bytes <= max_mb * 1e6


def filter_catalog(catalog_df, existing_ids, skip_invisible=True, limit=None):
    """Apply all filtering logic to catalog: existing datasets, INVISIBLE, and limit.

    This is the core filtering function that consolidates all filtering logic.

    Args:
        catalog_df: Full catalog DataFrame
        existing_ids: Set of productIds already in S3
        skip_invisible: Whether to skip INVISIBLE datasets (default True)
        limit: Maximum number of datasets to return (None for no limit)

    Returns:
        Filtered DataFrame of datasets to process
    """
    # Remove datasets already in S3
    filtered = catalog_df[~catalog_df['productId'].isin(existing_ids)]

    # Remove INVISIBLE datasets (massive internal tables)
    if skip_invisible:
        filtered = filtered[~filtered['title'].str.contains('INVISIBLE', na=False)]

    # Apply limit
    if limit:
        filtered = filtered.head(limit)

    return filtered


def convert_csv_to_parquet(csv_path, output_path):
    """Convert CSV to parquet using PyArrow for memory efficiency.

    Uses PyArrow's streaming CSV reader to handle large files without
    loading entire dataset into memory. Column names are sanitized
    during conversion.

    Args:
        csv_path: Path to input CSV file (str or Path)
        output_path: Path to output parquet file (str or Path)

    Returns:
        None (writes file to disk)
    """
    # Read CSV with PyArrow (memory efficient)
    table = pa_csv.read_csv(str(csv_path))

    # Sanitize column names
    sanitized_names = sanitize_column_names(table.column_names)
    table = table.rename_columns(sanitized_names)

    # Write parquet
    pq.write_table(table, str(output_path))


# === I/O Layer ===

def download_table(product_id, title):
    """Download CSV from StatsCan API and convert to parquet.

    This function handles all I/O operations: HTTP requests, file downloads,
    ZIP extraction, CSV parsing, and parquet writing.

    Args:
        product_id: Dataset product ID
        title: Dataset title

    Returns:
        Size of parquet file in MB, or None if skipped
    """
    url = f"{API_BASE}/getFullTableDownloadCSV/{product_id}/en"
    headers = {'User-Agent': 'Mozilla/5.0'}

    # Truncate title for display
    title_part = title[:50] + "..." if len(title) > 50 else title
    display_title = f"[{product_id}] {title_part}"

    # Get ZIP URL
    print(f"{display_title} - Starting...")
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    zip_url = data['object']

    # Check file size BEFORE downloading
    head_resp = requests.head(zip_url, headers=headers, timeout=30)
    head_resp.raise_for_status()
    total_size = int(head_resp.headers.get('content-length', 0))

    if not should_download(total_size, MAX_DOWNLOAD_MB):
        print(f"{display_title} - Skipped (>{MAX_DOWNLOAD_MB}MB, {total_size/1e6:.0f}MB)")
        return None

    # Stream ZIP to temp file on disk
    zip_resp = requests.get(zip_url, headers=headers, timeout=600, stream=True)
    zip_resp.raise_for_status()

    downloaded = 0
    last_pct = -1

    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp_zip:
        for chunk in zip_resp.iter_content(chunk_size=1024*1024):  # 1MB chunks
            tmp_zip.write(chunk)
            downloaded += len(chunk)

            # Print progress every 10%
            if total_size > 0:
                pct = int(100 * downloaded / total_size)
                if pct >= last_pct + 10:
                    print(f"{display_title} - Downloading {downloaded/1e6:.0f}/{total_size/1e6:.0f}MB ({pct}%)")
                    last_pct = pct

        zip_path = tmp_zip.name

    print(f"{display_title} - Downloaded, extracting...")

    # Check uncompressed size before extraction
    with zipfile.ZipFile(zip_path) as z:
        csv_info = z.infolist()[0]
        uncompressed_mb = csv_info.file_size / 1e6

        if not should_process_csv(csv_info.file_size, MAX_UNCOMPRESSED_MB):
            print(f"{display_title} - Skipped (CSV too large: {uncompressed_mb:.0f}MB uncompressed)")
            Path(zip_path).unlink()
            return None

    # Extract CSV to temp directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        with zipfile.ZipFile(zip_path) as z:
            csv_name = z.namelist()[0]
            csv_path = z.extract(csv_name, tmp_dir)

        # Convert CSV to parquet (memory efficient with PyArrow)
        print(f"{display_title} - Converting to parquet...")

        folder_name = create_folder_name(product_id, title)
        out_dir = Path("data") / folder_name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{product_id}.parquet"

        convert_csv_to_parquet(csv_path, out_file)

    # Clean up temp ZIP
    Path(zip_path).unlink()

    size_mb = out_file.stat().st_size / 1e6
    print(f"{display_title} - Complete ({size_mb:.1f}MB)")
    return size_mb


# === Orchestration ===

def process_dataset(product_id, title, state_lock, shared_state):
    """Worker function to process a single dataset.

    Wraps download_table() with error handling and shared state management.
    """
    try:
        size_mb = download_table(product_id, title)
        if size_mb is None:  # File was skipped
            return None

        # Update shared state
        with state_lock:
            shared_state['total_size_mb'] += size_mb
            shared_state['ingested'].append({
                'productId': product_id,
                'title': title,
                'size_mb': size_mb
            })

        return size_mb
    except Exception as e:
        title_part = title[:50] + "..." if len(title) > 50 else title
        display_title = f"[{product_id}] {title_part}"
        error_msg = type(e).__name__ if len(str(e)) > 50 else str(e)
        print(f"{display_title} - Error: {error_msg}")
        return None


def main():
    """Main ingestion orchestration."""
    # I/O: Load catalog
    catalog = pd.read_parquet('catalog.parquet')

    # I/O: Get existing datasets from S3
    existing = utils.get_existing_dataset_ids('statscan')
    print(f"Already have {len(existing)} datasets in S3")

    # I/O: Read limit from environment
    limit = int(os.getenv('LIMIT')) if os.getenv('LIMIT') else None

    # Core: Apply all filtering logic in one place
    catalog_to_process = filter_catalog(
        catalog,
        existing_ids=existing,
        skip_invisible=True,
        limit=limit
    )

    print(f"Processing {len(catalog_to_process)} datasets with {NUM_WORKERS} workers")
    print(f"Target: {MAX_TOTAL_GB} GB total\n")

    # Shared state for thread pool
    state_lock = threading.Lock()
    shared_state = {
        'total_size_mb': 0,
        'ingested': []
    }

    # Process datasets in parallel
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {}

        for idx, row in catalog_to_process.iterrows():
            product_id = row['productId']
            title = row['title']

            # Check if we've reached cap before submitting more work
            with state_lock:
                if shared_state['total_size_mb'] / 1000 >= MAX_TOTAL_GB:
                    print(f"\n✓ Reached {MAX_TOTAL_GB} GB cap, waiting for remaining jobs...")
                    break

            # Submit job
            future = executor.submit(process_dataset, product_id, title, state_lock, shared_state)
            futures[future] = (product_id, title)

        # Wait for all submitted jobs to complete
        for future in as_completed(futures):
            product_id, title = futures[future]
            try:
                future.result()  # This will raise if the worker raised
            except Exception as e:
                title_part = title[:50] + "..." if len(title) > 50 else title
                display_title = f"[{product_id}] {title_part}"
                print(f"{display_title} - Unexpected error: {type(e).__name__}")

    # Save manifest
    manifest = pd.DataFrame(shared_state['ingested'])
    manifest.to_csv('ingested.csv', index=False)

    total_size_mb = shared_state['total_size_mb']
    print(f"\n✓ Ingested {len(shared_state['ingested'])} datasets")
    print(f"✓ Total size: {total_size_mb/1000:.2f} GB")
    print(f"✓ Manifest saved to ingested.csv")


if __name__ == '__main__':
    main()
