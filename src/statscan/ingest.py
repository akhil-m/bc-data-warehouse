#!/usr/bin/env python3
"""Ingest multiple StatsCan datasets with size constraints."""

import pandas as pd
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import requests
import zipfile
import tempfile
import os
import subprocess
import sys
import tracemalloc
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from . import utils


API_BASE = "https://www150.statcan.gc.ca/t1/wds/rest"
MAX_TOTAL_GB = 10  # Just immigration data
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


def generate_conversion_script(csv_path, output_path):
    """Generate Python script for streaming CSV to parquet conversion.

    This is a pure function that returns the subprocess script content.
    The script processes CSV in batches to avoid loading entire file into memory,
    preventing OOM kills on large files (150MB+).

    ALL COLUMNS FORCED TO STRING TYPE:
    - Handles mixed types in same column (e.g., '4680, 4690' in integer column)
    - Preserves raw data exactly as-is (no type coercion)
    - Standard data lake pattern (type casting happens at query time in Athena)

    Handles StatsCan's standard table symbols (official list):
    https://www.statcan.gc.ca/en/concepts/definitions/guide-symbol

    Args:
        csv_path: Path to input CSV file (str or Path)
        output_path: Path to output parquet file (str or Path)

    Returns:
        String containing Python script to execute in subprocess
    """
    return f"""
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import pyarrow.compute as pc

# Configure CSV parsing for StatsCan standard table symbols
# Official list: https://www.statcan.gc.ca/en/concepts/definitions/guide-symbol
convert_options = pa_csv.ConvertOptions(
    null_values=[
        '',              # Empty string
        '.', '..', '...', # Not available/applicable
        'x', 'X',        # Suppressed
        'E', 'e',        # Use with caution
        'F', 'f',        # Too unreliable
        't', 'T',        # Terminated
        'A', 'B', 'C', 'D', # Quality grades
        'p', 'r',        # Preliminary/revised
        '0s'             # Rounded to zero
    ],
    strings_can_be_null=True
)

# Open CSV for streaming (processes in batches of ~64K rows)
with pa_csv.open_csv('{csv_path}', convert_options=convert_options) as reader:
    writer = None
    sanitized = None
    string_schema = None

    for batch in reader:
        if writer is None:
            # Sanitize column names on first batch (same logic as sanitize_column_names())
            columns = batch.schema.names
            sanitized = [col.replace(' ', '_').replace('/', '_').replace('-', '_') for col in columns]

            # Create schema with ALL columns as string type
            # This handles mixed types (integers, ranges, lists) in same column
            string_schema = pa.schema([
                pa.field(sanitized[i], pa.string())
                for i in range(len(batch.schema))
            ])

            # Initialize parquet writer
            writer = pq.ParquetWriter('{output_path}', string_schema)

        # Cast all columns to string, rename, and write batch
        string_batch = pa.RecordBatch.from_arrays(
            [pc.cast(batch.column(i), pa.string()) for i in range(batch.num_columns)],
            schema=string_schema
        )
        writer.write_batch(string_batch)

    # Close writer if any data was processed
    if writer:
        writer.close()
"""


def format_display_title(product_id, title, max_len=50):
    """Format dataset title for display with truncation.

    Args:
        product_id: Dataset product ID (int)
        title: Dataset title (str)
        max_len: Maximum title length before truncation (default 50)

    Returns:
        Formatted string in "[productId] title..." format
    """
    title_part = title[:max_len] + "..." if len(title) > max_len else title
    return f"[{product_id}] {title_part}"


def format_error_message(error, max_len=50):
    """Format exception for display output.

    Args:
        error: Exception object
        max_len: Maximum error message length (default 50)

    Returns:
        Error type name if message too long, otherwise full message
    """
    error_str = str(error)
    return type(error).__name__ if len(error_str) > max_len else error_str


def calculate_download_progress(downloaded, total):
    """Calculate download progress percentage.

    Args:
        downloaded: Bytes downloaded so far (int)
        total: Total bytes to download (int)

    Returns:
        Progress percentage as integer (0-100)
    """
    if total <= 0:
        return 0
    return int(100 * downloaded / total)


def should_print_progress(current_pct, last_printed_pct, interval=10):
    """Determine if progress update should be printed.

    Args:
        current_pct: Current progress percentage (int)
        last_printed_pct: Last printed percentage (int)
        interval: Print interval in percentage points (default 10)

    Returns:
        True if we've crossed the next interval threshold
    """
    return current_pct >= last_printed_pct + interval


def convert_csv_to_parquet(csv_path, output_path):
    """Convert CSV to parquet using PyArrow in a subprocess for memory isolation.

    Each conversion runs in a separate subprocess that exits after completion,
    ensuring all memory is released back to the OS. This prevents memory
    accumulation when processing hundreds of datasets sequentially.

    Args:
        csv_path: Path to input CSV file (str or Path)
        output_path: Path to output parquet file (str or Path)

    Returns:
        None (writes file to disk)

    Raises:
        subprocess.TimeoutExpired: If conversion takes longer than 600 seconds
        subprocess.CalledProcessError: If conversion fails
    """
    # Core: Generate conversion script
    script = generate_conversion_script(csv_path, output_path)

    # I/O: Run in subprocess for memory isolation (10 minute timeout)
    subprocess.run(
        [sys.executable, '-c', script],
        check=True,
        timeout=600
    )


# === I/O Layer ===

def download_table(product_id, title):
    """Download CSV from StatsCan API and convert to parquet.

    This function handles all I/O operations: HTTP requests, file downloads,
    ZIP extraction, CSV parsing, and parquet writing.

    Args:
        product_id: Dataset product ID
        title: Dataset title

    Returns:
        Tuple of (size_mb, file_path) where file_path is relative to data/, or None if skipped
    """
    url = f"{API_BASE}/getFullTableDownloadCSV/{product_id}/en"
    headers = {'User-Agent': 'Mozilla/5.0'}

    # Core: Format display title
    display_title = format_display_title(product_id, title)

    # Get ZIP URL
    print(f"{display_title} - Starting...")
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    zip_url = data['object']

    # Get file size for progress tracking
    head_resp = requests.head(zip_url, headers=headers, timeout=30)
    head_resp.raise_for_status()
    total_size = int(head_resp.headers.get('content-length', 0))

    # Stream ZIP to temp file on disk
    zip_resp = requests.get(zip_url, headers=headers, timeout=600, stream=True)
    zip_resp.raise_for_status()

    downloaded = 0
    last_pct = -1

    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp_zip:
        for chunk in zip_resp.iter_content(chunk_size=1024*1024):  # 1MB chunks
            tmp_zip.write(chunk)
            downloaded += len(chunk)

            # Core: Calculate and check if we should print progress
            current_pct = calculate_download_progress(downloaded, total_size)
            if should_print_progress(current_pct, last_pct):
                print(f"{display_title} - Downloading {downloaded/1e6:.0f}/{total_size/1e6:.0f}MB ({current_pct}%)")
                last_pct = current_pct

        zip_path = tmp_zip.name

    print(f"{display_title} - Downloaded, extracting...")

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
    file_path = str(out_file.relative_to('data'))

    # Log memory usage
    current, peak = tracemalloc.get_traced_memory()
    print(f"{display_title} - Complete ({size_mb:.1f}MB) [Memory: {current/1e6:.1f}MB current, {peak/1e6:.1f}MB peak]")
    return size_mb, file_path


# === Orchestration ===

def process_dataset(product_id, title, state_lock, shared_state):
    """Worker function to process a single dataset.

    Wraps download_table() with error handling and shared state management.
    """
    try:
        result = download_table(product_id, title)
        if result is None:  # File was skipped
            return None

        size_mb, file_path = result

        # Update shared state
        with state_lock:
            shared_state['total_size_mb'] += size_mb
            shared_state['ingested'].append({
                'productId': product_id,
                'title': title,
                'size_mb': size_mb,
                'file_path': file_path
            })

        return size_mb
    except subprocess.TimeoutExpired as e:
        # Subprocess conversion timeout
        display_title = format_display_title(product_id, title)
        print(f"{display_title} - Error: Conversion timeout (>600s)")
        return None
    except subprocess.CalledProcessError as e:
        # Subprocess conversion failed
        display_title = format_display_title(product_id, title)
        print(f"{display_title} - Error: Conversion failed (returncode {e.returncode})")
        return None
    except Exception as e:
        # Core: Format display strings
        display_title = format_display_title(product_id, title)
        error_msg = format_error_message(e)
        print(f"{display_title} - Error: {error_msg}")
        return None


def main():
    """Main ingestion orchestration."""
    # Start memory tracking
    tracemalloc.start()

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
                # Core: Format display strings
                display_title = format_display_title(product_id, title)
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
