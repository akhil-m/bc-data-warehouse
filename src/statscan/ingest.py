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


def _do_csv_conversion(csv_path, output_path):
    """Imperative shell for CSV to parquet conversion.

    This runs in a subprocess for memory isolation. After conversion completes,
    the subprocess exits and all memory is released back to the OS.

    ALL COLUMNS FORCED TO STRING TYPE:
    - Disables PyArrow type inference via column_types parameter
    - Handles mixed types in same column (e.g., '4680, 4690', '1011-C')
    - Preserves raw data exactly as-is (no type coercion)
    - Standard data lake pattern (type casting happens at query time in Athena)

    Handles StatsCan's standard table symbols (official list):
    https://www.statcan.gc.ca/en/concepts/definitions/guide-symbol

    Args:
        csv_path: Path to input CSV file (str or Path)
        output_path: Path to output parquet file (str or Path)

    Returns:
        None (writes file to disk)
    """
    import pyarrow.csv as pa_csv
    import pyarrow.parquet as pq

    # I/O: Get column names from CSV header (lightweight, doesn't load data)
    df_header = pd.read_csv(csv_path, nrows=0)
    original_columns = df_header.columns.tolist()

    # Core: Use pure functions for all logic
    sanitized_columns = sanitize_column_names(original_columns)
    string_schema = create_string_schema(sanitized_columns)
    null_values = get_statscan_null_values()
    column_types = create_column_type_map(original_columns)
    parse_options = create_parse_options()
    read_options = create_read_options()

    # Configure CSV parsing with robust options
    convert_options = pa_csv.ConvertOptions(
        null_values=null_values,
        strings_can_be_null=True,
        column_types=column_types,
        include_missing_columns=True  # Handle column count mismatches
    )

    # I/O: Stream CSV to parquet with robust parsing
    with pa_csv.open_csv(
        csv_path,
        parse_options=parse_options,
        convert_options=convert_options,
        read_options=read_options
    ) as reader:
        with pq.ParquetWriter(output_path, string_schema) as writer:
            for batch in reader:
                # Core: Rename columns using pure function
                renamed_batch = rename_batch_columns(batch, string_schema)
                # I/O: Write batch
                writer.write_batch(renamed_batch)


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


def get_statscan_null_values():
    """Get StatsCan standard table symbols to treat as nulls.

    Official list: https://www.statcan.gc.ca/en/concepts/definitions/guide-symbol

    Returns:
        List of strings to treat as null values in CSV parsing
    """
    return [
        '',              # Empty string
        '.', '..', '...', # Not available/applicable
        'x', 'X',        # Suppressed
        'E', 'e',        # Use with caution
        'F', 'f',        # Too unreliable
        't', 'T',        # Terminated
        'A', 'B', 'C', 'D', # Quality grades
        'p', 'r',        # Preliminary/revised
        '0s'             # Rounded to zero
    ]


def create_string_schema(column_names):
    """Create PyArrow schema with all columns as string type.

    Args:
        column_names: List of column names (already sanitized)

    Returns:
        PyArrow schema with all string fields
    """
    import pyarrow as pa
    return pa.schema([
        pa.field(name, pa.string())
        for name in column_names
    ])


def create_column_type_map(column_names):
    """Create column-to-type mapping for PyArrow CSV parsing.

    Args:
        column_names: List of original column names (before sanitization)

    Returns:
        Dict mapping each column name to pa.string() type
    """
    import pyarrow as pa
    return {col: pa.string() for col in column_names}


def rename_batch_columns(batch, schema):
    """Rename columns in a PyArrow RecordBatch.

    Args:
        batch: PyArrow RecordBatch with original column names
        schema: Target PyArrow schema with sanitized column names

    Returns:
        PyArrow RecordBatch with renamed columns
    """
    import pyarrow as pa
    return pa.RecordBatch.from_arrays(
        [batch.column(i) for i in range(batch.num_columns)],
        schema=schema
    )


def create_parse_options():
    """Create PyArrow ParseOptions for robust CSV parsing.

    Returns:
        PyArrow ParseOptions configured to handle:
        - Multiline cell values (newlines within quoted fields)
        - Empty lines in CSV files
    """
    import pyarrow.csv as pa_csv
    return pa_csv.ParseOptions(
        newlines_in_values=True,  # Handle newlines within quoted fields
        ignore_empty_lines=True   # Skip empty lines in CSV
    )


def create_read_options():
    """Create PyArrow ReadOptions for CSV reading.

    Returns:
        PyArrow ReadOptions with UTF-8 encoding
    """
    import pyarrow.csv as pa_csv
    return pa_csv.ReadOptions(
        encoding='utf8'
    )


def validate_zip_magic_bytes(file_path):
    """Validate that file is a valid ZIP archive by checking magic bytes.

    Args:
        file_path: Path to file to validate (str or Path)

    Returns:
        None if valid

    Raises:
        ValueError: If file is not a valid ZIP archive
    """
    with open(file_path, 'rb') as f:
        magic = f.read(4)
        if magic != b'PK\x03\x04':
            raise ValueError(
                f"Not a valid ZIP file (magic bytes: {magic.hex() if magic else 'empty'}). "
                f"StatsCan API may have returned an error page instead of dataset."
            )


def find_csv_in_zip(namelist):
    """Find data CSV file in ZIP archive, skipping metadata files.

    Args:
        namelist: List of filenames from ZipFile.namelist()

    Returns:
        CSV filename to process (data file, not metadata)

    Raises:
        ValueError: If no CSV file found or namelist is empty
    """
    if not namelist:
        raise ValueError("ZIP archive is empty")

    csv_files = [name for name in namelist if name.lower().endswith('.csv')]

    if not csv_files:
        raise ValueError(f"No CSV file found in ZIP. Files: {namelist}")

    # Skip MetaData CSV files (e.g., '98100137_MetaData.csv')
    data_csvs = [f for f in csv_files if 'MetaData' not in f]

    # Prefer data CSV, fallback to first CSV if all are metadata
    return data_csvs[0] if data_csvs else csv_files[0]


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
    # I/O: Run conversion in subprocess for memory isolation (10 minute timeout)
    script = f"from src.statscan.ingest import _do_csv_conversion; _do_csv_conversion('{csv_path}', '{output_path}')"
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

    print(f"{display_title} - Downloaded, validating...")

    # Core: Validate ZIP file before extraction
    validate_zip_magic_bytes(zip_path)

    print(f"{display_title} - Extracting...")

    # Extract CSV to temp directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        with zipfile.ZipFile(zip_path) as z:
            # Core: Find CSV file in ZIP (robust, handles edge cases)
            csv_name = find_csv_in_zip(z.namelist())
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
