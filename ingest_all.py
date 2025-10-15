#!/usr/bin/env python3
"""Ingest multiple StatsCan datasets with size constraints."""

import pandas as pd
import requests
import zipfile
import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import utils


API_BASE = "https://www150.statcan.gc.ca/t1/wds/rest"
MAX_TOTAL_GB = 10  # Just immigration data
MAX_DOWNLOAD_MB = 5000  # Skip CSV files larger than this
NUM_WORKERS = 1  # Sequential processing to avoid memory exhaustion


def download_table(product_id, title):
    """Download CSV and convert to parquet."""
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

    if total_size > MAX_DOWNLOAD_MB * 1e6:
        print(f"{display_title} - Skipped (>{MAX_DOWNLOAD_MB}MB, {total_size/1e6:.0f}MB)")
        return None

    # Stream ZIP to temp file on disk
    zip_resp = requests.get(zip_url, headers=headers, timeout=120, stream=True)
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

    # Extract CSV to temp directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        with zipfile.ZipFile(zip_path) as z:
            csv_name = z.namelist()[0]
            csv_path = z.extract(csv_name, tmp_dir)

        # Load CSV from disk
        print(f"{display_title} - Converting to parquet...")
        df = pd.read_csv(csv_path, low_memory=False)

    # Clean up temp ZIP
    Path(zip_path).unlink()

    # Sanitize column names for parquet/avro compatibility
    df.columns = [col.replace(' ', '_').replace('/', '_').replace('-', '_') for col in df.columns]

    # Save parquet
    clean_title = "".join(c if c.isalnum() or c in (' ', '-') else '' for c in title)
    clean_title = "-".join(clean_title.lower().split())
    folder_name = f"{product_id}-{clean_title}"

    out_dir = Path("data") / folder_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{product_id}.parquet"
    df.to_parquet(out_file, index=False)

    size_mb = out_file.stat().st_size / 1e6
    print(f"{display_title} - Complete ({size_mb:.1f}MB)")
    return size_mb


def process_dataset(product_id, title, state_lock, shared_state):
    """Worker function to process a single dataset."""
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
    # Load catalog
    catalog = pd.read_parquet('immigration_catalog.parquet')

    # Skip datasets already in S3
    existing = utils.get_existing_dataset_ids('statscan')
    print(f"Already have {len(existing)} datasets in S3")
    catalog = catalog[~catalog['productId'].isin(existing)]

    # Skip INVISIBLE datasets (massive internal tables, do later)
    invisible_count = catalog['title'].str.contains('INVISIBLE', na=False).sum()
    catalog = catalog[~catalog['title'].str.contains('INVISIBLE', na=False)]
    print(f"Skipping {invisible_count} INVISIBLE datasets")

    # Sort by interestingness score (descending)
    catalog = catalog.sort_values('score', ascending=False)

    print(f"Processing {len(catalog)} datasets with {NUM_WORKERS} workers")
    print(f"Target: {MAX_TOTAL_GB} GB total\n")

    # Shared state
    state_lock = threading.Lock()
    shared_state = {
        'total_size_mb': 0,
        'ingested': []
    }

    # Process datasets in parallel
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {}

        for idx, row in catalog.iterrows():
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
