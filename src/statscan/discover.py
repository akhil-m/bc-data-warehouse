#!/usr/bin/env python3
"""Discover StatsCan datasets."""

import requests
import pandas as pd


API_BASE = "https://www150.statcan.gc.ca/t1/wds/rest"


# === Functional Core (Pure Functions - No I/O) ===

def extract_catalog_metadata(cubes):
    """Extract metadata from StatsCan API cube list to DataFrame rows.

    Args:
        cubes: List of cube dicts from getAllCubesList API

    Returns:
        List of metadata dicts ready for DataFrame creation
    """
    rows = []
    for cube in cubes:
        rows.append({
            'productId': cube.get('productId'),
            'title': cube.get('cubeTitleEn'),
            'subject': cube.get('subjectEn'),
            'frequency': cube.get('frequencyCode'),
            'releaseTime': cube.get('releaseTime'),
            'dimensions': len(cube.get('dimensions', [])),
            'nbDatapoints': cube.get('nbDatapointsCube')
        })
    return rows


# === I/O Layer ===

def get_all_cubes():
    """Fetch all available tables from StatsCan."""
    url = f"{API_BASE}/getAllCubesList"
    headers = {'User-Agent': 'Mozilla/5.0'}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def main():
    # I/O: Fetch catalog from API
    print("Fetching catalog...")
    cubes = get_all_cubes()
    print(f"Found {len(cubes)} tables")

    # Core: Extract metadata to structured format
    rows = extract_catalog_metadata(cubes)
    df = pd.DataFrame(rows)

    # Save
    df.to_parquet('catalog.parquet', index=False)
    print(f"\nSaved to catalog.parquet")


if __name__ == '__main__':
    main()
