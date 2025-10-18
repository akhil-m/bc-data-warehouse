#!/usr/bin/env python3
"""Discover and rank StatsCan datasets."""

import requests
import pandas as pd
from datetime import datetime


API_BASE = "https://www150.statcan.gc.ca/t1/wds/rest"


def get_all_cubes():
    """Fetch all available tables from StatsCan."""
    url = f"{API_BASE}/getAllCubesList"
    headers = {'User-Agent': 'Mozilla/5.0'}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def score_dataset(cube):
    """Score a dataset by interestingness (0-100)."""
    score = 50  # base score

    # Prefer frequent updates
    freq = cube.get('frequencyCode', '')
    if freq in ['6', '7']:  # monthly, weekly
        score += 20
    elif freq in ['1', '2']:  # annual, semi-annual
        score += 10

    # Prefer recently updated
    release = cube.get('releaseTime', '')
    if release:
        try:
            rel_date = datetime.fromisoformat(release.replace('Z', '+00:00'))
            days_old = (datetime.now(rel_date.tzinfo) - rel_date).days
            if days_old < 30:
                score += 20
            elif days_old < 90:
                score += 10
        except:
            pass

    # Prefer larger tables (more dimensions = richer data)
    dims = cube.get('dimensions', [])
    score += min(len(dims) * 3, 30)

    return score


def main():
    print("Fetching catalog...")
    cubes = get_all_cubes()

    print(f"Found {len(cubes)} tables")

    # Extract and score
    rows = []
    for cube in cubes:
        rows.append({
            'productId': cube.get('productId'),
            'title': cube.get('cubeTitleEn'),
            'subject': cube.get('subjectEn'),
            'frequency': cube.get('frequencyCode'),
            'releaseTime': cube.get('releaseTime'),
            'dimensions': len(cube.get('dimensions', [])),
            'nbDatapoints': cube.get('nbDatapointsCube'),
            'score': score_dataset(cube)
        })

    df = pd.DataFrame(rows)
    df = df.sort_values('score', ascending=False)

    # Save
    df.to_parquet('catalog.parquet', index=False)
    print(f"\nSaved to catalog.parquet")
    print(f"\nTop 10 datasets:\n{df.head(10)[['productId', 'title', 'score']]}")


if __name__ == '__main__':
    main()
