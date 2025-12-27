#!/usr/bin/env python3
"""parse_ttfl_table.py

Parse the TTFL HTML response and extract the MuTabme table into:
- A list of dictionaries (default)
- A pandas DataFrame
- A numpy structured array

Requires: beautifulsoup4, pandas, numpy
Installation: pip install beautifulsoup4 pandas numpy
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


def parse_table(html_file: str) -> list[dict[str, Any]]:
    """
    Parse the MuTabme table from an HTML file.
    
    Returns:
        List of dictionaries with column headers as keys.
    """
    with open(html_file, encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
    
    table = soup.find('table', {'id': 'MuTabme'})
    if not table:
        raise ValueError("Table with id='MuTabme' not found in HTML")
    
    # Extract headers from <thead>
    headers = []
    thead = table.find('thead')
    if thead:
        header_row = thead.find('tr')
        if header_row:
            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
    
    if not headers:
        raise ValueError("Could not extract table headers")
    
    # Extract rows from <tbody>
    rows = []
    tbody = table.find('tbody')
    if tbody:
        for tr in tbody.find_all('tr'):
            cells = tr.find_all(['td', 'th'])
            if cells:
                # Convert empty cells to None or empty string
                row_data = [cell.get_text(strip=True) or None for cell in cells]
                # Pad with None if needed
                while len(row_data) < len(headers):
                    row_data.append(None)
                rows.append(dict(zip(headers, row_data[:len(headers)])))
    
    return rows


def to_dataframe(rows: list[dict[str, Any]]) -> Any:
    """Convert list of dicts to pandas DataFrame."""
    if not HAS_PANDAS:
        raise ImportError("pandas is required for DataFrame output. Install with: pip install pandas")
    return pd.DataFrame(rows)


def to_numpy_array(rows: list[dict[str, Any]]) -> Any:
    """Convert list of dicts to numpy structured array."""
    if not HAS_NUMPY:
        raise ImportError("numpy is required for numpy array output. Install with: pip install numpy")
    
    if not rows:
        return np.array([])
    
    # Create dtype from first row
    dtype = [(key, object) for key in rows[0].keys()]
    
    # Create structured array
    arr = np.zeros(len(rows), dtype=dtype)
    for i, row in enumerate(rows):
        for key, value in row.items():
            arr[i][key] = value
    
    return arr


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse TTFL HTML table (id=MuTabme) and export to multiple formats"
    )
    parser.add_argument("html_file", help="Path to HTML file (from fetch_ttfl.py)")
    parser.add_argument(
        "-f", "--format",
        choices=["dict", "json", "csv", "dataframe", "numpy"],
        default="dict",
        help="Output format (default: dict)"
    )
    parser.add_argument("-o", "--output", help="Output file path (optional)")
    args = parser.parse_args()
    
    try:
        rows = parse_table(args.html_file)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    if args.format == "dict":
        output = rows
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            print(f"Saved {len(rows)} rows to {args.output}")
        else:
            print(json.dumps(output, indent=2, ensure_ascii=False))
    
    elif args.format == "json":
        output = rows
        filename = args.output or "table_output.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f"Saved {len(rows)} rows to {filename}")
    
    elif args.format == "csv":
        if not HAS_PANDAS:
            print("Error: pandas required for CSV export. Install with: pip install pandas", file=sys.stderr)
            sys.exit(2)
        df = to_dataframe(rows)
        filename = args.output or "table_output.csv"
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"Saved {len(rows)} rows to {filename}")
    
    elif args.format == "dataframe":
        if not HAS_PANDAS:
            print("Error: pandas required. Install with: pip install pandas", file=sys.stderr)
            sys.exit(2)
        df = to_dataframe(rows)
        print(f"DataFrame with {len(df)} rows, {len(df.columns)} columns:")
        print(df)
    
    elif args.format == "numpy":
        if not HAS_NUMPY:
            print("Error: numpy required. Install with: pip install numpy", file=sys.stderr)
            sys.exit(2)
        arr = to_numpy_array(rows)
        filename = args.output or "table_output.npy"
        np.save(filename, arr)
        print(f"Saved {len(arr)} rows to {filename}")


if __name__ == "__main__":
    main()
