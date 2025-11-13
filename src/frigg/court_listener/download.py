import csv
import os
import subprocess
import sys

import pandas as pd

csv.field_size_limit(sys.maxsize)


def download(
    url: str = "https://storage.courtlistener.com/bulk-data/opinions-2025-10-31.csv.bz2",
):
    subprocess.run(["wget", "--continue", url])


def extract_bz2(path: str):
    subprocess.run(["lbzip2", "-d", path])


def convert(
    csv_path: str = "/opinions-2025-10-31.csv",
    output_dir: str = "parquet_chunks",
    chunksize: str = 50_000,
):
    os.makedirs(output_dir, exist_ok=True)

    df_header = pd.read_csv(csv_path, nrows=0)
    print("Columns from pandas:")
    print(df_header.columns.tolist())

    for i, chunk in enumerate(
        pd.read_csv(
            csv_path,
            quoting=1,  # csv.QUOTE_ALL
            escapechar="\\",
            engine="python",
            chunksize=chunksize,
            on_bad_lines="error",
            names=df_header.columns.tolist(),
        )
    ):
        out_path = os.path.join(output_dir, f"chunk_{i:05d}.parquet")
        chunk.to_parquet(out_path, compression="snappy", index=False)
        print(f"Saved {out_path}")
