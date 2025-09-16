#!/usr/bin/env python3
import argparse, os, duckdb

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--vertices-parquet', required=True, help='path to existing paper_years.parquet')
    ap.add_argument('--out', required=True, help='id_map.parquet')
    ap.add_argument('--threads', type=int, default=os.cpu_count())
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={args.threads}")

    if args.out.endswith('.parquet'):
        con.execute(f"""
            COPY (
              SELECT UID, paper_id
              FROM parquet_scan('{args.vertices_parquet}')
            ) TO '{args.out}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000);
        """)
    elif args.out.endswith('.csv.gz'):
        con.execute(f"""
            COPY (
              SELECT UID, paper_id
              FROM parquet_scan('{args.vertices_parquet}')
            ) TO '{args.out}'
            (FORMAT CSV, COMPRESSION GZIP, HEADER TRUE);
        """)
    else:
        raise SystemExit("Output must end with .parquet or .csv.gz")

    print("Wrote:", args.out)

if __name__ == '__main__':
    main()
