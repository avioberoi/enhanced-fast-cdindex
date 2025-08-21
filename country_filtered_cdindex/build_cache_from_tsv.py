#!/usr/bin/env python3
import argparse, os, duckdb

def _expand_path_for_duckdb_csv(path: str) -> str:
    """
    If the provided path is a directory, expand it to a glob pattern that
    DuckDB's read_csv can consume (reads all parts). Otherwise, return as-is.
    """
    return os.path.join(path, "*.csv.gz") if os.path.isdir(path) else path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--years-tsv', required=True)
    ap.add_argument('--edges-tsv', required=True)
    ap.add_argument('--out-dir',   required=True)
    ap.add_argument('--zstd', type=int, default=3)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    vpath = os.path.join(args.out_dir, 'paper_years.parquet')
    epath = os.path.join(args.out_dir, 'edges.parquet')

    con = duckdb.connect()
    con.execute("PRAGMA threads = {}".format(os.cpu_count()))

    # years: two tab-separated columns, no header: UID \t YEAR (strings)
    years_path = _expand_path_for_duckdb_csv(args.years_tsv)
    con.execute(f"""
        CREATE TABLE years AS
        SELECT
          (ROW_NUMBER() OVER () - 1)::INTEGER AS paper_id,
          uid::VARCHAR AS UID,
          CAST(year AS INTEGER) AS year
        FROM read_csv(
          '{years_path}',
          delim='\t',
          header=false,
          columns={{'uid':'VARCHAR','year':'VARCHAR'}},
          auto_detect=false
        );
    """)
    con.execute(f"COPY years TO '{vpath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000);")

    # edges: two tab-separated columns, no header: SRC_UID \t TGT_UID (strings)
    edges_path = _expand_path_for_duckdb_csv(args.edges_tsv)
    con.execute(f"""
        CREATE TABLE edges AS
        SELECT
          src_uid::VARCHAR AS source_uid,
          tgt_uid::VARCHAR AS target_uid
        FROM read_csv(
          '{edges_path}',
          delim='\t',
          header=false,
          columns={{'src_uid':'VARCHAR','tgt_uid':'VARCHAR'}},
          auto_detect=false
        );
    """)
    con.execute(f"COPY edges TO '{epath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000);")

    # Optional: write a tiny id map (UID -> paper_id) for outside-C++ use
    # con.execute(f"""
    #     COPY (SELECT UID, paper_id FROM years)
    #     TO '{os.path.join(args.out_dir, 'id_map.parquet')}'
    #     (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000, CODEC '{args.zstd}');
    # """)

    print("Wrote:", vpath, epath)

if __name__ == '__main__':
    main()
