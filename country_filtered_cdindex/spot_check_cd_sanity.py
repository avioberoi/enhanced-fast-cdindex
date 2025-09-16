#!/usr/bin/env python3
import argparse, os, random, duckdb, math
import pyarrow.parquet as pq
from fast_cdindex.cdindex_enhanced import EnhancedGraph

def pick_sample_uids(con, legacy_glob, sample, mismatches_tsv=None):
    if mismatches_tsv and os.path.exists(mismatches_tsv):
        # Prefer the worst offenders first
        q = f"""
          SELECT UID FROM read_csv('{mismatches_tsv}', delim='\t', header=TRUE)
          WHERE UID IS NOT NULL
          LIMIT {sample}
        """
        return [r[0] for r in con.execute(q).fetchall()]
    # else: random sample from legacy universe
    q = f"""
      SELECT UID
      FROM read_csv('{legacy_glob}', delim='\\t', header=TRUE, auto_detect=TRUE, strict_mode=FALSE, null_padding=TRUE)
      WHERE UID IS NOT NULL
      USING SAMPLE {sample} ROWS;
    """
    return [r[0] for r in con.execute(q).fetchall()]

def legacy_window_ok(ts_i, ts_f, years):
    return (ts_i > ts_f) and (ts_i <= ts_f + years)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache-dir", required=True)
    ap.add_argument("--id-map", required=True)   # id_map.parquet (UID,paper_id)
    ap.add_argument("--legacy-glob", required=True)
    ap.add_argument("--years", type=int, default=5)
    ap.add_argument("--sample", type=int, default=1000)
    ap.add_argument("--mismatches", default="") # optional TSV from validator to focus on bad cases
    ap.add_argument("--threads", type=int, default=min(32, os.cpu_count() or 8))
    ap.add_argument('--flip-edges', action='store_true',
                help='Input edges are (cited,citing); flip to (citing,cited) at ingest')
    args = ap.parse_args()

    vpath = os.path.join(args.cache_dir, "paper_years.parquet")
    epath = os.path.join(args.cache_dir, "edges.parquet")

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={args.threads}")

    # 1) pick sample UIDs
    uids = pick_sample_uids(con, args.legacy_glob, args.sample, args.mismatches)
    if not uids:
        print("No UIDs sampled; abort.")
        return

    # 2) map UIDs → paper_id, year, and fetch legacy cd5/ncites5
    con.execute(f"CREATE OR REPLACE TEMP VIEW idmap AS SELECT UID, paper_id FROM parquet_scan('{args.id_map}')")
    con.execute(f"CREATE OR REPLACE TEMP VIEW years AS SELECT UID, paper_id, year FROM parquet_scan('{vpath}')")

    legacy = con.execute(f"""
      WITH U AS (SELECT * FROM (VALUES {",".join(["(?)"]*len(uids))}) AS t(UID))
      SELECT y.UID, y.paper_id, y.year,
             avg(TRY_CAST(s.cd5 AS DOUBLE))    AS legacy_cd5,
             avg(TRY_CAST(s.ncites5 AS BIGINT)) AS legacy_nc5
      FROM U
      JOIN years y USING (UID)
      JOIN read_csv('{args.legacy_glob}', delim='\\t', header=TRUE,
                    auto_detect=TRUE, strict_mode=FALSE, null_padding=TRUE,
                    nullstr=['','NaN','nan','NA','null','NULL']) s
           USING (UID)
      GROUP BY 1,2,3
    """, uids).arrow()

    rows = legacy.to_pydict()
    sample = list(zip(rows["UID"], rows["paper_id"], rows["year"], rows["legacy_cd5"], rows["legacy_nc5"]))
    print(f"Sample size with legacy join: {len(sample)}")

    # 3) build graph
    g = EnhancedGraph()
    vt = pq.read_table(vpath)   # provides UID mapping inside C++
    et = pq.read_table(epath)
    g.add_vertices_from_arrow(vt)
    if args.flip_edges:
        g.set_flip_edge_direction_on_ingest(True)
    g.add_edges_from_arrow(et)
    g.prepare_for_searching()

    g.properties.ingest_arrow(vt)
    g.properties.build_indexes()
    print("built year bitmaps from vertices")

        
    # --- Preflight guardrail: B_any cache parity on a small sample ---
    sample_size = min(200, vt.num_rows)  # cheap guardrail
    # sample paper_ids from *this shard* to keep it deterministic
    paper_ids = vt.to_pydict()['paper_id']
    sample_ids = random.sample(paper_ids, k=min(sample_size, len(paper_ids)))
    bad = 0
    for pid in sample_ids:
        if not g.check_bany_cache(pid):
            bad += 1
            if bad <= 5:
                print(f"[guardrail] B_any cache mismatch for paper_id={pid}")
    if bad > 0:
        print(f"[guardrail] FAIL: {bad}/{len(sample_ids)} B_any mismatches — aborting to avoid bad run.")
        import sys; sys.exit(2)
    else:
        print(f"[guardrail] OK: B_any cache parity passed on {len(sample_ids)} samples.")

    # quick sanity: window must not be empty for dt>0 if there exist any papers in (t, t+dt]
    # we can simply compute cdindex on a handful and ensure not all return NaN
    probe_ids = random.sample(paper_ids, k=min(50, len(paper_ids)))
    nan_only = 0
    for pid in probe_ids:
        v = g.cdindex(pid, args.years)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            nan_only += 1
    if nan_only == len(probe_ids):
        raise RuntimeError("All probe cdindex() calls returned NaN — year bitmaps likely not initialized")
    print("[preflight] window probe passed")

    # helpers to avoid repeated Python→C++ calls
    get_year = g.get_timestamp
    get_in   = g.in_edges
    get_out  = g.out_edges
    get_F    = g.get_citers
    cd_cpp   = g.cdindex

    discrep = []
    agree   = 0
    total   = 0

    for uid, pid, fyear, lcd5, lnc5 in sample:
        total += 1

        # --- Reconstruct legacy sets on top of the graph ---
        # F_t: forward citers of focal in (t_f, t_f+Δ]
        # F = [i for i in get_F(pid, args.years) if legacy_window_ok(get_year(i), fyear, args.years)]
        Fset = set(get_F(pid, args.years))

        # Citers of references within legacy window
        refs = get_out(pid)  # focal references (no time restriction on j)
        R_citers = set()
        for r in refs:
            for i in get_in(r):
                if legacy_window_ok(get_year(i), fyear, args.years):
                    R_citers.add(i)

        # union
        It = Fset | R_citers
        denom = len(It)

        # f_it and b_it as in legacy
        if denom == 0:
            cd_legacy_replay = 0.0  # legacy divides by |it|; in practice they either avoid denom=0 or treat as 0
        else:
            # Build a set of focal references for O(1) overlap checks
            Rset = set(refs)
            A = 0  # cites focal only (no overlap with focal refs)
            C = 0  # cites focal AND shares ≥1 reference with focal
            for i in It:
                f_it = (i in Fset)
                if not f_it:
                    continue  # contributes 0
                # b_it: any overlap between i.out_edges and focal.out_edges?
                b_it = False
                for j in get_out(i):
                    if j in Rset:
                        b_it = True
                        break
                # contribution: -2*f*b + f
                if b_it: C += 1
                else:    A += 1
            cd_legacy_replay = (A - C) / denom

        cd_new = cd_cpp(pid, args.years)

        # Simple report
        ok_ncites = (lnc5 is None) or (lnc5 == len(Fset))  # legacy ncites5 vs our F count
        ok_cd = (lcd5 is None and (cd_new is None or math.isnan(cd_new))) or \
                (lcd5 is not None and cd_new is not None and abs(lcd5 - cd_new) <= 1e-12)

        if ok_ncites and ok_cd:
            agree += 1
        else:
            discrep.append({
                "UID": uid, "legacy_nc5": lnc5, "F_count": len(Fset),
                "legacy_cd5": lcd5, "cd_new": cd_new, "cd_legacy_replay": cd_legacy_replay,
                "denom": denom, "A": None, "C": None  # A/C omitted to keep it lightweight
            })
        
        # pull raw counts from C++ to localize the mismatch
        try:
            cF, cFB, bwin, den_cpp, cd_cpp_dbg = g.debug_counts(pid, args.years)
            # quick print for first few mismatches
            if len(discrep) <= 10:
                print(f"[dbg] UID={uid} pid={pid} "
                        f"F={len(Fset)}/{cF}  FB={cFB}  bwin={bwin}  "
                        f"den_py={denom} den_cpp={den_cpp}  "
                        f"cd_py={cd_legacy_replay} cd_cpp={cd_cpp_dbg}")
        except Exception as e:
            if len(discrep) <= 10:
                print(f"[dbg] debug_counts failed for UID={uid} pid={pid}: {e}")

        if total % 50 == 0:
            print(f"[{total}/{len(sample)}] agreements={agree}  discrepancies={len(discrep)}")

    print("\n=== Spot-check summary ===")
    print(f"checked: {total}, agreements: {agree}, discrepancies: {len(discrep)}")
    if discrep:
        # show a few
        print("examples:")
        for d in discrep[:10]:
            print(d)

if __name__ == "__main__":
    main()
