#!/usr/bin/env python3
import os, sys, math, random, gc, argparse, time
import pyarrow.parquet as pq
from fast_cdindex.cdindex_enhanced import EnhancedGraph, CiterFilter

US_NAMES = ['usa']
CN_NAMES = ['peoples r china']
EU_NAMES = ['eu']

def mem(msg):
    try:
        import psutil
        rss = psutil.Process().memory_info().rss/1024/1024
        print(f"[mem] {msg}: {rss:,.1f} MB", flush=True)
    except Exception:
        pass

def region_probe(g: EnhancedGraph, paper_ids, years, k=1000, tol=1e-12):
    if not paper_ids:
        return dict(tested=0)
    idxs = random.sample(range(len(paper_ids)), min(k, len(paper_ids)))
    p = [paper_ids[i] for i in idxs]
    base = [g.cdindex(v, years) for v in p]
    # keep only those with finite base
    keep = [i for i,b in enumerate(base) if isinstance(b, float) and not math.isnan(b)]
    if not keep:
        return dict(tested=0, base_nonnull=0)
    p = [p[i] for i in keep]
    base = [base[i] for i in keep]
    only_us  = [g.cdindex_filtered(v, years, CiterFilter.OnlyUS)   for v in p]
    excl_us  = [g.cdindex_filtered(v, years, CiterFilter.ExcludeUS) for v in p]
    only_eu  = [g.cdindex_filtered(v, years, CiterFilter.OnlyEU)   for v in p]
    excl_eu  = [g.cdindex_filtered(v, years, CiterFilter.ExcludeEU) for v in p]
    only_cn  = [g.cdindex_filtered(v, years, CiterFilter.OnlyCN)   for v in p]
    excl_cn  = [g.cdindex_filtered(v, years, CiterFilter.ExcludeCN) for v in p]

    def nonnull(xs): return sum(0 if (x is None or (isinstance(x,float) and math.isnan(x))) else 1 for x in xs)
    def diffcount(a,b):
        cnt = 0
        for x,y in zip(a,b):
            if isinstance(x,float) and isinstance(y,float):
                if math.isnan(x) and math.isnan(y): 
                    continue
                if not math.isfinite(x) or not math.isfinite(y): 
                    continue
                if abs(x-y) > tol: 
                    cnt += 1
        return cnt

    return dict(
        tested=len(p),
        base_nonnull=len(p),
        only_us_nonnull=nonnull(only_us),
        only_eu_nonnull=nonnull(only_eu),
        only_cn_nonnull=nonnull(only_cn),
        excl_us_diff=diffcount(base,excl_us),
        excl_eu_diff=diffcount(base,excl_eu),
        excl_cn_diff=diffcount(base,excl_cn),
    )

def build_graph(vpath, epath, flip_edges):
    vt = pq.read_table(vpath, columns=['paper_id','UID','year'])
    # et = pq.read_table(epath)
    g = EnhancedGraph()
    g.add_vertices_from_arrow(vt)
    if flip_edges:
        g.set_flip_edge_direction_on_ingest(True)
    
    import pyarrow as pa, pyarrow.dataset as ds, gc, math
    cols = ['source_id','target_id','source_uid','target_uid']  # whatever exists
    dataset = ds.dataset(epath, format="parquet")
    scanner = dataset.scanner(columns=[c for c in cols if c in dataset.schema.names],
                            use_threads=True,
                            batch_size=1<<20)  # tune (e.g., 1–8MB)

    batch_buf = []
    B = 0
    for i, batch in enumerate(scanner.to_batches()):
        batch_buf.append(batch)
        B += batch.num_rows
        # Coalesce a few batches to amortize overhead
        if B >= 5_000_000:  # tune by memory; ~5M edges per push works well
            g.add_edges_from_arrow(pa.Table.from_batches(batch_buf))
            batch_buf.clear(); B = 0
            if (i+1) % 20 == 0:
                gc.collect()
    if batch_buf:
        g.add_edges_from_arrow(pa.Table.from_batches(batch_buf))
    mem("after add_edges_from_arrow")
    del dataset; gc.collect()
    del scanner; gc.collect()
    del batch_buf; gc.collect()

    # pf = pq.ParquetFile(epath)
    # num_rgs = pf.num_row_groups
    # print(f"Streaming edges row-groups: {num_rgs}", flush=True)
    # for rg in range(num_rgs):
    #     et_rg = pf.read_row_group(
    #         rg,
    #         columns=['source_id','target_id','source_uid','target_uid']  # any subset present is fine
    #     )
    #     g.add_edges_from_arrow(et_rg)
    #     del et_rg
    #     if (rg+1) % 8 == 0:
    #         gc.collect()
    #         mem(f"after edges row-group {rg+1}/{num_rgs}")
    # mem("after all edges")
    # del pf; gc.collect()

    # g.add_edges_from_arrow(et)
    g.prepare_for_searching()
    # year bitmaps
    g.properties.ingest_arrow(vt)
    g.properties.build_indexes()
    return g, vt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cache-dir', required=True)
    ap.add_argument('--countries-parquet', required=True)
    ap.add_argument('--region-cache-dir', required=True)
    ap.add_argument('--years', type=int, default=5)
    ap.add_argument('--flip-edges', action='store_true')
    ap.add_argument('--sample', type=int, default=1000)
    args = ap.parse_args()

    vpath = os.path.join(args.cache_dir, 'paper_years.parquet')
    epath = os.path.join(args.cache_dir, 'edges.parquet')

    print("== Region cache probe ==")
    print(f"vertices: {vpath}")
    print(f"edges   : {epath}")
    print(f"regions : {args.region_cache_dir}")
    print(f"years   : {args.years}  flip_edges={args.flip_edges}")
    mem("start")

    # ---------- Graph A: load EXISTING region cache ----------
    gA, vtA = build_graph(vpath, epath, args.flip_edges)
    n = vtA.num_rows
    pids = vtA.column('paper_id').to_pylist()
    print(f"[A] graph built with {n:,} vertices", flush=True)
    try:
        gA.load_region_bitmaps(args.region_cache_dir)
        print(f"[A] loaded region cache from {args.region_cache_dir}", flush=True)
    except Exception as e:
        print(f"[A][ERROR] failed to load region cache: {e}", flush=True)
        sys.exit(2)
    probeA = region_probe(gA, pids, args.years, k=args.sample)
    print("[A] probe:", probeA, flush=True)
    mem("after A")

    # # ---------- Graph B: REBUILD regions from countries (no save) ----------
    # gB, vtB = build_graph(vpath, epath, args.flip_edges)
    # ct = pq.read_table(args.countries_parquet, columns=['UID','country'])
    # gB.set_country_lists(US_NAMES, CN_NAMES, EU_NAMES)
    # gB.ingest_countries_from_parquet(ct, 'UID', 'country')
    # print("[B] rebuilt regions from countries parquet (not saved)", flush=True)
    # probeB = region_probe(gB, vtB.column('paper_id').to_pylist(), args.years, k=args.sample)
    # print("[B] probe:", probeB, flush=True)
    # mem("after B")

    # print("\n== Conclusion ==")
    # if probeA.get('tested',0)==0:
    #     print("No testable papers; something is off with the graph build.", flush=True)
    #     sys.exit(3)

    # badA = (probeA['only_us_nonnull']==0 and probeA['only_eu_nonnull']==0 and probeA['only_cn_nonnull']==0
    #         and probeA['excl_us_diff']==0 and probeA['excl_eu_diff']==0 and probeA['excl_cn_diff']==0)

    # goodB = (probeB['only_us_nonnull']>0 or probeB['only_eu_nonnull']>0 or probeB['only_cn_nonnull']>0
    #          or probeB['excl_us_diff']>0 or probeB['excl_eu_diff']>0 or probeB['excl_cn_diff']>0)

    # if badA and goodB:
    #     print("Region cache is incompatible/stale (IDs or names). Rebuild it and re-run compute.", flush=True)
    #     sys.exit(0)
    # elif badA and not goodB:
    #     print("Both cached and rebuilt regions appear empty → country-name normalization likely wrong.", flush=True)
    #     sys.exit(1)
    # else:
    #     print("Cached regions look functional; the earlier flatlines must come from elsewhere.", flush=True)
    #     sys.exit(0)

if __name__ == "__main__":
    main()
