# Enhanced Fast CD-Index (with Country-Filtered Variants)

This repo contains a **memory-efficient, high-throughput** implementation of the Disruption (CD) index with **region-filtered scores** (US, EU, China), written in C++ (Arrow + CRoaring) with a Python wrapper and HPC-friendly tooling.

---

## Why we touched the legacy pipeline

### What the legacy code did

* For each focal paper **f** and window **(f.time, f.time+Δ]**, it explicitly built the **i-set** by scanning:

  * citers of the focal (`F_t`), and
  * citers of all predecessors (`B_t`) in the time window.
* Then, for every i ∈ i-set, computed `f_i` and `b_i` and accumulated **Σ(−2·f\_i·b\_i + f\_i)**, dividing by `|i-set|`.

This worked, but:

* Time-window handling depended on **cumulative prefix arrays** and bitmap subtraction, which blew up memory (we hit **\~119 GB** in realistic runs).
* The hot path did **two big intersections** with giant prefix bitmaps.
* Filtering by country was awkward/expensive.

---

## What we do now (Algebra changes)

We reformulate with three sets:

* **F\_t** — citers of the focal **f** in the window
* **B\_any** — union of **all citers of all predecessors** (unfiltered, time-invariant)
* **W\_t** — all papers published in the window (built by **union of year bitmaps**)

Then:

* **Numerator**:
  Σ(−2·f\_i·b\_i + f\_i) = `|F_t| − 2·|F_t ∩ B_any|`
* **Denominator**:
  `|F_t ∪ B_t|` where `B_t = B_any ∩ W_t`
  so `|F_t ∪ B_t| = |F_t| + |B_any ∩ W_t| − |F_t ∩ B_any|`

This eliminates per-query **B\_t materialization** and the giant prefix arrays entirely.

### Country filtering (include/exclude)

Let **R** be a region bitmap (US/EU/CN). We **only filter the citer side**.
When we ask “what disruption looks like from outside the US”, we **still compute scores for papers with US authors**, and we include **everything they cited**; we just **exclude US citers** from the forward-looking side.

* **Include-only (OnlyR)**

  * Numerator: `|F_t ∩ R| − 2·|F_t ∩ B_any ∩ R|`
  * Denominator: `|(F_t ∩ R) ∪ (B_any ∩ W_t ∩ R)|`
* **Exclude (ExcludeR)**
  Do the complements via counts (no complement bitmaps):

  * `cF_C = |F_t| − |F_t ∩ R|`
  * `cFB_C = |F_t ∩ B_any| − |F_t ∩ B_any ∩ R|`
  * `bwin_C = |B_any ∩ W_t| − |B_any ∩ W_t ∩ R|`

**Multi-country papers** belong to all regions they’re mapped to (“OR” semantics).
Example: a paper tagged both `usa` and `eu`:

* contributes to **OnlyUS** and **OnlyEU**,
* is removed by **ExcludeUS** and **ExcludeEU**.

---

## Implementation highlights

### Core (C++)

* **CRoaring** bitmaps for sets; **fastunion** for year windows; **Arrow** for I/O.
* **W\_t on demand**: union of only the needed per-year bitmaps. No prefix arrays.
* **B\_any caching** (per focal): time-invariant, stored once, reused.
* **Time-window LRU** cache (for `W_t`) keyed by `(f.time, Δ)`.
* **Predecessor citer caches** (filtered/unfiltered) where needed.
* **Incoming edges sorted by time** for `F_t` via binary search.
* **Shared ownership** (`std::shared_ptr<const Roaring>`) returned from caches to avoid use-after-free.
* **Thread-safe** region bitmaps via `std::call_once`.
* **NaN semantics**: when the filtered i-set is empty (denominator = 0), CD returns **NaN**.
  `mCD = CD * iindex` also returns **NaN** when `iindex==0`.

### PropertyStore

* Ingests Arrow Tables and builds **categorical bitmaps**, including **year** and **country**.
* For the 299M country rows: we support an **eager region-only ingest** that directly builds **US/EU/CN** bitmaps from normalized strings, **without** materializing bitmaps for every country.
* Optionally persist **region bitmaps** to disk (`us.roar`, `eu.roar`, `cn.roar`) and reload them for later runs.

### Python wrapper

* Thin wrapper over the C++ API with Arrow passthrough and micro-benchmark access.

---

## Performance

What changes matter most:

* **Removed prefix arrays** → massive **memory savings** (we eliminated the \~119 GB explosion).
* **One intersection instead of two** on the hot path (`B_any ∩ W_t` only).
* **Small `W_t`** (few years union) + LRU caching.
* **Time-invariant `B_any` per focal** (cached once).
* **Sorted incoming edges** → `F_t` via two binary searches.

The code carries a micro-benchmark:

```
g_benchmark: counts, time breakdown (F_t build / B_any+W_t / cardinalities), cache hit rates
```

Use it to confirm throughput on your hardware & dataset.

---

## Building

### Dependencies

* C++17 toolchain
* [Apache Arrow C++](https://arrow.apache.org/) + Arrow Python
* [CRoaring](https://github.com/RoaringBitmap/CRoaring)
* [pybind11](https://github.com/pybind/pybind11)

### Build the Python extension

```
pip install pyarrow
# ensure CRoaring + Arrow C++ are discoverable (LD_LIBRARY_PATH / CMAKE_PREFIX_PATH)
python setup.py build_ext -j$(nproc) install
```

---

## Public API (Python)

```python
from fast_cdindex.cdindex_enhanced import EnhancedGraph, CiterFilter
import pyarrow.parquet as pq

# 1) Load caches
g = EnhancedGraph()
vt = pq.read_table("paper_years.parquet")   # columns: paper_id (u32), year (i32)
et = pq.read_table("edges.parquet")         # columns: source_id (u32), target_id (u32)
g.add_vertices_from_arrow(vt)
g.add_edges_from_arrow(et)
g.prepare_for_searching()

# 2) Build year bitmaps (PropertyStore) once
g.properties.ingest_arrow(vt)
g.properties.build_indexes()

# 3) Regions (use saved cache if available)
g.load_region_bitmaps("region_cache/")  # loads us.roar / eu.roar / cn.roar
# or build once from countries parquet:
# ct = pq.read_table("paper_countries.parquet", columns=["UID","country"])
# g.set_country_lists(['usa'], ['peoples r china'], ['eu'])
# g.ingest_countries_from_parquet(ct, uid_col="UID", country_col="country")
# g.save_region_bitmaps("region_cache/")

# 4) Compute
pid = 123456
cd = g.cdindex(pid, years=150)
cd_only_us = g.cdindex_filtered(pid, 150, CiterFilter.OnlyUS)
cd_excl_us = g.cdindex_filtered(pid, 150, CiterFilter.ExcludeUS)
iidx = g.iindex(pid, 150)
mcd = g.mcdindex(pid, 150)
```

**Notes**

* `cdindex` returns **NaN** when the denominator (filtered i-set) is empty.
* The **filter applies only to citers**; the focal and its references are unaffected.

---

## CLI & HPC workflow

We use two scripts for scale-out workflows.

### 1) Build Parquet cache from TSV (one-time)

Converts:

* `paper_years_all.tsv` → `paper_years.parquet`
* `edges_all.tsv` → `edges.parquet`
  …with deterministic `UID → paper_id (uint32)` mapping.

> See `country_filtered_cdindex/build_cache_from_tsv.py` and its SLURM wrapper.

### 2) Compute filtered CD (sharded)

Loads the Parquet caches, loads or builds region bitmaps (prefer loading), then computes:

* `cd`,
* `cd_only_us`, `cd_excl_us`,
* `cd_only_eu`, `cd_excl_eu`,
* `cd_only_cn`, `cd_excl_cn`.

It writes **compressed TSV chunks every \~340k papers** to avoid data loss on preemption.

**Run locally**

```bash
python country_filtered_cdindex/compute_filtered_cd.py \
  --cache-dir country_filtered_cdindex/data_cache \
  --countries-parquet /path/paper_countries.parquet \
  --region-cache-dir country_filtered_cdindex/region_cache \
  --out-prefix country_filtered_cdindex/out/scores \
  --years 150 --total-parts 10 --part-id 0 --chunk-size 340000
```

**SLURM: build region cache once**

```bash
sbatch country_filtered_cdindex/sbatch_build_region_cache.sbatch
```

**SLURM: compute across multiple partitions (“clusters”)**
Submit the same array script on two partitions with different fan-out env vars:

```bash
# Partition A
sbatch --partition=ssd --export=ALL,CLUSTERS=2,CLUSTER_INDEX=0,TOTAL_PARTS=50 \
  country_filtered_cdindex/sbatch_compute_filtered_cd.sbatch

# Partition B
sbatch --partition=hbm --export=ALL,CLUSTERS=2,CLUSTER_INDEX=1,TOTAL_PARTS=50 \
  country_filtered_cdindex/sbatch_compute_filtered_cd.sbatch
```

Global shards = `CLUSTERS × TOTAL_PARTS`. Each task writes:

```
out/scores.part<GLOBAL_PART_ID>.chunk<CHUNK_IDX>.csv.gz
```

---

## Data expectations

* `paper_years.parquet`: `paper_id (uint32)`, `year (int32)` (one-to-one)
* `edges.parquet`: `source_id (uint32)`, `target_id (uint32)` (≈1.55B edges)
* `paper_countries.parquet`: `UID (string)`, `country (string)` (299M rows, normalized lowercase: `usa`, `eu`, `peoples r china`)

**Normalization policy**

* Countries normalized to lowercase.
* **EU** is a single bucket (`eu`). **UK is not in EU** (policy).
* A paper may map to **multiple** regions (OR semantics).

---

## Validation

There’s a validation driver that:

* Loads the graph from cache,
* Recomputes `cd`, `mcd`, `iindex`,
* Compares against legacy scores with **tolerance**,
* Reports diffs, match rates, and performance counters.

You can also run **legacy diagnostic mode** to cross-check algebra equivalence on subsets.

---

## Tuning & env vars

* `INGEST_CHUNK_SIZE` (default 1,000,000): Arrow batch size.
* `MAX_CACHE_ENTRIES` (default 4096): LRU capacities.
* `BATCH_PARALLEL_THRESHOLD`, `INNER_PARALLEL_THRESHOLD`: OpenMP knobs.
* `OMP_NUM_THREADS`, `MKL_NUM_THREADS`: CPU threading.

---

## File layout (high-level)

```
fast_cdindex/
  cdindex_enhanced.h / .cc      # C++ core + PropertyStore
  pybind.cpp                     # Python bindings
  cdindex_enhanced.py            # Python wrapper

country_filtered_cdindex/
  build_cache_from_tsv.py        # TSV → Parquet
  compute_filtered_cd.py         # Compute un/filtered CD (chunked outputs)
  sbatch_build_region_cache.sbatch
  sbatch_compute_filtered_cd.sbatch
  data_cache/                    # parquet caches
  region_cache/                  # us.roar / eu.roar / cn.roar
  out/                           # results
```

---

## Gotchas & conventions

* **CD returns NaN** when the filtered i-set is empty (denominator = 0). We propagate NaNs to the TSV as `"NaN"`.
* **Filter applies to citers only**; it does **not** change the focal or its references.
* Always call `prepare_for_searching()` after loading edges to enable fast `F_t`.
* Prefer **loading** region bitmaps (roar files) in large runs; building once is enough.

---

## License

TBD.

---

## Acknowledgments

Thanks to the original pipeline authors. This rework keeps the semantics while dramatically reducing memory and improving throughput on large WoS-scale datasets.
