#!/bin/bash
#SBATCH --job-name=build_region_cache
#SBATCH --partition=ssd
#SBATCH --account=ssd
#SBATCH --qos=ssd
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --mem=180GB
#SBATCH --time=08:00:00
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/region_cache_%j.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/region_cache_%j.err

PYTHON_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"
PROJECT_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex"
CACHE_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/data_cache"
COUNTRIES_PARQUET="/project/jevans/tip/data/wos_2023/paper_countries_fixed_w_EU.parquet"
REGION_CACHE_DIR="${PROJECT_DIR}/country_filtered_cdindex/region_bitmaps"

# env
set -euo pipefail
ulimit -n 65535 || true
export LD_LIBRARY_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="${PROJECT_DIR}:${PROJECT_DIR}/fast_cdindex:${PYTHONPATH:-}"
export PYTHONUNBUFFERED=1
export OMP_NUM_THREADS=48
export MKL_NUM_THREADS=48
export MKL_DYNAMIC=FALSE
export OMP_PROC_BIND=close
export OMP_PLACES=cores
export CACHE_DIR
export COUNTRIES_PARQUET
export REGION_CACHE_DIR

echo "== Building region cache =="
echo "Host: $(hostname)"
echo "Date: $(date)"
echo "Python: ${PYTHON_PATH}"
echo "Cache dir: ${CACHE_DIR}"
echo "Countries parquet: ${COUNTRIES_PARQUET}"
echo "Region cache dir: ${REGION_CACHE_DIR}"
echo

"${PYTHON_PATH}" - <<'PY'
import os, gc, time
import pyarrow.parquet as pq
import pyarrow.compute as pc
from fast_cdindex.cdindex_enhanced import EnhancedGraph

def mem(stage):
    try:
        import psutil
        rss = psutil.Process().memory_info().rss / 1024 / 1024
        print(f"[{stage}] RSS: {rss:.1f} MB", flush=True)
    except Exception:
        pass

US_NAMES = ['usa']
CN_NAMES = ['peoples r china']
EU_NAMES = ['eu']  # already normalized upstream

CACHE_DIR      = os.environ.get('CACHE_DIR', '/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/data_cache')
COUNTRIES_PATH = os.environ.get('COUNTRIES_PARQUET', '/project/jevans/tip/data/wos_2023/paper_countries_fixed_w_EU.parquet')
REGION_DIR     = os.environ.get('REGION_CACHE_DIR', '/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/region_bitmaps')

print("Starting region bitmap creation...", flush=True); mem("Start")

vpath = os.path.join(CACHE_DIR, 'paper_years.parquet')

print("Loading vertices (UID, year, id)...", flush=True)
vt = pq.read_table(vpath, columns=['paper_id','UID','year'])
print(f"Loaded {vt.num_rows:,} vertices", flush=True); mem("After vertices")

print("Creating EnhancedGraph and adding vertices...", flush=True)
g = EnhancedGraph()
g.add_vertices_from_arrow(vt)
mem("After add_vertices")

print("Loading countries parquet (UID, country)...", flush=True)
ct = pq.read_table(COUNTRIES_PATH, columns=['UID','country'])
print(f"Loaded {ct.num_rows:,} country rows", flush=True); mem("After countries")

print("Setting country lists and ingesting...", flush=True)
g.set_country_lists(US_NAMES, CN_NAMES, EU_NAMES)
g.ingest_countries_from_parquet(ct, 'UID', 'country')
mem("After country ingestion")

print("Creating region directory and removing old files...", flush=True)
os.makedirs(REGION_DIR, exist_ok=True)
for fn in ("us.roar","cn.roar","eu.roar"):
    try: os.remove(os.path.join(REGION_DIR, fn))
    except FileNotFoundError: pass

# Sanity: label counts (before we delete ct)
try:
    lower = pc.utf8_lower(ct['country'])
    def cnt(name):
        return int(pc.sum(pc.cast(pc.equal(lower, pc.scalar(name)), 'int64')).as_py())
    print("[sanity] country label rows:",
          {"usa": cnt("usa"), "eu": cnt("eu"), "peoples r china": cnt("peoples r china")}, flush=True)
except Exception as e:
    print("[sanity] label count skipped:", e)

# Free memory before bitmap creation
del ct; gc.collect()

print("Saving region bitmaps (portable format)...", flush=True)
g.save_region_bitmaps(REGION_DIR)
mem("After saving")

# Sanity: file sizes
sizes = {f: (os.path.getsize(os.path.join(REGION_DIR, f))
             if os.path.exists(os.path.join(REGION_DIR, f)) else 0)
         for f in ("us.roar","eu.roar","cn.roar")}
print("[sanity] region file sizes (bytes):", sizes, flush=True)

# Extra: load back to verify readability on this node
try:
    ok = g.load_region_bitmaps(REGION_DIR)
    print("Reload check:", ok, flush=True)
except Exception as e:
    print("Reload check failed:", e, flush=True)

print("Region bitmap creation completed successfully!", flush=True)
PY
