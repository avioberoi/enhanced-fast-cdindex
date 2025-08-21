#!/bin/bash
#SBATCH --job-name=build_region_cache
#SBATCH --partition=jevans
#SBATCH --account=pi-jevans
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --mem=1024GB
#SBATCH --time=08:00:00
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/region_cache_%j.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/region_cache_%j.err


PYTHON_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"
PROJECT_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex"
CACHE_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/data_cache" # contains paper_years.parquet, edges.parquet
COUNTRIES_PARQUET="/project/jevans/tip/data/wos_2023/paper_countries_fixed_w_EU.parquet"
REGION_CACHE_DIR="${PROJECT_DIR}/country_filtered_cdindex/region_bitmaps"  # where us.roar/cn.roar/eu.roar will be written

# env
export LD_LIBRARY_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib:${LD_LIBRARY_PATH}"
export PYTHONPATH="${PROJECT_DIR}:${PROJECT_DIR}/fast_cdindex:${PYTHONPATH}"
export PYTHONUNBUFFERED=1
export OMP_NUM_THREADS=48
export MKL_NUM_THREADS=48
export OMP_PROC_BIND=close
export OMP_PLACES=cores

set -euo pipefail

echo "== Building region cache =="
echo "Host: $(hostname)"
echo "Date: $(date)"
echo "Python: ${PYTHON_PATH}"
echo "Cache dir: ${CACHE_DIR}"
echo "Countries parquet: ${COUNTRIES_PARQUET}"
echo "Region cache dir: ${REGION_CACHE_DIR}"
echo

"${PYTHON_PATH}" - <<PY
import os
import gc
import psutil
import pyarrow.parquet as pq
from fast_cdindex.cdindex_enhanced import EnhancedGraph

def print_memory_usage(stage):
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024
    print(f"[{stage}] Memory usage: {memory_mb:.1f} MB", flush=True)

US_NAMES = ['usa']
CN_NAMES = ['peoples r china']
EU_NAMES = ['eu']

CACHE_DIR      = '${CACHE_DIR}'
COUNTRIES_PATH = '${COUNTRIES_PARQUET}'
REGION_DIR     = '${REGION_CACHE_DIR}'

print("Starting region bitmap creation...", flush=True)
print_memory_usage("Start")

vpath = os.path.join(CACHE_DIR, 'paper_years.parquet')
epath = os.path.join(CACHE_DIR, 'edges.parquet')

print("Loading vertices...", flush=True)
vt = pq.read_table(vpath)
print(f"Loaded {vt.num_rows:,} vertices", flush=True)
print_memory_usage("After vertices")

print("Loading edges...", flush=True)
et = pq.read_table(epath)
print(f"Loaded {et.num_rows:,} edges", flush=True)
print_memory_usage("After edges")

print("Creating EnhancedGraph...", flush=True)
g = EnhancedGraph()
print_memory_usage("After graph creation")

print("Adding vertices to graph...", flush=True)
g.add_vertices_from_arrow(vt)
print_memory_usage("After adding vertices")

# Clear vertex table from memory
del vt
gc.collect()
print_memory_usage("After clearing vertices")

print("Adding edges to graph...", flush=True)
g.add_edges_from_arrow(et)
print_memory_usage("After adding edges")

# Clear edge table from memory
del et
gc.collect()
print_memory_usage("After clearing edges")

print("Preparing graph for searching...", flush=True)
g.prepare_for_searching()
print_memory_usage("After prepare")

print("Ingesting vertex properties...", flush=True)
# Re-read vertices for properties (smaller memory footprint)
vt_props = pq.read_table(vpath)
g.properties.ingest_arrow(vt_props)
g.properties.build_indexes()
del vt_props
gc.collect()
print_memory_usage("After properties")

print("Loading country data...", flush=True)
ct = pq.read_table(COUNTRIES_PATH, columns=['UID','country'])
print(f"Loaded {ct.num_rows:,} country records", flush=True)
print_memory_usage("After countries")

print("Setting country lists...", flush=True)
g.set_country_lists(US_NAMES, CN_NAMES, EU_NAMES)
print_memory_usage("After country lists")

print("Ingesting countries from parquet...", flush=True)
g.ingest_countries_from_parquet(ct, 'UID', 'country')
del ct
gc.collect()
print_memory_usage("After country ingestion")

print("Creating region directory...", flush=True)
os.makedirs(REGION_DIR, exist_ok=True)

print("Saving region bitmaps...", flush=True)
g.save_region_bitmaps(REGION_DIR)
print_memory_usage("After saving")

print("Saved region bitmaps to:", REGION_DIR, flush=True)
print("Region bitmap creation completed successfully!", flush=True)
PY
