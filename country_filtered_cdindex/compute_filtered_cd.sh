#!/bin/bash
#SBATCH --job-name=compute_cd
#SBATCH --partition=ssd
#SBATCH --account=ssd
#SBATCH --qos=ssd
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --mem=180GB
#SBATCH --time=36:00:00
#SBATCH --array=0-9
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/0_compute_cd_%A_%a.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/0_compute_cd_%A_%a.err

PYTHON_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"
PROJECT_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex"
CACHE_DIR="${PROJECT_DIR}/country_filtered_cdindex/data_cache"
COUNTRIES_PARQUET="/project/jevans/tip/data/wos_2023/paper_countries_fixed_w_EU.parquet"
REGION_CACHE_DIR="${PROJECT_DIR}/country_filtered_cdindex/region_bitmaps"
# OUT_PREFIX="/project/jevans/tip/disruption/code_wos_2023/filtered_scores/scores_filtered"
OUT_PREFIX="/project/jevans/tip/disruption/code_wos_2023/filtered_scores_final/scores_filtered"
YEARS=5

# Explicit 10-way partitioning (freeze original row slicing)
: "${GLOBAL_PART_ID:=${SLURM_ARRAY_TASK_ID}}"  # 0..9 for the 10 global parts
# For distributed submissions across partitions:
# CLUSTER_OFFSET=0: parts 0-4 (has chunks 0-2 done, skip them)
# CLUSTER_OFFSET=5: parts 5-9 (fresh start from chunk 0)
if [[ -n "${CLUSTER_OFFSET:-}" ]]; then
    GLOBAL_PART_ID=$((GLOBAL_PART_ID + CLUSTER_OFFSET))
fi
: "${FIRST_CHUNK:=}"      # optional: start at specific chunk within part
: "${NUM_CHUNKS:=}"       # optional: process only this many chunks then exit
: "${LOG_EVERY:=500000}"
: "${CHUNK_SIZE:=340000}"
### --------------------

set -euo pipefail

# Increase file descriptor limit for Arrow Parquet handling
ulimit -n 65535 || true

# env
export LD_LIBRARY_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="${PROJECT_DIR}:${PROJECT_DIR}/fast_cdindex:${PYTHONPATH:-}"
export OMP_NUM_THREADS=48
export MKL_NUM_THREADS=48
export MKL_DYNAMIC=FALSE
export OMP_PROC_BIND=close
export OMP_PLACES=cores
export MAX_CACHE_ENTRIES=16384
# export CDINDEX_NO_FASTUNION=1
unset CDINDEX_SAFE_MODE
unset CDINDEX_ANDCARD_SAFE
unset CDINDEX_NO_FASTUNION

echo "== Compute filtered CD-index =="
echo "Job: $SLURM_JOB_ID  ArrayTask: $SLURM_ARRAY_TASK_ID"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo "Global part: GLOBAL_PART_ID=${GLOBAL_PART_ID} FIRST_CHUNK=${FIRST_CHUNK:-auto} NUM_CHUNKS=${NUM_CHUNKS:-all}"
echo "Flip edges: yes"
echo "Output prefix: ${OUT_PREFIX}"
echo

"${PYTHON_PATH}" "${PROJECT_DIR}/country_filtered_cdindex/compute_filtered_cdindex.py" \
  --cache-dir "${CACHE_DIR}" \
  --countries-parquet "${COUNTRIES_PARQUET}" \
  --region-cache-dir "${REGION_CACHE_DIR}" \
  --out-prefix "${OUT_PREFIX}" \
  --years ${YEARS} \
  --clusters 10 \
  --cluster-index ${GLOBAL_PART_ID} \
  --total-parts 1 \
  --part-id 0 \
  --log-every ${LOG_EVERY} \
  --chunk-size ${CHUNK_SIZE} \
  --flip-edges \
  ${FIRST_CHUNK:+--first-chunk} ${FIRST_CHUNK} \
  ${NUM_CHUNKS:+--num-chunks} ${NUM_CHUNKS}