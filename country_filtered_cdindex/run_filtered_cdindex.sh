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
#SBATCH --array=0-4
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/compute_cd_%A_%a.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/compute_cd_%A_%a.err

### ---- EDIT THESE ----
PYTHON_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"
PROJECT_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex"
CACHE_DIR="${PROJECT_DIR}/country_filtered_cdindex/data_cache"
COUNTRIES_PARQUET="/project/jevans/tip/data/wos_2023/paper_countries_fixed_w_EU.parquet"
REGION_CACHE_DIR="${PROJECT_DIR}/country_filtered_cdindex/region_bitmaps"
OUT_PREFIX="/project/jevans/tip/disruption/code_wos_2023/WoS_data/scores_filtered"
YEARS=5

# per-cluster fanout controls:
: "${CLUSTERS:=2}"        # total number of clusters/partitions we’re using
: "${CLUSTER_INDEX:=0}"   # 0..CLUSTERS-1 for this cluster
: "${TOTAL_PARTS:=5}"    # array size per cluster (must match --array size)
: "${LOG_EVERY:=100000}"
: "${CHUNK_SIZE:=340000}"
### --------------------

# env
export LD_LIBRARY_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib:${LD_LIBRARY_PATH}"
export PYTHONPATH="${PROJECT_DIR}:${PROJECT_DIR}/fast_cdindex:${PYTHONPATH}"
export OMP_NUM_THREADS=48
export MKL_NUM_THREADS=48
export OMP_PROC_BIND=close
export OMP_PLACES=cores

set -euo pipefail

echo "== Compute filtered CD-index =="
echo "Job: $SLURM_JOB_ID  ArrayTask: $SLURM_ARRAY_TASK_ID"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo "Cluster fan-out: CLUSTERS=${CLUSTERS} CLUSTER_INDEX=${CLUSTER_INDEX} TOTAL_PARTS=${TOTAL_PARTS}"
echo

"${PYTHON_PATH}" "${PROJECT_DIR}/country_filtered_cdindex/compute_filtered_cd.py" \
  --cache-dir "${CACHE_DIR}" \
  --countries-parquet "${COUNTRIES_PARQUET}" \
  --region-cache-dir "${REGION_CACHE_DIR}" \
  --out-prefix "${OUT_PREFIX}" \
  --years ${YEARS} \
  --clusters ${CLUSTERS} \
  --cluster-index ${CLUSTER_INDEX} \
  --total-parts ${TOTAL_PARTS} \
  --part-id ${SLURM_ARRAY_TASK_ID} \
  --log-every ${LOG_EVERY} \
  --chunk-size ${CHUNK_SIZE}