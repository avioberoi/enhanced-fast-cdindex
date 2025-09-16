#!/bin/bash
#SBATCH --job-name=cd_excl
#SBATCH --partition=ssd
#SBATCH --account=ssd
#SBATCH --qos=ssd
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --mem=180GB
#SBATCH --time=36:00:00
#SBATCH --array=0-4
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/0cd_excl_%A_%a.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/0cd_excl_%A_%a.err

EXCLUDE_REGION="us"
OUT_PREFIX="/project/jevans/tip/disruption/code_wos_2023/filtered_scores_invdl_cntries/${EXCLUDE_REGION}/scores_excl"

PYTHON_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"
PROJECT_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex"
CACHE_DIR="${PROJECT_DIR}/country_filtered_cdindex/data_cache"
COUNTRIES_PARQUET="/project/jevans/tip/data/wos_2023/paper_countries_fixed_w_EU.parquet"
REGION_CACHE_DIR="${PROJECT_DIR}/country_filtered_cdindex/region_bitmaps"
YEARS=5

: "${TOTAL_PARTS:=5}"
: "${LOG_EVERY:=500000}"
: "${CHUNK_SIZE:=340000}"

# env
export LD_LIBRARY_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="${PROJECT_DIR}:${PROJECT_DIR}/fast_cdindex:${PYTHONPATH:-}"
export PYTHONUNBUFFERED=1
export OMP_NUM_THREADS=24
export MKL_NUM_THREADS=24
export OMP_PROC_BIND=close
export OMP_PLACES=cores
export MAX_CACHE_ENTRIES=32768
# Independent job architecture - no shared database
unset CDINDEX_SAFE_MODE
unset CDINDEX_ANDCARD_SAFE
unset CDINDEX_NO_FASTUNION
set -euo pipefail

echo "== Compute cd_excl (${EXCLUDE_REGION}) =="
echo "Job: $SLURM_JOB_ID  ArrayTask: $SLURM_ARRAY_TASK_ID"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo "Array sharding: TOTAL_PARTS=${TOTAL_PARTS}"
echo "Output prefix: ${OUT_PREFIX}"
echo

ARGS=(
  "${PROJECT_DIR}/country_filtered_cdindex/compute_cd_excl.py"
  --cache-dir "${CACHE_DIR}"
  --countries-parquet "${COUNTRIES_PARQUET}"
  --region-cache-dir "${REGION_CACHE_DIR}"
  --out-prefix "${OUT_PREFIX}"
  --exclude-region "${EXCLUDE_REGION}"
  --years ${YEARS}
  --total-parts ${TOTAL_PARTS}
  --part-id ${SLURM_ARRAY_TASK_ID}
  --log-every ${LOG_EVERY}
  --chunk-size ${CHUNK_SIZE}
  --flip-edges
)

# Run with timeout to ensure graceful shutdown before SLURM kills the job
timeout 35h "${PYTHON_PATH}" "${ARGS[@]}"
exit_code=$?

if [ $exit_code -eq 124 ]; then
    echo "[timeout] Job reached 35h timeout, SIGTERM sent for graceful shutdown"
    exit 0  # Treat timeout as successful (partial work saved)
elif [ $exit_code -ne 0 ]; then
    echo "[error] Job failed with exit code $exit_code"
    exit $exit_code
else
    echo "[success] Job completed successfully"
    exit 0
fi
