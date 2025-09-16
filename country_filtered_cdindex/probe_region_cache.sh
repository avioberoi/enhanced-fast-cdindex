#!/bin/bash
#SBATCH --job-name=probe_region_cache
#SBATCH --partition=ssd
#SBATCH --account=ssd
#SBATCH --qos=ssd
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --mem=180GB
#SBATCH --time=06:00:00
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/probe_region_cache_%j.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/probe_region_cache_%j.err

PYTHON_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"
PROJECT_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex"
CACHE_DIR="${PROJECT_DIR}/country_filtered_cdindex/data_cache"
COUNTRIES_PARQUET="/project/jevans/tip/data/wos_2023/paper_countries_fixed_w_EU.parquet"
REGION_CACHE_DIR="${PROJECT_DIR}/country_filtered_cdindex/region_bitmaps"
YEARS=5
FLIP="--flip-edges"

export LD_LIBRARY_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib:${LD_LIBRARY_PATH}"
export PYTHONPATH="${PROJECT_DIR}:${PROJECT_DIR}/fast_cdindex:${PYTHONPATH}"
export PYTHONUNBUFFERED=1
export OMP_NUM_THREADS=48
export MKL_NUM_THREADS=48
export OMP_PROC_BIND=close
export OMP_PLACES=cores

set -euo pipefail

echo "== Probe region cache =="
echo "Host: $(hostname)"
echo "Date: $(date)"
echo

"${PYTHON_PATH}" "${PROJECT_DIR}/country_filtered_cdindex/probe_region_cache.py" \
  --cache-dir "${CACHE_DIR}" \
  --countries-parquet "${COUNTRIES_PARQUET}" \
  --region-cache-dir "${REGION_CACHE_DIR}" \
  --years ${YEARS} ${FLIP} \
  --sample 1000