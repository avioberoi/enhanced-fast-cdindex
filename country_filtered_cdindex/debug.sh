#!/bin/bash
#SBATCH --job-name=spot_cd
#SBATCH --partition=jevans
#SBATCH --account=pi-jevans
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=512GB
#SBATCH --time=02:00:00
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/spot_cd_%j.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/spot_cd_%j.err

set -euo pipefail

PY="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"
SCRIPT="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/spot_check_cd_sanity.py"
CACHE="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/data_cache"
IDMAP="${CACHE}/id_map.parquet"
LEGACY="/project/jevans/tip/disruption/code_wos_2023/WoS_data/scores_all/*.csv.gz"
MISM="/project/jevans/tip/disruption/code_wos_2023/filtered_scores_remapped/validation_mismatches_y5_diag.tsv"

export OMP_NUM_THREADS=32
export MKL_NUM_THREADS=32
export OMP_PROC_BIND=close
export OMP_PLACES=cores
PROJECT_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex"

export LD_LIBRARY_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib:${LD_LIBRARY_PATH:-}"
# export PYTHONPATH="${PROJECT_DIR}:${PROJECT_DIR}/fast_cdindex:${PYTHONPATH}"
export PYTHONPATH="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex:${PYTHONPATH:-}"

"$PY" "$SCRIPT" \
  --cache-dir "$CACHE" \
  --id-map "$IDMAP" \
  --legacy-glob "$LEGACY" \
  --years 5 \
  --sample 500 \
  --mismatches "$MISM" \
  --threads 32 \
  --flip-edges 