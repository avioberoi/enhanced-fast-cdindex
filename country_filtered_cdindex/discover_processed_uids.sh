#!/bin/bash
#SBATCH --job-name=discover_uids
#SBATCH --partition=ssd
#SBATCH --account=ssd
#SBATCH --qos=ssd
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16GB
#SBATCH --time=1:00:00
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/discover_%j.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/discover_%j.err

# Configuration - MODIFY THESE VARIABLES
EXCLUDE_REGION="eu"  # us, eu, or cn
SEARCH_DIRS="/project/jevans/tip/disruption/code_wos_2023/filtered_scores_invdl_cntries/eu"
RUN_NAME="resume_$(date +%Y%m%d_%H%M%S)"

# Paths
PYTHON_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"
PROJECT_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex"
OUTPUT_DIR="/project/jevans/tip/disruption/code_wos_2023/filtered_scores_invdl_cntries/${EXCLUDE_REGION}/${RUN_NAME}"

# Environment
export PYTHONPATH="${PROJECT_DIR}:${PROJECT_DIR}/fast_cdindex:${PYTHONPATH:-}"
export PYTHONUNBUFFERED=1
set -euo pipefail

echo "== Discovery Phase =="
echo "Job: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo "Exclude Region: $EXCLUDE_REGION"
echo "Search Directories: $SEARCH_DIRS"
echo "Output Directory: $OUTPUT_DIR"
echo

# Create output directory
mkdir -p "$OUTPUT_DIR"
mkdir -p "${PROJECT_DIR}/country_filtered_cdindex/slurm"

# Run discovery
"$PYTHON_PATH" "${PROJECT_DIR}/country_filtered_cdindex/discover_processed_uids.py" \
    --search-dirs $SEARCH_DIRS \
    --out-dir "$OUTPUT_DIR" \
    --exclude-region "$EXCLUDE_REGION"

echo
echo "== Discovery Complete =="
echo "Created: $OUTPUT_DIR/global_processed_uids.pkl"
echo "Next step: Edit RUN_DIR in compute_cd_excl.sh to point to $OUTPUT_DIR"
