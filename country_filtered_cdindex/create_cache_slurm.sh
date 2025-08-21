#!/bin/bash
#SBATCH --job-name=data_cache
#SBATCH --partition=ssd
#SBATCH --account=ssd
#SBATCH --qos=ssd
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --mem=128GB
#SBATCH --time=04:00:00
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/create_country_cache_%j.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/create_country_cache_%j.err

echo "=== SLURM Job Info ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo "Memory requested: 128GB"
echo "CPUs: 48"
echo ""

export PYTHON_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"

cd /project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/

echo "=== Starting Data Cache Creation ==="
echo "Working directory: $(pwd)"
echo "Python: "$PYTHON_PATH""
echo ""

echo "Caching TSV Data..."
"$PYTHON_PATH" \
  country_filtered_cdindex/build_cache_from_tsv.py \
  --years-tsv "/project/jevans/tip/disruption/code_wos_2023/WoS_data/paper_years_all.tsv" \
  --edges-tsv "/project/jevans/tip/disruption/code_wos_2023/WoS_data/edges_all.tsv" \
  --out-dir "country_filtered_cdindex/data_cache"

if [ $? -eq 0 ]; then
    echo "Data cache made successfully"
else
    echo "Data cache failed"
    exit 1
fi

echo ""
echo "=== Job Completed ==="
echo "End time: $(date)"
