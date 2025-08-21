#!/bin/bash
#SBATCH --job-name=create_country_cache
#SBATCH --partition=ssd
#SBATCH --account=ssd
#SBATCH --qos=ssd
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --mem=128GB
#SBATCH --time=02:00:00
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/create_country_cache_%j.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/create_country_cache_%j.err

# This job creates the country-augmented cache for filtered CD-index computations

echo "=== SLURM Job Info ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo "Memory requested: 128GB"
echo "CPUs: 48"
echo ""

export PYTHON_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"
export LD_LIBRARY_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib:${LD_LIBRARY_PATH}"
export PYTHONPATH="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex:/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/fast_cdindex:/project/jevans/tip/disruption/code_wos_2023:${PYTHONPATH}" 

# Optimize for data processing
export OMP_NUM_THREADS=48
export MKL_NUM_THREADS=48
export POLARS_MAX_THREADS=48

# Change to working directory
cd /project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/

echo "=== Starting Country Cache Creation ==="
echo "Working directory: $(pwd)"
echo "Python: "$PYTHON_PATH""
echo ""

# Verify the enhanced package is built
echo "Verifying enhanced package..."
"$PYTHON_PATH" -c "from fast_cdindex.cdindex_enhanced import EnhancedGraph; print('Enhanced package OK')"
if [ $? -ne 0 ]; then
    echo "Error: Enhanced package not available"
    exit 1
fi
echo ""

# Test with small dataset first (10K papers)
echo "Stage 1: Small test (1K papers)..."
"$PYTHON_PATH" \
  country_filtered_cdindex/augment_cache_with_countries.py \
  --existing-cache-dir "/project/jevans/tip/disruption/code_wos_2023/benchmarking/tsv_cache" \
  --country-file "/project/jevans/tip/data/wos_2023/paper_countries_fixed_w_EU.parquet" \
  --output-dir "country_filtered_cdindex/test_cache_10k" \
  --limit-papers 10000 \
  --log-level INFO

if [ $? -eq 0 ]; then
    echo "Small test completed successfully"
else
    echo "Small test failed"
    exit 1
fi

# Full dataset
echo ""
echo "Stage 3: Full dataset..."
"$PYTHON_PATH" \
  country_filtered_cdindex/augment_cache_with_countries.py \
  --existing-cache-dir "/project/jevans/tip/disruption/code_wos_2023/benchmarking/tsv_cache" \
  --country-file "/project/jevans/tip/data/wos_2023/paper_countries_fixed_w_EU.parquet" \
  --output-dir "country_filtered_cdindex/data_cache" \
  --log-level INFO

if [ $? -eq 0 ]; then
    echo "Full dataset completed successfully"
    echo ""
    echo "=== Final Results ==="
    echo "Small test cache: country_filtered_cdindex/test_cache_1k"
    echo "Medium test cache: country_filtered_cdindex/test_cache_50k"  
    echo "Full cache: country_filtered_cdindex/full_cache"
    echo ""
    echo "Ready for filtered CD-index computations!"
else
    echo "Full dataset failed"
    exit 1
fi

echo ""
echo "=== Job Completed ==="
echo "End time: $(date)"
