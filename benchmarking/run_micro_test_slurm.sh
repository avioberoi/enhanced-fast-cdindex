#!/bin/bash
#SBATCH --job-name=cdindex_micro_test
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/benchmarking/benchmark_logs/micro_test_%j.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/benchmarking/benchmark_logs/micro_test_%j.err
#SBATCH --partition=jevans
#SBATCH --account=pi-jevans
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64GB
#SBATCH --time=30:00

set -e

echo "Enhanced CD-Index Micro-Benchmark Test"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_JOB_NODELIST"
echo "Start time: $(date)"
echo ""

# Navigate to benchmarking directory
cd /project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/benchmarking

# Environment setup
export PYTHON_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"
export LD_LIBRARY_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib:${LD_LIBRARY_PATH}"
export PYTHONPATH="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex:/project/jevans/tip/disruption/code_wos_2023:${PYTHONPATH}"
export PYTHONUNBUFFERED=1

echo "Using Python: $PYTHON_PATH"

# Create directories
mkdir -p slurm benchmark_results benchmark_plots benchmark_logs

# Quick validation
echo "Running validation test..."
"$PYTHON_PATH" quick_test.py

if [ $? -eq 0 ]; then
    echo "Validation passed, running micro benchmark..."
    
    # Run micro benchmark
    "$PYTHON_PATH" run_benchmarks.py \
        --type micro \
        --size-small \
        --data-cache "/project/jevans/tip/disruption/code_wos_2023/benchmarking/data_cache" \
        --output-dir benchmark_results \
        --verbose \
        --report \
        --plots
    
    echo "Micro benchmark completed!"
    echo "Results:"
    ls -la benchmark_results/
    
else
    echo "Validation failed, skipping benchmark"
    exit 1
fi

echo "Test completed at $(date)"
