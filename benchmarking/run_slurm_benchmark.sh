#!/bin/bash
#SBATCH --job-name=enhanced_cdindex_benchmark
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/benchmarking/benchmark_logs/benchmark_%j.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/benchmarking/benchmark_logs/benchmark_%j.err
#SBATCH --partition=jevans
#SBATCH --account=pi-jevans
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64GB
#SBATCH --time=02:00:00

set -e

echo "Enhanced CD-Index Benchmarking Suite"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_JOB_NODELIST"
echo "CPUs: $SLURM_CPUS_PER_TASK"
echo "Memory: ${SLURM_MEM_PER_NODE}MB"
echo "Start time: $(date)"
echo ""

# Navigate to the benchmarking directory
cd /project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/benchmarking

# Set up environment with absolute paths
export PYTHON_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"
export LD_LIBRARY_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib:${LD_LIBRARY_PATH}"
export PYTHONPATH="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex:/project/jevans/tip/disruption/code_wos_2023:${PYTHONPATH}"

# Performance optimization environment variables
export TIME_WINDOW_YEARS="${TIME_WINDOW_YEARS:-5}"
export INGEST_CHUNK_SIZE="${INGEST_CHUNK_SIZE:-1000000}"
export CHUNK_SIZE="${CHUNK_SIZE:-10000}"
export MAX_CACHE_ENTRIES="${MAX_CACHE_ENTRIES:-32}"
export PYTHONUNBUFFERED=1

# Set CPU affinity for optimal performance
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export MKL_NUM_THREADS=$SLURM_CPUS_PER_TASK

echo "Environment setup:"
echo "  Python: $PYTHON_PATH"
echo "  LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
echo "  PYTHONPATH: $PYTHONPATH"
echo "  TIME_WINDOW_YEARS: $TIME_WINDOW_YEARS"
echo "  CHUNK_SIZE: $CHUNK_SIZE"
echo ""

# Create necessary directories
mkdir -p slurm
mkdir -p benchmark_results
mkdir -p benchmark_results/results
mkdir -p benchmark_results/plots
mkdir -p benchmark_results/logs
mkdir -p benchmark_plots
mkdir -p benchmark_logs

# Check Python environment
echo "Checking Python environment..."
if [ ! -f "$PYTHON_PATH" ]; then
    echo "Error: Python environment not found at $PYTHON_PATH"
    exit 1
fi

# Verify the benchmarking suite
echo "Validating benchmarking suite..."
"$PYTHON_PATH" quick_test.py
if [ $? -ne 0 ]; then
    echo "Error: Benchmarking suite validation failed"
    exit 1
fi
echo "Benchmarking suite validation passed"
echo ""

# Setup the benchmarking environment
echo "Setting up benchmarking environment..."
"$PYTHON_PATH" setup.py --data-cache=/project/jevans/tip/disruption/code_wos_2023/benchmarking/data_cache
if [ $? -ne 0 ]; then
    echo "Warning: Setup completed with warnings"
fi
echo ""

# Determine benchmark type from command line arguments or default to micro
BENCHMARK_TYPE="${1:-micro}"
BENCHMARK_SIZE="${2:-medium}"
CONFIG_FILE="${3:-}"

echo "Running benchmark type: $BENCHMARK_TYPE"
echo "Benchmark size: $BENCHMARK_SIZE"

# Build the benchmark command
BENCHMARK_CMD="$PYTHON_PATH run_benchmarks.py --type $BENCHMARK_TYPE"

# Add size argument
if [ "$BENCHMARK_SIZE" != "medium" ]; then
    BENCHMARK_CMD="$BENCHMARK_CMD --size-$BENCHMARK_SIZE"
fi

# Add config file if specified
if [ -n "$CONFIG_FILE" ]; then
    BENCHMARK_CMD="$BENCHMARK_CMD --config $CONFIG_FILE"
fi

# Add data cache path
BENCHMARK_CMD="$BENCHMARK_CMD --data-cache /project/jevans/tip/disruption/code_wos_2023/benchmarking/data_cache"

# Add output directory
BENCHMARK_CMD="$BENCHMARK_CMD --output-dir benchmark_results"

echo "Executing benchmark command:"
echo "  $BENCHMARK_CMD"
echo ""

# Run the benchmark
echo "Starting benchmark at $(date)..."
$BENCHMARK_CMD

BENCHMARK_EXIT_CODE=$?

echo ""
echo "Benchmark completed at $(date)!"

if [ $BENCHMARK_EXIT_CODE -eq 0 ]; then
    echo "Benchmark completed successfully"
    
    # Display results summary
    echo ""
    echo "Results Summary:"
    echo "================"
    ls -la benchmark_results/ | tail -n +2
    
    # If plots were generated, list them
    if [ -d "benchmark_plots" ] && [ "$(ls -A benchmark_plots)" ]; then
        echo ""
        echo "Generated Plots:"
        echo "==============="
        ls -la benchmark_plots/ | tail -n +2
    fi
    
else
    echo "Benchmark failed with exit code $BENCHMARK_EXIT_CODE"
fi

# Display job statistics
echo ""
echo "Job Statistics:"
echo "==============="
echo "  Job ID: $SLURM_JOB_ID"
echo "  Node: $SLURM_JOB_NODELIST"
echo "  CPUs Used: $SLURM_CPUS_PER_TASK"
echo "  Start time: $(date)"
echo "  End time: $(date)"

exit $BENCHMARK_EXIT_CODE
