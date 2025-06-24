#!/bin/bash
# Run Enhanced CD-Index Benchmarking Suite with Correct Environment
#
# This script ensures the correct Python environment and paths are used
# when running the benchmarking suite.

# Set up environment
export PYTHON_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"
export LD_LIBRARY_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib:${LD_LIBRARY_PATH}"
export PYTHONPATH="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex:/project/jevans/tip/disruption/code_wos_2023:${PYTHONPATH}"

# Benchmark directory
BENCHMARK_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/benchmarking"

echo "Enhanced CD-Index Benchmarking Suite Runner"
echo "==========================================="
echo "Python: $PYTHON_PATH"
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
echo "PYTHONPATH: $PYTHONPATH"
echo "Benchmark Dir: $BENCHMARK_DIR"
echo ""

# Change to benchmark directory
cd "$BENCHMARK_DIR"

# Check if no arguments provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <script_name> [args...]"
    echo ""
    echo "Available scripts:"
    echo "  setup.py              - Setup the benchmarking environment"
    echo "  quick_test.py         - Quick validation test"
    echo "  run_benchmarks.py     - Main benchmark runner"
    echo "  examples.py           - Example usage demonstrations"
    echo ""
    echo "Examples:"
    echo "  $0 setup.py"
    echo "  $0 quick_test.py"
    echo "  $0 run_benchmarks.py --micro"
    echo "  $0 run_benchmarks.py --full --config configs/production.yaml"
    exit 1
fi

# Run the specified script with the correct Python environment
echo "Running: $PYTHON_PATH $@"
echo ""
exec "$PYTHON_PATH" "$@"
