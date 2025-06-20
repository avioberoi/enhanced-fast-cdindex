#!/bin/bash

# Convenience script to run enhanced cdindex tests with proper environment setup

# Set up library paths for the conda environment
export LD_LIBRARY_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib:/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib/python3.9/site-packages/pyarrow"

# Set up Python path
export PYTHONPATH="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex"

# Python executable
PYTHON="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"

# Change to the enhanced-fast-cdindex directory
cd /project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex

echo "Enhanced CD-Index Environment Setup"
echo "====================================="
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
echo "PYTHONPATH: $PYTHONPATH"
echo "Python: $PYTHON"
echo ""

# If arguments provided, run them as a command
if [ $# -gt 0 ]; then
    echo "Running: $@"
    echo ""
    exec "$@"
else
    echo "Available commands:"
    echo "  $0 $PYTHON -m pytest tests/test_enhanced.py -v    # Run tests"
    echo "  $0 $PYTHON -c \"from fast_cdindex.cdindex_enhanced import EnhancedGraph; print('Ready!')\"  # Test import"
    echo "  $0 $PYTHON your_script.py                         # Run your script"
fi
