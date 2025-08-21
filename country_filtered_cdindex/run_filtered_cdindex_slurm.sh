#!/bin/bash
#SBATCH --job-name=filtered_cdindex
#SBATCH --partition=ssd
#SBATCH --account=ssd
#SBATCH --qos=ssd
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --mem=128GB
#SBATCH --time=24:00:00
#SBATCH --array=0-9
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/filtered_cdindex_%A_%a.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/filtered_cdindex_%A_%a.err

# Distributed Filtered CD-Index Computation
# Each array job processes a subset of papers for scalability

echo "=== SLURM Array Job Info ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo "Memory: 128GB"
echo "CPUs: 48"
echo ""

# Configuration
TOTAL_ARRAYS=10
TASK_ID=$SLURM_ARRAY_TASK_ID

# Cache directory (from cache creation job)
# PRODUCTION: Use full dataset for comprehensive country-filtered CD-index analysis
CACHE_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/full_cache"

# Results directory (use local directory to avoid permission issues)
RESULTS_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/filtered_results"
mkdir -p $RESULTS_DIR

# Set up environment with absolute paths (exact same as working create_country_cache_slurm.sh)
export PYTHON_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python"
export LD_LIBRARY_PATH="/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib:${LD_LIBRARY_PATH}"
export PYTHONPATH="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex:/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/fast_cdindex:/project/jevans/tip/disruption/code_wos_2023:${PYTHONPATH}"

# CPU optimization
export OMP_NUM_THREADS=48
export MKL_NUM_THREADS=48
export OMP_PROC_BIND=close
export OMP_PLACES=cores

# Change to working directory
cd /project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex

echo "=== Configuration ==="
echo "Cache directory: $CACHE_DIR"
echo "Results directory: $RESULTS_DIR"
echo "Array task: $TASK_ID / $TOTAL_ARRAYS"
echo ""

# Verify the enhanced package is built
echo "Verifying enhanced package..."
"$PYTHON_PATH" -c "from fast_cdindex.cdindex_enhanced import EnhancedGraph; print('Enhanced package OK')"
if [ $? -ne 0 ]; then
    echo "Error: Enhanced package not available"
    exit 1
fi
echo ""

# Verify cache exists
if [ ! -d "$CACHE_DIR" ]; then
    echo "ERROR: Cache directory not found: $CACHE_DIR"
    echo "Please run create_country_cache_slurm.sh first!"
    exit 1
fi

# Check for required cache files
required_files=("paper_years.parquet" "edges.parquet" "combined_properties.parquet")
for file in "${required_files[@]}"; do
    if [ ! -f "$CACHE_DIR/$file" ]; then
        echo "ERROR: Required cache file missing: $CACHE_DIR/$file"
        exit 1
    fi
done

echo "Cache verification passed"
echo ""

# Output file for this array task
OUTPUT_FILE="$RESULTS_DIR/filtered_cdindex_part_${TASK_ID}.csv.gz"

echo "=== Starting Filtered CD-Index Computation ==="
echo "Output file: $OUTPUT_FILE"
echo ""

# PRODUCTION: Full dataset computation
PAPERS_PER_TASK=50000   # Process 50K papers per task (10 tasks × 50K = 500K papers)
CHUNK_SIZE=10000        # Large chunks for optimal performance

"$PYTHON_PATH" \
  country_filtered_cdindex/compute_filtered_cdindex.py \
  --cache-dir "$CACHE_DIR" \
  --output-file "$OUTPUT_FILE" \
  --sample-papers $PAPERS_PER_TASK \
  --time-delta 5 \
  --chunk-size $CHUNK_SIZE

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo "Array task $TASK_ID completed successfully"
    echo "Results saved to: $OUTPUT_FILE"
    
    # Check output file
    if [ -f "$OUTPUT_FILE" ]; then
        file_size=$(du -h "$OUTPUT_FILE" | cut -f1)
        line_count=$(zcat "$OUTPUT_FILE" | wc -l)
        echo "Output file size: $file_size"
        echo "Lines in output: $line_count"
    fi
else
    echo ""
    echo "Array task $TASK_ID failed with exit code: $exit_code"
    exit $exit_code
fi

echo ""
echo "=== Array Task Completed ==="
echo "Task ID: $TASK_ID"
echo "End time: $(date)"
