#!/bin/bash
#SBATCH --job-name=cdindex_validation
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/benchmarking/validation_logs/validation_%A_%a.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/benchmarking/validation_logs/validation_%A_%a.err
#SBATCH --partition=ssd
#SBATCH --account=ssd
#SBATCH --qos=ssd
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --mem=180GB
#SBATCH --time=12:00:00
# Single job for testing (no array)

set -e

echo "Enhanced CD-Index Validation Job"
echo "Job ID: $SLURM_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
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
export INGEST_CHUNK_SIZE="${INGEST_CHUNK_SIZE:-1000000}"
export CHUNK_SIZE="${CHUNK_SIZE:-50000}"
export MAX_CACHE_ENTRIES="${MAX_CACHE_ENTRIES:-4096}"
export BATCH_PARALLEL_THRESHOLD="${BATCH_PARALLEL_THRESHOLD:-10000}"
export INNER_PARALLEL_THRESHOLD="${INNER_PARALLEL_THRESHOLD:-1000}"
export PYTHONUNBUFFERED=1

# PERFORMANCE FIX: Set CPU affinity for optimal performance
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export MKL_NUM_THREADS=1  # Prevent MKL thread oversubscription
export OMP_PROC_BIND=close
export OMP_PLACES=cores

echo "Environment setup:"
echo "  Python: $PYTHON_PATH"
echo "  LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
echo "  PYTHONPATH: $PYTHONPATH"
echo "  CHUNK_SIZE: $CHUNK_SIZE"
echo "  MAX_CACHE_ENTRIES: $MAX_CACHE_ENTRIES"
echo ""

# Create necessary directories
mkdir -p validation_logs
mkdir -p validation_results

# Check Python environment
echo "Checking Python environment..."
if [ ! -f "$PYTHON_PATH" ]; then
    echo "Error: Python environment not found at $PYTHON_PATH"
    exit 1
fi

# Verify the enhanced package is built
echo "Verifying enhanced package..."
"$PYTHON_PATH" -c "from fast_cdindex.cdindex_enhanced import EnhancedGraph; print('Enhanced package OK')"
if [ $? -ne 0 ]; then
    echo "Error: Enhanced package not available"
    exit 1
fi

# Validation parameters
DATA_CACHE_DIR="/project/jevans/tip/disruption/code_wos_2023/benchmarking/data_cache"
SCORES_DIR="/project/jevans/tip/disruption/code_wos_2023/WoS_data/scores_all"
TOTAL_PARTS=200  # Process 1 out of 200 total parts for testing
PART_ID=0        # Process first part (part 0 out of 200)
# Note: This will process approximately 1 file out of 200 total files

# Debug mode flags (override by env if needed)
USE_TSV_BUILD=${USE_TSV_BUILD:-1}           # 1 = build graph from TSV like compute_cd_one.py
LIMIT_PAPERS=${LIMIT_PAPERS:-}             # full validation by default, can override via env
QUICK_BENCHMARK=${QUICK_BENCHMARK:-0}       # 1 = enable quick micro-benchmark mode (1000 papers)
MAX_FILES=${MAX_FILES:-10}                  # Limit to 10 files for testing 
WOS_ROOT=${WOS_ROOT:-/project/jevans/tip/disruption/code_wos_2023/WoS_data}
TSV_VERTEX_DIR=${TSV_VERTEX_DIR:-$WOS_ROOT/paper_years_all.tsv}
TSV_EDGE_DIR=${TSV_EDGE_DIR:-$WOS_ROOT/edges_all.tsv}
LEGACY_MODE=${LEGACY_MODE:-0}               # 0 = use enhanced C++ implementation, 1 = use legacy diagnostic mode
WRITE_CACHE_DIR=${WRITE_CACHE_DIR:-}        # Don't write cache by default
REUSE_CACHE_DIR=${REUSE_CACHE_DIR:-/project/jevans/tip/disruption/code_wos_2023/benchmarking/tsv_cache}

echo "Validation parameters:"
echo "  Data cache: $DATA_CACHE_DIR"
echo "  Scores directory: $SCORES_DIR"
echo "  Total parts: $TOTAL_PARTS"
echo "  Part ID: $PART_ID"
echo "  Max files to process: $MAX_FILES"
echo ""

# Verify directories exist
if [ ! -d "$SCORES_DIR" ]; then
    echo "Error: Scores directory not found: $SCORES_DIR"
    exit 1
fi

# Only verify parquet caches if not using TSV build
if [ "$USE_TSV_BUILD" != "1" ]; then
  if [ ! -d "$DATA_CACHE_DIR" ]; then
      echo "Error: Data cache directory not found: $DATA_CACHE_DIR"
      exit 1
  fi

  # Check for required cache files (can be files or directories)
  VERTICES_CACHE="$DATA_CACHE_DIR/paper_years.parquet"
  EDGES_CACHE="$DATA_CACHE_DIR/edges.parquet"

  if [ ! -e "$VERTICES_CACHE" ]; then
      echo "Error: Vertices cache not found: $VERTICES_CACHE"
      exit 1
  fi

  if [ ! -e "$EDGES_CACHE" ]; then
      echo "Error: Edges cache not found: $EDGES_CACHE"
      exit 1
  fi

  echo "Cache files found:"
  if [ -d "$VERTICES_CACHE" ]; then
      echo "  Vertices: $VERTICES_CACHE (partitioned dataset)"
      echo "    Parts: $(ls $VERTICES_CACHE/part-*.parquet 2>/dev/null | wc -l) parquet files"
  else
      echo "  Vertices: $VERTICES_CACHE (single file)"
  fi

  if [ -d "$EDGES_CACHE" ]; then
      echo "  Edges: $EDGES_CACHE (partitioned dataset)"
      echo "    Parts: $(ls $EDGES_CACHE/part-*.parquet 2>/dev/null | wc -l) parquet files"
  else
      echo "  Edges: $EDGES_CACHE (single file)"
  fi

  echo "Cache files verified:"
  echo "  Vertices: $VERTICES_CACHE"
  echo "  Edges: $EDGES_CACHE"
  echo ""
fi

# Count score files
SCORE_FILE_COUNT=$(ls -1 "$SCORES_DIR"/*.csv.gz 2>/dev/null | wc -l)
echo "Found $SCORE_FILE_COUNT score files to validate"

if [ "$SCORE_FILE_COUNT" -eq 0 ]; then
    echo "Error: No score files found in $SCORES_DIR"
    exit 1
fi

# Build the validation command
VALIDATION_CMD="$PYTHON_PATH validate_enhanced_cdindex.py"
VALIDATION_CMD="$VALIDATION_CMD --data-cache $DATA_CACHE_DIR"
VALIDATION_CMD="$VALIDATION_CMD --scores-dir $SCORES_DIR"
VALIDATION_CMD="$VALIDATION_CMD --part-id $PART_ID"
VALIDATION_CMD="$VALIDATION_CMD --total-parts $TOTAL_PARTS"
VALIDATION_CMD="$VALIDATION_CMD --log-level INFO"

# Switch to TSV-based graph for higher fidelity with original pipeline
if [ "$USE_TSV_BUILD" = "1" ]; then
  echo "Using TSV-based graph build (compute_cd_one.py compatible)"
  VALIDATION_CMD="$VALIDATION_CMD --use-tsv --wos-root $WOS_ROOT --tsv-vertex-dir $TSV_VERTEX_DIR --tsv-edge-dir $TSV_EDGE_DIR"
fi

# Quick benchmark mode
if [ "$QUICK_BENCHMARK" = "1" ]; then
  echo "Enabling quick micro-benchmark mode"
  VALIDATION_CMD="$VALIDATION_CMD --quick-benchmark"
fi

# Limit to a small subset if specified
if [ -n "$LIMIT_PAPERS" ] && [ "$LIMIT_PAPERS" -gt 0 ]; then
  echo "Limiting validation to first $LIMIT_PAPERS papers"
  VALIDATION_CMD="$VALIDATION_CMD --limit-papers $LIMIT_PAPERS"
else
  if [ "$QUICK_BENCHMARK" != "1" ]; then
    echo "Running full validation (no paper limit)"
  fi
fi

# Enable legacy diagnostic mode if requested
if [ "$LEGACY_MODE" = "1" ]; then
  echo "Enabling legacy diagnostic mode (citers-of-f denominator)"
  VALIDATION_CMD="$VALIDATION_CMD --legacy-mode"
fi

# Parquet cache controls for TSV builds
if [ -n "$REUSE_CACHE_DIR" ]; then
  echo "Reusing TSV parquet cache at $REUSE_CACHE_DIR"
  VALIDATION_CMD="$VALIDATION_CMD --reuse-cache-dir $REUSE_CACHE_DIR"
fi
if [ -n "$WRITE_CACHE_DIR" ]; then
  echo "Will write TSV parquet cache to $WRITE_CACHE_DIR"
  VALIDATION_CMD="$VALIDATION_CMD --write-cache-dir $WRITE_CACHE_DIR"
fi

# Only validate CD-index (skip mCD-index and I-index for faster processing)
echo "Validating CD-index only (skipping mCD-index and I-index)"
VALIDATION_CMD="$VALIDATION_CMD --validate-cdindex-only"

# Limit number of files to process
if [ -n "$MAX_FILES" ] && [ "$MAX_FILES" -gt 0 ]; then
  echo "Testing production-grade improvements: processing $MAX_FILES files, part $((PART_ID+1))/$TOTAL_PARTS"
  VALIDATION_CMD="$VALIDATION_CMD --max-files $MAX_FILES"
fi

echo "Executing validation command:"
echo "  $VALIDATION_CMD"
echo ""

# Run the validation
echo "Starting validation at $(date)..."
echo "Processing part $((PART_ID + 1)) of $TOTAL_PARTS..."

$VALIDATION_CMD

VALIDATION_EXIT_CODE=$?

echo ""
echo "Validation completed at $(date)!"

if [ $VALIDATION_EXIT_CODE -eq 0 ]; then
    echo "Validation part $PART_ID completed successfully"
    
    # Display results summary if available
    SUMMARY_FILE="validation_results/validation_summary_part_$(printf %03d $PART_ID).txt"
    if [ -f "$SUMMARY_FILE" ]; then
        echo ""
        echo "Validation Summary:"
        echo "=================="
        cat "$SUMMARY_FILE"
        echo ""
    fi
    
    # List generated files
    echo "Generated files:"
    ls -la validation_results/*part_$(printf %03d $PART_ID)* 2>/dev/null || echo "No part-specific files found"
    
else
    echo "Validation part $PART_ID failed with exit code $VALIDATION_EXIT_CODE"
fi

# Display job statistics
echo ""
echo "Job Statistics:"
echo "==============="
echo "  Job ID: $SLURM_JOB_ID"
echo "  Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "  Node: $SLURM_JOB_NODELIST"
echo "  CPUs Used: $SLURM_CPUS_PER_TASK"
echo "  Memory Requested: ${SLURM_MEM_PER_NODE}MB"
echo "  Start time: $(date)"

# Memory usage info
if command -v free >/dev/null 2>&1; then
    echo ""
    echo "Memory Usage:"
    free -h
fi

echo ""
echo "Validation job part $PART_ID finished."

exit $VALIDATION_EXIT_CODE
