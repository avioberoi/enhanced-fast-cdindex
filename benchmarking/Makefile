# Enhanced CD-Index Benchmarking Suite Makefile

# Configuration
PYTHON = /project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python
DATA_CACHE = /project/jevans/tip/disruption/code_wos_2023/benchmarking/data_cache

# Default target
.PHONY: help
help:
	@echo "Enhanced CD-Index Benchmarking Suite"
	@echo "===================================="
	@echo ""
	@echo "Setup targets:"
	@echo "  setup              - Run setup script"
	@echo "  test               - Quick validation test"
	@echo "  clean              - Clean output directories"
	@echo ""
	@echo "Benchmark targets:"
	@echo "  micro              - Run micro benchmark"
	@echo "  optimization       - Run optimization benchmark"
	@echo "  full               - Run full benchmark"
	@echo ""
	@echo "SLURM targets:"
	@echo "  submit-test        - Submit quick test to SLURM"
	@echo "  submit-micro       - Submit micro benchmark to SLURM"
	@echo "  submit-full        - Submit full benchmark to SLURM"

# Setup targets
.PHONY: setup
setup:
	@echo "Setting up benchmarking environment..."
	./run_with_env.sh setup.py --data-cache $(DATA_CACHE)

.PHONY: test
test:
	@echo "Running validation tests..."
	./run_with_env.sh quick_test.py

.PHONY: clean
clean:
	@echo "Cleaning output directories..."
	rm -rf benchmark_results/* benchmark_plots/* benchmark_logs/* slurm/*
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Benchmark targets
.PHONY: micro
micro:
	@echo "Running micro benchmark..."
	./run_with_env.sh run_benchmarks.py --type micro --size-medium \
		--data-cache $(DATA_CACHE) --report --plots

.PHONY: optimization
optimization:
	@echo "Running optimization benchmark..."
	./run_with_env.sh run_benchmarks.py --type optimization --size-medium \
		--data-cache $(DATA_CACHE) --report --plots

.PHONY: full
full:
	@echo "Running full benchmark..."
	./run_with_env.sh run_benchmarks.py --type full --size-large \
		--data-cache $(DATA_CACHE) --report --plots

# SLURM targets
.PHONY: submit-test
submit-test:
	@echo "Submitting quick test to SLURM..."
	sbatch run_micro_test_slurm.sh

.PHONY: submit-micro
submit-micro:
	@echo "Submitting micro benchmark to SLURM..."
	sbatch run_slurm_benchmark.sh micro medium

.PHONY: submit-full
submit-full:
	@echo "Submitting full benchmark to SLURM..."
	sbatch run_slurm_benchmark.sh full large

# Utility targets
.PHONY: status
status:
	@echo "SLURM job status:"
	@squeue -u $$USER || echo "No SLURM jobs found"

.PHONY: logs
logs:
	@echo "Recent SLURM logs:"
	@ls -t slurm/*.out 2>/dev/null | head -1 | xargs tail -f || echo "No log files found"
