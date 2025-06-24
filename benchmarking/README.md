# Enhanced CD-Index Benchmarking Suite

A comprehensive, production-ready benchmarking framework for testing and evaluating the performance of the Enhanced CD-Index implementation.

## 🚀 Quick Start

### For SLURM Cluster

```bash
# Quick 30-minute test
sbatch run_micro_test_slurm.sh

# Full benchmarks
sbatch run_slurm_benchmark.sh micro small
sbatch run_slurm_benchmark.sh optimization medium
sbatch run_slurm_benchmark.sh full large
```

### Interactive Usage

```bash
# Validate setup
./run_with_env.sh quick_test.py

# Run micro benchmark
./run_with_env.sh run_benchmarks.py --type micro --size-small \
  --data-cache /project/jevans/tip/disruption/code_wos_2023/benchmarking/data_cache
```

## 📁 File Structure

```
benchmarking/
├── Core Components
│   ├── config.py              # Configuration management
│   ├── data_loader.py         # Data loading from cache
│   ├── benchmark_runner.py    # Benchmark execution
│   ├── results_analyzer.py    # Analysis and reporting
│   └── utils.py              # Utility functions
├── CLI & Automation
│   ├── run_benchmarks.py      # Main CLI interface
│   ├── quick_test.py          # Validation suite
│   ├── setup.py              # Environment setup
│   └── examples.py           # Usage examples
├── SLURM Integration
│   ├── run_slurm_benchmark.sh      # Main SLURM script
│   ├── run_micro_test_slurm.sh     # Quick test script
│   └── run_with_env.sh             # Environment runner
├── Configuration
│   └── configs/
│       ├── micro_config.json       # Micro benchmark config
│       └── full_config.json        # Full benchmark config
└── Documentation
    ├── README.md                    # This file
    ├── SLURM_READY.md              # Production deployment guide
    └── Makefile                    # Build automation
```

## 🔧 Configuration

### Environment
- **Python**: `/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python`
- **Data Cache**: `/project/jevans/tip/disruption/code_wos_2023/benchmarking/data_cache`

### Benchmark Types
- **Micro**: Fast tests on data subsets (recommended for development)
- **Optimization**: Performance optimization testing
- **Full**: Complete dataset evaluation (production benchmarks)

### Dataset Sizes
- **Small**: 100K vertices, 1K benchmark papers (quick testing)
- **Medium**: 1M vertices, 10K benchmark papers (default)
- **Large**: 5M vertices, 50K benchmark papers (comprehensive)

## 📊 Output

Benchmarks generate:
- **Results**: `benchmark_results/*.json` - Performance metrics
- **Plots**: `benchmark_plots/*.png` - Visualization charts
- **Reports**: `benchmark_results/*.html` - Analysis reports
- **Logs**: `slurm/*.out/.err` - Execution logs

## 🔍 Validation

The suite includes comprehensive validation:
```bash
./run_with_env.sh quick_test.py
```

This checks:
- ✅ File structure integrity
- ✅ Module imports and dependencies
- ✅ Configuration management
- ✅ Data loader functionality
- ✅ Benchmark runner capabilities
- ✅ Results analyzer features

## 📈 Usage Examples

### Command Line Interface

```bash
# List available configurations
./run_with_env.sh run_benchmarks.py --list-configs

# Run with custom configuration
./run_with_env.sh run_benchmarks.py --config configs/micro_config.json

# Generate reports and plots
./run_with_env.sh run_benchmarks.py --type micro --report --plots

# Compare with previous results
./run_with_env.sh run_benchmarks.py --type micro --compare old_results.json
```

### Programmatic Usage

```python
from config import get_micro_config
from benchmark_runner import BenchmarkRunner
from results_analyzer import ResultsAnalyzer

# Initialize
config = get_micro_config()
runner = BenchmarkRunner(config)

# Run benchmarks
results = runner.run_micro_benchmark()

# Analyze results
analyzer = ResultsAnalyzer(config)
analysis = analyzer.analyze_micro_results(results)
analyzer.generate_html_report(results, analysis)
```

## 🎯 SLURM Job Management

### Submit Jobs
```bash
# Quick test (30 min, 8 cores, 32GB)
sbatch run_micro_test_slurm.sh

# Production benchmark (2 hours, 16 cores, 64GB)
sbatch run_slurm_benchmark.sh micro medium
```

### Monitor Jobs
```bash
squeue -u $USER
tail -f slurm/benchmark_*.out
```

### Job Parameters
- **Partition**: jevans
- **Account**: pi-jevans
- **Time Limits**: 30min (test), 2h (full)
- **Resources**: Configurable CPU/memory based on benchmark type

## 🛠️ Dependencies

**Required**:
- pyarrow >= 5.0.0
- pandas >= 1.3.0

**Optional** (for full functionality):
- matplotlib >= 3.3.0 (plotting)
- psutil >= 5.7.0 (system monitoring)
- numpy >= 1.20.0 (numerical operations)

## 🔧 Build System

Use the Makefile for common tasks:
```bash
make setup          # Run setup script
make test           # Quick validation
make micro          # Run micro benchmark
make clean          # Clean output directories
make help           # Show all targets
```

## 🚀 Production Deployment

The benchmarking suite is production-ready with:
- ✅ Comprehensive validation (6/6 tests passing)
- ✅ SLURM cluster integration
- ✅ Proper environment management
- ✅ Cached data pipeline
- ✅ Robust error handling
- ✅ Automated reporting

See `SLURM_READY.md` for detailed deployment instructions.

## 📝 Notes

- All scripts automatically use the correct Python environment
- Data cache is shared across all benchmarks for efficiency
- Configuration validation ensures reliable operation
- Modular design allows easy extension for new benchmark types
- Comprehensive logging aids in debugging and monitoring
```bash
pip install matplotlib psutil
```

## Quick Start

### 1. Basic Micro Benchmark
```bash
# Run a quick micro benchmark with default settings
python run_benchmarks.py --type micro

# Run with verbose output
python run_benchmarks.py --type micro --verbose
```

### 2. Generate Reports and Plots
```bash
# Run micro benchmark with analysis report and plots
python run_benchmarks.py --type micro --report --plots
```

### 3. Compare Performance
```bash
# Run benchmark and compare with previous results
python run_benchmarks.py --type micro --compare previous_results.json
```

### 4. Custom Configuration
```bash
# Use a custom configuration file
python run_benchmarks.py --config my_config.json --type micro
```

### 5. Quick Environment Test
```bash
# Test that everything is set up correctly
python quick_test.py
```

## Configuration

### Preset Configurations

The suite includes several preset configurations:

- **`micro`**: Fast micro-benchmarks for development and testing
- **`full`**: Full-scale benchmarks for comprehensive evaluation  
- **`optimization`**: Optimization-focused testing

### Custom Configuration

Create a JSON configuration file:

```json
{
  "data": {
    "micro_vertices": 100000,
    "micro_benchmark_papers": 1000,
    "cache_dir": "./data_cache"
  },
  "performance": {
    "time_windows": [3, 5, 10],
    "batch_sizes": [100, 1000, 5000],
    "max_workers": 8
  },
  "output": {
    "results_dir": "./benchmark_results",
    "log_level": "INFO",
    "generate_html_report": true
  }
}
```

### Environment Variables

Key environment variables for optimization testing:

- `CHUNK_SIZE`: Chunk size for processing (default: 100000)
- `BATCH_PARALLEL_THRESHOLD`: Threshold for parallel batch processing
- `MAX_CACHE_ENTRIES`: Maximum filter cache entries
- `CDINDEX_TIMING_DEBUG`: Enable timing debug output

## Usage Examples

### Example 1: Development Testing
```bash
# Quick micro benchmark for development
python run_benchmarks.py --preset micro --verbose
```

### Example 2: Optimization Testing
```bash
# Test optimization features
export CHUNK_SIZE=200000
export CDINDEX_TIMING_DEBUG=1
python run_benchmarks.py --preset optimization --report
```

### Example 3: Full Performance Evaluation
```bash
# Comprehensive benchmarking with full analysis
python run_benchmarks.py --preset full --report --plots --output-dir ./results_$(date +%Y%m%d)
```

### Example 4: Performance Regression Testing
```bash
# Compare against baseline
python run_benchmarks.py --type micro --compare baseline_results.json --report
```

## Output Files

The benchmarking suite generates several types of output:

### Results Directory
- `benchmark_results_*.json`: Raw benchmark data
- `benchmark_analysis_*.json`: Statistical analysis
- `benchmark_comparison_*.json`: Performance comparisons
- `benchmark_report_*.html`: HTML reports

### Plots Directory  
- `throughput_vs_batch_size.png`: Batch size performance
- `filter_overhead.png`: Filter performance impact
- `memory_usage.png`: Memory usage analysis

### Logs Directory
- `benchmark_*.log`: Detailed execution logs

## Understanding Results

### Key Metrics

- **Throughput**: Papers processed per second
- **Memory Usage**: Peak memory consumption in MB
- **CPU Utilization**: Average CPU usage percentage
- **Filter Overhead**: Performance impact of filtering (%)

### Interpreting Reports

The HTML reports include:

1. **Performance Summary**: Overall throughput and resource usage
2. **Batch Size Analysis**: Optimal batch sizes for different scenarios
3. **Filter Impact**: Overhead introduced by different filters
4. **Recommendations**: Automated performance suggestions

### Example Analysis

```json
{
  "summary": {
    "overall_performance": {
      "max_throughput": 15000.0,
      "avg_throughput": 8500.0
    }
  },
  "recommendations": [
    "For 5-year windows, optimal batch size is 10,000 papers",
    "Consider optimizing filters with high overhead (>50%)"
  ]
}
```

## Extending the Suite

### Adding New Benchmark Types

1. Create a new method in `BenchmarkRunner`:
```python
def run_custom_benchmark(self) -> Dict[str, Any]:
    # Your benchmark implementation
    pass
```

2. Add the benchmark type to `run_benchmarks.py`:
```python
elif args.type == 'custom':
    results = runner.run_custom_benchmark()
```

### Custom Analysis

Extend `ResultsAnalyzer` for custom analysis:

```python
def analyze_custom_results(self, results: Dict[str, Any]) -> Dict[str, Any]:
    # Your analysis implementation
    pass
```

## Troubleshooting

### Common Issues

1. **Missing Data Files**
   ```
   Error: WoS data directory not found
   ```
   - Ensure `WoS_data` directory exists with required files
   - Update `wos_data_dir` in configuration

2. **Import Errors**
   ```
   ImportError: No module named 'fast_cdindex'
   ```
   - Ensure Enhanced CD-Index is built and installed
   - Check Python path includes the enhanced-fast-cdindex directory

3. **Memory Issues**
   ```
   MemoryError during benchmark execution
   ```
   - Reduce `micro_vertices` or `batch_sizes` in configuration
   - Increase available system memory

4. **Permission Errors**
   ```
   PermissionError: Cannot create directory
   ```
   - Ensure write permissions for output directories
   - Update output paths in configuration

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
python run_benchmarks.py --type micro --verbose
```

## Performance Tips

### For Development
- Use `micro` preset for fast iterations
- Set smaller `micro_vertices` (e.g., 10,000) for quick tests
- Use `--validate-only` to check setup without running benchmarks

### For Production Evaluation
- Use `full` preset for comprehensive testing
- Enable memory monitoring for resource analysis
- Generate reports and plots for documentation

### For Optimization Testing
- Set relevant environment variables before running
- Use `optimization` preset to test specific features
- Compare results across different configurations

## API Reference

### BenchmarkConfig
```python
config = BenchmarkConfig(
    data=DataConfig(micro_vertices=100000),
    performance=PerformanceConfig(time_windows=[5, 10]),
    output=OutputConfig(log_level="DEBUG")
)
```

### BenchmarkRunner
```python
runner = BenchmarkRunner(config)
results = runner.run_micro_benchmark()
```

### ResultsAnalyzer
```python
analyzer = ResultsAnalyzer(config)
analysis = analyzer.analyze_micro_results(results)
plots = analyzer.generate_plots(results)
```

## Contributing

To contribute to the benchmarking suite:

1. Follow the modular architecture
2. Add comprehensive logging
3. Include error handling
4. Update documentation
5. Add tests for new functionality

## License

This benchmarking suite is part of the Enhanced CD-Index project.
