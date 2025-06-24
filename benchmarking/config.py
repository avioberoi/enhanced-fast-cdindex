"""
Benchmark Configuration Management

Centralized configuration for all benchmarking parameters including:
- Data sizes and sample configurations
- Performance test parameters
- Output and logging settings
- Environment variables
"""

import os
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import json


@dataclass
class DataConfig:
    """Configuration for data preparation and loading."""
    # Sample sizes for different benchmark types
    micro_vertices: int = 1_000_000
    micro_edges_factor: float = 3.0  # Expected edges per vertex
    micro_benchmark_papers: int = 10_000
    
    # Full benchmark configuration
    full_vertices: Optional[int] = None  # Use all available
    full_benchmark_papers: int = 100_000
    
    # Data paths
    wos_data_dir: str = field(default_factory=lambda: os.path.join(os.getcwd(), 'WoS_data'))
    cache_dir: str = field(default_factory=lambda: os.path.join(os.getcwd(), 'data_cache'))
    
    # File names
    vertices_file: str = "paper_years_all.tsv"
    edges_file: str = "edges_all.tsv"


@dataclass
class PerformanceConfig:
    """Configuration for performance testing parameters."""
    # Time windows to test (years)
    time_windows: List[int] = field(default_factory=lambda: [3, 5, 10])
    
    # Batch sizes for batch operation testing
    batch_sizes: List[int] = field(default_factory=lambda: [100, 1000, 5000, 10000, 50000])
    
    # Single operation sample sizes
    single_sample_sizes: List[int] = field(default_factory=lambda: [100, 500, 1000])
    
    # Filter configurations for testing
    filter_configs: List[Dict[str, Any]] = field(default_factory=lambda: [
        {},  # No filter (baseline)
        {"year": list(range(2000, 2010))},  # Decade filter
        {"year": list(range(2010, 2020))},  # Recent decade
        {"year": list(range(1990, 2000))},  # Older decade
        {"year": list(range(2015, 2021))},  # Small recent range
    ])
    
    # Parallel processing
    max_workers: Optional[int] = None  # Will auto-detect
    chunk_size: int = 100_000
    
    # Memory monitoring
    enable_memory_monitoring: bool = True
    memory_sample_interval: float = 0.1  # seconds


@dataclass 
class OptimizationConfig:
    """Configuration for optimization testing."""
    # Environment variables to test
    test_env_vars: Dict[str, List[Any]] = field(default_factory=lambda: {
        'CHUNK_SIZE': [50000, 100000, 200000, 500000],
        'BATCH_PARALLEL_THRESHOLD': [1000, 5000, 10000, 20000],
        'MAX_CACHE_ENTRIES': [8, 16, 32, 64],
        'INGEST_CHUNK_SIZE': [500000, 1000000, 2000000],
    })
    
    # Smart dispatch testing
    smart_dispatch_sizes: List[int] = field(default_factory=lambda: [10, 100, 1000, 10000])
    
    # Optimization features to test
    test_features: List[str] = field(default_factory=lambda: [
        'compute_then_build',
        'timing_instrumentation', 
        'large_chunks',
        'filter_caching',
        'batch_optimization'
    ])


@dataclass
class OutputConfig:
    """Configuration for output and reporting."""
    # Output directories
    results_dir: str = field(default_factory=lambda: os.path.join(os.getcwd(), 'benchmark_results'))
    plots_dir: str = field(default_factory=lambda: os.path.join(os.getcwd(), 'benchmark_plots'))
    logs_dir: str = field(default_factory=lambda: os.path.join(os.getcwd(), 'benchmark_logs'))
    
    # File formats
    results_format: str = "json"  # json, csv, parquet
    plot_format: str = "png"  # png, pdf, svg
    
    # Logging
    log_level: str = "INFO"
    log_to_file: bool = True
    log_to_console: bool = True
    
    # Report generation
    generate_html_report: bool = True
    generate_pdf_report: bool = False
    include_plots: bool = True


class BenchmarkConfig:
    """Main configuration class that combines all configuration sections."""
    
    def __init__(self, 
                 data: Optional[DataConfig] = None,
                 performance: Optional[PerformanceConfig] = None,
                 optimization: Optional[OptimizationConfig] = None,
                 output: Optional[OutputConfig] = None):
        
        self.data = data or DataConfig()
        self.performance = performance or PerformanceConfig()
        self.optimization = optimization or OptimizationConfig()
        self.output = output or OutputConfig()
        
        # Auto-detect CPU count if not specified
        if self.performance.max_workers is None:
            self.performance.max_workers = self._detect_cpu_count()
    
    def _detect_cpu_count(self) -> int:
        """Detect optimal number of worker processes."""
        try:
            # Try SLURM first
            slurm_cpus = os.environ.get('SLURM_CPUS_PER_TASK')
            if slurm_cpus:
                return int(slurm_cpus)
        except (ValueError, TypeError):
            pass
        
        try:
            # Fall back to system CPU count
            return os.cpu_count() or 4
        except:
            return 4
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of issues."""
        issues = []
        
        # Check if we have either raw data or cached data
        has_raw_data = (
            os.path.exists(self.data.wos_data_dir) and
            os.path.exists(os.path.join(self.data.wos_data_dir, self.data.vertices_file)) and
            os.path.exists(os.path.join(self.data.wos_data_dir, self.data.edges_file))
        )
        
        has_cached_micro_data = (
            os.path.exists(self.data.cache_dir) and
            os.path.exists(os.path.join(self.data.cache_dir, 'micro_vertices.parquet')) and
            os.path.exists(os.path.join(self.data.cache_dir, 'micro_edges.parquet'))
        )
        
        has_cached_full_data = (
            os.path.exists(self.data.cache_dir) and
            os.path.exists(os.path.join(self.data.cache_dir, 'paper_years.parquet')) and
            os.path.exists(os.path.join(self.data.cache_dir, 'edges.parquet'))
        )
        
        if not has_raw_data and not has_cached_micro_data and not has_cached_full_data:
            issues.append("No data source found: need either raw WoS data files or cached data")
            if not os.path.exists(self.data.wos_data_dir):
                issues.append(f"WoS data directory not found: {self.data.wos_data_dir}")
            else:
                vertices_path = os.path.join(self.data.wos_data_dir, self.data.vertices_file)
                edges_path = os.path.join(self.data.wos_data_dir, self.data.edges_file)
                if not os.path.exists(vertices_path):
                    issues.append(f"Vertices file not found: {vertices_path}")
                if not os.path.exists(edges_path):
                    issues.append(f"Edges file not found: {edges_path}")
            
            if not os.path.exists(self.data.cache_dir):
                issues.append(f"Cache directory not found: {self.data.cache_dir}")
        
        # Check output directories can be created
        for dir_path in [self.output.results_dir, self.output.plots_dir, self.output.logs_dir]:
            try:
                os.makedirs(dir_path, exist_ok=True)
            except Exception as e:
                issues.append(f"Cannot create directory {dir_path}: {e}")
        
        # Validate numeric parameters
        if self.data.micro_vertices <= 0:
            issues.append("micro_vertices must be positive")
        if self.performance.max_workers <= 0:
            issues.append("max_workers must be positive")
        if self.performance.chunk_size <= 0:
            issues.append("chunk_size must be positive")
        
        return issues
    
    def setup_environment(self) -> None:
        """Set up environment variables for benchmarking."""
        # Set up basic environment
        os.environ['CDINDEX_TIMING_DEBUG'] = '1'
        os.environ['PYTHONUNBUFFERED'] = '1'
        
        # Set optimization parameters
        os.environ['CHUNK_SIZE'] = str(self.performance.chunk_size)
        os.environ['INGEST_CHUNK_SIZE'] = str(self.performance.chunk_size)
        
        # Create output directories
        for dir_path in [self.output.results_dir, self.output.plots_dir, self.output.logs_dir]:
            os.makedirs(dir_path, exist_ok=True)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        def _dataclass_to_dict(obj):
            if hasattr(obj, '__dataclass_fields__'):
                return {k: _dataclass_to_dict(v) for k, v in obj.__dict__.items()}
            elif isinstance(obj, list):
                return [_dataclass_to_dict(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: _dataclass_to_dict(v) for k, v in obj.items()}
            else:
                return obj
        
        return {
            'data': _dataclass_to_dict(self.data),
            'performance': _dataclass_to_dict(self.performance),
            'optimization': _dataclass_to_dict(self.optimization),
            'output': _dataclass_to_dict(self.output)
        }
    
    def save(self, filepath: str) -> None:
        """Save configuration to file."""
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
    
    @classmethod
    def load(cls, filepath: str) -> 'BenchmarkConfig':
        """Load configuration from file."""
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # Convert dict back to dataclasses
        return cls(
            data=DataConfig(**data.get('data', {})),
            performance=PerformanceConfig(**data.get('performance', {})),
            optimization=OptimizationConfig(**data.get('optimization', {})),
            output=OutputConfig(**data.get('output', {}))
        )


# Predefined configurations for common use cases
def get_micro_config() -> BenchmarkConfig:
    """Get configuration optimized for micro-benchmarks."""
    return BenchmarkConfig(
        data=DataConfig(
            micro_vertices=100_000,
            micro_benchmark_papers=1_000
        ),
        performance=PerformanceConfig(
            batch_sizes=[100, 500, 1000, 5000],
            single_sample_sizes=[50, 100, 200]
        )
    )


def get_full_config() -> BenchmarkConfig:
    """Get configuration for full-scale benchmarks."""
    return BenchmarkConfig(
        data=DataConfig(
            micro_vertices=10_000_000,
            micro_benchmark_papers=100_000
        ),
        performance=PerformanceConfig(
            batch_sizes=[1000, 5000, 10000, 50000, 100000],
            time_windows=[5, 10]
        )
    )


def get_optimization_config() -> BenchmarkConfig:
    """Get configuration optimized for testing optimizations."""
    config = get_full_config()
    config.optimization.test_features = [
        'compute_then_build',
        'timing_instrumentation',
        'large_chunks',
        'filter_caching'
    ]
    return config
