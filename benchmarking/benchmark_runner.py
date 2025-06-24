"""
Benchmark Runner Module

Orchestrates and executes different types of benchmarks including:
- Micro benchmarks for detailed performance analysis
- Macro benchmarks for full-scale testing
- Optimization benchmarks for feature testing
- Comparative benchmarks against baselines
"""

import os
import sys
import time
import logging
import gc
import traceback
from typing import Dict, List, Any, Optional, Tuple, Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
import json

# Add the parent directory to import modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Try to import optional dependencies
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

from config import BenchmarkConfig
from data_loader import DataLoader


@dataclass
class BenchmarkResult:
    """Container for benchmark results."""
    name: str
    duration: float
    throughput: float
    memory_mb: float
    cpu_percent: float
    metadata: Dict[str, Any]
    success: bool = True
    error: Optional[str] = None


class PerformanceMonitor:
    """Monitor system performance during benchmarks."""
    
    def __init__(self, sample_interval: float = 0.1):
        self.sample_interval = sample_interval
        self.process = psutil.Process() if PSUTIL_AVAILABLE else None
        self.start_memory = 0
        self.peak_memory = 0
        self.cpu_samples = []
        
    def start(self):
        """Start monitoring."""
        if self.process:
            self.start_memory = self.process.memory_info().rss / (1024 * 1024)  # MB
            self.peak_memory = self.start_memory
            self.cpu_samples = []
    
    def sample(self):
        """Take a performance sample."""
        if self.process:
            current_memory = self.process.memory_info().rss / (1024 * 1024)
            self.peak_memory = max(self.peak_memory, current_memory)
            self.cpu_samples.append(self.process.cpu_percent())
    
    def get_stats(self) -> Dict[str, float]:
        """Get performance statistics."""
        if not self.process:
            return {'memory_mb': 0, 'cpu_percent': 0}
        
        return {
            'memory_mb': self.peak_memory - self.start_memory,
            'cpu_percent': sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0
        }


class BenchmarkRunner:
    """
    Main benchmark execution engine.
    
    Provides methods for running different types of benchmarks and collecting
    comprehensive performance metrics.
    """
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.data_loader = DataLoader(config)
        self.results = []
        
        # Set up environment
        self.config.setup_environment()
    
    def run_micro_benchmark(self) -> Dict[str, Any]:
        """
        Run comprehensive micro-benchmarks on a representative data subset.
        
        Returns:
            Dictionary containing all micro-benchmark results
        """
        self.logger.info("Starting micro-benchmark suite...")
        
        # Load micro dataset
        try:
            data = self.data_loader.load_micro_dataset()
            graph = data['graph']
            benchmark_ids = data['benchmark_ids']
            vertex_count = data['vertex_count']
            edge_count = data['edge_count']
        except Exception as e:
            self.logger.error(f"Failed to load micro dataset: {e}")
            return {'error': str(e)}
        
        results = {
            'metadata': {
                'vertex_count': vertex_count,
                'edge_count': edge_count,
                'benchmark_papers': len(benchmark_ids),
                'timestamp': time.time()
            },
            'benchmarks': {}
        }
        
        # Convert benchmark IDs to Python list for easier handling
        id_list = benchmark_ids.to_pylist()
        
        # Run benchmarks for each time window
        for time_window in self.config.performance.time_windows:
            self.logger.info(f"Testing time window: {time_window} years")
            
            window_key = f"time_window_{time_window}"
            results['benchmarks'][window_key] = {}
            
            # 1. Single computation benchmarks
            results['benchmarks'][window_key]['single'] = self._run_single_benchmarks(
                graph, id_list, time_window
            )
            
            # 2. Batch computation benchmarks
            results['benchmarks'][window_key]['batch'] = self._run_batch_benchmarks(
                graph, id_list, time_window
            )
            
            # 3. Filtered computation benchmarks
            results['benchmarks'][window_key]['filtered'] = self._run_filtered_benchmarks(
                graph, id_list, time_window
            )
            
            # 4. Smart dispatch benchmarks
            results['benchmarks'][window_key]['smart_dispatch'] = self._run_smart_dispatch_benchmarks(
                graph, id_list, time_window
            )
        
        self.logger.info("Micro-benchmark suite completed!")
        return results
    
    def _run_single_benchmarks(self, graph, id_list: List[int], time_window: int) -> Dict[str, Any]:
        """Run single computation benchmarks."""
        self.logger.info("Running single computation benchmarks...")
        
        results = {}
        
        for sample_size in self.config.performance.single_sample_sizes:
            if sample_size > len(id_list):
                continue
                
            sample_ids = id_list[:sample_size]
            
            monitor = PerformanceMonitor(self.config.performance.memory_sample_interval)
            monitor.start()
            
            start_time = time.perf_counter()
            scores = []
            
            for i, paper_id in enumerate(sample_ids):
                if (i + 1) % 100 == 0:
                    monitor.sample()
                
                score = graph.cdindex(paper_id, time_window)
                scores.append(score)
            
            duration = time.perf_counter() - start_time
            throughput = sample_size / duration
            
            perf_stats = monitor.get_stats()
            
            results[f"size_{sample_size}"] = {
                'duration': duration,
                'throughput': throughput,
                'sample_size': sample_size,
                'memory_mb': perf_stats['memory_mb'],
                'cpu_percent': perf_stats['cpu_percent'],
                'scores_sample': scores[:10]  # Keep first 10 for validation
            }
            
            self.logger.info(f"  Size {sample_size}: {duration:.3f}s, {throughput:.1f} papers/sec")
        
        return results
    
    def _run_batch_benchmarks(self, graph, id_list: List[int], time_window: int) -> Dict[str, Any]:
        """Run batch computation benchmarks."""
        self.logger.info("Running batch computation benchmarks...")
        
        if not PYARROW_AVAILABLE:
            return {'error': 'PyArrow not available for batch operations'}
        
        results = {}
        
        for batch_size in self.config.performance.batch_sizes:
            if batch_size > len(id_list):
                continue
            
            batch_ids = id_list[:batch_size]
            batch_array = pa.array(batch_ids, type=pa.uint32())
            
            monitor = PerformanceMonitor(self.config.performance.memory_sample_interval)
            monitor.start()
            
            start_time = time.perf_counter()
            result_table = graph.cdindex_batch(batch_array, time_window)
            duration = time.perf_counter() - start_time
            
            throughput = batch_size / duration
            perf_stats = monitor.get_stats()
            
            results[f"size_{batch_size}"] = {
                'duration': duration,
                'throughput': throughput,
                'batch_size': batch_size,
                'memory_mb': perf_stats['memory_mb'],
                'cpu_percent': perf_stats['cpu_percent'],
                'result_rows': result_table.num_rows,
                'result_columns': result_table.num_columns
            }
            
            self.logger.info(f"  Batch {batch_size}: {duration:.3f}s, {throughput:.1f} papers/sec")
        
        return results
    
    def _run_filtered_benchmarks(self, graph, id_list: List[int], time_window: int) -> Dict[str, Any]:
        """Run filtered computation benchmarks."""
        self.logger.info("Running filtered computation benchmarks...")
        
        if not PYARROW_AVAILABLE:
            return {'error': 'PyArrow not available for filtered operations'}
        
        results = {}
        test_size = min(1000, len(id_list))  # Use smaller size for filtered tests
        test_ids = id_list[:test_size]
        test_array = pa.array(test_ids, type=pa.uint32())
        
        for i, filter_config in enumerate(self.config.performance.filter_configs):
            filter_key = f"filter_{i+1}"
            
            monitor = PerformanceMonitor(self.config.performance.memory_sample_interval)
            monitor.start()
            
            start_time = time.perf_counter()
            
            if filter_config:  # Non-empty filter
                result_table = graph.cdindex_filtered_batch(test_array, time_window, filter_config)
            else:  # Empty filter (baseline)
                result_table = graph.cdindex_batch(test_array, time_window)
                
            duration = time.perf_counter() - start_time
            throughput = test_size / duration
            
            perf_stats = monitor.get_stats()
            
            results[filter_key] = {
                'duration': duration,
                'throughput': throughput,
                'filter_config': filter_config,
                'test_size': test_size,
                'memory_mb': perf_stats['memory_mb'],
                'cpu_percent': perf_stats['cpu_percent'],
                'result_rows': result_table.num_rows
            }
            
            filter_desc = str(filter_config) if filter_config else "unfiltered"
            self.logger.info(f"  {filter_desc}: {duration:.3f}s, {throughput:.1f} papers/sec")
        
        return results
    
    def _run_smart_dispatch_benchmarks(self, graph, id_list: List[int], time_window: int) -> Dict[str, Any]:
        """Run smart dispatch benchmarks."""
        self.logger.info("Running smart dispatch benchmarks...")
        
        if not PYARROW_AVAILABLE:
            return {'error': 'PyArrow not available for smart dispatch tests'}
        
        results = {}
        
        # Test configurations: (size, filter)
        test_configs = [
            (100, {}),
            (1000, {}),
            (100, {"year": list(range(2000, 2010))}),
            (1000, {"year": list(range(2000, 2010))}),
        ]
        
        for size, filter_config in test_configs:
            if size > len(id_list):
                continue
                
            test_ids = id_list[:size]
            test_array = pa.array(test_ids, type=pa.uint32())
            
            config_key = f"size_{size}_" + ("filtered" if filter_config else "unfiltered")
            
            monitor = PerformanceMonitor(self.config.performance.memory_sample_interval)
            monitor.start()
            
            start_time = time.perf_counter()
            
            if filter_config:
                result_table = graph.cdindex_filtered_batch(test_array, time_window, filter_config)
            else:
                result_table = graph.cdindex_batch(test_array, time_window)
            
            duration = time.perf_counter() - start_time
            throughput = size / duration
            
            perf_stats = monitor.get_stats()
            
            results[config_key] = {
                'duration': duration,
                'throughput': throughput,
                'size': size,
                'filter_config': filter_config,
                'memory_mb': perf_stats['memory_mb'],
                'cpu_percent': perf_stats['cpu_percent'],
                'result_rows': result_table.num_rows
            }
            
            self.logger.info(f"  {config_key}: {duration:.3f}s, {throughput:.1f} papers/sec")
        
        return results
    
    def run_optimization_benchmark(self) -> Dict[str, Any]:
        """
        Run optimization-focused benchmarks to test performance improvements.
        
        Returns:
            Dictionary containing optimization benchmark results
        """
        self.logger.info("Starting optimization benchmark suite...")
        
        results = {
            'metadata': {
                'timestamp': time.time(),
                'config': self.config.to_dict()
            },
            'optimizations': {}
        }
        
        # Test different optimization configurations
        for feature in self.config.optimization.test_features:
            self.logger.info(f"Testing optimization: {feature}")
            
            try:
                feature_results = self._test_optimization_feature(feature)
                results['optimizations'][feature] = feature_results
            except Exception as e:
                self.logger.error(f"Error testing {feature}: {e}")
                results['optimizations'][feature] = {'error': str(e)}
        
        return results
    
    def _test_optimization_feature(self, feature: str) -> Dict[str, Any]:
        """Test a specific optimization feature."""
        # This would implement specific tests for each optimization feature
        # For now, return a placeholder
        return {
            'feature': feature,
            'status': 'not_implemented',
            'message': f'Testing for {feature} not yet implemented'
        }
    
    def run_full_benchmark(self) -> Dict[str, Any]:
        """
        Run full-scale benchmarks on the complete dataset.
        
        Returns:
            Dictionary containing full benchmark results
        """
        self.logger.info("Starting full benchmark suite...")
        
        # Load full dataset
        try:
            vertices_table, edges_table = self.data_loader.load_wos_data()
        except Exception as e:
            self.logger.error(f"Failed to load full dataset: {e}")
            return {'error': str(e)}
        
        results = {
            'metadata': {
                'vertex_count': vertices_table.num_rows,
                'edge_count': edges_table.num_rows,
                'timestamp': time.time()
            },
            'benchmarks': {}
        }
        
        # For full benchmarks, we'd typically run larger-scale tests
        # This is a placeholder for the actual implementation
        self.logger.info("Full benchmark implementation pending...")
        
        return results
    
    def save_results(self, results: Dict[str, Any], filename: str) -> None:
        """Save benchmark results to file."""
        output_path = os.path.join(self.config.output.results_dir, filename)
        
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        self.logger.info(f"Results saved to: {output_path}")
    
    def run_all_benchmarks(self) -> Dict[str, Any]:
        """
        Run all configured benchmarks.
        
        Returns:
            Comprehensive results from all benchmark types
        """
        self.logger.info("Starting complete benchmark suite...")
        
        all_results = {
            'config': self.config.to_dict(),
            'start_time': time.time(),
            'benchmarks': {}
        }
        
        # Run micro benchmarks
        try:
            self.logger.info("=" * 60)
            self.logger.info("RUNNING MICRO BENCHMARKS")
            self.logger.info("=" * 60)
            
            micro_results = self.run_micro_benchmark()
            all_results['benchmarks']['micro'] = micro_results
            
        except Exception as e:
            self.logger.error(f"Micro benchmark failed: {e}")
            all_results['benchmarks']['micro'] = {'error': str(e)}
        
        # Run optimization benchmarks
        try:
            self.logger.info("=" * 60)
            self.logger.info("RUNNING OPTIMIZATION BENCHMARKS")
            self.logger.info("=" * 60)
            
            opt_results = self.run_optimization_benchmark()
            all_results['benchmarks']['optimization'] = opt_results
            
        except Exception as e:
            self.logger.error(f"Optimization benchmark failed: {e}")
            all_results['benchmarks']['optimization'] = {'error': str(e)}
        
        all_results['end_time'] = time.time()
        all_results['total_duration'] = all_results['end_time'] - all_results['start_time']
        
        self.logger.info("Complete benchmark suite finished!")
        return all_results
