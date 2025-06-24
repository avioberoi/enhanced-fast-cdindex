"""
Results Analysis and Reporting Module

Provides tools for analyzing benchmark results including:
- Performance comparison and trending
- Statistical analysis and visualization
- Report generation (HTML, PDF)
- Data export and formatting
"""

import os
import json
import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import statistics
from dataclasses import dataclass

try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from config import BenchmarkConfig


@dataclass
class PerformanceComparison:
    """Container for performance comparison data."""
    metric: str
    baseline: float
    current: float
    improvement_percent: float
    is_better: bool


class ResultsAnalyzer:
    """
    Analyzes benchmark results and generates reports.
    
    Provides methods for:
    - Statistical analysis of performance data
    - Comparison between different runs
    - Visualization generation
    - Report creation
    """
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Ensure output directories exist
        os.makedirs(self.config.output.results_dir, exist_ok=True)
        os.makedirs(self.config.output.plots_dir, exist_ok=True)
    
    def load_results(self, filename: str) -> Dict[str, Any]:
        """Load benchmark results from file."""
        filepath = os.path.join(self.config.output.results_dir, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Results file not found: {filepath}")
        
        with open(filepath, 'r') as f:
            return json.load(f)
    
    def analyze_micro_results(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze micro-benchmark results.
        
        Args:
            results: Micro-benchmark results dictionary
            
        Returns:
            Analysis summary dictionary
        """
        if 'benchmarks' not in results:
            return {'error': 'Invalid results format - missing benchmarks'}
        
        analysis = {
            'summary': {},
            'performance_trends': {},
            'statistics': {},
            'recommendations': []
        }
        
        # Analyze each time window
        for window_key, window_data in results['benchmarks'].items():
            if not window_key.startswith('time_window_'):
                continue
                
            time_window = int(window_key.split('_')[-1])
            analysis['performance_trends'][time_window] = self._analyze_window_performance(window_data)
        
        # Generate summary statistics
        analysis['summary'] = self._generate_summary_stats(results)
        
        # Generate recommendations
        analysis['recommendations'] = self._generate_recommendations(analysis)
        
        return analysis
    
    def _analyze_window_performance(self, window_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze performance for a specific time window."""
        analysis = {
            'single_performance': {},
            'batch_performance': {},
            'filter_impact': {},
            'optimal_configs': {}
        }
        
        # Analyze single computation performance
        if 'single' in window_data:
            single_data = window_data['single']
            throughputs = [result['throughput'] for result in single_data.values() 
                          if isinstance(result, dict) and 'throughput' in result]
            
            if throughputs:
                analysis['single_performance'] = {
                    'max_throughput': max(throughputs),
                    'min_throughput': min(throughputs),
                    'avg_throughput': statistics.mean(throughputs),
                    'throughput_std': statistics.stdev(throughputs) if len(throughputs) > 1 else 0
                }
        
        # Analyze batch performance
        if 'batch' in window_data:
            batch_data = window_data['batch']
            batch_throughputs = []
            batch_sizes = []
            
            for key, result in batch_data.items():
                if isinstance(result, dict) and 'throughput' in result:
                    batch_throughputs.append(result['throughput'])
                    batch_sizes.append(result.get('batch_size', 0))
            
            if batch_throughputs:
                # Find optimal batch size
                max_throughput_idx = batch_throughputs.index(max(batch_throughputs))
                optimal_batch_size = batch_sizes[max_throughput_idx]
                
                analysis['batch_performance'] = {
                    'max_throughput': max(batch_throughputs),
                    'optimal_batch_size': optimal_batch_size,
                    'throughput_range': max(batch_throughputs) - min(batch_throughputs),
                    'size_throughput_pairs': list(zip(batch_sizes, batch_throughputs))
                }
        
        # Analyze filter impact
        if 'filtered' in window_data:
            filter_data = window_data['filtered']
            baseline_throughput = None
            filter_throughputs = {}
            
            for key, result in filter_data.items():
                if isinstance(result, dict) and 'throughput' in result:
                    filter_config = result.get('filter_config', {})
                    if not filter_config:  # Baseline (no filter)
                        baseline_throughput = result['throughput']
                    else:
                        filter_throughputs[key] = {
                            'throughput': result['throughput'],
                            'config': filter_config
                        }
            
            if baseline_throughput and filter_throughputs:
                analysis['filter_impact'] = {
                    'baseline_throughput': baseline_throughput,
                    'filtered_results': {}
                }
                
                for key, data in filter_throughputs.items():
                    overhead = (baseline_throughput - data['throughput']) / baseline_throughput * 100
                    analysis['filter_impact']['filtered_results'][key] = {
                        'throughput': data['throughput'],
                        'overhead_percent': overhead,
                        'config': data['config']
                    }
        
        return analysis
    
    def _generate_summary_stats(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate overall summary statistics."""
        summary = {
            'test_metadata': results.get('metadata', {}),
            'overall_performance': {},
            'memory_usage': {},
            'cpu_utilization': {}
        }
        
        # Collect all throughput values
        all_throughputs = []
        all_memory = []
        all_cpu = []
        
        def collect_metrics(data):
            if isinstance(data, dict):
                if 'throughput' in data:
                    all_throughputs.append(data['throughput'])
                if 'memory_mb' in data:
                    all_memory.append(data['memory_mb'])
                if 'cpu_percent' in data:
                    all_cpu.append(data['cpu_percent'])
                
                for value in data.values():
                    if isinstance(value, dict):
                        collect_metrics(value)
        
        collect_metrics(results.get('benchmarks', {}))
        
        # Calculate summary statistics
        if all_throughputs:
            summary['overall_performance'] = {
                'max_throughput': max(all_throughputs),
                'avg_throughput': statistics.mean(all_throughputs),
                'throughput_std': statistics.stdev(all_throughputs) if len(all_throughputs) > 1 else 0,
                'sample_count': len(all_throughputs)
            }
        
        if all_memory:
            summary['memory_usage'] = {
                'max_memory_mb': max(all_memory),
                'avg_memory_mb': statistics.mean(all_memory),
                'memory_std': statistics.stdev(all_memory) if len(all_memory) > 1 else 0
            }
        
        if all_cpu:
            summary['cpu_utilization'] = {
                'max_cpu_percent': max(all_cpu),
                'avg_cpu_percent': statistics.mean(all_cpu),
                'cpu_std': statistics.stdev(all_cpu) if len(all_cpu) > 1 else 0
            }
        
        return summary
    
    def _generate_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate performance recommendations based on analysis."""
        recommendations = []
        
        # Check batch performance
        for time_window, data in analysis.get('performance_trends', {}).items():
            batch_perf = data.get('batch_performance', {})
            if batch_perf:
                optimal_size = batch_perf.get('optimal_batch_size')
                if optimal_size:
                    recommendations.append(
                        f"For {time_window}-year windows, optimal batch size is {optimal_size:,} papers"
                    )
        
        # Check filter overhead
        high_overhead_filters = []
        for time_window, data in analysis.get('performance_trends', {}).items():
            filter_impact = data.get('filter_impact', {})
            if filter_impact and 'filtered_results' in filter_impact:
                for key, result in filter_impact['filtered_results'].items():
                    overhead = result.get('overhead_percent', 0)
                    if overhead > 50:  # More than 50% overhead
                        high_overhead_filters.append((time_window, key, overhead))
        
        if high_overhead_filters:
            recommendations.append(
                "Consider optimizing filters with high overhead (>50%): " +
                ", ".join([f"{tw}y-{key}" for tw, key, _ in high_overhead_filters])
            )
        
        # Memory usage recommendations
        summary = analysis.get('summary', {})
        memory_stats = summary.get('memory_usage', {})
        if memory_stats:
            max_memory = memory_stats.get('max_memory_mb', 0)
            if max_memory > 1000:  # More than 1GB
                recommendations.append(
                    f"High memory usage detected ({max_memory:.1f}MB). Consider memory optimization."
                )
        
        return recommendations
    
    def compare_results(self, baseline_file: str, current_file: str) -> Dict[str, Any]:
        """
        Compare two sets of benchmark results.
        
        Args:
            baseline_file: Filename of baseline results
            current_file: Filename of current results
            
        Returns:
            Comparison analysis dictionary
        """
        try:
            baseline = self.load_results(baseline_file)
            current = self.load_results(current_file)
        except FileNotFoundError as e:
            return {'error': str(e)}
        
        comparison = {
            'metadata': {
                'baseline_file': baseline_file,
                'current_file': current_file,
                'comparison_time': time.time()
            },
            'performance_changes': {},
            'summary': {}
        }
        
        # Compare performance metrics
        comparison['performance_changes'] = self._compare_performance_metrics(baseline, current)
        
        # Generate comparison summary
        comparison['summary'] = self._generate_comparison_summary(comparison['performance_changes'])
        
        return comparison
    
    def _compare_performance_metrics(self, baseline: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
        """Compare performance metrics between two result sets."""
        changes = {}
        
        # Extract throughput values from both results
        def extract_throughputs(data, path=""):
            throughputs = {}
            if isinstance(data, dict):
                if 'throughput' in data:
                    throughputs[path] = data['throughput']
                for key, value in data.items():
                    if isinstance(value, dict):
                        sub_path = f"{path}.{key}" if path else key
                        throughputs.update(extract_throughputs(value, sub_path))
            return throughputs
        
        baseline_throughputs = extract_throughputs(baseline.get('benchmarks', {}))
        current_throughputs = extract_throughputs(current.get('benchmarks', {}))
        
        # Compare common metrics
        for path in set(baseline_throughputs.keys()) & set(current_throughputs.keys()):
            baseline_val = baseline_throughputs[path]
            current_val = current_throughputs[path]
            
            if baseline_val > 0:
                improvement = (current_val - baseline_val) / baseline_val * 100
                changes[path] = PerformanceComparison(
                    metric='throughput',
                    baseline=baseline_val,
                    current=current_val,
                    improvement_percent=improvement,
                    is_better=improvement > 0
                )
        
        return changes
    
    def _generate_comparison_summary(self, changes: Dict[str, PerformanceComparison]) -> Dict[str, Any]:
        """Generate summary of performance changes."""
        if not changes:
            return {'message': 'No comparable metrics found'}
        
        improvements = [c.improvement_percent for c in changes.values()]
        better_count = sum(1 for c in changes.values() if c.is_better)
        
        return {
            'total_comparisons': len(changes),
            'improvements': better_count,
            'regressions': len(changes) - better_count,
            'avg_improvement': statistics.mean(improvements),
            'max_improvement': max(improvements),
            'min_improvement': min(improvements),
            'significant_changes': [
                (path, comp.improvement_percent) 
                for path, comp in changes.items() 
                if abs(comp.improvement_percent) > 10  # More than 10% change
            ]
        }
    
    def generate_plots(self, results: Dict[str, Any], prefix: str = "") -> List[str]:
        """
        Generate visualization plots from benchmark results.
        
        Args:
            results: Benchmark results dictionary
            prefix: Prefix for plot filenames
            
        Returns:
            List of generated plot filenames
        """
        if not MATPLOTLIB_AVAILABLE:
            self.logger.warning("Matplotlib not available, skipping plot generation")
            return []
        
        plot_files = []
        
        try:
            # Throughput vs Batch Size plot
            throughput_plot = self._plot_throughput_vs_batch_size(results, prefix)
            if throughput_plot:
                plot_files.append(throughput_plot)
            
            # Filter overhead plot
            filter_plot = self._plot_filter_overhead(results, prefix)
            if filter_plot:
                plot_files.append(filter_plot)
            
            # Memory usage plot
            memory_plot = self._plot_memory_usage(results, prefix)
            if memory_plot:
                plot_files.append(memory_plot)
            
        except Exception as e:
            self.logger.error(f"Error generating plots: {e}")
        
        return plot_files
    
    def _plot_throughput_vs_batch_size(self, results: Dict[str, Any], prefix: str) -> Optional[str]:
        """Generate throughput vs batch size plot."""
        try:
            plt.figure(figsize=(10, 6))
            
            # Extract batch performance data
            for window_key, window_data in results.get('benchmarks', {}).items():
                if not window_key.startswith('time_window_'):
                    continue
                
                time_window = int(window_key.split('_')[-1])
                batch_data = window_data.get('batch', {})
                
                batch_sizes = []
                throughputs = []
                
                for result in batch_data.values():
                    if isinstance(result, dict) and 'throughput' in result and 'batch_size' in result:
                        batch_sizes.append(result['batch_size'])
                        throughputs.append(result['throughput'])
                
                if batch_sizes and throughputs:
                    plt.plot(batch_sizes, throughputs, marker='o', label=f'{time_window} years')
            
            plt.xlabel('Batch Size')
            plt.ylabel('Throughput (papers/sec)')
            plt.title('Throughput vs Batch Size')
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.xscale('log')
            
            filename = f"{prefix}throughput_vs_batch_size.{self.config.output.plot_format}"
            filepath = os.path.join(self.config.output.plots_dir, filename)
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.close()
            
            return filename
            
        except Exception as e:
            self.logger.error(f"Error creating throughput plot: {e}")
            return None
    
    def _plot_filter_overhead(self, results: Dict[str, Any], prefix: str) -> Optional[str]:
        """Generate filter overhead plot."""
        try:
            plt.figure(figsize=(12, 6))
            
            filter_names = []
            overheads = []
            colors = []
            
            # Extract filter data
            for window_key, window_data in results.get('benchmarks', {}).items():
                if not window_key.startswith('time_window_'):
                    continue
                
                filter_data = window_data.get('filtered', {})
                baseline_throughput = None
                
                # Find baseline
                for result in filter_data.values():
                    if isinstance(result, dict) and not result.get('filter_config', {}):
                        baseline_throughput = result.get('throughput')
                        break
                
                if not baseline_throughput:
                    continue
                
                # Calculate overheads
                for key, result in filter_data.items():
                    if isinstance(result, dict) and result.get('filter_config', {}):
                        throughput = result.get('throughput', 0)
                        overhead = (baseline_throughput - throughput) / baseline_throughput * 100
                        
                        filter_names.append(f"{key} ({window_key})")
                        overheads.append(overhead)
                        colors.append('red' if overhead > 20 else 'orange' if overhead > 10 else 'green')
            
            if filter_names and overheads:
                bars = plt.bar(range(len(filter_names)), overheads, color=colors)
                plt.xlabel('Filter Configuration')
                plt.ylabel('Overhead (%)')
                plt.title('Filter Performance Overhead')
                plt.xticks(range(len(filter_names)), filter_names, rotation=45, ha='right')
                plt.grid(True, alpha=0.3)
                
                # Add legend
                red_patch = mpatches.Patch(color='red', label='High overhead (>20%)')
                orange_patch = mpatches.Patch(color='orange', label='Medium overhead (10-20%)')
                green_patch = mpatches.Patch(color='green', label='Low overhead (<10%)')
                plt.legend(handles=[red_patch, orange_patch, green_patch])
            
            filename = f"{prefix}filter_overhead.{self.config.output.plot_format}"
            filepath = os.path.join(self.config.output.plots_dir, filename)
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.close()
            
            return filename
            
        except Exception as e:
            self.logger.error(f"Error creating filter overhead plot: {e}")
            return None
    
    def _plot_memory_usage(self, results: Dict[str, Any], prefix: str) -> Optional[str]:
        """Generate memory usage plot."""
        try:
            plt.figure(figsize=(10, 6))
            
            # Extract memory usage data
            test_names = []
            memory_values = []
            
            def collect_memory_data(data, path=""):
                if isinstance(data, dict):
                    if 'memory_mb' in data and 'throughput' in data:
                        test_names.append(path)
                        memory_values.append(data['memory_mb'])
                    
                    for key, value in data.items():
                        if isinstance(value, dict):
                            sub_path = f"{path}.{key}" if path else key
                            collect_memory_data(value, sub_path)
            
            collect_memory_data(results.get('benchmarks', {}))
            
            if test_names and memory_values:
                plt.bar(range(len(test_names)), memory_values)
                plt.xlabel('Test Configuration')
                plt.ylabel('Memory Usage (MB)')
                plt.title('Memory Usage by Test Configuration')
                plt.xticks(range(len(test_names)), 
                          [name.split('.')[-1] for name in test_names], 
                          rotation=45, ha='right')
                plt.grid(True, alpha=0.3)
            
            filename = f"{prefix}memory_usage.{self.config.output.plot_format}"
            filepath = os.path.join(self.config.output.plots_dir, filename)
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.close()
            
            return filename
            
        except Exception as e:
            self.logger.error(f"Error creating memory usage plot: {e}")
            return None
    
    def generate_html_report(self, results: Dict[str, Any], analysis: Dict[str, Any], 
                           filename: str = "benchmark_report.html") -> str:
        """
        Generate an HTML report from benchmark results and analysis.
        
        Args:
            results: Benchmark results dictionary
            analysis: Analysis results dictionary
            filename: Output filename
            
        Returns:
            Path to generated HTML report
        """
        html_content = self._create_html_report(results, analysis)
        
        filepath = os.path.join(self.config.output.results_dir, filename)
        with open(filepath, 'w') as f:
            f.write(html_content)
        
        self.logger.info(f"HTML report generated: {filepath}")
        return filepath
    
    def _create_html_report(self, results: Dict[str, Any], analysis: Dict[str, Any]) -> str:
        """Create HTML report content."""
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Enhanced CD-Index Benchmark Report</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                h1 { color: #2c3e50; }
                h2 { color: #34495e; border-bottom: 2px solid #ecf0f1; padding-bottom: 10px; }
                .metric { background: #f8f9fa; padding: 10px; margin: 10px 0; border-left: 4px solid #3498db; }
                .recommendation { background: #fff3cd; padding: 10px; margin: 10px 0; border-left: 4px solid #ffc107; }
                table { border-collapse: collapse; width: 100%; margin: 20px 0; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
                .good { color: #27ae60; }
                .bad { color: #e74c3c; }
            </style>
        </head>
        <body>
            <h1>Enhanced CD-Index Benchmark Report</h1>
        """
        
        # Add metadata
        metadata = results.get('metadata', {})
        if metadata:
            html += "<h2>Test Metadata</h2>"
            html += f"<div class='metric'>Vertex Count: {metadata.get('vertex_count', 'N/A'):,}</div>"
            html += f"<div class='metric'>Edge Count: {metadata.get('edge_count', 'N/A'):,}</div>"
            html += f"<div class='metric'>Benchmark Papers: {metadata.get('benchmark_papers', 'N/A'):,}</div>"
        
        # Add summary statistics
        summary = analysis.get('summary', {})
        if summary:
            html += "<h2>Performance Summary</h2>"
            
            overall = summary.get('overall_performance', {})
            if overall:
                html += f"<div class='metric'>Max Throughput: {overall.get('max_throughput', 0):.1f} papers/sec</div>"
                html += f"<div class='metric'>Average Throughput: {overall.get('avg_throughput', 0):.1f} papers/sec</div>"
            
            memory = summary.get('memory_usage', {})
            if memory:
                html += f"<div class='metric'>Max Memory Usage: {memory.get('max_memory_mb', 0):.1f} MB</div>"
        
        # Add recommendations
        recommendations = analysis.get('recommendations', [])
        if recommendations:
            html += "<h2>Recommendations</h2>"
            for rec in recommendations:
                html += f"<div class='recommendation'>{rec}</div>"
        
        html += """
        </body>
        </html>
        """
        
        return html
