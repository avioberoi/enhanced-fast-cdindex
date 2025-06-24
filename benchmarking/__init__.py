"""
Enhanced CD-Index Benchmarking Suite

A comprehensive benchmarking framework for testing and evaluating the performance
of the enhanced CD-index implementation.

This package provides:
- Data preparation utilities
- Micro and macro benchmarking tools
- Performance analysis and reporting
- Configuration management
- Result visualization
"""

__version__ = "1.0.0"
__author__ = "AVI"

# Import modules conditionally to handle missing dependencies gracefully
try:
    from config import BenchmarkConfig
    from data_loader import DataLoader
    from benchmark_runner import BenchmarkRunner
    from results_analyzer import ResultsAnalyzer
    
    __all__ = [
        'BenchmarkConfig',
        'DataLoader', 
        'BenchmarkRunner',
        'ResultsAnalyzer'
    ]
except ImportError as e:
    # If dependencies are missing, provide helpful error message
    import warnings
    warnings.warn(f"Some benchmarking modules could not be imported: {e}. "
                 "Run 'python setup.py' to check dependencies.", ImportWarning)
    
    __all__ = []
