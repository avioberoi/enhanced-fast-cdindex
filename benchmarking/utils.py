#!/usr/bin/env python3
"""
Utility functions for the benchmarking suite.

Common functions used across multiple modules.
"""

import os
import time
import logging
import functools
from typing import Any, Callable, Dict, List, Optional
import json

def timer(func: Callable) -> Callable:
    """
    Decorator to time function execution.
    
    Args:
        func: Function to time
        
    Returns:
        Wrapped function that logs execution time
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        
        logger = logging.getLogger(func.__module__)
        logger.debug(f"{func.__name__} took {end_time - start_time:.3f} seconds")
        
        return result
    return wrapper


def ensure_dir(path: str) -> None:
    """
    Ensure directory exists, creating if necessary.
    
    Args:
        path: Directory path to ensure exists
    """
    os.makedirs(path, exist_ok=True)


def format_number(num: float, precision: int = 2) -> str:
    """
    Format number with appropriate units and precision.
    
    Args:
        num: Number to format
        precision: Decimal precision
        
    Returns:
        Formatted number string
    """
    if num >= 1_000_000:
        return f"{num / 1_000_000:.{precision}f}M"
    elif num >= 1_000:
        return f"{num / 1_000:.{precision}f}K"
    else:
        return f"{num:.{precision}f}"


def format_duration(seconds: float) -> str:
    """
    Format duration in human-readable format.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string
    """
    if seconds >= 3600:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"
    elif seconds >= 60:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        return f"{seconds:.2f}s"


def format_bytes(bytes_count: int) -> str:
    """
    Format byte count in human-readable format.
    
    Args:
        bytes_count: Number of bytes
        
    Returns:
        Formatted byte count string
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_count < 1024.0:
            return f"{bytes_count:.1f} {unit}"
        bytes_count /= 1024.0
    return f"{bytes_count:.1f} PB"


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero.
    
    Args:
        numerator: Numerator
        denominator: Denominator  
        default: Default value if division by zero
        
    Returns:
        Division result or default
    """
    return numerator / denominator if denominator != 0 else default


def calculate_improvement(baseline: float, current: float) -> float:
    """
    Calculate percentage improvement.
    
    Args:
        baseline: Baseline value
        current: Current value
        
    Returns:
        Improvement percentage (positive = better)
    """
    if baseline == 0:
        return 0.0
    return (current - baseline) / baseline * 100


def load_json_file(filepath: str) -> Optional[Dict[str, Any]]:
    """
    Safely load JSON file.
    
    Args:
        filepath: Path to JSON file
        
    Returns:
        Loaded data or None if error
    """
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.getLogger(__name__).error(f"Error loading JSON file {filepath}: {e}")
        return None


def save_json_file(data: Dict[str, Any], filepath: str) -> bool:
    """
    Safely save data to JSON file.
    
    Args:
        data: Data to save
        filepath: Output file path
        
    Returns:
        True if successful, False otherwise
    """
    try:
        ensure_dir(os.path.dirname(filepath))
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        logging.getLogger(__name__).error(f"Error saving JSON file {filepath}: {e}")
        return False


def get_timestamp_string() -> str:
    """
    Get current timestamp as string.
    
    Returns:
        Timestamp string in format YYYYMMDD_HHMMSS
    """
    return time.strftime("%Y%m%d_%H%M%S")


def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """
    Flatten nested dictionary.
    
    Args:
        d: Dictionary to flatten
        parent_key: Parent key prefix
        sep: Separator for keys
        
    Returns:
        Flattened dictionary
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def validate_positive_number(value: Any, name: str) -> float:
    """
    Validate that a value is a positive number.
    
    Args:
        value: Value to validate
        name: Name of the value (for error messages)
        
    Returns:
        Validated number
        
    Raises:
        ValueError: If value is not a positive number
    """
    try:
        num = float(value)
        if num <= 0:
            raise ValueError(f"{name} must be positive, got {num}")
        return num
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid {name}: {e}")


def extract_nested_value(data: Dict[str, Any], path: str, default: Any = None) -> Any:
    """
    Extract value from nested dictionary using dot notation.
    
    Args:
        data: Dictionary to search
        path: Dot-separated path (e.g., 'a.b.c')
        default: Default value if path not found
        
    Returns:
        Found value or default
    """
    keys = path.split('.')
    current = data
    
    try:
        for key in keys:
            current = current[key]
        return current
    except (KeyError, TypeError):
        return default


def merge_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively merge two dictionaries.
    
    Args:
        dict1: First dictionary
        dict2: Second dictionary (takes precedence)
        
    Returns:
        Merged dictionary
    """
    result = dict1.copy()
    
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = value
    
    return result


class ProgressReporter:
    """Simple progress reporter for long-running operations."""
    
    def __init__(self, total: int, name: str = "Progress", 
                 report_interval: int = 1000):
        self.total = total
        self.name = name
        self.report_interval = report_interval
        self.current = 0
        self.start_time = time.time()
        self.logger = logging.getLogger(__name__)
    
    def update(self, increment: int = 1) -> None:
        """Update progress counter."""
        self.current += increment
        
        if self.current % self.report_interval == 0 or self.current == self.total:
            self._report()
    
    def _report(self) -> None:
        """Report current progress."""
        elapsed = time.time() - self.start_time
        percent = (self.current / self.total) * 100 if self.total > 0 else 0
        rate = self.current / elapsed if elapsed > 0 else 0
        
        if self.current < self.total and rate > 0:
            eta = (self.total - self.current) / rate
            eta_str = f", ETA: {format_duration(eta)}"
        else:
            eta_str = ""
        
        self.logger.info(
            f"{self.name}: {self.current:,}/{self.total:,} "
            f"({percent:.1f}%, {rate:.1f} items/sec{eta_str})"
        )


class BenchmarkContext:
    """Context manager for benchmarking operations."""
    
    def __init__(self, name: str, logger: Optional[logging.Logger] = None):
        self.name = name
        self.logger = logger or logging.getLogger(__name__)
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        self.logger.info(f"Starting {self.name}...")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.perf_counter()
        duration = self.end_time - self.start_time
        
        if exc_type is None:
            self.logger.info(f"Completed {self.name} in {format_duration(duration)}")
        else:
            self.logger.error(f"Failed {self.name} after {format_duration(duration)}: {exc_val}")
    
    def get_duration(self) -> float:
        """Get duration of the operation."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0


def create_summary_table(data: List[Dict[str, Any]], 
                        columns: List[str],
                        title: str = "Summary") -> str:
    """
    Create a simple text table from data.
    
    Args:
        data: List of dictionaries containing data
        columns: List of column names to include
        title: Table title
        
    Returns:
        Formatted table string
    """
    if not data:
        return f"{title}: No data available"
    
    # Calculate column widths
    widths = {col: max(len(col), max(len(str(row.get(col, ''))) for row in data)) 
              for col in columns}
    
    # Create table
    lines = [title, "=" * len(title), ""]
    
    # Header
    header = " | ".join(col.ljust(widths[col]) for col in columns)
    lines.append(header)
    lines.append("-" * len(header))
    
    # Data rows
    for row in data:
        row_str = " | ".join(str(row.get(col, '')).ljust(widths[col]) for col in columns)
        lines.append(row_str)
    
    return "\n".join(lines)


# Version and metadata
__version__ = "1.0.0"
__all__ = [
    'timer', 'ensure_dir', 'format_number', 'format_duration', 'format_bytes',
    'safe_divide', 'calculate_improvement', 'load_json_file', 'save_json_file',
    'get_timestamp_string', 'flatten_dict', 'validate_positive_number',
    'extract_nested_value', 'merge_dicts', 'ProgressReporter', 'BenchmarkContext',
    'create_summary_table'
]
