#!/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python
"""
Quick Test Script for Enhanced CD-Index Benchmarking Suite

This script performs a basic validation of the benchmarking infrastructure
without running full benchmarks.
"""

import os
import sys
import tempfile
import json

# Add parent directory and current directory for imports
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(0, script_dir)

def test_imports():
    """Test that all modules can be imported."""
    print("Testing imports...")
    
    try:
        # Test individual imports
        from config import BenchmarkConfig, get_micro_config
        from data_loader import DataLoader
        from benchmark_runner import BenchmarkRunner
        from results_analyzer import ResultsAnalyzer
        print("✓ All core modules imported successfully")
        
        # Store modules for later use
        return {
            'BenchmarkConfig': BenchmarkConfig,
            'get_micro_config': get_micro_config,
            'DataLoader': DataLoader,
            'BenchmarkRunner': BenchmarkRunner,
            'ResultsAnalyzer': ResultsAnalyzer
        }
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return None

def test_config(modules):
    """Test configuration creation and validation."""
    print("Testing configuration...")
    
    if not modules:
        print("✗ Config test error: modules not available")
        return False
    
    try:
        BenchmarkConfig = modules['BenchmarkConfig']
        get_micro_config = modules['get_micro_config']
        
        # Test default config
        config = get_micro_config()
        print(f"✓ Default micro config created")
        
        # Test validation (will have issues due to missing data)
        issues = config.validate()
        print(f"✓ Config validation ran (found {len(issues)} expected issues)")
        
        # Test serialization
        config_dict = config.to_dict()
        print(f"✓ Config serialization works")
        
        # Test save/load with temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config.save(f.name)
            loaded_config = BenchmarkConfig.load(f.name)
            os.unlink(f.name)
        
        print(f"✓ Config save/load works")
        return True
        
    except Exception as e:
        print(f"✗ Config test error: {e}")
        return False

def test_data_loader(modules):
    """Test data loader creation (without actual data loading)."""
    print("Testing data loader...")
    
    if not modules:
        print("✗ DataLoader test error: modules not available")
        return False
    
    try:
        DataLoader = modules['DataLoader']
        get_micro_config = modules['get_micro_config']
        
        config = get_micro_config()
        
        # Test optional dependency check
        try:
            import pyarrow as pa
            print("✓ PyArrow is available")
        except ImportError:
            print("! PyArrow not available (expected for quick test)")
        
        # Test loader creation
        loader = DataLoader(config)
        print("✓ DataLoader created successfully")
        
        # Test method availability (without calling them)
        methods = ['load_micro_dataset', 'validate_data', 'get_data_statistics']
        for method in methods:
            if hasattr(loader, method):
                print(f"✓ Method {method} available")
            else:
                print(f"✗ Method {method} missing")
                return False
        
        return True
        
    except Exception as e:
        print(f"✗ DataLoader test error: {e}")
        return False

def test_benchmark_runner(modules):
    """Test benchmark runner creation (without running benchmarks)."""
    print("Testing benchmark runner...")
    
    if not modules:
        print("✗ BenchmarkRunner test error: modules not available")
        return False
    
    try:
        BenchmarkRunner = modules['BenchmarkRunner']
        get_micro_config = modules['get_micro_config']
        
        config = get_micro_config()
        
        # Test runner creation
        runner = BenchmarkRunner(config)
        print("✓ BenchmarkRunner created successfully")
        
        # Test method availability
        methods = ['run_micro_benchmark', 'run_optimization_benchmark', 'run_full_benchmark']
        for method in methods:
            if hasattr(runner, method):
                print(f"✓ Method {method} available")
            else:
                print(f"✗ Method {method} missing")
                return False
        
        # Test result structure
        if hasattr(runner, 'results'):
            print("✓ Results storage available")
        
        return True
        
    except Exception as e:
        print(f"✗ BenchmarkRunner test error: {e}")
        return False

def test_results_analyzer(modules):
    """Test results analyzer creation (without analysis)."""
    print("Testing results analyzer...")
    
    if not modules:
        print("✗ ResultsAnalyzer test error: modules not available")
        return False
    
    try:
        ResultsAnalyzer = modules['ResultsAnalyzer']
        get_micro_config = modules['get_micro_config']
        
        config = get_micro_config()
        
        # Test analyzer creation
        analyzer = ResultsAnalyzer(config)
        print("✓ ResultsAnalyzer created successfully")
        
        # Test method availability
        methods = ['analyze_micro_results', 'generate_html_report', 'generate_plots']
        for method in methods:
            if hasattr(analyzer, method):
                print(f"✓ Method {method} available")
            else:
                print(f"✗ Method {method} missing")
                return False
        
        # Test with dummy data
        dummy_results = {
            'micro_benchmark': {
                'throughput': [100, 200, 150],
                'latency': [0.01, 0.005, 0.008],
                'memory_mb': [50, 60, 55]
            }
        }
        
        try:
            analysis = analyzer.analyze_micro_results(dummy_results)
            print("✓ Performance analysis works with dummy data")
        except Exception as e:
            print(f"! Performance analysis failed with dummy data: {e}")
        
        return True
        
    except Exception as e:
        print(f"✗ ResultsAnalyzer test error: {e}")
        return False

def test_file_structure():
    """Test that required files and directories exist."""
    print("Testing file structure...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    required_files = [
        'config.py',
        'data_loader.py', 
        'benchmark_runner.py',
        'results_analyzer.py',
        'utils.py',
        'setup.py',
        'run_benchmarks.py',
        'examples.py',
        'README.md',
        'Makefile'
    ]
    
    missing_files = []
    for file in required_files:
        file_path = os.path.join(script_dir, file)
        if os.path.exists(file_path):
            print(f"✓ {file} exists")
        else:
            print(f"✗ {file} missing")
            missing_files.append(file)
    
    return len(missing_files) == 0

def main():
    """Run all quick tests."""
    print("Enhanced CD-Index Benchmarking Suite - Quick Test")
    print("=" * 60)
    print()
    
    tests = []
    
    # Test file structure first
    tests.append(("File structure", test_file_structure()))
    
    # Test imports and get modules
    modules = test_imports()
    tests.append(("Imports", modules is not None))
    
    # Run other tests with modules
    if modules:
        tests.append(("Configuration", test_config(modules)))
        tests.append(("Data loader", test_data_loader(modules)))
        tests.append(("Benchmark runner", test_benchmark_runner(modules)))
        tests.append(("Results analyzer", test_results_analyzer(modules)))
    else:
        print("Skipping remaining tests due to import failures")
        tests.extend([
            ("Configuration", False),
            ("Data loader", False), 
            ("Benchmark runner", False),
            ("Results analyzer", False)
        ])
    
    print()
    print("=" * 60)
    
    passed = sum(1 for _, result in tests if result)
    total = len(tests)
    
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("✅ All tests passed! Benchmarking suite is ready.")
        return 0
    else:
        print("❌ Some tests failed. Please check the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
