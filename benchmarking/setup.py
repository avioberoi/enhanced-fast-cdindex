#!/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python
"""
Setup Script for Enhanced CD-Index Benchmarking Suite

This script helps set up the benchmarking environment including:
- Dependency checking and installation guidance
- Directory structure creation
- Configuration file generation
- Environment validation
"""

import os
import sys
import subprocess
import tempfile
import argparse
from pathlib import Path

def check_python_version():
    """Check if Python version is supported."""
    print("Checking Python version...")
    
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 7):
        print(f"❌ Python {version.major}.{version.minor} is not supported")
        print("   Please upgrade to Python 3.7 or later")
        return False
    
    print(f"✅ Python {version.major}.{version.minor}.{version.micro} is supported")
    return True

def check_dependencies():
    """Check for required and optional dependencies."""
    print("\nChecking dependencies...")
    
    required_deps = [
        ('pyarrow', 'PyArrow for data handling'),
        ('pandas', 'Pandas for data analysis')
    ]
    
    optional_deps = [
        ('matplotlib', 'Matplotlib for plotting'),
        ('psutil', 'psutil for system monitoring'),
        ('numpy', 'NumPy for numerical operations')
    ]
    
    missing_required = []
    missing_optional = []
    
    # Check required dependencies
    for module, description in required_deps:
        try:
            __import__(module)
            print(f"✅ {module} - {description}")
        except ImportError:
            print(f"❌ {module} - {description} (REQUIRED)")
            missing_required.append(module)
    
    # Check optional dependencies
    for module, description in optional_deps:
        try:
            __import__(module)
            print(f"✅ {module} - {description}")
        except ImportError:
            print(f"⚠️  {module} - {description} (optional)")
            missing_optional.append(module)
    
    return missing_required, missing_optional

def install_dependencies(required_deps, optional_deps, install_optional=False):
    """Provide installation guidance for missing dependencies."""
    if not required_deps and not (optional_deps and install_optional):
        return True
    
    print("\nDependency Installation:")
    print("=" * 40)
    
    to_install = required_deps[:]
    if install_optional:
        to_install.extend(optional_deps)
    
    if to_install:
        pip_command = f"pip install {' '.join(to_install)}"
        conda_command = f"conda install {' '.join(to_install)}"
        
        print("Install using pip:")
        print(f"  {pip_command}")
        print("\nOr using conda:")
        print(f"  {conda_command}")
        
        if required_deps:
            print(f"\n⚠️  Required dependencies missing: {', '.join(required_deps)}")
            print("   Benchmarking will not work without these packages.")
            return False
    
    if optional_deps and not install_optional:
        print(f"\nOptional dependencies available: {', '.join(optional_deps)}")
        print("Use --install-optional to include these in setup")
    
    return True

def check_enhanced_cdindex():
    """Check if Enhanced CD-Index is available."""
    print("\nChecking Enhanced CD-Index...")
    
    try:
        # Try to import the enhanced CD-Index module
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        from fast_cdindex.cdindex_enhanced import EnhancedGraph
        print("✅ Enhanced CD-Index module is available")
        
        # Test basic functionality
        graph = EnhancedGraph()
        print("✅ Enhanced CD-Index can be instantiated")
        return True
        
    except ImportError as e:
        print(f"❌ Enhanced CD-Index not available: {e}")
        print("   Make sure the Enhanced CD-Index is built and installed")
        print("   Check that the enhanced-fast-cdindex directory is in your Python path")
        return False
    except Exception as e:
        print(f"⚠️  Enhanced CD-Index import succeeded but instantiation failed: {e}")
        print("   This might indicate a build or configuration issue")
        return False

def create_directory_structure(base_dir):
    """Create the benchmarking directory structure."""
    print(f"\nCreating directory structure in {base_dir}...")
    
    dirs_to_create = [
        'benchmark_results',
        'benchmark_plots', 
        'benchmark_logs',
        'data_cache',
        'configs'
    ]
    
    created_dirs = []
    for dir_name in dirs_to_create:
        dir_path = os.path.join(base_dir, dir_name)
        try:
            os.makedirs(dir_path, exist_ok=True)
            print(f"✅ Created {dir_name}/")
            created_dirs.append(dir_path)
        except Exception as e:
            print(f"❌ Failed to create {dir_name}/: {e}")
            return []
    
    return created_dirs

def create_sample_configs(config_dir):
    """Create sample configuration files."""
    print(f"\nCreating sample configuration files in {config_dir}...")
    
    # Basic micro config
    micro_config = {
        "data": {
            "micro_vertices": 50000,
            "micro_benchmark_papers": 1000,
            "wos_data_dir": "../WoS_data",
            "cache_dir": "./data_cache"
        },
        "performance": {
            "time_windows": [5],
            "batch_sizes": [100, 500, 1000],
            "single_sample_sizes": [50, 100],
            "max_workers": 4
        },
        "output": {
            "results_dir": "./benchmark_results",
            "plots_dir": "./benchmark_plots",
            "logs_dir": "./benchmark_logs",
            "log_level": "INFO",
            "generate_html_report": True
        }
    }
    
    # Full config  
    full_config = {
        "data": {
            "micro_vertices": 1000000,
            "micro_benchmark_papers": 10000,
            "wos_data_dir": "../WoS_data",
            "cache_dir": "./data_cache"
        },
        "performance": {
            "time_windows": [3, 5, 10],
            "batch_sizes": [100, 1000, 5000, 10000],
            "single_sample_sizes": [100, 500, 1000],
            "max_workers": 8
        },
        "output": {
            "results_dir": "./benchmark_results",
            "plots_dir": "./benchmark_plots", 
            "logs_dir": "./benchmark_logs",
            "log_level": "INFO",
            "generate_html_report": True
        }
    }
    
    configs = [
        ('micro_config.json', micro_config),
        ('full_config.json', full_config)
    ]
    
    created_configs = []
    for filename, config_data in configs:
        config_path = os.path.join(config_dir, filename)
        try:
            import json
            with open(config_path, 'w') as f:
                json.dump(config_data, f, indent=2)
            print(f"✅ Created {filename}")
            created_configs.append(config_path)
        except Exception as e:
            print(f"❌ Failed to create {filename}: {e}")
    
    return created_configs

def run_quick_test():
    """Run the quick test to validate setup."""
    print("\nRunning quick validation test...")
    
    try:
        # Import and run the quick test
        from quick_test import main as quick_test_main
        result = quick_test_main()
        
        if result == 0:
            print("✅ Quick test passed - setup is working correctly!")
            return True
        else:
            print("❌ Quick test failed - there are setup issues")
            return False
            
    except Exception as e:
        print(f"❌ Could not run quick test: {e}")
        return False

def check_data_availability(wos_data_dir, cache_dir=None):
    """Check if data is available either as raw files or cached data."""
    print(f"\nChecking data availability...")
    
    # Check for cached micro benchmark data first
    if cache_dir and os.path.exists(cache_dir):
        print(f"Checking cache directory: {cache_dir}")
        cache_files = os.listdir(cache_dir)
        micro_files = [f for f in cache_files if 'micro_' in f and f.endswith('.parquet')]
        
        if micro_files:
            print(f"✅ Found cached micro benchmark data:")
            for f in micro_files:
                file_path = os.path.join(cache_dir, f)
                file_size = os.path.getsize(file_path)
                print(f"   {f} ({file_size:,} bytes)")
            print("   Micro benchmarks can run using cached data")
            
            # Check if raw WoS files are also available
            print(f"\nChecking raw WoS data files in {wos_data_dir}...")
            if check_raw_wos_files(wos_data_dir):
                print("✅ Both cached and raw data available - full benchmarks possible")
                return "full"
            else:
                print("⚠️  Raw WoS files not found - only micro benchmarks possible")
                return "micro_only"
        else:
            print(f"⚠️  Cache directory exists but no micro benchmark data found")
    
    # If no cache or cache doesn't have micro data, check raw files
    print(f"Checking raw WoS data files in {wos_data_dir}...")
    if check_raw_wos_files(wos_data_dir):
        print("✅ Raw WoS data available - benchmarks can generate cache")
        return "raw_only"
    else:
        print("❌ No data source available (neither cache nor raw files)")
        return "none"

def check_raw_wos_files(wos_data_dir):
    """Check if raw WoS data files are available."""
    required_files = [
        'paper_years_all.tsv',
        'edges_all.tsv'
    ]
    
    if not os.path.exists(wos_data_dir):
        print(f"   WoS data directory not found: {wos_data_dir}")
        return False
    
    missing_files = []
    for filename in required_files:
        filepath = os.path.join(wos_data_dir, filename)
        if os.path.exists(filepath):
            file_size = os.path.getsize(filepath)
            print(f"   ✅ {filename} ({file_size:,} bytes)")
        else:
            print(f"   ❌ {filename} (missing)")
            missing_files.append(filename)
    
    return len(missing_files) == 0

def print_summary(success, next_steps):
    """Print setup summary and next steps."""
    print("\n" + "=" * 60)
    print("SETUP SUMMARY")
    print("=" * 60)
    
    if success:
        print("🎉 Setup completed successfully!")
        print("\nThe benchmarking suite is ready to use.")
    else:
        print("⚠️  Setup completed with issues.")
        print("\nSome components may not work correctly.")
    
    if next_steps:
        print("\nNext steps:")
        for i, step in enumerate(next_steps, 1):
            print(f"  {i}. {step}")
    
    print("\nUsage examples:")
    print("  python run_benchmarks.py --type micro")
    print("  python run_benchmarks.py --config configs/micro_config.json")
    print("  python quick_test.py")

def main():
    """Main setup function."""
    parser = argparse.ArgumentParser(description='Setup Enhanced CD-Index Benchmarking Suite')
    parser.add_argument('--base-dir', default='.', help='Base directory for setup')
    parser.add_argument('--wos-data-dir', help='Path to WoS data directory')
    parser.add_argument('--data-cache', help='Path to existing data cache directory')
    parser.add_argument('--install-optional', action='store_true', 
                       help='Include optional dependencies in installation guidance')
    parser.add_argument('--skip-test', action='store_true',
                       help='Skip the quick validation test')
    
    args = parser.parse_args()
    
    print("Enhanced CD-Index Benchmarking Suite Setup")
    print("=" * 50)
    
    success = True
    next_steps = []
    
    # Check Python version
    if not check_python_version():
        success = False
        next_steps.append("Upgrade to Python 3.7 or later")
    
    # Check dependencies
    missing_required, missing_optional = check_dependencies()
    if not install_dependencies(missing_required, missing_optional, args.install_optional):
        success = False
        next_steps.append("Install required dependencies")
    
    # Check Enhanced CD-Index
    if not check_enhanced_cdindex():
        success = False
        next_steps.append("Build and install Enhanced CD-Index")
    
    # Create directory structure
    created_dirs = create_directory_structure(args.base_dir)
    if not created_dirs:
        success = False
        next_steps.append("Manually create required directories")
    
    # Create sample configs
    config_dir = os.path.join(args.base_dir, 'configs')
    created_configs = create_sample_configs(config_dir)
    if not created_configs:
        success = False
        next_steps.append("Manually create configuration files")
    
    # Check data availability
    wos_data_dir = args.wos_data_dir or os.path.join(os.path.dirname(args.base_dir), 'WoS_data')
    cache_dir = args.data_cache or os.path.join(os.path.dirname(args.base_dir), 'benchmarking', 'data_cache')
    
    data_status = check_data_availability(wos_data_dir, cache_dir)
    if data_status == "none":
        success = False
        next_steps.append("Ensure either WoS data files or cached data are available")
    elif data_status == "micro_only":
        next_steps.append("For full benchmarks, ensure raw WoS data files are available")
    elif data_status == "raw_only":
        next_steps.append("Micro benchmark cache will be generated on first run")
    
    # Run quick test
    if not args.skip_test and success:
        if not run_quick_test():
            success = False
            next_steps.append("Debug setup issues using --verbose flag")
    
    print_summary(success, next_steps)
    
    return 0 if success else 1

if __name__ == '__main__':
    sys.exit(main())
