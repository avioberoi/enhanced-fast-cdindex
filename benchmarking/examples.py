#!/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python
"""
Example Script: Enhanced CD-Index Benchmarking Suite Usage

This script demonstrates various ways to use the benchmarking suite programmatically.
"""

import os
import sys
import time
from pathlib import Path

# Add benchmarking suite to path
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(script_dir)

# Example 1: Simple micro benchmark
def example_simple_micro():
    """Run a simple micro benchmark with default configuration."""
    print("Example 1: Simple Micro Benchmark")
    print("-" * 40)
    
    try:
        from config import get_micro_config
        from benchmark_runner import BenchmarkRunner
        
        # Get default micro configuration
        config = get_micro_config()
        
        # Override some settings for this example
        config.data.micro_vertices = 10000  # Smaller for demo
        config.data.micro_benchmark_papers = 500
        config.performance.batch_sizes = [100, 500]  # Fewer batch sizes
        config.performance.time_windows = [5]  # Single time window
        
        # Create and run benchmark
        runner = BenchmarkRunner(config)
        results = runner.run_micro_benchmark()
        
        if results and 'error' not in results:
            print("✅ Micro benchmark completed successfully!")
            
            # Print some key results
            metadata = results.get('metadata', {})
            print(f"   Vertices: {metadata.get('vertex_count', 'N/A'):,}")
            print(f"   Edges: {metadata.get('edge_count', 'N/A'):,}")
            print(f"   Benchmark papers: {metadata.get('benchmark_papers', 'N/A'):,}")
            
            # Save results
            runner.save_results(results, "example_micro_results.json")
            print("   Results saved to example_micro_results.json")
            
        else:
            error_msg = results.get('error', 'Unknown error') if results else 'No results returned'
            print(f"❌ Micro benchmark failed: {error_msg}")
            
    except Exception as e:
        print(f"❌ Error running micro benchmark: {e}")

# Example 2: Custom configuration
def example_custom_config():
    """Demonstrate creating and using custom configuration."""
    print("\nExample 2: Custom Configuration")
    print("-" * 40)
    
    try:
        from config import BenchmarkConfig, DataConfig, PerformanceConfig, OutputConfig
        
        # Create custom configuration
        custom_config = BenchmarkConfig(
            data=DataConfig(
                micro_vertices=20000,
                micro_benchmark_papers=1000,
                cache_dir="./example_cache"
            ),
            performance=PerformanceConfig(
                time_windows=[3, 5],
                batch_sizes=[200, 1000],
                max_workers=2
            ),
            output=OutputConfig(
                results_dir="./example_results",
                log_level="DEBUG",
                generate_html_report=True
            )
        )
        
        # Validate configuration
        issues = custom_config.validate()
        if issues:
            print("⚠️  Configuration issues found:")
            for issue in issues[:3]:  # Show first 3 issues
                print(f"   - {issue}")
        else:
            print("✅ Custom configuration is valid")
        
        # Save configuration for later use
        custom_config.save("example_config.json")
        print("   Configuration saved to example_config.json")
        
        # Load it back to verify
        loaded_config = BenchmarkConfig.load("example_config.json")
        print("   Configuration loaded successfully")
        
    except Exception as e:
        print(f"❌ Error with custom configuration: {e}")

# Example 3: Analysis and reporting
def example_analysis():
    """Demonstrate results analysis and reporting."""
    print("\nExample 3: Results Analysis")
    print("-" * 40)
    
    try:
        from config import get_micro_config
        from results_analyzer import ResultsAnalyzer
        
        config = get_micro_config()
        config.output.results_dir = "./example_results"
        config.output.plots_dir = "./example_plots"
        
        analyzer = ResultsAnalyzer(config)
        
        # Create dummy results for analysis demo
        dummy_results = {
            "metadata": {
                "vertex_count": 50000,
                "edge_count": 150000,
                "benchmark_papers": 1000,
                "timestamp": time.time()
            },
            "benchmarks": {
                "time_window_5": {
                    "single": {
                        "size_100": {
                            "duration": 0.5,
                            "throughput": 200.0,
                            "memory_mb": 15.0,
                            "cpu_percent": 45.0
                        },
                        "size_500": {
                            "duration": 2.1,
                            "throughput": 238.0,
                            "memory_mb": 18.0,
                            "cpu_percent": 52.0
                        }
                    },
                    "batch": {
                        "size_100": {
                            "duration": 0.08,
                            "throughput": 1250.0,
                            "batch_size": 100,
                            "memory_mb": 12.0,
                            "cpu_percent": 35.0
                        },
                        "size_1000": {
                            "duration": 0.45,
                            "throughput": 2222.0,
                            "batch_size": 1000,
                            "memory_mb": 25.0,
                            "cpu_percent": 60.0
                        }
                    },
                    "filtered": {
                        "filter_1": {
                            "duration": 0.12,
                            "throughput": 833.0,
                            "filter_config": {},
                            "test_size": 100
                        },
                        "filter_2": {
                            "duration": 0.18,
                            "throughput": 556.0,
                            "filter_config": {"year": [2000, 2001, 2002]},
                            "test_size": 100
                        }
                    }
                }
            }
        }
        
        # Analyze the results
        analysis = analyzer.analyze_micro_results(dummy_results)
        
        if analysis and 'error' not in analysis:
            print("✅ Results analysis completed!")
            
            # Show some analysis results
            summary = analysis.get('summary', {})
            overall = summary.get('overall_performance', {})
            if overall:
                print(f"   Max throughput: {overall.get('max_throughput', 0):.1f} papers/sec")
                print(f"   Avg throughput: {overall.get('avg_throughput', 0):.1f} papers/sec")
            
            recommendations = analysis.get('recommendations', [])
            if recommendations:
                print("   Recommendations:")
                for rec in recommendations[:2]:  # Show first 2
                    print(f"     - {rec}")
            
            # Generate HTML report
            report_file = analyzer.generate_html_report(dummy_results, analysis, "example_report.html")
            print(f"   HTML report generated: {os.path.basename(report_file)}")
            
        else:
            error_msg = analysis.get('error', 'Unknown error') if analysis else 'No analysis returned'
            print(f"❌ Analysis failed: {error_msg}")
            
    except Exception as e:
        print(f"❌ Error with analysis: {e}")

# Example 4: Environment validation
def example_validation():
    """Demonstrate environment and setup validation."""
    print("\nExample 4: Environment Validation")
    print("-" * 40)
    
    try:
        # Check Python version
        import sys
        version = sys.version_info
        print(f"✅ Python {version.major}.{version.minor}.{version.micro}")
        
        # Check required modules
        required_modules = [
            ('pyarrow', 'Data handling'),
            ('pandas', 'Data analysis'),
        ]
        
        for module_name, description in required_modules:
            try:
                module = __import__(module_name)
                version = getattr(module, '__version__', 'unknown')
                print(f"✅ {module_name} {version} - {description}")
            except ImportError:
                print(f"❌ {module_name} - {description} (missing)")
        
        # Check optional modules
        optional_modules = [
            ('matplotlib', 'Plotting'),
            ('psutil', 'System monitoring'),
        ]
        
        for module_name, description in optional_modules:
            try:
                module = __import__(module_name)
                version = getattr(module, '__version__', 'unknown')
                print(f"✅ {module_name} {version} - {description}")
            except ImportError:
                print(f"⚠️  {module_name} - {description} (optional, missing)")
        
        # Try to import benchmarking suite modules
        try:
            from config import BenchmarkConfig
            print("✅ Benchmarking suite configuration module")
        except ImportError as e:
            print(f"❌ Benchmarking suite config: {e}")
        
        try:
            from data_loader import DataLoader
            print("✅ Benchmarking suite data loader module")
        except ImportError as e:
            print(f"❌ Benchmarking suite data loader: {e}")
        
        try:
            from benchmark_runner import BenchmarkRunner
            print("✅ Benchmarking suite runner module")
        except ImportError as e:
            print(f"❌ Benchmarking suite runner: {e}")
        
    except Exception as e:
        print(f"❌ Error during validation: {e}")

def main():
    """Run all examples."""
    print("Enhanced CD-Index Benchmarking Suite Examples")
    print("=" * 60)
    
    # Set up example directory
    example_dir = "benchmarking_examples"
    os.makedirs(example_dir, exist_ok=True)
    os.chdir(example_dir)
    
    print(f"Working in directory: {os.getcwd()}")
    
    # Run examples
    examples = [
        example_validation,      # Start with validation
        example_custom_config,   # Show configuration
        example_analysis,        # Show analysis (with dummy data)
        # example_simple_micro,  # Skip actual benchmark for demo
    ]
    
    for example in examples:
        try:
            example()
        except Exception as e:
            print(f"❌ Example failed: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("Examples completed!")
    print("\nGenerated files:")
    for file in os.listdir("."):
        if file.endswith(('.json', '.html')):
            size = os.path.getsize(file)
            print(f"  {file} ({size:,} bytes)")
    
    print("\nTo run actual benchmarks:")
    print("  cd ..")
    print("  python run_benchmarks.py --type micro")

if __name__ == '__main__':
    main()
