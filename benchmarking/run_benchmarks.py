#!/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python
"""
Main Benchmark Execution Script

Command-line interface for running various types of benchmarks on the
Enhanced CD-Index implementation.

Usage:
    python run_benchmarks.py [options]

Examples:
    # Run micro benchmarks with default config
    python run_benchmarks.py --type micro
    
    # Run optimization benchmarks with custom config
    python run_benchmarks.py --type optimization --config custom_config.json
    
    # Run all benchmarks and generate report
    python run_benchmarks.py --type all --report
"""

import os
import sys
import argparse
import logging
import time
from pathlib import Path

# Add parent directory for imports
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(script_dir))

from config import BenchmarkConfig, get_micro_config, get_full_config, get_optimization_config
from benchmark_runner import BenchmarkRunner
from results_analyzer import ResultsAnalyzer


def setup_logging(config: BenchmarkConfig) -> None:
    """Set up logging configuration."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    handlers = []
    
    # Console handler
    if config.output.log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(console_handler)
    
    # File handler
    if config.output.log_to_file:
        # Ensure logs directory exists
        os.makedirs(config.output.logs_dir, exist_ok=True)
        
        log_file = os.path.join(config.output.logs_dir, f"benchmark_{int(time.time())}.log")
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(file_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, config.output.log_level.upper()),
        handlers=handlers,
        format=log_format
    )
    
    # Suppress noisy third-party loggers
    logging.getLogger('matplotlib').setLevel(logging.WARNING)
    logging.getLogger('PIL').setLevel(logging.WARNING)


def validate_environment() -> list:
    """Validate the environment and return list of issues."""
    issues = []
    
    # Check for required modules
    required_modules = [
        ('fast_cdindex.cdindex_enhanced', 'Enhanced CD-Index implementation'),
        ('pyarrow', 'PyArrow for data handling'),
    ]
    
    for module_name, description in required_modules:
        try:
            __import__(module_name)
        except ImportError:
            issues.append(f"Missing required module: {module_name} ({description})")
    
    # Check for optional modules
    optional_modules = [
        ('matplotlib', 'Plotting functionality'),
        ('pandas', 'Data analysis'),
        ('psutil', 'System monitoring'),
    ]
    
    missing_optional = []
    for module_name, description in optional_modules:
        try:
            __import__(module_name)
        except ImportError:
            missing_optional.append(f"{module_name} ({description})")
    
    if missing_optional:
        issues.append(f"Missing optional modules (reduced functionality): {', '.join(missing_optional)}")
    
    return issues


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Enhanced CD-Index Benchmark Suite',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --type micro                           # Run micro benchmarks
  %(prog)s --type optimization                    # Run optimization tests
  %(prog)s --type all --report                    # Run all benchmarks with report
  %(prog)s --config my_config.json --type micro  # Use custom configuration
  %(prog)s --list-configs                         # Show available configs
        """
    )
    
    parser.add_argument(
        '--type', 
        choices=['micro', 'optimization', 'full', 'all'],
        default='micro',
        help='Type of benchmark to run (default: micro)'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        help='Path to custom configuration file (JSON)'
    )
    
    parser.add_argument(
        '--preset',
        choices=['micro', 'full', 'optimization'],
        help='Use a preset configuration'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Override output directory'
    )
    
    parser.add_argument(
        '--report',
        action='store_true',
        help='Generate analysis report after benchmarks'
    )
    
    parser.add_argument(
        '--plots',
        action='store_true',
        help='Generate visualization plots'
    )
    
    parser.add_argument(
        '--compare',
        type=str,
        help='Compare results with previous benchmark file'
    )
    
    parser.add_argument(
        '--list-configs',
        action='store_true',
        help='List available preset configurations'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate environment and configuration'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--data-cache',
        type=str,
        help='Path to data cache directory'
    )
    
    parser.add_argument(
        '--size-small',
        action='store_true',
        help='Use small dataset size (quick test)'
    )
    
    parser.add_argument(
        '--size-medium',
        action='store_true',
        help='Use medium dataset size (default)'
    )
    
    parser.add_argument(
        '--size-large',
        action='store_true',
        help='Use large dataset size'
    )
    
    args = parser.parse_args()
    
    # Handle list-configs
    if args.list_configs:
        print("Available preset configurations:")
        print("  micro        - Fast micro-benchmarks for development")
        print("  full         - Full-scale benchmarks")
        print("  optimization - Optimization-focused testing")
        return 0
    
    # Load configuration
    try:
        if args.config:
            print(f"Loading configuration from: {args.config}")
            config = BenchmarkConfig.load(args.config)
        elif args.preset:
            print(f"Using preset configuration: {args.preset}")
            if args.preset == 'micro':
                config = get_micro_config()
            elif args.preset == 'full':
                config = get_full_config()
            elif args.preset == 'optimization':
                config = get_optimization_config()
        else:
            print("Using default micro configuration")
            config = get_micro_config()
    
    except Exception as e:
        print(f"Error loading configuration: {e}")
        return 1
    
    # Override data cache directory if specified
    if args.data_cache:
        config.data.cache_dir = args.data_cache
        print(f"Using data cache: {args.data_cache}")
    
    # Handle dataset size options
    if args.size_small:
        config.data.micro_vertices = 100_000
        config.data.micro_benchmark_papers = 1_000
        print("Using small dataset size")
    elif args.size_large:
        config.data.micro_vertices = 5_000_000
        config.data.micro_benchmark_papers = 50_000
        print("Using large dataset size")
    else:
        print("Using medium dataset size")
    
    # Override output directory if specified
    if args.output_dir:
        config.output.results_dir = os.path.join(args.output_dir, 'results')
        config.output.plots_dir = os.path.join(args.output_dir, 'plots')
        config.output.logs_dir = os.path.join(args.output_dir, 'logs')
    
    # Override logging level if verbose
    if args.verbose:
        config.output.log_level = 'DEBUG'
    
    # Ensure all output directories exist before setting up logging
    for directory in [config.output.results_dir, config.output.plots_dir, config.output.logs_dir]:
        os.makedirs(directory, exist_ok=True)
    
    # Set up logging
    setup_logging(config)
    logger = logging.getLogger(__name__)
    
    logger.info("Enhanced CD-Index Benchmark Suite")
    logger.info("=" * 60)
    
    # Validate environment
    logger.info("Validating environment...")
    env_issues = validate_environment()
    if env_issues:
        for issue in env_issues:
            logger.warning(issue)
    
    # Validate configuration
    logger.info("Validating configuration...")
    config_issues = config.validate()
    if config_issues:
        for issue in config_issues:
            logger.error(issue)
        
        if any("not found" in issue for issue in config_issues):
            logger.error("Critical configuration issues found. Exiting.")
            return 1
    
    if args.validate_only:
        if env_issues or config_issues:
            logger.info("Validation completed with issues (see above)")
            return 1
        else:
            logger.info("Validation completed successfully")
            return 0
    
    # Initialize benchmark runner
    logger.info("Initializing benchmark runner...")
    runner = BenchmarkRunner(config)
    
    # Run benchmarks
    results = None
    start_time = time.time()
    
    try:
        if args.type == 'micro':
            logger.info("Running micro benchmarks...")
            results = runner.run_micro_benchmark()
            
        elif args.type == 'optimization':
            logger.info("Running optimization benchmarks...")
            results = runner.run_optimization_benchmark()
            
        elif args.type == 'full':
            logger.info("Running full benchmarks...")
            results = runner.run_full_benchmark()
            
        elif args.type == 'all':
            logger.info("Running all benchmarks...")
            results = runner.run_all_benchmarks()
        
        if results and 'error' in results:
            logger.error(f"Benchmark failed: {results['error']}")
            return 1
            
    except KeyboardInterrupt:
        logger.info("Benchmark interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Benchmark failed with exception: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return 1
    
    end_time = time.time()
    duration = end_time - start_time
    
    logger.info(f"Benchmarks completed in {duration:.2f} seconds")
    
    # Save results
    if results:
        timestamp = int(time.time())
        results_filename = f"benchmark_results_{args.type}_{timestamp}.json"
        runner.save_results(results, results_filename)
        
        # Generate analysis and reports
        if args.report or args.plots or args.compare:
            logger.info("Generating analysis...")
            analyzer = ResultsAnalyzer(config)
            
            # Analyze results
            if args.type in ['micro', 'all']:
                micro_results = results if args.type == 'micro' else results.get('benchmarks', {}).get('micro', {})
                if micro_results and not micro_results.get('error'):
                    analysis = analyzer.analyze_micro_results(micro_results)
                    
                    # Save analysis
                    analysis_filename = f"benchmark_analysis_{args.type}_{timestamp}.json"
                    analyzer_results_path = os.path.join(config.output.results_dir, analysis_filename)
                    with open(analyzer_results_path, 'w') as f:
                        import json
                        json.dump(analysis, f, indent=2)
                    
                    logger.info(f"Analysis saved to: {analysis_filename}")
                    
                    # Generate plots
                    if args.plots:
                        logger.info("Generating plots...")
                        plot_files = analyzer.generate_plots(micro_results, f"{args.type}_{timestamp}_")
                        if plot_files:
                            logger.info(f"Generated {len(plot_files)} plots: {', '.join(plot_files)}")
                        else:
                            logger.warning("No plots generated")
                    
                    # Generate HTML report
                    if args.report:
                        logger.info("Generating HTML report...")
                        report_file = analyzer.generate_html_report(
                            micro_results, 
                            analysis, 
                            f"benchmark_report_{args.type}_{timestamp}.html"
                        )
                        logger.info(f"HTML report generated: {os.path.basename(report_file)}")
            
            # Compare with previous results
            if args.compare:
                logger.info(f"Comparing with previous results: {args.compare}")
                comparison = analyzer.compare_results(args.compare, results_filename)
                
                if 'error' not in comparison:
                    comparison_filename = f"benchmark_comparison_{timestamp}.json"
                    comparison_path = os.path.join(config.output.results_dir, comparison_filename)
                    with open(comparison_path, 'w') as f:
                        import json
                        json.dump(comparison, f, indent=2)
                    
                    logger.info(f"Comparison saved to: {comparison_filename}")
                    
                    # Print comparison summary
                    summary = comparison.get('summary', {})
                    if summary:
                        improvements = summary.get('improvements', 0)
                        regressions = summary.get('regressions', 0)
                        avg_improvement = summary.get('avg_improvement', 0)
                        
                        logger.info(f"Performance comparison summary:")
                        logger.info(f"  Improvements: {improvements}")
                        logger.info(f"  Regressions: {regressions}")
                        logger.info(f"  Average improvement: {avg_improvement:.2f}%")
                else:
                    logger.error(f"Comparison failed: {comparison['error']}")
    
    logger.info("Benchmark suite completed successfully!")
    return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
