# Legacy CD-Index Implementation

This directory contains the original, legacy implementation of the CD-Index computation before the enhanced memory-efficient version was developed.

## Contents

- `cdindex.py` - Original Python implementation with Graph class
- `pycdindex.cpp` - Legacy C++ Python bindings
- `cdindex.cpp` - Original C++ implementation 
- `cdindex.h` - Legacy C++ header file
- `main.cpp` - Legacy main entry point
- `setup.py` - Legacy Python module setup
- `Makefile` - Legacy build configuration

## Why this code was replaced

The legacy implementation had several performance and memory issues:

1. **Memory explosion**: Time-window handling depended on cumulative prefix arrays and bitmap subtraction, which consumed ~119 GB in realistic runs
2. **Inefficient operations**: The hot path performed two big intersections with giant prefix bitmaps
3. **Limited filtering**: Country-based filtering was awkward and expensive to implement

## Enhanced Implementation

The current enhanced implementation (in parent directories) addresses these issues with:

- Reformed algebra that eliminates per-query B_t materialization
- Memory-efficient data structures using Arrow + CRoaring
- Native country filtering capabilities
- Significant performance improvements

## Building Legacy Code

If you need to build the legacy implementation:

```bash
cd legacy/
make clean && make
make test
```

Note: The legacy tests may require the old module structure to be available.

## Migration Notes

The enhanced implementation maintains API compatibility where possible, but users should migrate to the new `EnhancedGraph` class for better performance and functionality.