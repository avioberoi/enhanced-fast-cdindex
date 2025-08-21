#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""setup_enhanced.py: Build script for the enhanced cdindex Python module with pybind11."""

from pybind11.setup_helpers import Pybind11Extension, build_ext
from setuptools import setup, find_packages
import pybind11
import pyarrow as pa
import os

# Define the extension module
ext_modules = [
    Pybind11Extension(
        "_cdindex",
        ["src/cdindex_enhanced.cpp", "src/pybind.cpp"],
        include_dirs=[
            "src",
            pa.get_include(),
        ],
        libraries=["arrow", "arrow_python", "roaring"],
        library_dirs=pa.get_library_dirs() + ["/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/lib"],
        language="c++",
        cxx_std=17,
        extra_compile_args=["-O3", "-march=native", "-DNDEBUG", "-flto"],
    ),
]

setup(
    name="fast_cdindex_enhanced",
    version="1.0.0",
    description="Enhanced package for computing the CD index with filtering capabilities",
    author="Enhanced Development Team",
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
    packages=find_packages(),
    install_requires=[
        "pybind11",
        "pyarrow>=12.0.0",
        "numpy",
    ],
    python_requires=">=3.8",
)
