# Import only time_utilities to avoid conflicts with enhanced module
try:
  from fast_cdindex.time_utilities import *
except ImportError:
  from time_utilities import *
