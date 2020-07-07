from Cython.Build import cythonize
#from distutils.core import setup, Extension
from setuptools import Extension, setup

import os
import numpy as np
import pyarrow as pa

srcdir = "@CMAKE_CURRENT_SOURCE_DIR@"
dbe = Extension("omnidbe.dbe",
                ["omnidbe/dbe.pyx"],
                language='c++17',
                include_dirs=[
                  np.get_include(),
                  pa.get_include(),
                  "@CMAKE_SOURCE_DIR@",
                  srcdir
                ],
                library_dirs=pa.get_library_dirs() + ['.'],
                runtime_library_dirs=pa.get_library_dirs() + ['$ORIGIN/../../../'],
                libraries=pa.get_libraries() + ['DBEngine', 'boost_system'],
                extra_compile_args=['-std=c++17'],
              )

setup(
  name = 'omnidbe',
  version='0.3',
  ext_modules = cythonize(dbe,
    compiler_directives={'c_string_type': "str", 'c_string_encoding': "utf8", 'language_level': "3"},
    include_path=[srcdir]),
  package_data = {
    "omnidbe": ['__init__.py']
  },
  data_files=[
    #("lib", ["$<TARGET_FILE:DBEngine>"]),
    ('include', [
      srcdir+"/DBEngine.h",
      srcdir+"/DBEngine.pxd",
    ])
  ],
)
