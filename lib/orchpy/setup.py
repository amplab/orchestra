#from distutils.core import setup
#from distutils.extension import Extension
from setuptools import setup, Extension, find_packages
from Cython.Build import cythonize

# because of relative paths, this must be run from inside orchestra/lib/orchpy/

setup(
  name = "orchestra",
  version = "0.1.dev0",
  ext_modules = cythonize([
    Extension("orchpy/main",
      sources = ["orchpy/main.pyx"], libraries=["orchestralib"],
      library_dirs=['../../target/debug/']),
    Extension("orchpy/unison", sources = ["orchpy/unison.pyx"])],
    compiler_directives={'language_level': 3}),
  use_2to3=True,
  packages=find_packages()
)
