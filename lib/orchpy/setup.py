from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

# because of relative paths, this must be run from inside orchestra/lib/orchpy/

setup(
  ext_modules = cythonize([
    Extension("main",
      sources = ["main.pyx"], libraries=["orchestralib"],
      library_dirs=['../../target/debug/']),
    Extension("unison", sources = ["unison.pyx"])])
)
