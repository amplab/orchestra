from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

setup(
  ext_modules = cythonize([
    Extension("main",
      sources = ["main.pyx"], libraries=["orchestralib"],
      library_dirs=['/home/pcmoritz/hermes/target/debug/']),
    Extension("unison", sources = ["unison.pyx"])])
)
