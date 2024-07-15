import sys
from setuptools import setup, find_packages

def readme():
    with open('README.md') as f:
        return f.read()

# Numpy dropped support for python 2.7 in 1.17
# Pandas dropped support for python 2.7 after 0.24.0
min_numpy_ver = "1.13.3"
max_numpy_ver = ""
pandas_install_rule = 'pandas'
if sys.version_info < (3, 0):
    max_numpy_ver = "1.17"
    pandas_install_rule += "<=0.24.0"
    
numpy_install_rule = "numpy >= {numpy_ver}".format(numpy_ver=min_numpy_ver)
if max_numpy_ver:
    numpy_install_rule += ", < {numpy_ver}".format(numpy_ver=max_numpy_ver)

setup(name = 'emspy',
      version = '0.6.6',
      description = 'A Python EMS RESTful API Client/Wrapper',
      long_description=readme(),
      long_description_content_type="text/markdown",
      classifiers = [
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License'
        ],
      url = 'https://github.com/ge-flight-analytics/emspy',
      author = 'GE Flight Analytics',
      author_email = 'AviationAdiSupport@ge.com',
      license = 'MIT',
      packages = find_packages(exclude=("docs","tests")),
      install_requires = [numpy_install_rule, pandas_install_rule, 'future', 'networkx'],
      zip_safe = False)
      