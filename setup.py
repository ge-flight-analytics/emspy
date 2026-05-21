from setuptools import setup, find_packages

def readme():
    with open('README.md') as f:
        return f.read()

setup(name = 'emspy',
      version = '0.7.0',
      description = 'A Python EMS RESTful API Client/Wrapper',
      long_description=readme(),
      long_description_content_type="text/markdown",
      classifiers = [
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License'
        ],
      url = 'https://github.com/ge-flight-analytics/emspy',
      author = 'GE Flight Analytics',
      author_email = 'AviationAdiSupport@ge.com',
      license = 'MIT',
      packages = find_packages(exclude=("docs","tests")),
      python_requires = '>=3.7',
      install_requires = ['numpy>=1.13.3', 'pandas>=1.5', 'future', 'networkx'],
      zip_safe = False)
      