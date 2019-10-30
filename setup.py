from setuptools import setup, find_packages

setup(name = 'emspy',
      version = '0.2.0',
      description = 'A Python EMS RESTful API Client/Wrapper',
      long_description="README.md",
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
      install_requires = ['numpy', 'pandas', 'future', 'networkx'],
      zip_safe = False)
      
