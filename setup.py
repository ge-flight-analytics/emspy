from setuptools import setup, find_packages

setup(name = 'emspy',
      version = '0.2.1',
      description = 'A Python EMS RESTful API Client/Wrapper',
      classifiers = [
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License'
        ],
      url = 'https://github.com/ge-flight-analytics/emspy',
      author = 'Kyungjin Moon',
      author_email = 'jimin96@gmail.com',
      license = 'MIT',
      packages = find_packages(),
      install_requires = ['numpy', 'pandas==0.24.1', 'future', 'networkx'],
      zip_safe = False)
      
