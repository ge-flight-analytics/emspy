from setuptools import setup

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
      packages = ['emspy'],
      install_requires = ['numpy', 'pandas', 'future'],
      zip_safe = False)
      