from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='pds4_utils',
    version='0.1',
    author='Mark S. Bentley',
    author_email='mark@lunartech.org',
    description='A collection of PDS4 utilities',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/msbentley/pds4_utils",
    download_url = 'https://github.com/msbentley/pds4_utils/archive/0.1.tar.gz',
    install_requires=['pandas','pyyaml','lxml','pds4-tools'],
    python_requires='>=3.0',
    keywords = ['PDS','archive','data'],
    packages=['pds4_utils'],
    zip_safe=False)
