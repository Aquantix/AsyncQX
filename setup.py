from platform import python_version
from setuptools import setup, find_packages

with open('requirements.txt') as fh:
    requirements = fh.readlines()

setup(
    name="asyncqx",
    packages=find_packages(),
    install_requires=requirements
)
