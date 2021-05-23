from platform import python_version
from setuptools import setup, find_packages

with open('requirements.txt') as fh:
    requirements = fh.readlines()

setup(
    name="asyncqx",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=requirements
)
