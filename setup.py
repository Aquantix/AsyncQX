from setuptools import setup, find_packages

with open('requirements.txt') as fh:
    requirements = fh.readlines()

setup(
    name="asyncqx",
    version="1.0.0",
    author="Drew Wagner",
    author_email="drew.wagner@aquantix.ai",
    description="A publisher-subscriber implementation backed by RabbitMQ",
    url="https://github.com/Aquantix/AsyncQX",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=requirements
)
