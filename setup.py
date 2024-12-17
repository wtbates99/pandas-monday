from setuptools import setup, find_packages

setup(
    name="pandas-monday",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "requests",
        "tqdm",
    ],
    python_requires=">=3.7",
)
