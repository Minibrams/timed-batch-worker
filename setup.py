import setuptools

with open('README.md') as fp:
    long_description = fp.read()

setuptools.setup(
    name="timed-batch-worker",
    version="0.0.3",
    author="Anders Brams",
    author_email="anders@brams.dk",
    description="Python library for efficiently batch-processing workloads asynchronously with batch-size- and time-based flushes.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords=[
        "batch",
        "worker",
        "asynchronous",
        "threading",
        "timed"
    ],
    url="https://github.com/Minibrams/timed-batch-worker",
    packages=setuptools.find_packages(where='src'),
    package_dir={
        '': 'src'
    },
    install_requires=[],
    python_requires='>=3',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
