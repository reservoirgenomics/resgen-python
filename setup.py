import io
import os
import os.path

from setuptools import setup, find_packages, Command

HERE = os.path.dirname(os.path.abspath(__file__))


def read(*parts, **kwargs):
    filepath = os.path.join(HERE, *parts)
    encoding = kwargs.pop("encoding", "utf-8")
    with io.open(filepath, encoding=encoding) as fh:
        text = fh.read()
    return text


def get_requirements(path):
    content = read(path)
    return [req for req in content.split("\n") if req != "" and not req.startswith("#")]


setup_args = {
    "name": "resgen-python",
    "version": "0.4.0",
    "packages": find_packages(),
    "description": "Python bindings for the resgen genomic data service",
    # "long_description": read("README.md"),
    "long_description_content_type": "text/markdown",
    "url": "https://github.com/reservoirgenomics/resgen-python",
    "include_package_data": True,
    "zip_safe": False,
    "author": "Peter Kerpedjiev",
    "author_email": "pkerpedjiev@gmail.com",
    "keywords": ["resgen"],
    "classifiers": [
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Multimedia :: Graphics",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    "install_requires": get_requirements("requirements.txt"),
    "setup_requires": [],
    "tests_require": ["pytest"],
    "entry_points": {"console_scripts": ["resgen = resgen.cli:cli"]}
}

setup(**setup_args)
