#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("CHANGELOG.rst") as changelog_file:
    changelog = changelog_file.read()

with open("LICENSE") as license_file:
    license = license_file.read()


requirements = [
    # TODO: put package requirements here
]

test_requirements = [
    "pytest",
]


setup(
    name="nabatpy",
    version="0.0.1",
    description="Python tools for accessing and manipulating North American Bat Monitoring data.",
    long_description=readme + "\n\n" + changelog,
    author="Colin Talbert",
    author_email="talbertc@usgs.gov",
    url="",
    packages=[
        "nabatpy",
    ],
    package_dir={"nabatpy":
                 "nabatpy"},
    include_package_data=True,
    install_requires=requirements,
    license=license,
    zip_safe=False,
    keywords="nabatpy",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
        "Natural Language :: English",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
    ],
    test_suite="tests",
    tests_require=test_requirements
)
