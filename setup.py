# Copyright 2018 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
from pathlib import Path

from setuptools import setup

THIS_DIR = os.path.abspath(os.path.dirname(__file__))


def find_version():
    """Grab the version out of htcondor_jobs/__init__.py without importing it."""
    version_file_text = (Path(THIS_DIR) / "htcondor_jobs" / "version.py").read_text()
    version_match = re.search(
        r"^__version__ = ['\"]([^'\"]*)['\"]", version_file_text, re.M
    )
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name="htcondor-jobs",
    version=find_version(),
    author="Josh Karpel",
    author_email="josh.karpel@gmail.com",
    description="High-level job submission and management for the HTCondor Python bindings",
    long_description=Path("README.md").read_text(),
    long_description_content_type="text/markdown",
    url="https://github.com/htcondor/htcondor-jobs",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Topic :: System :: Distributed Computing",
    ],
    packages=["htcondor_jobs"],
    install_requires=Path("requirements.txt").read_text().splitlines(),
)
