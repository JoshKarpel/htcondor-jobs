# Copyright 2019 HTCondor Team, Computer Sciences Department,
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

import pytest

import time

import htcondor

import htcondor_jobs as jobs
from htcondor_jobs.locate import get_schedd


def test_no_args():
    assert isinstance(get_schedd(), htcondor.Schedd)


def test_caching():
    schedd = get_schedd()
    again = get_schedd()

    assert schedd is again
