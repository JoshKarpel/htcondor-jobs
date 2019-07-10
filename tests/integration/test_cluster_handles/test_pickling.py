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

import htcondor_jobs as jobs


@pytest.fixture(scope="function")
def roundtripped_handle(short_sleep, tmp_path):
    path = tmp_path / "handle.pkl"
    a = jobs.submit(short_sleep)

    a.save(path)
    b = jobs.ClusterHandle.load(path)

    return a, b


def test_save_then_load_is_equal_and_same_hash(roundtripped_handle):
    a, b = roundtripped_handle

    assert a == b
    assert hash(a) == hash(b)


def test_states_are_same(roundtripped_handle):
    a, b = roundtripped_handle

    a.wait(timeout=180)

    assert list(a.state) == list(b.state)
