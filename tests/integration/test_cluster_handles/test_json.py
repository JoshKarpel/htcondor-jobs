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

import classad

import htcondor_jobs as jobs


@pytest.fixture(scope="function")
def roundtripped_handle(short_sleep):
    a = jobs.submit(short_sleep)

    j = a.to_json()
    b = jobs.ClusterHandle.from_json(j)

    return a, b


def test_save_then_load_is_equal_and_same_hash(roundtripped_handle):
    a, b = roundtripped_handle

    assert a == b
    assert hash(a) == hash(b)


def test_clusterad_is_reconstructed_correctly(roundtripped_handle):
    a, b = roundtripped_handle

    # we have to do this awful manual nonsense because if the values are
    # expressions, doing a == will just return a new expression.
    # ... so instead, we manually check that the lengths and contents are the same
    # (plain dict equality will internally use ==, so no help there)
    da = dict(a.clusterad)
    db = dict(b.clusterad)
    assert len(da) == len(db)
    for (ka, va) in da.items():
        if isinstance(va, classad.ExprTree):
            assert str(va) == str(db[ka])
        else:
            assert va == db[ka]


def test_states_are_same(roundtripped_handle):
    a, b = roundtripped_handle

    a.wait(timeout=180)

    assert list(a.state) == list(b.state)
