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

import htcondor_jobs as jobs


DESCRIPTORS = {"foo": "0", "bar": "baz"}


def test_init_from_kwargs():
    d = jobs.SubmitDescription(foobar="wizbang")

    assert d["foobar"] == "wizbang"


def test_init_from_dict():
    d = jobs.SubmitDescription({"foobar": "wizbang"})

    assert d["foobar"] == "wizbang"


def test_init_from_combined():
    d = jobs.SubmitDescription({"foobar": "wizbang"}, foobar="woah")

    assert d["foobar"] == "woah"


@pytest.fixture(scope="function")
def desc():
    return jobs.SubmitDescription(**DESCRIPTORS.copy())


def test_keys(desc):
    assert tuple(desc.keys()) == tuple(DESCRIPTORS.keys())


def test_values(desc):
    assert tuple(desc.values()) == tuple(DESCRIPTORS.values())


def test_items(desc):
    assert tuple(desc.items()) == tuple(DESCRIPTORS.items())


def test_getitem(desc):
    for k, v in DESCRIPTORS.items():
        assert desc[k] == v


def test_setitem(desc):
    desc["hello"] = "wizbang"

    assert desc["hello"] == "wizbang"


def test_delitem(desc):
    key = tuple(DESCRIPTORS.keys())[0]
    del desc[key]

    assert key not in desc


def test_str(desc):
    assert str(desc) == "foo = 0\nbar = baz"
