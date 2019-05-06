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
from htcondor_jobs.submit import check_itemdata


DESCRIPTORS = {"foo": "0", "bar": "baz"}


def test_mismatch_lens_sequences_raises():
    itemdata = [["foo"], ["bar", "bang"]]

    with pytest.raises(jobs.exceptions.InvalidItemdata):
        check_itemdata(itemdata)


def test_key_mismatch_dicts_raises():
    itemdata = [{"foo": 0}, {"bar": 0}]

    with pytest.raises(jobs.exceptions.InvalidItemdata):
        check_itemdata(itemdata)


def test_empty_itemdata_raises():
    itemdata = []

    with pytest.raises(jobs.exceptions.InvalidItemdata):
        check_itemdata(itemdata)
