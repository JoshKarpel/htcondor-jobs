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


def test_string_form():
    c = ~jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0)

    assert str(c) == "!(foo == 0)"


def test_iter():
    c = ~jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0)

    assert next(iter(c)) is c


def test_len():
    c = ~jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0)

    assert len(c) == 1
