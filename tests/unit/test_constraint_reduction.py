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

import operator

import htcondor_jobs as jobs


def test_reduce_comparison_is_self():
    a = jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0)

    assert a.reduce() is a


def test_and_true():
    a = jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0)
    b = jobs.true

    m = (a & b).reduce()

    assert len(m) == 1
    assert m == a


def test_and_false():
    a = jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0)
    b = jobs.false

    m = (a & b).reduce()

    assert m is jobs.false


def test_or_true():
    a = jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0)
    b = jobs.true

    m = (a | b).reduce()

    assert len(m) == 1
    assert m == jobs.true


def test_or_false():
    a = jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0)
    b = jobs.false

    m = (a | b).reduce()

    assert m == a


@pytest.mark.parametrize("combinator", [operator.and_, operator.or_])
def test_deduplication(combinator):
    a = jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0)
    b = jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0)

    d = combinator(a, b).reduce()

    assert len(d) == 1
    assert d == a
