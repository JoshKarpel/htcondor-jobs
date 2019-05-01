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

import operator


def test_and_string_form():
    a = jobs.ComparisonConstraint("Foo", jobs.Operator.Equals, 500)
    b = jobs.ComparisonConstraint("Bar", jobs.Operator.LessEquals, 10)

    m = a & b

    assert str(m) == "(Foo == 500) && (Bar <= 10)"


def test_or_string_form():
    a = jobs.ComparisonConstraint("Foo", jobs.Operator.Equals, 500)
    b = jobs.ComparisonConstraint("Bar", jobs.Operator.LessEquals, 10)

    m = a | b

    assert str(m) == "(Foo == 500) || (Bar <= 10)"


@pytest.mark.parametrize("combinator", [operator.and_, operator.or_])
def test_len_of_multiconstraints(combinator):
    a = jobs.ComparisonConstraint("Foo", jobs.Operator.Equals, 500)
    b = jobs.ComparisonConstraint("Bar", jobs.Operator.LessEquals, 10)

    m = combinator(a, b)

    assert len(m) == 2


@pytest.mark.parametrize("mc", [jobs.And, jobs.Or])
def test_chained_multiconstraint_stays_flat(mc):
    a = jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0)
    b = jobs.ComparisonConstraint("bar", jobs.Operator.Equals, 0)
    c = jobs.ComparisonConstraint("baz", jobs.Operator.Equals, 0)

    assert len(mc(mc(a, b), c)) == 3


@pytest.mark.parametrize("combinator", [operator.and_, operator.or_])
def test_chained_multiconstraint_stays_flat_using_operators(combinator):
    a = jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0)
    b = jobs.ComparisonConstraint("bar", jobs.Operator.Equals, 0)
    c = jobs.ComparisonConstraint("baz", jobs.Operator.Equals, 0)

    assert len(combinator(combinator(a, b), c))
