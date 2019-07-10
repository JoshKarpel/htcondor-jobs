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


@pytest.mark.parametrize(
    "c, t",
    [
        (jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0), "foo == 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.NotEquals, 0), "foo != 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.Greater, 0), "foo > 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.GreaterEquals, 0), "foo >= 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.Less, 0), "foo < 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.LessEquals, 0), "foo <= 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.Is, 0), "foo =?= 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.Isnt, 0), "foo =!= 0"),
    ],
)
def test_string_form(c, t):
    assert str(c) == t


def test_len():
    c = jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0)

    assert len(c) == 1


@pytest.mark.parametrize(
    "c, t",
    [
        (jobs.ComparisonConstraint("foo", jobs.Operator.Equals, 0), "foo == 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.NotEquals, 0), "foo != 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.Greater, 0), "foo > 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.GreaterEquals, 0), "foo >= 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.Less, 0), "foo < 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.LessEquals, 0), "foo <= 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.Is, 0), "foo =?= 0"),
        (jobs.ComparisonConstraint("foo", jobs.Operator.Isnt, 0), "foo =!= 0"),
    ],
)
def test_from_expr(c, t):
    assert jobs.ComparisonConstraint.from_expr(t) == c
