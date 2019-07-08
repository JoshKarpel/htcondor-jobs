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


@pytest.mark.parametrize("combinator", [operator.and_, operator.or_])
def test_cannot_combine_handles_with_different_collectors(combinator):
    h1 = jobs.ConstraintHandle(
        jobs.ComparisonConstraint("foo", jobs.Operator.Equals, "bar"), collector="foo"
    )
    h2 = jobs.ConstraintHandle(
        jobs.ComparisonConstraint("foo", jobs.Operator.Equals, "bar"), collector="bar"
    )

    with pytest.raises(jobs.exceptions.InvalidHandle):
        combinator(h1, h2)


@pytest.mark.parametrize("combinator", [operator.and_, operator.or_])
def test_cannot_combine_handles_with_different_schedulers(combinator):
    h1 = jobs.ConstraintHandle(
        jobs.ComparisonConstraint("foo", jobs.Operator.Equals, "bar"), scheduler="foo"
    )
    h2 = jobs.ConstraintHandle(
        jobs.ComparisonConstraint("foo", jobs.Operator.Equals, "bar"), scheduler="bar"
    )

    with pytest.raises(jobs.exceptions.InvalidHandle):
        combinator(h1, h2)


@pytest.mark.parametrize("combinator", [operator.and_, operator.or_])
def test_can_combine_handle_with_constraint(combinator):
    h = jobs.ConstraintHandle(
        jobs.ComparisonConstraint("foo", jobs.Operator.Equals, "bar")
    )
    c = jobs.ComparisonConstraint("fizz", jobs.Operator.Equals, "buzz")

    combined = combinator(h, c)

    assert isinstance(combined, jobs.ConstraintHandle)


@pytest.mark.parametrize("combinator", [operator.and_, operator.or_])
def test_can_combine_handle_with_comparison_constraint_string(combinator):
    h = jobs.ConstraintHandle(
        jobs.ComparisonConstraint("foo", jobs.Operator.Equals, "bar")
    )
    c = "fizz == buzz"

    combined = combinator(h, c)

    assert isinstance(combined, jobs.ConstraintHandle)


@pytest.mark.parametrize("combinator", [operator.and_, operator.or_])
def test_cannot_combine_handle_with_arbitrary_string(combinator):
    h = jobs.ConstraintHandle(
        jobs.ComparisonConstraint("foo", jobs.Operator.Equals, "bar")
    )
    c = "dsifjaodgj"

    with pytest.raises(jobs.exceptions.ExpressionParseFailed):
        combined = combinator(h, c)


@pytest.mark.parametrize("combinator", [operator.and_, operator.or_])
@pytest.mark.parametrize("bad_value", [None, True, 1, 5.5, {}, [], set()])
def test_cannot_combine_handle_with_other_types(combinator, bad_value):
    h = jobs.ConstraintHandle(
        jobs.ComparisonConstraint("foo", jobs.Operator.Equals, "bar")
    )
    c = bad_value

    with pytest.raises(jobs.exceptions.InvalidHandle):
        combined = combinator(h, c)


@pytest.mark.parametrize("combinator", [operator.and_, operator.or_])
def test_cannot_combine_handle_with_bad_operator(combinator):
    h = jobs.ConstraintHandle(
        jobs.ComparisonConstraint("foo", jobs.Operator.Equals, "bar")
    )
    c = "foo !?= bar"

    with pytest.raises(jobs.exceptions.ExpressionParseFailed):
        combined = combinator(h, c)
