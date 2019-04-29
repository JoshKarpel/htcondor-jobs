import pytest

import htcondor_jobs as jobs

import operator


def test_and():
    a = jobs.ComparisonConstraint("Foo", jobs.Operator.Equals, 500)
    b = jobs.ComparisonConstraint("Bar", jobs.Operator.LessEquals, 10)

    m = a & b

    assert str(m) == "(Foo == 500) && (Bar <= 10)"


@pytest.mark.parametrize("combinator", [operator.and_, operator.or_, operator.xor])
def test_len_of_multiconstraints(combinator):
    a = jobs.ComparisonConstraint("Foo", jobs.Operator.Equals, 500)
    b = jobs.ComparisonConstraint("Bar", jobs.Operator.LessEquals, 10)

    m = combinator(a, b)

    assert len(m) == 2
