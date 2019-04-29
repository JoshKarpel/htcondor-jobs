import pytest

import htcondor_jobs as jobs

import operator


def test_reduce_comparison_is_self():
    a = jobs.ComparisonConstraint('foo', jobs.Operator.Equals, 0)

    assert a.reduce() is a


def test_and_with_true():
    a = jobs.ComparisonConstraint('foo', jobs.Operator.Equals, 0)
    t = jobs.Boolean.true

    m = (a & t).reduce()

    assert len(m) == 1
    assert m == a


def test_and_with_false():
    a = jobs.ComparisonConstraint('foo', jobs.Operator.Equals, 0)
    t = jobs.Boolean.false

    m = (a & t).reduce()

    assert m is jobs.Boolean.false


def test_or_with_true():
    a = jobs.ComparisonConstraint('foo', jobs.Operator.Equals, 0)
    t = jobs.Boolean.true

    m = (a & t).reduce()

    assert len(m) == 1
    assert m == a


def test_or_with_false():
    a = jobs.ComparisonConstraint('foo', jobs.Operator.Equals, 0)
    t = jobs.Boolean.false

    m = (a & t).reduce()

    assert m == a
