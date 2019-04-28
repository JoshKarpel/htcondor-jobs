import htcondor_jobs as jobs

import operator


def test_and():
    a = jobs.ComparisonConstraint('ClusterID', jobs.Operator.Equals, 500)
    b = jobs.ComparisonConstraint('ProcID', jobs.Operator.LessEquals, 10)

    m = a & b

    assert str(m) == '(ClusterId == 500) && (ProcID <= 10)'


@pytest.mark.parametrize(
    'combinator',
    [operator.and_, operator.or_, operator.xor]
)
def test_len_of_multiconstraints(combinator):
    a = jobs.ComparisonConstraint('ClusterID', jobs.Operator.Equals, 500)
    b = jobs.ComparisonConstraint('ProcID', jobs.Operator.LessEquals, 10)

    m = combinator(a, b)

    assert len(m) == 2
