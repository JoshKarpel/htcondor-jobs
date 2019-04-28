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

from typing import Union

import abc
import enum
import dataclasses


# https://en.wikipedia.org/wiki/Quine%E2%80%93McCluskey_algorithm

class StrEnum(str, enum.Enum):
    pass


class Operator(StrEnum):
    Equals = '=='
    GreaterEquals = '>='
    LessEquals = '<='
    Greater = '>'
    Less = '<'


@dataclasses.dataclass(frozen = True)
class Expression:
    key: str
    operator: Operator
    value: Union[int, str, float]

    def __post_init__(self):
        object.__setattr__(self, 'value', str(self.value))


class Constraint(abc.ABC):
    @abc.abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def __iter__(self) -> 'Constraint':
        raise NotImplementedError

    def __and__(self, other) -> 'Constraint':
        if other is Boolean.true:
            return self
        elif other is Boolean.false:
            return Boolean.false

        left = isinstance(self, And)
        right = isinstance(other, And)
        if left and right:
            return And(*self, *other)
        elif left:
            return And(*self, other)
        elif right:
            return And(self, *other)

        return And(self, other)

    def __or__(self, other) -> 'Constraint':
        if other is Boolean.true:
            return Boolean.true
        elif other is Boolean.false:
            return self

        left = isinstance(self, Or)
        right = isinstance(other, Or)
        if left and right:
            return Or(*self, *other)
        elif left:
            return Or(*self, other)
        elif right:
            return Or(self, *other)

        return Or(self, other)

    def __xor__(self, other) -> 'Constraint':
        return Xor(self, other)

    def __invert__(self) -> 'Constraint':
        return Not(self)


class Boolean(StrEnum):
    true = 'true'
    false = 'false'


class MultiConstraint(Constraint):
    def __init__(self, *constraints: Constraint):
        self.constraints = set(constraints)

    def __iter__(self):
        yield from self.constraints

    def __repr__(self):
        return f'{self.__class__.__name__}({", ".join(repr(c) for c in self.constraints)}'


class And(MultiConstraint):
    def __str__(self):
        return ' && '.join(f'({c})' for c in self.constraints)


class Or(MultiConstraint):
    def __str__(self):
        return ' || '.join(f'({c})' for c in self.constraints)


class Xor(MultiConstraint):
    def __str__(self):
        z = Or(*(And(*(d if d == c else Not(d) for d in self)) for c in self))
        return str(z)


class Not(Constraint):
    def __init__(self, constraint: Constraint):
        self.constraint = constraint

    def __iter__(self):
        yield self

    def __str__(self):
        return f'!({self.constraint})'


class ComparisonConstraint(Constraint):
    def __init__(self, key: str, operator: Operator, value: Union[int, float, str]):
        self.expr = Expression(key, operator, value)

    def __iter__(self):
        yield self

    def __str__(self):
        return f'{self.expr.key}{self.expr.operator}{self.expr.value}'

    def __repr__(self):
        return f'{self.__class__.__name__}({self.expr})'


class InCluster(ComparisonConstraint):
    def __init__(self, clusterid: int):
        super().__init__(key = 'ClusterID', operator = Operator.Equals, value = clusterid)
