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
from typing import Union, Iterator, Iterable

import abc
import enum
import dataclasses
import itertools


# https://en.wikipedia.org/wiki/Quine%E2%80%93McCluskey_algorithm


class StrEnum(str, enum.Enum):
    pass


class Operator(StrEnum):
    Equals = "=="
    GreaterEquals = ">="
    LessEquals = "<="
    Greater = ">"
    Less = "<"


@dataclasses.dataclass(frozen=True)
class Expression:
    key: str
    operator: Operator
    value: Union[int, str, float]


def flatten(iterables):
    return itertools.chain.from_iterable(iterables)


class Constraint(abc.ABC):
    @abc.abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError

    def reduce(self) -> "Constraint":
        return self

    @abc.abstractmethod
    def __len__(self):
        raise NotImplementedError

    @abc.abstractmethod
    def __iter__(self) -> Iterator["Constraint"]:
        raise NotImplementedError

    def __and__(self, other) -> "Constraint":
        return And(self, other)

    def __or__(self, other) -> "Constraint":
        return Or(self, other)

    def __xor__(self, other) -> "Constraint":
        return Xor(self, other)

    def __invert__(self) -> "Constraint":
        return Not(self)


class Boolean(Constraint):
    def __iter__(self):
        yield self

    def __len__(self):
        return 1

    @abc.abstractmethod
    def __bool__(self):
        raise NotImplementedError


class true(Boolean):
    def __str__(self):
        return "true"

    def __bool__(self):
        return True


class false(Boolean):
    def __str__(self):
        return "false"

    def __bool__(self):
        return False


true = true()
false = false()


class MultiConstraint(Constraint):
    def __init__(self, *constraints: Union[Constraint, Iterable[Constraint]]):
        self._constraints = list(flatten(constraints))

    def __iter__(self):
        yield from self._constraints

    def __len__(self):
        return len(self._constraints)

    def __contains__(self, item):
        return item in self._constraints

    def __repr__(self):
        return f'{self.__class__.__name__}({", ".join(repr(c) for c in self)}'


class And(MultiConstraint):
    def __str__(self):
        return " && ".join(f"({c})" for c in self)

    def reduce(self) -> Constraint:
        # AND false is always false
        if false in self:
            return false

        # unique-ify
        constraints = set(c.reduce() for c in self if not isinstance(c, Boolean))

        # empty AND is false
        if len(constraints) == 0:
            return false
        elif len(constraints) == 1:
            return constraints.pop()
        else:
            return And(constraints)


class Or(MultiConstraint):
    def __str__(self):
        return " || ".join(f"({c})" for c in self)

    def reduce(self) -> Constraint:
        # OR true is always true
        if true in self:
            return true

        # unique-ify
        constraints = set(c.reduce() for c in self if not isinstance(c, Boolean))

        # empty OR is true
        if len(constraints) == 0:
            return true
        elif len(constraints) == 1:
            return constraints.pop()
        else:
            return Or(constraints)


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
        return f"!({self.constraint})"


class ComparisonConstraint(Constraint):
    def __init__(self, key: str, operator: Operator, value: Union[int, float, str]):
        self.expr = Expression(key, operator, value)

    def __iter__(self):
        yield self

    def __len__(self):
        return 1

    def __str__(self):
        return f"{self.expr.key} {self.expr.operator} {self.expr.value}"

    def __repr__(self):
        return f"{self.__class__.__name__}({self.expr})"


class InCluster(ComparisonConstraint):
    def __init__(self, clusterid: int):
        super().__init__(key="ClusterId", operator=Operator.Equals, value=clusterid)
