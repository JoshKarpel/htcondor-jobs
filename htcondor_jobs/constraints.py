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

from typing import Union, Iterator, Iterable, Any
import logging

import abc
import enum
import dataclasses
import itertools


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())


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
    value: Union[str, int, float]


def flatten(iterables):
    return itertools.chain.from_iterable(iterables)


class Constraint(abc.ABC):
    @abc.abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError

    def reduce(self) -> "Constraint":
        return self

    @abc.abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def __iter__(self) -> Iterator["Constraint"]:
        raise NotImplementedError

    def __and__(self, other: "Constraint") -> "Constraint":
        return And(self, other)

    def __or__(self, other: "Constraint") -> "Constraint":
        return Or(self, other)

    def __xor__(self, other: "Constraint") -> "Constraint":
        return Xor(self, other)

    def __invert__(self) -> "Constraint":
        return Not(self)

    def __eq__(self, other: Any) -> bool:
        return bool(isinstance(other, Constraint) and str(self) == str(other))

    def __hash__(self) -> int:
        return hash(str(self))


class BooleanConstraint(Constraint):
    def __iter__(self) -> Iterator["BooleanConstraint"]:
        yield self

    def __len__(self) -> int:
        return 1

    @abc.abstractmethod
    def __bool__(self) -> bool:
        raise NotImplementedError


class _true(BooleanConstraint):
    def __str__(self) -> str:
        return "true"

    def __bool__(self) -> bool:
        return True

    def __invert__(self) -> BooleanConstraint:
        return false


class _false(BooleanConstraint):
    def __str__(self) -> str:
        return "false"

    def __bool__(self) -> bool:
        return False

    def __invert__(self) -> BooleanConstraint:
        return true


# expose them as singletons
true = _true()
false = _false()


class MultiConstraint(Constraint):
    def __init__(self, *constraints: Union[Constraint, Iterable[Constraint]]):
        self._constraints = list(flatten(constraints))

    def __iter__(self) -> Iterator[Constraint]:
        yield from self._constraints

    def __len__(self) -> int:
        return len(self._constraints)

    def __contains__(self, item: Any) -> bool:
        return item in self._constraints

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({", ".join(repr(c) for c in self)}'

    def reduce(self) -> "Constraint":
        # unique-ify
        constraints = set(
            c.reduce() for c in self if not isinstance(c, BooleanConstraint)
        )

        # empty is true
        if len(constraints) == 0:
            return true
        elif len(constraints) == 1:
            return constraints.pop()
        else:
            return type(self)(constraints)


class And(MultiConstraint):
    def __str__(self) -> str:
        return " && ".join(f"({c})" for c in self)

    def reduce(self) -> Constraint:
        # AND false is always false
        if false in self:
            return false

        return super().reduce()


class Or(MultiConstraint):
    def __str__(self) -> str:
        return " || ".join(f"({c})" for c in self)

    def reduce(self) -> Constraint:
        # OR true is always true
        if true in self:
            return true

        return super().reduce()


class Xor(MultiConstraint):
    def __str__(self) -> str:
        z = Or(*(And(*(d if d == c else Not(d) for d in self)) for c in self))
        return str(z)

    def reduce(self) -> "Constraint":
        # true XOR true is false
        if self._constraints.count(true) > 1:
            return false

        return super().reduce()


class Not(Constraint):
    def __init__(self, constraint: Constraint):
        self.constraint = constraint

    def __iter__(self) -> Iterator[Constraint]:
        yield self

    def __len__(self) -> int:
        return 1

    def __str__(self) -> str:
        return f"!({self.constraint})"

    def reduce(self) -> "Constraint":
        if self.constraint is true:
            return false
        elif self.constraint is false:
            return true

        return super().reduce()


class ComparisonConstraint(Constraint):
    def __init__(self, key: str, operator: Operator, value: Union[int, float, str]):
        self.expr = Expression(key, operator, value)

    def __iter__(self) -> Iterator[Constraint]:
        yield self

    def __len__(self) -> int:
        return 1

    def __str__(self) -> str:
        return f"{self.expr.key} {self.expr.operator} {self.expr.value}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.expr})"


class InCluster(ComparisonConstraint):
    def __init__(self, clusterid: int):
        super().__init__(key="ClusterId", operator=Operator.Equals, value=clusterid)
