import abc

from . import constraints


class Handle(abc.ABC):
    @abc.abstractmethod
    def constraint(self) -> constraints.Constraint:
        raise NotImplementedError
