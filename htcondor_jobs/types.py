from typing import Optional, Union, Mapping, Sequence, TypeVar

import htcondor

T_COLLECTOR_LOCATION = Optional[Union[htcondor.Collector, str]]
T_SCHEDD_LOCATION = Optional[Union[htcondor.Schedd, str]]
T_ITEMDATA = Union[str, int, float]
T_ITEMDATA_MAPPING = Mapping[str, T_ITEMDATA]
T_ITEMDATA_SEQUENCE = Sequence[T_ITEMDATA]
T_ITEMDATA_ELEMENT = TypeVar(
    "T_ITEMDATA_ELEMENT", T_ITEMDATA_MAPPING, T_ITEMDATA_SEQUENCE
)
