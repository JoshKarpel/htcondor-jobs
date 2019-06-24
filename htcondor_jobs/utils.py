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

from typing import Optional, Any, Mapping

import enum


class StrEnum(str, enum.Enum):
    pass


class SlotPickleMixin:
    """A mixin class which lets classes with __slots__ be pickled."""

    __slots__ = ()

    def __getstate__(self):
        # get all the __slots__ in the inheritance tree
        # if any class has a __dict__, it will be included! no special case needed
        slots = sum((getattr(c, "__slots__", ()) for c in self.__class__.__mro__), ())

        state = dict(
            (slot, getattr(self, slot)) for slot in slots if hasattr(self, slot)
        )

        # __weakref__ should always be removed from the state dict
        state.pop("__weakref__", None)

        return state

    def __setstate__(self, state: Mapping):
        for slot, value in state.items():
            object.__setattr__(self, slot, value)


def chain_get(mapping: Mapping, *keys: str, default: Optional[Any] = None):
    """
    As Mapping.get(key, default), except that it will try multiple keys before returning the default.

    Parameters
    ----------
    mapping
        The :class:`collections.abc.Mapping` to get from.
    keys
        The keys to try, in order.
    default
        What to return if none of the keys are in the mapping.
        Defaults to ``None``.

    Returns
    -------
    val :
        The value of the first key that was in the mapping,
        or the ``default`` if none of the keys were in the mapping.
    """
    for k in keys:
        try:
            return mapping[k]
        except KeyError:
            pass

    return default
