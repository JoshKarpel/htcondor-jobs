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

from typing import Optional, Any, Mapping, Iterable

import time
import enum


class StrEnum(str, enum.Enum):
    pass


def chain_get(mapping: Mapping, keys: Iterable[str], default: Optional[Any] = None):
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


class Timer:
    def __init__(self):
        self.start = None
        self.end = None

    @property
    def elapsed(self):
        """The elapsed time in seconds from the start of the timer to the end."""
        if self.start is None:
            raise ValueError("Timer hasn't started yet!")

        if self.end is None:
            raise ValueError("Timer hasn't stopped yet!")

        return self.end - self.start

    def __enter__(self):
        self.start = time.monotonic()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = time.monotonic()
