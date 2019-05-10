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

from typing import Optional, Any, Union
import logging

import time
import collections.abc

import htcondor

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())


class TimedCacheItem:
    __slots__ = ("timestamp", "value")

    def __init__(self, value: Any):
        self.timestamp = time.time()
        self.value = value


class TimedCache(collections.abc.MutableMapping):
    """
    As a dictionary, except that the entries expire after a specified amount of time.
    """

    def __init__(self, *, cache_time: Union[int, float]):
        """
        Parameters
        ----------
        cache_time
            The amount of time to store entries for, in seconds.
        """
        self.cache_time = cache_time
        self.cache = {}

    def __setitem__(self, key, value):
        self.cache[key] = TimedCacheItem(value)

    def __getitem__(self, key):
        item = self.cache[key]
        if self._is_item_expired(item):
            del self[key]
            raise KeyError(f"key {key} found, but has expired")
        return item.value

    def _is_item_expired(self, item: TimedCacheItem) -> bool:
        return time.time() > (item.timestamp + self.cache_time)

    def __delitem__(self, key):
        del self.cache[key]

    def __iter__(self):
        yield from self.cache

    def __len__(self):
        return len(self.cache)


SCHEDD_CACHE = TimedCache(cache_time=60)


def get_schedd(
    collector: Optional[str] = None, scheduler: Optional[str] = None
) -> htcondor.Schedd:
    """
    Get the :class:`htcondor.Schedd` that represents the HTCondor scheduler,
    as found by the collector and scheduler names given.

    This function caches its results for 60 seconds.

    Parameters
    ----------
    collector
    scheduler

    Returns
    -------
    schedd :
    """
    try:
        schedd = SCHEDD_CACHE[collector, scheduler]
    except KeyError:
        schedd = _locate_schedd(collector, scheduler)
        SCHEDD_CACHE[collector, scheduler] = schedd

    return schedd


def _locate_schedd(
    collector: Optional[str], scheduler: Optional[str]
) -> htcondor.Schedd:
    if scheduler is None:
        return htcondor.Schedd()

    coll = htcondor.Collector(collector)
    schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, scheduler)
    return htcondor.Schedd(schedd_ad)
