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

from typing import Optional, Union, List, Iterable, Mapping, Sequence, Any
import logging

import collections.abc

import htcondor

from . import descriptions, handles, locate, exceptions

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())

T_ITEMDATA_ELEMENT = Union[Sequence, Mapping[str, Any]]


def submit(
    description: descriptions.SubmitDescription,
    count: Optional[int] = 1,
    itemdata: Optional[Iterable[T_ITEMDATA_ELEMENT]] = None,
    collector: Optional[str] = None,
    scheduler: Optional[str] = None,
):
    with Transaction(collector=collector, scheduler=scheduler) as txn:
        handle = txn.submit(description, count, itemdata)

    return handle


class Transaction:
    def __init__(
        self, collector: Optional[str] = None, scheduler: Optional[str] = None
    ):
        self.collector = collector
        self.scheduler = scheduler

        self._submits = []
        self._txn = None
        self._schedd = None

    def submit(
        self,
        description: descriptions.SubmitDescription,
        count: Optional[int] = 1,
        itemdata: Optional[Iterable[T_ITEMDATA_ELEMENT]] = None,
    ):
        sub = htcondor.Submit(str(description))

        if itemdata is not None:
            itemdata = list(itemdata)
            check_itemdata(itemdata)
            itemdata_msg = f" and {len(itemdata)} elements of itemdata"
        else:
            itemdata_msg = ""

        result = sub.queue_with_itemdata(self._txn, count, itemdata)
        handle = handles.ClusterHandle.from_submit_result(result)

        logger.info(
            f"Submitted {sub} to {self._schedd} on transaction {self._txn} with count {count}{itemdata_msg}"
        )

        return handle

    def __enter__(self) -> "Transaction":
        self._schedd = locate.get_schedd(self.collector, self.scheduler)
        self._txn.__enter__()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._txn.__exit__(exc_type, exc_val, exc_tb)


def check_itemdata(itemdata: List[T_ITEMDATA_ELEMENT]):
    if len(itemdata) < 1:
        raise exceptions.InvalidItemdata("empty itemdata")

    if isinstance(itemdata[0], collections.abc.Mapping):
        return _check_itemdata_as_mappings(itemdata)
    elif isinstance(itemdata[0], collections.abc.Sequence):
        return _check_itemdata_as_sequences(itemdata)

    raise exceptions.InvalidItemdata("unknown problem")


def _check_itemdata_as_mappings(itemdata: List[Mapping]):
    """All of the provided itemdata must have exactly identical keys, which must be strings."""
    first_item = itemdata[0]
    first_keys = set(first_item.keys())
    for item in itemdata:
        # keys must be strings
        if any(not isinstance(key, str) for key in item.keys()):
            raise exceptions.InvalidItemdata("keys must be strings")

        # key sets must all be the same
        if len(set(item.keys()) - first_keys) != 0:
            raise exceptions.InvalidItemdata("key mismatch")


def _check_itemdata_as_sequences(itemdata: List[Sequence]):
    """All of the provided itemdata must be the same length."""
    first_item = itemdata[0]
    first_len = len(first_item)
    for item in itemdata:
        # same length
        if len(item) != first_len:
            raise exceptions.InvalidItemdata("bad len")
