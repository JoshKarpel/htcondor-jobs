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

from typing import Optional, Union, List, Iterable

import collections.abc

import htcondor

from . import descriptions, handles, locate, exceptions

T_ITEMDATA = Iterable[Union[collections.abc.Sequence, collections.abc.Mapping]]


def submit(
    description: descriptions.SubmitDescription,
    count: Optional[int] = 1,
    itemdata: Optional[T_ITEMDATA] = None,
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
        itemdata: Optional[T_ITEMDATA] = None,
    ):
        sub = htcondor.Submit(str(description))

        itemdata = list(itemdata)
        check_itemdata(itemdata)

        result = sub.queue_with_itemdata(self._txn, count, itemdata)
        handle = handles.ClusterHandle.from_submit_result(result)
        return handle

    def __enter__(self) -> "Transaction":
        self._schedd = locate.get_schedd(self.collector, self.scheduler)
        self._txn.__enter__()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._txn.__exit__(exc_type, exc_val, exc_tb)


def check_itemdata(
    itemdata: List[Union[collections.abc.Mapping, collections.abc.Sequence]]
):
    if len(itemdata) < 1:
        raise exceptions.InvalidItemdata("empty itemdata")

    if isinstance(itemdata[0], collections.abc.Mapping):
        return _check_itemdata_as_mappings(itemdata)
    elif isinstance(itemdata[0], collections.abc.Sequence):
        return _check_itemdata_as_sequences(itemdata)

    raise exceptions.InvalidItemdata("unknown problem")


def _check_itemdata_as_mappings(itemdata: List[collections.abc.Mapping]):
    first_item = itemdata[0]
    first_keys = set(first_item.keys())
    for item in itemdata:
        if len(set(item.keys()) - first_keys) != 0:
            raise exceptions.InvalidItemdata("key mismatch")


def _check_itemdata_as_sequences(itemdata: List[collections.abc.Sequence]):
    first_item = itemdata[0]
    first_len = len(first_item)
    for item in itemdata:
        if len(item) != first_len:
            raise exceptions.InvalidItemdata("bad len")
