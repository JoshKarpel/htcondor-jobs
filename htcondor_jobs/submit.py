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

from typing import Optional, Union, List, Iterable, Mapping, Sequence, TypeVar
import logging

import collections.abc

import htcondor

from . import descriptions, handles, locate, exceptions

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())

T_ITEMDATA = Union[str, int, float]
T_ITEMDATA_MAPPING = Mapping[str, T_ITEMDATA]
T_ITEMDATA_SEQUENCE = Sequence[T_ITEMDATA]
T_ITEMDATA_ELEMENT = TypeVar(
    "T_ITEMDATA_ELEMENT", T_ITEMDATA_MAPPING, T_ITEMDATA_SEQUENCE
)


def submit(
    description: descriptions.SubmitDescription,
    count: Optional[int] = 1,
    itemdata: Optional[Iterable[T_ITEMDATA_ELEMENT]] = None,
    collector: Optional[str] = None,
    scheduler: Optional[str] = None,
) -> handles.ClusterHandle:
    """
    Submit a single cluster of jobs based on a submit description.

    Parameters
    ----------
    description
        A submit description.
    count
        The number of jobs to submit **for each element of the itemdata**.
        If ``itemdata`` is ``None``, this is the total number of jobs to submit.
    itemdata
    collector
    scheduler

    Returns
    -------
    handle : :class:`ClusterHandle`
        A handle pointing to the jobs that were submitted.
    """
    with Transaction(collector=collector, scheduler=scheduler) as txn:
        handle = txn.submit(description, count, itemdata)

    return handle


class Transaction:
    def __init__(
        self, collector: Optional[str] = None, scheduler: Optional[str] = None
    ):
        """
        Open a transaction with a schedd.
        If you are submitting many clusters at once,
        you should do so on a single transaction.

        Parameters
        ----------
        collector
        scheduler
        """
        self.collector = collector
        self.scheduler = scheduler

        self._schedd: Optional[htcondor.Schedd] = None
        self._txn: Optional[htcondor.Transaction] = None

    def submit(
        self,
        description: descriptions.SubmitDescription,
        count: Optional[int] = 1,
        itemdata: Optional[Iterable[T_ITEMDATA_ELEMENT]] = None,
    ) -> handles.ClusterHandle:
        """
        Identical to :func:`submit`,
        except without the ``collector`` and ``scheduler`` arguments,
        which are instead given to the :class:`Transaction`.
        """
        if any((self._schedd is None, self._txn is None)):
            raise exceptions.UninitializedTransaction(
                "the transaction has not been initialized (use it as a context manager)"
            )

        sub = description.as_submit()

        if itemdata is not None:
            itemdata = list(itemdata)
            check_itemdata(itemdata)
            itemdata_msg = f" and {len(itemdata)} elements of itemdata"
        else:
            itemdata_msg = ""

        result = sub.queue_with_itemdata(self._txn, count, itemdata)
        handle = handles.ClusterHandle.from_submit_result(result)

        logger.info(
            f"Submitted {repr(sub)} to {self._schedd} on transaction {self._txn} with count {count}{itemdata_msg}"
        )

        return handle

    def __enter__(self) -> "Transaction":
        self._schedd = locate.get_schedd(self.collector, self.scheduler)
        self._txn = self._schedd.transaction()
        self._txn.__enter__()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._txn.__exit__(exc_type, exc_val, exc_tb)


def check_itemdata(itemdata: List[T_ITEMDATA_ELEMENT]) -> None:
    if len(itemdata) < 1:
        raise exceptions.InvalidItemdata("empty itemdata")

    if isinstance(itemdata[0], collections.abc.Mapping):
        return _check_itemdata_as_mappings(itemdata)
    elif isinstance(itemdata[0], collections.abc.Sequence):
        return _check_itemdata_as_sequences(itemdata)

    raise exceptions.InvalidItemdata("illegal itemdata type")


def _check_itemdata_as_mappings(itemdata: List[T_ITEMDATA_MAPPING]) -> None:
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


def _check_itemdata_as_sequences(itemdata: List[T_ITEMDATA_SEQUENCE]) -> None:
    """All of the provided itemdata must be the same length."""
    first_item = itemdata[0]
    first_len = len(first_item)
    for item in itemdata:
        # same length
        if len(item) != first_len:
            raise exceptions.InvalidItemdata("bad len")
