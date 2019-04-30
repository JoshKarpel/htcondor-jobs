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

from typing import Optional, Dict, Union
import logging


import htcondor

from . import handles

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())


class Cluster:
    def __init__(
        self,
        descriptors: Optional[Dict[str, Union[str, int, float]]] = None,
        count: int = 1,
        itemdata: Optional[Dict[str, Union[str, int, float]]] = None,
    ):
        if descriptors is None:
            descriptors = {}
        self._descriptors = htcondor.Submit(descriptors)

        self.count = count
        self.itemdata = itemdata

    @property
    def count(self) -> int:
        return self._count

    @count.setter
    def count(self, value: int) -> None:
        self._count = value
        self._descriptors.setQArgs(self._count)

    def __getitem__(self, key: str) -> Union[str, int, float]:
        return self._descriptors[key]

    def __setitem__(self, key: str, value: Union[str, int, float]) -> None:
        self._descriptors[key] = value

    def __delitem__(self, key: str) -> None:
        del self._descriptors[key]

    def keys(self):
        return self._descriptors.keys()

    def values(self):
        return self._descriptors.values()

    def items(self):
        return self._descriptors.items()

    def _queue(self, transaction: htcondor.Transaction) -> handles.ClusterHandle:
        result = self._descriptors.queue_with_itemdata(
            transaction, itemdata=self.itemdata
        )

        return handles.ClusterHandle(
            clusterid=result.cluster(), clusterad=result.clusterad()
        )
