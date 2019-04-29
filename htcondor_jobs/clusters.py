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

import htcondor

from . import handles


class Cluster:
    def __init__(self, descriptors: dict = None, count: int = 1, itemdata=None):
        if descriptors is None:
            descriptors = {}
        self.descriptors = htcondor.Submit(descriptors)

        self.count = count

        self.itemdata = itemdata

    @property
    def count(self) -> int:
        return self._count

    @count.setter
    def count(self, value: int):
        self._count = value
        self.descriptors.setQArgs(self._count)

    def __getitem__(self, key):
        return self.descriptors[key]

    def __setitem__(self, key, value):
        self.descriptors[key] = value

    def __delitem__(self, key):
        del self.descriptors[key]

    def keys(self):
        return self.descriptors.keys()

    def values(self):
        return self.descriptors.values()

    def items(self):
        return self.descriptors.items()

    def _queue(self, transaction: htcondor.Transaction):
        result = self.queue_with_itemdata(transaction, itemdata=self.itemdata)

        return handles.ClusterHandle(
            clusterid=result.cluster(), clusterad=result.clusterad()
        )
