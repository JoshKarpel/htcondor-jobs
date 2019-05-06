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

from typing import Union, Iterator
import logging

import collections.abc

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())


class SubmitDescription(collections.abc.MutableMapping):
    def __init__(self, mapping=None, **descriptors: Union[str, int, float]):
        if mapping is None:
            mapping = {}
        self._descriptors = dict(mapping, **descriptors)

    def __getitem__(self, key: str) -> Union[str, int, float]:
        return self._descriptors[key]

    def __setitem__(self, key: str, value: Union[str, int, float]) -> None:
        self._descriptors[key] = value

    def __delitem__(self, key: str) -> None:
        del self._descriptors[key]

    def __iter__(self) -> Iterator[str]:
        yield from self._descriptors.keys()

    def __len__(self) -> int:
        return len(self._descriptors)

    def __str__(self) -> str:
        # todo: must get quoting rules right
        return "\n".join(f"{k} = {v}" for k, v in self.items())
