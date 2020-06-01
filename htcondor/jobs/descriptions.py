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

from typing import Union, Iterator, MutableMapping, Mapping, Optional
import logging

import htcondor
import classad

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

T_SUBMIT_VALUE = Union[str, int, float, bool, classad.ExprTree]


class SubmitDescription(MutableMapping[str, T_SUBMIT_VALUE]):
    """
    Describes a single cluster of jobs.
    The description behaves like a dictionary of key-values pairs,
    where each pair is a submit descriptor
    (see `the manual <https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html#submit-description-file-commands>`_).
    :class:`SubmitDescription` supports the standard dictionary methods such as
    ``get``, ``setdefault``, ``keys``, ``items``, etc.,
    as well as the ``[]`` operator for both getting and setting.
    """

    def __init__(self, mapping: Optional[Mapping] = None, **descriptors: T_SUBMIT_VALUE):
        """
        Parameters
        ----------
        mapping
            An optional mapping which provides initial key-value pairs for the
            description.
        descriptors
            Additional submit descriptors, provided as keyword arguments.
        """
        if mapping is None:
            mapping = {}
        self._descriptors = dict(mapping, **descriptors)

    def __getitem__(self, key: str) -> T_SUBMIT_VALUE:
        return self._descriptors[key]

    def __setitem__(self, key: str, value: T_SUBMIT_VALUE) -> None:
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

    def as_submit(self) -> htcondor.Submit:
        """
        Generate a :class:`htcondor.Submit`
        from this :class:`SubmitDescription`.
        """
        return htcondor.Submit(str(self))

    def copy(self, **descriptors):
        """
        Produce a copy of this :class:`SubmitDescription`,
        with the given ``descriptors`` changed.
        """
        return self.__class__(self._descriptors, **descriptors)
