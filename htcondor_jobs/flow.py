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

from typing import List, Optional, Union, Iterator, Any
import logging


import abc
import time
import collections

import htcondor

from . import status, handles

from . import constraints, locate, status, exceptions

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())


def is_done(handle) -> bool:
    return all(s is status.JobStatus.COMPLETED for s in handle.state.values())


LOOP_MIN_DELAY = 0.25


def execute(*generators):
    """
    the generators should either yield handles,
    AND/OR yield an iterable of generators that themselves yield handles
    (they can do both/either as many times as they like)
    """
    generator_to_handle = {g: next(g) for g in generators}
    subgens = collections.defaultdict(set)

    while len(generator_to_handle) != 0:
        loop_start = time.time()
        add = []
        done = []
        for gen, hnd in generator_to_handle.items():
            # print("gen", gen, "hnd", hnd)
            print(hnd, hnd.state if hnd is not True else hnd)
            if len(subgens[gen]) == 0 and (hnd is True or is_done(hnd)):
                try:
                    v = next(gen)
                    # print("next(gen)", v)
                    if not isinstance(v, handles.Handle):
                        for subgen in v:
                            add.append(subgen)
                            subgens[gen].add(subgen)
                        generator_to_handle[gen] = True
                    else:
                        generator_to_handle[gen] = v
                except StopIteration:
                    print(f"{hnd} finished")
                    done.append(gen)
        for a in add:
            generator_to_handle[a] = next(a)
        for d in done:
            generator_to_handle.pop(d)
            for subs in subgens.values():
                subs.discard(d)
        time.sleep(max(LOOP_MIN_DELAY - (time.time() - loop_start), 0))
        # print()
