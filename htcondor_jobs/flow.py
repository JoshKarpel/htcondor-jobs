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
import itertools

import htcondor

from . import status, handles

from . import constraints, locate, status, exceptions

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())


def is_done(handle) -> bool:
    return all(s is status.JobStatus.COMPLETED for s in handle.state.values())


LOOP_MIN_DELAY = 0.25


def _extract(new_v):
    if isinstance(new_v, tuple):
        new_v, new_func = new_v
    else:
        new_v, new_func = new_v, is_done

    return new_v, new_func


def execute(*generators):
    """
    the generators should either yield handles,
    AND/OR yield an iterable of generators that themselves yield handles
    (they can do both/either as many times as they like)
    """
    generator_to_handle = {}
    for g in generators:
        v = next(g)
        if isinstance(v, tuple):
            v, func = v
        else:
            v, func = v, is_done
        generator_to_handle[g] = v, func
    subgens = collections.defaultdict(set)

    cycle = itertools.count(0)
    while len(generator_to_handle) != 0:
        c = next(cycle)
        logger.debug(f"starting flow loop {c}")
        loop_start = time.time()

        for gen, (hnd, func) in list(generator_to_handle.items()):
            if hnd is not None:
                logger.debug(f"state of handle {hnd} is {hnd.state}")

            if func(hnd):
                try:
                    next_hnd = next(gen)

                    # a handle, or a tuple with a handle and an edge function
                    if isinstance(next_hnd, handles.Handle) or (
                        isinstance(next_hnd, tuple)
                        and len(next_hnd) == 2
                        and callable(next_hnd[1])
                    ):
                        generator_to_handle[gen] = _extract(next_hnd)

                    # next_hnd is actually an iterable of generators that produce handles
                    else:
                        # get all of the subgenerators running
                        # and start tracking them in the flow loop
                        # and in the subgenerator tracker
                        for subgen in next_hnd:
                            generator_to_handle[subgen] = _extract(next(subgen))
                            subgens[gen].add(subgen)

                        # now we're really hacking
                        # replace the test function for the parent generator
                        # with a test for the subgenerators being done
                        # must use trick to early-bind gen into the lambda
                        generator_to_handle[gen] = (
                            None,
                            lambda _, gen=gen: len(subgens[gen]) == 0,
                        )
                except StopIteration:
                    logger.debug(f"handle {hnd} satisfied {func}, moving on")
                    generator_to_handle.pop(gen)
                    for subs in subgens.values():
                        subs.discard(gen)

        loop_time = time.time() - loop_start
        sleep = max(LOOP_MIN_DELAY - loop_time, 0)

        logger.debug(
            f"finished flow loop {c} in {loop_time:.6f} seconds, sleeping {sleep:.6f} seconds until next loop"
        )
        time.sleep(sleep)
