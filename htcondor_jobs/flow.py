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


LOOP_MIN_DELAY = 0.5


def _unpack_handle_and_test(next_handle):
    if isinstance(next_handle, tuple):
        next_handle, test = next_handle
    else:
        next_handle, test = next_handle, is_done

    return next_handle, test


class SubGeneratorTracker:
    def __init__(self):
        self.gen_to_subgens = collections.defaultdict(set)
        self.subgen_to_gen = dict()

    def add_subgen(self, gen, subgen):
        self.gen_to_subgens[gen].add(subgen)
        self.subgen_to_gen[subgen] = gen

    def rm_subgen(self, subgen):
        try:
            gen = self.subgen_to_gen.pop(subgen)
            self.gen_to_subgens[gen].discard(subgen)
        except KeyError:
            pass

    def get_subgens(self, gen):
        return self.gen_to_subgens[gen]


class Executor:
    def __init__(self, *generators):
        self.generator_to_handle_and_test = {}
        self.subgen_tracker = SubGeneratorTracker()

        for g in generators:
            self.advance(g)

    def execute(self):
        """
        the generators should either yield handles,
        AND/OR yield an iterable of generators that themselves yield handles
        (they can do both/either as many times as they like)
        """
        cycle = itertools.count(0)
        while len(self.generator_to_handle_and_test) != 0:
            c = next(cycle)
            logger.debug(f"starting flow loop {c}")
            loop_start = time.time()

            for gen, (hnd, test) in list(self.generator_to_handle_and_test.items()):
                logger.debug(
                    f"checking generator {gen} with handler {hnd} and test {test}"
                )
                if hnd is not None:
                    logger.debug(f"state of handle {hnd} is {hnd.state}")

                if not test(hnd):
                    continue

                logger.debug(f"handle {hnd} satisfied {test}, moving on")
                try:
                    self.advance(gen)
                except StopIteration:
                    logger.debug(f"generator {gen} exhausted")
                    self.generator_to_handle_and_test.pop(gen)
                    self.subgen_tracker.rm_subgen(gen)

            loop_time = time.time() - loop_start
            sleep = max(LOOP_MIN_DELAY - loop_time, 0)

            logger.debug(
                f"finished flow loop {c} in {loop_time:.6f} seconds, sleeping {sleep:.6f} seconds before next loop"
            )
            time.sleep(sleep)

    def advance(self, gen):
        next_hnd = next(gen)
        # a handle, or a tuple with a handle and an edge function
        if isinstance(next_hnd, handles.Handle) or (
            isinstance(next_hnd, tuple) and len(next_hnd) == 2 and callable(next_hnd[1])
        ):
            self.generator_to_handle_and_test[gen] = _unpack_handle_and_test(next_hnd)

        # next_hnd is actually an iterable of generators that produce handles
        else:
            # get all of the subgenerators running
            # and start tracking them in the flow loop
            # and in the subgenerator tracker
            for subgen in next_hnd:
                logger.debug(f"adding subgenerator {subgen} to {gen}")
                self.advance(subgen)
                self.subgen_tracker.add_subgen(gen, subgen)

            # now we're really hacking
            # replace the test function for the parent generator
            # with a test for the subgenerators being done
            # must use trick to early-bind gen into the lambda
            self.generator_to_handle_and_test[gen] = (
                None,
                lambda _, gen=gen: len(self.subgen_tracker.get_subgens(gen)) == 0,
            )


def execute(*generators):
    ex = Executor(*generators)
    ex.execute()
