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

import logging

import enum
import array
import collections
from pathlib import Path
import functools
import weakref
from typing import MutableSequence, Union

import htcondor

from . import handles, utils, exceptions

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class JobStatus(enum.IntEnum):
    IDLE = 1
    RUNNING = 2
    REMOVED = 3
    COMPLETED = 4
    HELD = 5
    TRANSFERRING_OUTPUT = 6
    SUSPENDED = 7
    UNMATERIALIZED = 100


JOB_EVENT_STATUS_TRANSITIONS = {
    htcondor.JobEventType.SUBMIT: JobStatus.IDLE,
    htcondor.JobEventType.JOB_EVICTED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_UNSUSPENDED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_RELEASED: JobStatus.IDLE,
    htcondor.JobEventType.SHADOW_EXCEPTION: JobStatus.IDLE,
    htcondor.JobEventType.JOB_RECONNECT_FAILED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_TERMINATED: JobStatus.COMPLETED,
    htcondor.JobEventType.EXECUTE: JobStatus.RUNNING,
    htcondor.JobEventType.JOB_HELD: JobStatus.HELD,
    htcondor.JobEventType.JOB_SUSPENDED: JobStatus.SUSPENDED,
    htcondor.JobEventType.JOB_ABORTED: JobStatus.REMOVED,
}

NO_EVENT_LOG = object()


def update_before(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        self._update()
        return func(self, *args, **kwargs)

    return wrapper


class ClusterState:
    """
    A class that manages the state of the cluster tracked by a :class:`ClusterHandle`.
    It reads from the cluster's event log internally and provides a variety of views
    of the individual job states.

    .. warning::

        :class:`ClusterState` objects should not be instantiated manually.
        :class:`ClusterHandle` will create them automatically when needed.
    """

    def __init__(self, handle: "handles.ClusterHandle"):
        self._handle = weakref.proxy(handle)
        self._clusterid = handle.clusterid
        self._offset = handle.first_proc

        raw_event_log_path = utils.chain_get(
            handle.clusterad, ("UserLog", "DAGManNodesLog"), default=NO_EVENT_LOG
        )
        if raw_event_log_path is NO_EVENT_LOG:
            raise exceptions.NoJobEventLog(
                "this cluster does not have a job event log, so it cannot track job state"
            )
        self._event_log_path = Path(raw_event_log_path).absolute()

        self._events = None

        self._data = self._make_initial_data(handle)
        self._counts = collections.Counter(JobStatus(js) for js in self._data)

    def _make_initial_data(self, handle: "handles.ClusterHandle") -> MutableSequence:
        return [JobStatus.UNMATERIALIZED for _ in range(len(handle))]

    def _update(self):
        if self._events is None:
            logger.debug(
                f"Looking for event log for handle {self._handle} at {self._event_log_path}"
            )
            self._events = htcondor.JobEventLog(self._event_log_path.as_posix()).events(
                0
            )
            logger.debug(
                f"Initialized event log reader for handle {self._handle}, targeting {self._event_log_path}"
            )

        with utils.Timer() as timer:
            num_handled = 0
            num_skipped = 0
            for event in self._events:
                if event.cluster != self._clusterid:
                    num_skipped += 1
                    continue

                num_handled += 1

                new_status = JOB_EVENT_STATUS_TRANSITIONS.get(event.type, None)
                if new_status is not None:
                    key = event.proc - self._offset

                    # update counts
                    old_status = self._data[key]
                    self._counts[old_status] -= 1
                    self._counts[new_status] += 1

                    # set new status on individual job
                    self._data[key] = new_status

        if num_handled > 0:
            logger.debug(
                f"Handled {num_handled} new events (skipped {num_skipped}) for {self._handle} (took {timer.elapsed:.2f} seconds)."
            )

    @update_before
    def __getitem__(self, proc: Union[int, slice]) -> JobStatus:
        if isinstance(proc, int):
            return self._data[proc - self._offset]
        elif isinstance(proc, slice):
            start, stop, stride = proc.indices(len(self))
            return self._data[start - self._offset : stop - self._offset : stride]

    @update_before
    def counts(self) -> collections.Counter:
        """
        Return the number of jobs in each :class:`JobStatus`, as a :class:`collections.Counter`.
        """
        return self._counts.copy()

    @update_before
    def __iter__(self):
        yield from self._data

    @update_before
    def __str__(self):
        return str(self._data)

    @update_before
    def __repr__(self):
        return repr(self._data)

    def __len__(self):
        return len(self._data)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._handle == other._handle

    def all_complete(self) -> bool:
        """
        Return ``True`` if **all** of the jobs in the cluster are complete.
        Note that this definition includes jobs that have left the queue,
        not just ones that are in the "Completed" state in the queue.
        """
        return self.counts()[JobStatus.COMPLETED] == len(self)

    def any_complete(self) -> bool:
        """
        Return ``True`` if **any** of the jobs in the cluster are complete.
        Note that this definition includes jobs that have left the queue,
        not just ones that are in the "Completed" state in the queue.
        """
        return self.counts()[JobStatus.COMPLETED] > 0

    def any_idle(self) -> bool:
        """Return ``True`` if **any** of the jobs in the cluster are idle."""
        return self.counts()[JobStatus.IDLE] > 0

    def any_running(self) -> bool:
        """Return ``True`` if **any** of the jobs in the cluster are running."""
        return self.counts()[JobStatus.RUNNING] > 0

    def any_held(self) -> bool:
        """Return ``True`` if **any** of the jobs in the cluster are held."""
        return self.counts()[JobStatus.HELD] > 0


class CompactClusterState(ClusterState):
    """
    A specialized :class:`ClusterState` that uses a more compact
    internal data structure for storing job state.
    """

    # The internal storage is an unsigned byte array.
    # Because JobStatus is an IntEnum, we can insert JobStatus values directly
    # as long as they're small.
    # However, when they come back out, they'll just be integers, and we need
    # to turn them back into JobStatus.

    def _make_initial_data(self, handle: "handles.ClusterHandle") -> MutableSequence:
        return array.array("B", [JobStatus.UNMATERIALIZED for _ in range(len(handle))])

    def __getitem__(self, proc: int):
        return JobStatus(super().__getitem__(proc))
