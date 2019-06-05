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

import htcondor

from . import handles, exceptions

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())


class JobStatus(enum.IntEnum):
    IDLE = 1
    RUNNING = 2
    REMOVED = 3
    COMPLETED = 4
    HELD = 5
    TRANSFERRING_OUTPUT = 6
    SUSPENDED = 7  # todo: ?
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


class ClusterState:
    def __init__(self, handle: "handles.ClusterHandle"):
        self._handle = handle
        self._clusterid = handle.clusterid
        self._offset = handle.first_proc

        try:
            event_log_path = Path(handle.clusterad["UserLog"]).absolute()
        except KeyError:
            raise exceptions.NoJobEventLog(
                "this cluster does not have a job event log, so it cannot track job state"
            )
        self._events = htcondor.JobEventLog(event_log_path.as_posix()).events(0)

        self._data = self._make_initial_data(handle)
        self._counts = collections.Counter(JobStatus(js) for js in self._data)

    def _make_initial_data(self, handle: "handles.ClusterHandle"):
        return [JobStatus.UNMATERIALIZED for _ in range(len(handle))]

    def _update(self):
        logger.debug(f"triggered status update for handle {self._handle}")

        for event in self._events:
            if event.cluster != self._clusterid:
                continue

            new_status = JOB_EVENT_STATUS_TRANSITIONS.get(event.type, None)
            if new_status is not None:
                key = event.proc - self._offset

                # update counts
                old_status = self._data[key]
                self._counts[old_status] -= 1
                self._counts[new_status] += 1

                # set new status on individual job
                self._data[key] = new_status

        logger.debug(f"new status counts for {self._handle}: {self._counts}")

    def __getitem__(self, proc: int) -> JobStatus:
        self._update()
        return self._data[proc - self._offset]

    @property
    def counts(self):
        self._update()
        return self._counts

    def __iter__(self):
        self._update()
        yield from self._data

    def __str__(self):
        self._update()
        return str(self._data)

    def __repr__(self):
        self._update()
        return repr(self._data)

    def __len__(self):
        return len(self._data)

    def is_complete(self) -> bool:
        """Return ``True`` if **all** of the jobs in the cluster are complete."""
        return self.counts[JobStatus.COMPLETED] == len(self)

    def any_running(self) -> bool:
        """Return ``True`` if **any** of the jobs in the cluster are running."""
        return self.counts[JobStatus.RUNNING] > 0

    def any_in_queue(self) -> bool:
        """Return ``True`` if **any** of the jobs in the cluster are still in the queue (idle, running, or held)."""
        c = self.counts
        jobs_in_queue = sum(
            (c[JobStatus.IDLE], c[JobStatus.RUNNING], c[JobStatus.HELD])
        )
        return jobs_in_queue > 0

    def any_held(self) -> bool:
        """Return ``True`` if **any** of the jobs in the cluster are held."""
        return self.counts[JobStatus.HELD] > 0


class CompactClusterState(ClusterState):
    def _make_initial_data(self, handle: "handles.ClusterHandle"):
        return array.array("B", [JobStatus.UNMATERIALIZED for _ in range(len(handle))])

    def __getitem__(self, proc: int):
        return JobStatus(super().__getitem__(proc))
