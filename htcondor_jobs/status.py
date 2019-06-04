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
        self._clusterid = handle.clusterid
        self._offset = handle.first_proc

        event_log_path = Path(handle.clusterad["UserLog"])
        self._events = htcondor.JobEventLog(event_log_path.as_posix()).events(0)

        # can trade time for memory by using an array.array here with code "B"
        # 9 bytes -> 1 byte per job
        # but will need to wrap output of __getitem__ in JobStatus to convert back
        self._data = self._initial_data(handle)

        self._summary = collections.Counter(JobStatus(js) for js in self._data)

    def _initial_data(self, handle):
        return [JobStatus.UNMATERIALIZED for _ in range(len(handle))]

    def _update(self):
        for event in self._events:
            if event.cluster != self._clusterid:
                continue
            new_status = JOB_EVENT_STATUS_TRANSITIONS.get(event.type, None)
            if new_status is not None:
                key = event.proc - self._offset

                # update summary
                old_status = self._data[key]
                self._summary[old_status] -= 1
                self._summary[new_status] += 1

                # set new status on job
                self._data[key] = new_status

    def __getitem__(self, proc) -> JobStatus:
        self._update()
        return self._data[proc - self._offset]

    @property
    def summary(self):
        self._update()
        return self._summary

    def __iter__(self):
        yield from self._data

    def __str__(self):
        return str(self._data)

    def __repr__(self):
        return repr(self._data)

    def __len__(self):
        return len(self._data)


class CompactClusterState(ClusterState):
    def _initial_data(self, handle):
        return array.array("B", [JobStatus.UNMATERIALIZED for _ in range(len(handle))])

    def __getitem__(self, proc):
        return JobStatus(super().__getitem__(proc))
