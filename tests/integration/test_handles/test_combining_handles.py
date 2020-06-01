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

import time

import pytest

from htcondor import jobs


def test_and_of_cluster_handles_gives_right_number_of_jobs_in_query(long_sleep):
    a = jobs.submit(long_sleep, count=1)
    b = jobs.submit(long_sleep, count=1)

    num_jobs = len(list((a & b).query()))

    assert num_jobs == 0


def test_or_of_cluster_handles_gives_right_number_of_jobs_in_query(long_sleep):
    a = jobs.submit(long_sleep, count=1)
    b = jobs.submit(long_sleep, count=1)

    num_jobs = len(list((a | b).query()))

    assert num_jobs == 2


def test_hold_half_of_cluster(long_sleep):
    a = jobs.submit(long_sleep, count=4)

    (a & "ProcID < 2").hold()
    time.sleep(5)

    assert a.state[:2] == [jobs.JobStatus.HELD, jobs.JobStatus.HELD]
    assert a.state.counts()[jobs.JobStatus.HELD] == 2
