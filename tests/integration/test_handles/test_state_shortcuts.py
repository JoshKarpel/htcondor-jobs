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

import pytest

import time

import htcondor_jobs as jobs


def test_complete(short_sleep):
    handle = jobs.submit(short_sleep, count=1)

    handle.wait(timeout=180)

    assert handle.state.is_complete()


def test_is_in_queue_when_idle(long_sleep):
    handle = jobs.submit(long_sleep, count=1)

    handle.wait(condition=lambda h: h.state[0] is jobs.JobStatus.IDLE)
    # yes, it could start running now... oh well
    assert handle.state.is_in_queue()


def test_is_in_queue_when_held(long_sleep):
    handle = jobs.submit(long_sleep, count=1)

    handle.hold()
    handle.wait(condition=lambda h: h.state[0] is jobs.JobStatus.HELD)

    assert handle.state.is_in_queue()


def test_is_in_queue_when_running(long_sleep):
    handle = jobs.submit(long_sleep, count=1)

    handle.wait(condition=lambda h: h.state[0] is jobs.JobStatus.RUNNING)

    assert handle.state.is_in_queue()


def test_is_running(long_sleep):
    handle = jobs.submit(long_sleep, count=1)

    handle.wait(condition=lambda h: h.state[0] is jobs.JobStatus.RUNNING)

    assert handle.state.is_running()
