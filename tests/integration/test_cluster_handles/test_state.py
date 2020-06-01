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

from htcondor import jobs


def test_no_job_event_log():
    desc = jobs.SubmitDescription(executable="/bin/sleep", arguments="5m")

    handle = jobs.submit(desc, count=1)

    with pytest.raises(jobs.exceptions.NoJobEventLog):
        handle.state


def test_hold(long_sleep):
    handle = jobs.submit(long_sleep, count=1)

    handle.hold()

    assert handle.state[0] is jobs.JobStatus.HELD


def test_hold_count(long_sleep, tmp_path):
    handle = jobs.submit(long_sleep, count=1)

    handle.hold()

    assert handle.state.counts()[jobs.JobStatus.HELD] == 1


def test_is_complete(short_sleep):
    handle = jobs.submit(short_sleep, count=1)

    handle.wait(timeout=180)

    assert handle.state.all_complete()


def test_any_running(long_sleep):
    handle = jobs.submit(long_sleep, count=1)

    handle.wait(condition=lambda h: h.state[0] is jobs.JobStatus.RUNNING, timeout=180)

    assert handle.state.any_running()


def test_any_held(long_sleep):
    handle = jobs.submit(long_sleep, count=1)

    handle.hold()
    handle.wait(condition=lambda h: h.state[0] is jobs.JobStatus.HELD, timeout=180)

    assert handle.state.any_held()
