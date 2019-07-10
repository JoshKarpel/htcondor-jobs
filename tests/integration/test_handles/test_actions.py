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


def get_status(handle):
    ad = next(handle.query(projection=["JobStatus"]))
    return ad["JobStatus"]


def test_hold(long_sleep):
    handle = jobs.submit(long_sleep, count=1)

    handle.hold()

    time.sleep(5)

    status = get_status(handle)
    assert status == jobs.JobStatus.HELD


@pytest.mark.parametrize(
    "action", ["remove", "hold", "release", "pause", "resume", "vacate"]
)
def test_actions_will_execute(long_sleep, action):
    handle = jobs.submit(long_sleep, count=1)

    getattr(handle, action)()
