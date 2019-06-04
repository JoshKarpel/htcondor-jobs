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
import os

import htcondor_jobs as jobs
from htcondor_jobs.locate import SCHEDD_CACHE


@pytest.fixture(scope="function", autouse=True)
def clear_schedd_cache():
    SCHEDD_CACHE.clear()


@pytest.fixture(scope="function", autouse=True)
def clear_queue():
    yield
    os.system("condor_rm --all")  # todo: do better!


@pytest.fixture(scope="function")
def long_sleep(tmp_path):
    return jobs.SubmitDescription(
        executable="/bin/sleep",
        arguments="5m",
        log=(tmp_path / "events.log").as_posix(),
    )


@pytest.fixture(scope="function")
def short_sleep(tmp_path):
    return jobs.SubmitDescription(
        executable="/bin/sleep",
        arguments="1s",
        log=(tmp_path / "events.log").as_posix(),
    )
