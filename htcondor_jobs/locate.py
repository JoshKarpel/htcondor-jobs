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

from typing import Optional, Any, Union, TypeVar, Generic, Dict
import logging

import htcondor

from .types import T_COLLECTOR_LOCATION, T_SCHEDD_LOCATION

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def locate_collector(collector: T_COLLECTOR_LOCATION) -> htcondor.Collector:
    if collector is None:
        return htcondor.Collector()
    elif isinstance(collector, str):
        return htcondor.Collector(collector)
    return collector


def locate_schedd(
    collector: T_COLLECTOR_LOCATION, schedd: T_SCHEDD_LOCATION
) -> htcondor.Schedd:
    if schedd is None:
        return htcondor.Schedd(
            locate_collector(collector).locate(htcondor.DaemonTypes.Schedd)
        )
    elif isinstance(schedd, str):
        return htcondor.Schedd(
            locate_collector(collector).locate(htcondor.DaemonTypes.Schedd, schedd)
        )
    return schedd
