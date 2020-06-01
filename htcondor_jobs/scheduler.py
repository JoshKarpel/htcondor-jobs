# Copyright 2020 HTCondor Team, Computer Sciences Department,
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

from typing import Union, Optional

import logging

import htcondor

from .submit import submit
from . import handles, personal
from .types import T_COLLECTOR_LOCATION, T_SCHEDD_LOCATION

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(
        self, collector: T_COLLECTOR_LOCATION = None, schedd: T_SCHEDD_LOCATION = None,
    ):
        self._collector = collector
        self._schedd = schedd
        self._personal_condor: Optional[personal.PersonalCondor] = None

    @classmethod
    def from_personal(cls, personal_condor: personal.PersonalCondor) -> "Scheduler":
        sched = cls(
            collector=personal_condor.collector(), schedd=personal_condor.schedd()
        )
        sched._personal_condor = personal_condor
        return sched

    @classmethod
    def persona(cls, *args, **kwargs) -> "Scheduler":
        pc = personal.PersonalCondor(*args, **kwargs)
        pc.start()
        return cls.from_personal(pc)

    @classmethod
    def local(cls) -> "Scheduler":
        return cls()

    @classmethod
    def remote(
        cls,
        collector: Optional[Union[htcondor.Collector, str]] = None,
        schedd: Optional[Union[htcondor.Schedd, str]] = None,
    ) -> "Scheduler":
        return cls(collector, schedd)

    def collector(self) -> htcondor.Collector:
        if self._collector is None:
            return htcondor.Collector()
        elif isinstance(self._collector, str):
            return htcondor.Collector(self._collector)
        return self._collector

    def schedd(self) -> htcondor.Schedd:
        if self._schedd is None:
            return htcondor.Schedd(self.collector().locate(htcondor.DaemonTypes.Schedd))
        elif isinstance(self._schedd, str):
            return htcondor.Schedd(
                self.collector().locate(htcondor.DaemonTypes.Schedd, self._schedd)
            )
        return self._schedd

    def query(
        self,
        constraint="true",
        projection=None,
        limit=-1,
        opts=htcondor.QueryOpts.Default,
    ):
        """
        Perform a job information query against the pool's schedd.

        Parameters
        ----------
        constraint
        projection
        limit
        opts

        Returns
        -------

        """
        if projection is None:
            projection = []

        ads = self.schedd().query(
            constraint=constraint, attr_list=projection, limit=limit, opts=opts
        )

        logger.debug(
            'Got {} ads from queue query with constraint "{}"'.format(
                len(ads), constraint
            )
        )

        return ads

    def act(self, action, constraint="true"):
        """
        Perform a job action against the pool's schedd.

        Parameters
        ----------
        action
        constraint

        Returns
        -------

        """
        logger.debug(
            'Executing action: {} with constraint "{}"'.format(action, constraint)
        )
        return self.schedd().act(action, constraint)

    def edit(self, attr, value, constraint="true"):
        """
        Perform a job attribute edit action against the pool's schedd.

        Parameters
        ----------
        attr
        value
        constraint

        Returns
        -------

        """
        logger.debug(
            'Executing edit: setting {} to {} with constraint "{}"'.format(
                attr, value, constraint
            )
        )
        return self.schedd().edit(constraint, attr, value)

    def submit(self, description, count=1, itemdata=None) -> handles.ClusterHandle:
        """
        Submit jobs to the pool.

        Parameters
        ----------
        description
        count
        itemdata

        Returns
        -------

        """
        return submit(
            description,
            count=count,
            itemdata=itemdata,
            collector=self.collector(),
            schedd=self.schedd(),
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._personal_condor is not None:
            self._personal_condor.stop()
