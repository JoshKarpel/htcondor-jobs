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

from typing import List, Optional, Union, Iterator, Any
import logging

import abc
import time


import htcondor
import classad

from . import constraints, locate, status, exceptions

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())


class Handle(abc.ABC):
    """
    A handle for a group of jobs defined by a constraint, given as a string.
    The handle can be used to query, act on, or edit those jobs.
    """

    def __init__(
        self, collector: Optional[str] = None, scheduler: Optional[str] = None
    ):
        self.collector = collector
        self.scheduler = scheduler

    @property
    def constraint_string(self) -> str:
        raise NotImplementedError

    def __repr__(self):
        return f"{self.__class__.__name__}(constraint = {self.constraint_string})"

    @property
    def schedd(self):
        return locate.get_schedd(self.collector, self.scheduler)

    def query(
        self,
        projection: List[str] = None,
        opts: Optional[htcondor.QueryOpts] = None,
        limit: Optional[int] = None,
    ) -> Iterator[classad.ClassAd]:
        """
        Query against this set of jobs.

        Parameters
        ----------
        projection
            The :class:`classad.ClassAd` attributes to retrieve, as a list of case-insensitive strings.
            If ``None`` (the default), all attributes will be returned.
        opts
        limit
            The total number of matches to return from the query.
            If ``None`` (the default), return all matches.

        Returns
        -------
        ads :
            An iterator over the :class:`classad.ClassAd` that match the constraint.
        """
        if projection is None:
            projection = []

        if opts is None:
            opts = htcondor.QueryOpts.Default

        if limit is None:
            limit = -1

        cs = self.constraint_string
        logger.info(
            f"Executing query against schedd {self.schedd} with constraint {cs}, projection {projection}, and limit {limit}"
        )
        return self.schedd.xquery(cs, projection=projection, opts=opts, limit=limit)

    def _act(self, action: htcondor.JobAction) -> classad.ClassAd:
        cs = self.constraint_string
        logger.info(
            f"Executing action {action} against schedd {self.schedd} with constraint {cs}"
        )
        return self.schedd.act(action, cs)

    def remove(self) -> classad.ClassAd:
        """
        Remove jobs from the queue.

        Returns
        -------
        ad
            An ad describing the results of the action.
        """
        return self._act(htcondor.JobAction.Remove)

    def hold(self) -> classad.ClassAd:
        """
        Hold jobs.

        Returns
        -------
        ad
            An ad describing the results of the action.
        """
        return self._act(htcondor.JobAction.Hold)

    def release(self) -> classad.ClassAd:
        """
        Release held jobs.
        They will return to the queue in the idle state.

        Returns
        -------
        ad
            An ad describing the results of the action.
        """
        return self._act(htcondor.JobAction.Release)

    def pause(self) -> classad.ClassAd:
        """
        Pause jobs.
        Jobs will stop running, but will hold on to their claimed resources.

        Returns
        -------
        ad
            An ad describing the results of the action.
        """
        return self._act(htcondor.JobAction.Suspend)

    def resume(self) -> classad.ClassAd:
        """
        Resume (un-pause) jobs.

        Returns
        -------
        ad
            An ad describing the results of the action.
        """
        return self._act(htcondor.JobAction.Continue)

    def vacate(self) -> classad.ClassAd:
        """
        Vacate running jobs.
        This will force them off of their current execute resource, causing them to become idle again.

        Returns
        -------
        ad
            An ad describing the results of the action.
        """
        return self._act(htcondor.JobAction.Vacate)

    def edit(self, attr: str, value: Union[str, int, float]) -> classad.ClassAd:
        """
        Edit attributes of jobs.

        .. warning ::
            Many attribute edits will not affect jobs that have already matched.
            For example, changing ``RequestMemory`` will not affect the memory allocation
            of a job that is already executing.
            In that case, you would need to vacate (or release the job if it was held)
            before the edit had the desired effect.

        Parameters
        ----------
        attr
            The attribute to edit. Case-insensitive.
        value
            The new value for the attribute.

        Returns
        -------
        ad
            An ad describing the results of the edit.
        """
        cs = self.constraint_string
        logger.info(
            f"Executing edit {attr} = {value} against schedd {self.schedd} with constraint {cs}"
        )
        return self.schedd.edit(cs, attr, str(value))


class ConstraintHandle(Handle):
    """
    A handle defined by a :class:`constraints.Constraint`.
    """

    def __init__(
        self,
        constraint: constraints.Constraint,
        collector: Optional[str] = None,
        scheduler: Optional[str] = None,
    ):
        super().__init__(collector=collector, scheduler=scheduler)

        self._constraint = constraint

    @property
    def constraint(self) -> constraints.Constraint:
        return self._constraint

    @property
    def constraint_string(self) -> str:
        return str(self.constraint)

    def __repr__(self):
        return f"{self.__class__.__name__}(constraint = {self.constraint})"

    def reduce(self) -> "ConstraintHandle":
        return ConstraintHandle(
            self.constraint.reduce(), collector=self.collector, scheduler=self.scheduler
        )

    def __and__(self, other: "ConstraintHandle") -> "ConstraintHandle":
        if not all(
            (self.collector == other.collector, self.scheduler == other.scheduler)
        ):
            raise exceptions.InvalidHandle(
                "Cannot construct a handle for separate schedds"
            )

        return ConstraintHandle(
            self.constraint & other.constraint,
            collector=self.collector,
            scheduler=self.scheduler,
        )

    def __or__(self, other: "ConstraintHandle") -> "ConstraintHandle":
        if not all(
            (self.collector == other.collector, self.scheduler == other.scheduler)
        ):
            raise exceptions.InvalidHandle(
                "Cannot construct a handle for separate schedds"
            )

        return ConstraintHandle(
            self.constraint | other.constraint,
            collector=self.collector,
            scheduler=self.scheduler,
        )

    def __eq__(self, other: Any) -> bool:
        return all(
            (
                isinstance(other, ConstraintHandle),
                self.constraint == other.constraint,
                self.collector == other.collector,
                self.scheduler == other.scheduler,
            )
        )


class ClusterHandle(ConstraintHandle):
    def __init__(
        self,
        submit_result: htcondor.SubmitResult,
        collector: Optional[str] = None,
        scheduler: Optional[str] = None,
    ):
        self.clusterid = submit_result.cluster()
        self.clusterad = submit_result.clusterad()
        self._first_proc = submit_result.first_proc()
        self._num_procs = submit_result.num_procs()

        super().__init__(
            constraint=constraints.InCluster(self.clusterid),
            collector=collector,
            scheduler=scheduler,
        )

        # must delay this until after init, because at this point the submit
        # transaction may not be done yet
        self._state = None

    def __int__(self):
        return self.clusterid

    def __repr__(self):
        batch_name = self.clusterad.get("JobBatchName", None)
        batch = f", JobBatchName = {batch_name}" if batch_name is not None else ""
        return f"{self.__class__.__name__}(ClusterID = {self.clusterid}{batch})"

    def __len__(self):
        return self._num_procs

    @property
    def first_proc(self):
        return self._first_proc

    @property
    def state(self) -> status.ClusterState:
        if self._state is None:
            # todo: use a CompactClusterState instead if cluster is large?
            self._state = status.ClusterState(self)
        return self._state

    def wait(
        self,
        timeout: Optional[Union[int, float]] = None,
        test_delay: Union[int, float] = 0.25,
    ):
        """
        Wait for all of the cluster's jobs to complete.

        Parameters
        ----------
        timeout
            The maximum amount of time to wait before raising a
            :class:`exceptions.WaitedTooLong` exception.
        test_delay
            The amount of time to wait between test loops.

        Returns
        -------
        elapsed_time :
            The amount of time spent waiting.
        """
        start_time = time.time()
        while self.state.counts[status.JobStatus.COMPLETED] != len(self):
            if timeout is not None and time.time() > start_time + timeout:
                raise exceptions.WaitedTooLong
            time.sleep(test_delay)
        return time.time() - start_time
