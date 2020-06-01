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

from typing import List, Optional, Union, Iterator, Any, Callable
import logging

import abc
import time
from pathlib import Path
import pickle

import htcondor
import classad

from . import locate, status, utils, exceptions

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Handle(abc.ABC):
    """
    A connection to a set of jobs defined by a constraint.
    The handle can be used to query, act on, or edit those jobs.
    """

    def __init__(self, collector: Optional[str] = None, scheduler: Optional[str] = None):
        self.collector = collector
        self.scheduler = scheduler

    @property
    def constraint_string(self) -> str:
        raise NotImplementedError

    def __repr__(self):
        return f"{self.__class__.__name__}(constraint = {self.constraint_string})"

    def __eq__(self, other):
        return all(
            (
                isinstance(other, self.__class__),
                self.constraint_string == other.constraint_string,
                self.collector == other.collector,
                self.scheduler == other.scheduler,
            )
        )

    def __hash__(self):
        return hash((self.__class__, self.constraint_string, self.collector, self.scheduler))

    @property
    def schedd(self):
        return locate.locate_schedd(self.collector, self.scheduler)

    def query(
        self,
        projection: List[str] = None,
        options: Optional[htcondor.QueryOpts] = None,
        limit: Optional[int] = None,
    ) -> Iterator[classad.ClassAd]:
        """
        Query against this set of jobs.

        Parameters
        ----------
        projection
            The :class:`classad.ClassAd` attributes to retrieve, as a list of case-insensitive strings.
            If ``None`` (the default), all attributes will be returned.
        options
        limit
            The total number of matches to return from the query.
            If ``None`` (the default), return all matches.

        Returns
        -------
        ads : Iterator[:class:`classad.ClassAd`]
            An iterator over the :class:`classad.ClassAd` that match the constraint.
        """
        if projection is None:
            projection = []

        if options is None:
            options = htcondor.QueryOpts.Default

        if limit is None:
            limit = -1

        cs = self.constraint_string
        logger.info(
            f"Executing query against schedd {self.schedd} with constraint {cs}, projection {projection}, and limit {limit}"
        )
        return self.schedd.query(cs, attr_list=projection, opts=options, limit=limit)

    def _act(self, action: htcondor.JobAction) -> classad.ClassAd:
        cs = self.constraint_string
        logger.info(f"Executing action {action} against schedd {self.schedd} with constraint {cs}")
        return self.schedd.act(action, cs)

    def remove(self) -> classad.ClassAd:
        """
        Remove jobs from the queue.

        Returns
        -------
        ad : :class:`classad.ClassAd`
            An ad describing the results of the action.
        """
        return self._act(htcondor.JobAction.Remove)

    def hold(self) -> classad.ClassAd:
        """
        Hold jobs.

        Returns
        -------
        ad : :class:`classad.ClassAd`
            An ad describing the results of the action.
        """
        return self._act(htcondor.JobAction.Hold)

    def release(self) -> classad.ClassAd:
        """
        Release held jobs.
        They will return to the queue in the idle state.

        Returns
        -------
        ad : :class:`classad.ClassAd`
            An ad describing the results of the action.
        """
        return self._act(htcondor.JobAction.Release)

    def pause(self) -> classad.ClassAd:
        """
        Pause jobs.
        Jobs will stop running, but will hold on to their claimed resources.

        Returns
        -------
        ad : :class:`classad.ClassAd`
            An ad describing the results of the action.
        """
        return self._act(htcondor.JobAction.Suspend)

    def resume(self) -> classad.ClassAd:
        """
        Resume (un-pause) jobs.

        Returns
        -------
        ad : :class:`classad.ClassAd`
            An ad describing the results of the action.
        """
        return self._act(htcondor.JobAction.Continue)

    def vacate(self) -> classad.ClassAd:
        """
        Vacate running jobs.
        This will force them off of their current execute resource, causing them to become idle again.

        Returns
        -------
        ad : :class:`classad.ClassAd`
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
        ad : :class:`classad.ClassAd`
            An ad describing the results of the edit.
        """
        cs = self.constraint_string
        logger.info(
            f"Executing edit {attr} = {value} against schedd {self.schedd} with constraint {cs}"
        )
        return self.schedd.edit(cs, attr, str(value))


class ConstraintHandle(Handle):
    """
    A connection to a set of jobs defined by a :class:`Constraint`.
    The handle can be used to query, act on, or edit those jobs.
    """

    def __init__(
        self,
        constraint: Union[classad.ExprTree, str],
        collector: Optional[str] = None,
        scheduler: Optional[str] = None,
    ):
        super().__init__(collector=collector, scheduler=scheduler)

        if isinstance(constraint, str):
            constraint = classad.ExprTree(constraint)
        self._constraint = constraint

    @property
    def constraint(self) -> classad.ExprTree:
        return self._constraint

    @property
    def constraint_string(self) -> str:
        return str(self.constraint)

    def __repr__(self):
        return f"{self.__class__.__name__}(constraint = {self.constraint})"

    def __and__(
        self, other: Union["ConstraintHandle", classad.ExprTree, str]
    ) -> "ConstraintHandle":
        return self._combine(other, classad.ExprTree.and_)

    def __or__(self, other: Union["ConstraintHandle", classad.ExprTree, str]) -> "ConstraintHandle":
        return self._combine(other, classad.ExprTree.or_)

    def _combine(self, other: Union["ConstraintHandle", classad.ExprTree, str], combinator):
        if isinstance(other, ConstraintHandle) and (
            self.collector != other.collector or self.scheduler != other.scheduler
        ):
            raise exceptions.InvalidHandle("Cannot construct a handle for separate schedds")

        if isinstance(other, ConstraintHandle):
            c = other.constraint
        elif isinstance(other, classad.ExprTree):
            c = other
        elif isinstance(other, str):
            c = classad.ExprTree(other)
        else:
            raise exceptions.InvalidHandle(
                f"Cannot construct a combined handle from {self} and {other} because it is not a ConstraintHandle, ExprTree, or cannot be parsed into an ExprTree"
            )

        return ConstraintHandle(
            combinator(self.constraint, c), collector=self.collector, scheduler=self.scheduler,
        )

    def save(self, path: Path, protocol: Optional[int] = None) -> None:
        """
        Save this :class:`ConstraintHandle` to a file at ``path`` for later use (see :meth:`ConstraintHandle.load`).

        Parameters
        ----------
        path
            The path to save the handle to.
        protocol
            The `Pickle protocol version <https://docs.python.org/3/library/pickle.html#data-stream-format>`_ to use.
        """
        with path.open(mode="wb") as f:
            pickle.dump(self, f, protocol=protocol)

    @classmethod
    def load(cls, path: Path) -> "ConstraintHandle":
        """
        Load a :class:`ConstraintHandle` from a file at ``path`` that was created by :meth:`ConstraintHandle.save`.

        Parameters
        ----------
        path
            The path to load the handle from.

        Returns
        -------
        handle :
            The loaded handle.
        """
        with path.open(mode="rb") as f:
            return pickle.load(f)


COMPACT_STATE_SWITCHOVER_SIZE = 100_000


class ClusterHandle(ConstraintHandle):
    """
    A subclass of :class:`ConstraintHandle` that targets a single cluster of jobs,
    as produced by :func:`submit`.

    Because this handle targets a single cluster of jobs, it has superpowers.
    If the cluster has an event log
    (``log = <path>`` in the :class:`SubmitDescription`,
    see `the docs <https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html>`_),
    this handle's ``state`` attribute will be a :class:`ClusterState` that provides
    information about the current state of the jobs in the cluster.
    """

    def __init__(
        self,
        submit_result: htcondor.SubmitResult,
        collector: Optional[str] = None,
        scheduler: Optional[str] = None,
    ):
        self._clusterid = submit_result.cluster()
        self._clusterad = submit_result.clusterad()
        self._first_proc = submit_result.first_proc()
        self._num_procs = submit_result.num_procs()

        super().__init__(
            constraint=classad.ExprTree(f"ClusterID == {self.clusterid}"),
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
    def clusterid(self):
        return self._clusterid

    @property
    def clusterad(self):
        return self._clusterad

    @property
    def first_proc(self):
        return self._first_proc

    @property
    def state(self) -> status.ClusterState:
        """A :class:`ClusterState` that provides information about job state for this cluster."""
        if self._state is None:
            if len(self) > COMPACT_STATE_SWITCHOVER_SIZE:
                state_type = status.CompactClusterState
            else:
                state_type = status.ClusterState

            self._state = state_type(self)

        return self._state

    def wait(
        self,
        condition: Callable[["ClusterHandle"], bool] = None,
        timeout: Optional[Union[int, float]] = None,
        test_delay: Union[int, float] = 1,
    ) -> float:
        """
        Wait for some condition to be satisfied.
        By default, this waits until all of the jobs in the cluster are complete,
        equivalent to

        .. code:: python

            handle.wait(condition=lambda hnd: hnd.state.is_complete())

        Where possible, for increased efficiency, use :class:`ClusterState` methods or
        status counts instead of raw job statuses to determine the state of the
        cluster.

        Parameters
        ----------
        condition
            A callable that defines the state to wait for.
            It will be called with the :class:`ClusterHandle` as its only argument,
            and when it returns ``True``, ``wait_for_condition`` will complete.
            **The default condition is to wait for all jobs to be :class:`JobStatus.COMPLETED`.**
        timeout
            The maximum amount of time to wait before raising a
            :class:`exceptions.WaitedTooLong` exception.
            **The ``condition`` will always be checked at least once, even if ``timeout <= 0``**.
        test_delay
            The amount of time to wait between test loops.

        Returns
        -------
        elapsed_time :
            The amount of time spent waiting.
        """
        if condition is None:
            condition = lambda hnd: hnd.state.all_complete()

        start_time = time.time()
        while not condition(self):
            if timeout is not None and time.time() > start_time + timeout:
                raise exceptions.Timeout(
                    f"waited too long for handle {self} to satisfy {condition}"
                )
            time.sleep(test_delay)
        return time.time() - start_time

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_state"] = None  # remove state tracker
        return state

    def to_json(self) -> dict:
        """Return a JSON-formatted dictionary that describes the :class:`ClusterHandle`."""
        return dict(
            clusterid=self.clusterid,
            clusterad=str(self.clusterad),
            first_proc=self.first_proc,
            num_procs=len(self),
            collector=self.collector,
            scheduler=self.scheduler,
        )

    @classmethod
    def from_json(cls, json: dict):
        """Return a :class:`ClusterHandle` from the dictionary produced by :meth:`ClusterHandle.to_json`."""
        submit_result = _MockSubmitResult(
            clusterid=json["clusterid"],
            clusterad=classad.parseOne(json["clusterad"]),
            first_proc=json["first_proc"],
            num_procs=json["num_procs"],
        )

        return cls(submit_result, collector=json["collector"], scheduler=json["scheduler"])


class _MockSubmitResult:
    """
    This class is used purely to transform unpacked submit results back into
    "submit results" to accommodate the :class:`ClusterHandle` constructor.
    **Should not be used in user code.**
    """

    def __init__(self, clusterid, clusterad, first_proc, num_procs):
        self._clusterid = clusterid
        self._clusterad = clusterad
        self._first_proc = first_proc
        self._num_procs = num_procs

    def cluster(self):
        return self._clusterid

    def clusterad(self):
        return self._clusterad

    def first_proc(self):
        return self._first_proc

    def num_procs(self):
        return self._num_procs
